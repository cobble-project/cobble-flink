package io.cobble.flink.table;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.ScanCursor;
import io.cobble.ScanOptions;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.state.CobbleOptions;
import io.cobble.flink.state.CobbleStateBackend;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Local MiniCluster proof that the Cobble state source runtime reads real keyed-state checkpoints
 * produced by the Cobble state backend. The cluster/checkpoint artifacts are intentionally left on
 * disk under the {@code @TempDir} for reviewer inspection.
 */
class CobbleStateSourceProofITTest {

    private static final int PARALLELISM = 4;

    @TempDir private Path tempDir;

    @Test
    void readsValueStateCheckpoint() throws Exception {
        Tuple2<String, Long> discovered = runStatefulJob();
        String operatorId = discovered.f0.split("@", 2)[0];
        String resolvedCheckpointRootUri = discovered.f0.split("@", 2)[1];

        StateSourceConfig config =
                stateSourceConfig(
                        resolvedCheckpointRootUri,
                        operatorId,
                        "value-state",
                        "value",
                        java.util.Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        discovered.f1);

        List<RowData> rows = drainStateRows(config);
        assertFalse(rows.isEmpty(), "expected at least one value-state row");
        for (RowData row : rows) {
            assertTrue(row.getInt(0) >= 0, "state key should be non-negative");
        }
    }

    @Test
    void readsListStateCheckpoint() throws Exception {
        Tuple2<String, Long> discovered = runStatefulJob();
        String operatorId = discovered.f0.split("@", 2)[0];
        String resolvedCheckpointRootUri = discovered.f0.split("@", 2)[1];

        StateSourceConfig config =
                stateSourceConfig(
                        resolvedCheckpointRootUri,
                        operatorId,
                        "list-state",
                        "list",
                        java.util.Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "element", "INT", StateSourceField.Group.LIST_ELEMENT, 0)),
                        discovered.f1);

        List<RowData> rows = drainStateRows(config);
        assertFalse(rows.isEmpty(), "expected at least one list-state row");
        for (RowData row : rows) {
            assertTrue(row.getInt(0) >= 0, "state key should be non-negative");
        }
    }

    private Tuple2<String, Long> runStatefulJob() throws Exception {
        Path checkpointRoot = java.nio.file.Paths.get(".tmp/proof/checkpoints");
        Path localState = java.nio.file.Paths.get(".tmp/proof/local-state");
        java.nio.file.Files.createDirectories(checkpointRoot);
        java.nio.file.Files.createDirectories(localState);
        return runStatefulJob(checkpointRoot, localState);
    }

    private static StateSourceConfig stateSourceConfig(
            String checkpointRootUri,
            String operatorId,
            String stateName,
            String stateKind,
            java.util.List<StateSourceField> fields,
            long checkpointId)
            throws Exception {
        return new StateSourceConfig(
                checkpointRootUri,
                StateSourceConfig.Layout.CHECKPOINT_ROOT,
                operatorId,
                stateName,
                stateKind,
                "latest",
                "batch",
                resolveSchemaCheckpointId(checkpointRootUri, operatorId),
                -1,
                0L,
                fields);
    }

    private Tuple2<String, Long> runStatefulJob(Path checkpointRoot, Path localState)
            throws Exception {
        org.apache.flink.configuration.Configuration clusterConfiguration =
                new org.apache.flink.configuration.Configuration();
        clusterConfiguration.setString(
                org.apache.flink.configuration.HighAvailabilityOptions.HA_MODE,
                io.cobble.flink.state.CobbleHighAvailabilityServicesFactory.class.getName());
        clusterConfiguration.setString("cobble.ha.delegate.type", "NONE");
        clusterConfiguration.setString(
                org.apache.flink.configuration.JobManagerOptions.ADDRESS, "localhost");
        clusterConfiguration.setInteger(
                org.apache.flink.configuration.JobManagerOptions.PORT, 6123);
        clusterConfiguration.setString(
                org.apache.flink.configuration.RestOptions.ADDRESS, "localhost");
        clusterConfiguration.setInteger(org.apache.flink.configuration.RestOptions.PORT, 0);
        clusterConfiguration.set(
                org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                checkpointRoot.toUri().toString());

        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(clusterConfiguration)
                                .setNumberTaskManagers(2)
                                .setNumberSlotsPerTaskManager(2)
                                .build());
        cluster.before();
        try {
            Configuration jobConfig = new Configuration();
            jobConfig.set(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointRoot.toUri().toString());
            jobConfig.set(CobbleOptions.LOCAL_DIRECTORIES, localState.toString());
            jobConfig.set(CobbleOptions.MEMTABLE_BUFFER_RATIO, 0.25d);
            jobConfig.set(CobbleOptions.MEMTABLE_BUFFER_COUNT, 4);
            jobConfig.set(CobbleOptions.DIRECT_IO_BUFFER_SIZE, MemorySize.parse("8kb"));
            jobConfig.set(CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE, 128);

            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(jobConfig);
            env.setParallelism(PARALLELISM);
            env.enableCheckpointing(5_000L);
            env.getCheckpointConfig().setCheckpointTimeout(180_000L);
            env.getCheckpointConfig()
                    .setExternalizedCheckpointCleanup(
                            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            env.setRestartStrategy(RestartStrategies.noRestart());
            env.setStateBackend(
                    new CobbleStateBackend().configure(jobConfig, getClass().getClassLoader()));

            env.addSource(new ContinuousIntegerSource())
                    .name("proof-source")
                    .uid("proof-source")
                    .keyBy(value -> value % PARALLELISM)
                    .map(new ValueStateMapper())
                    .name("proof-stateful")
                    .uid("proof-stateful")
                    .setMaxParallelism(128);

            JobGraph jobGraph = env.getStreamGraph().getJobGraph();
            JobID jobId = jobGraph.getJobID();
            cluster.getClusterClient().submitJob(jobGraph).get(30, TimeUnit.SECONDS);
            waitForAllTaskRunning(cluster.getMiniCluster(), jobId, false);
            // Wait for a completed checkpoint and give the CobbleCompletedCheckpointStore time to
            // materialize the global snapshot and inspect-schema registry.
            cluster.getMiniCluster().triggerCheckpoint(jobId).get(30, TimeUnit.SECONDS);
            // CommonTestUtils.waitForCheckpoint has no timeout and never checks for job failure, so
            // a
            // failed job would loop forever. Poll the checkpoint stats with an explicit deadline
            // and
            // bail out immediately when the job enters a globally terminal state.
            waitForCompletedCheckpoint(cluster.getMiniCluster(), jobId, 1, Duration.ofSeconds(60));
            Thread.sleep(2_000L);
            try {
                cluster.getClusterClient().cancel(jobId).get(30, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // The job may have already finished; the checkpoint artifacts are already on disk.
            }
        } finally {
            cluster.after();
        }

        return discoverOperatorAndCheckpoint(checkpointRoot);
    }

    private static List<RowData> drainStateRows(StateSourceConfig config) throws Exception {
        List<CobbleStateSourceSplit> splits =
                CobbleStateSourceRuntime.createStateSourceSplits(config);
        assertFalse(splits.isEmpty(), "expected at least one split");
        CobbleStateSourceRuntime.ReaderHandle handle =
                CobbleStateSourceRuntime.openReader(config, splits.get(0).checkpointId);
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                CobbleStateSourceRuntime.loadRuntimeSchema(config);
        CobbleStateRowDecoder decoder = new CobbleStateRowDecoder(config, runtimeSchema);
        String columnFamily = runtimeSchema.schema.columnFamily();
        List<RowData> rows = new ArrayList<>();
        try {
            for (CobbleStateSourceSplit split : splits) {
                for (int keyGroup = split.keyGroupStart;
                        keyGroup <= split.keyGroupEnd;
                        keyGroup++) {
                    ScanOptions options =
                            CobbleStateSourceRuntime.scanOptions(columnFamily, Integer.MAX_VALUE);
                    ScanCursor cursor;
                    try {
                        cursor =
                                handle.reader.scanWithOptions(
                                        keyGroup,
                                        CobbleStateSourceRuntime.emptyScanKey(),
                                        CobbleStateSourceRuntime.maxScanKey(),
                                        options);
                    } catch (RuntimeException e) {
                        // A shard may not carry this column family when it never wrote data for the
                        // state; treat that key-group as empty.
                        if (e.getMessage() != null
                                && e.getMessage().contains("Unknown column family")) {
                            continue;
                        }
                        throw e;
                    }
                    try (ScanCursor scan = cursor) {
                        ScanCursor.Entry entry = scan.nextEntry();
                        while (entry != null) {
                            rows.addAll(
                                    decoder.decode(
                                            entry.key, entry.columns, split.splitId(), keyGroup));
                            entry = scan.nextEntry();
                        }
                    }
                }
            }
        } finally {
            handle.close();
        }
        return rows;
    }

    private static Tuple2<String, Long> discoverOperatorAndCheckpoint(Path checkpointRoot)
            throws Exception {
        String operatorId = null;
        Path checkpointDir = null;
        long latestCheckpointId = -1L;
        try (Stream<Path> walk = Files.walk(checkpointRoot)) {
            for (Path path : (Iterable<Path>) walk::iterator) {
                String name = path.getFileName() == null ? "" : path.getFileName().toString();
                if (name.startsWith("COBBLE-SNAPSHOT-") && name.endsWith("-MANIFEST")) {
                    operatorId =
                            name.substring(
                                    "COBBLE-SNAPSHOT-".length(),
                                    name.length() - "-MANIFEST".length());
                    Path chkDir = path.getParent();
                    String chkName =
                            chkDir.getFileName() == null ? "" : chkDir.getFileName().toString();
                    if (chkName.startsWith("chk-")) {
                        try {
                            long id = Long.parseLong(chkName.substring("chk-".length()));
                            if (id > latestCheckpointId) {
                                latestCheckpointId = id;
                                checkpointDir = chkDir.getParent();
                            }
                        } catch (NumberFormatException ignored) {
                        }
                    }
                }
            }
        }
        assertTrue(operatorId != null, "no COBBLE-SNAPSHOT manifest found");
        assertTrue(latestCheckpointId > 0, "no chk-* checkpoint found");
        assertTrue(checkpointDir != null, "could not resolve checkpoint root");
        return Tuple2.of(operatorId + "@" + checkpointDir.toUri().toString(), latestCheckpointId);
    }

    private static long resolveSchemaCheckpointId(String checkpointRootUri, String operatorId)
            throws Exception {
        Path eventsDir =
                java.nio.file.Paths.get(java.net.URI.create(checkpointRootUri))
                        .resolve("cobble")
                        .resolve(operatorId)
                        .resolve("inspect-schema")
                        .resolve("events");
        long best = -1L;
        try (Stream<Path> walk = Files.list(eventsDir)) {
            for (Path path : (Iterable<Path>) walk::iterator) {
                InspectSchemaRegistryLayout.SchemaEvent event =
                        InspectSchemaRegistryLayout.parseEventFileName(
                                path.getFileName().toString());
                if (event != null && event.checkpointId() > best) {
                    best = event.checkpointId();
                }
            }
        }
        assertTrue(best > 0, "no inspect-schema event found");
        return best;
    }

    /**
     * Polls the checkpoint stats until {@code checkpointId} is completed, with an explicit deadline
     * and job-failure detection. Unlike {@code CommonTestUtils.waitForCheckpoint}, this never loops
     * forever: it fails fast when the job enters a globally terminal state and always times out.
     */
    private static void waitForCompletedCheckpoint(
            MiniCluster miniCluster, JobID jobId, long checkpointId, Duration timeout)
            throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            org.apache.flink.runtime.executiongraph.AccessExecutionGraph graph =
                    miniCluster.getExecutionGraph(jobId).get();
            JobStatus status = graph.getState();
            if (status.isGloballyTerminalState()) {
                throw new AssertionError(
                        "Job "
                                + jobId
                                + " entered terminal state "
                                + status
                                + " before checkpoint "
                                + checkpointId
                                + " completed.");
            }
            org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot stats =
                    graph.getCheckpointStatsSnapshot();
            if (stats != null
                    && stats.getHistory() != null
                    && stats.getHistory().getLatestCompletedCheckpoint() != null
                    && stats.getHistory().getLatestCompletedCheckpoint().getCheckpointId()
                            >= checkpointId) {
                return;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError(
                "Timed out waiting for checkpoint "
                        + checkpointId
                        + " on job "
                        + jobId
                        + " after "
                        + timeout
                        + ".");
    }

    private static final class ContinuousIntegerSource extends RichParallelSourceFunction<Integer> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            int nextValue = getRuntimeContext().getIndexOfThisSubtask();
            int step = getRuntimeContext().getNumberOfParallelSubtasks();
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(nextValue);
                    nextValue += step;
                }
                Thread.sleep(1L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class ValueStateMapper extends RichMapFunction<Integer, Integer> {
        private transient ValueState<Integer> valueState;
        private transient ListState<Integer> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "value-state", BasicTypeInfo.INT_TYPE_INFO));
            listState =
                    getRuntimeContext()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "list-state", BasicTypeInfo.INT_TYPE_INFO));
        }

        @Override
        public Integer map(Integer value) throws Exception {
            Integer current = valueState.value();
            valueState.update(current == null ? value : current + value);
            listState.add(value);
            return value;
        }
    }
}
