package io.cobble.flink.table;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    @Test
    void listStateReaderCountsOneNativeEntryAcrossPartialCheckpointResume() throws Exception {
        Tuple2<String, Long> discovered = runStatefulJob();
        String operatorId = discovered.f0.split("@", 2)[0];
        String checkpointRootUri = discovered.f0.split("@", 2)[1];
        StateSourceConfig config =
                stateSourceConfig(
                        checkpointRootUri,
                        operatorId,
                        "list-state",
                        "list",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "element", "INT", StateSourceField.Group.LIST_ELEMENT, 0)),
                        discovered.f1);

        List<CobbleStateSourceSplit> splits =
                CobbleStateSourceRuntime.createStateSourceSplits(config);
        CapturingSourceMetrics firstMetrics = new CapturingSourceMetrics();
        CollectingOutput firstOutput = new CollectingOutput();
        CobbleStateSourceSplit partialSplit;
        CobbleStateSourceReader firstReader =
                new CobbleStateSourceReader(config, new TestingReaderContext(firstMetrics.group()));
        try {
            firstReader.start();
            firstReader.addSplits(splits);
            firstReader.notifyNoMoreSplits();
            pollUntilRows(firstReader, firstOutput, 1);
            partialSplit = findPartialSplit(firstReader.snapshotState(1L));
        } finally {
            firstReader.close();
        }

        assertEquals(1L, firstMetrics.count("cobble.nativeEntriesReadTotal"));
        assertEquals(1L, firstMetrics.count("records"));
        assertTrue(firstMetrics.count("cobble.nativeBytesReadTotal") > 0L);
        assertNotNull(partialSplit.partialEntryKey);
        assertEquals(1, partialSplit.partialEmittedCount);

        CapturingSourceMetrics resumedMetrics = new CapturingSourceMetrics();
        CollectingOutput resumedOutput = new CollectingOutput();
        CobbleStateSourceReader resumedReader =
                new CobbleStateSourceReader(
                        config, new TestingReaderContext(resumedMetrics.group()));
        try {
            resumedReader.start();
            resumedReader.addSplits(java.util.Collections.singletonList(partialSplit));
            resumedReader.notifyNoMoreSplits();
            for (int polls = 0; polls < 10_000; polls++) {
                InputStatus status = resumedReader.pollNext(resumedOutput);
                assertTrue(status != InputStatus.END_OF_INPUT || !resumedOutput.rows.isEmpty());
                if (findSplit(resumedReader.snapshotState(2L), partialSplit.splitId).partialEntryKey
                        == null) {
                    break;
                }
                if (polls == 9_999) {
                    throw new AssertionError("Timed out draining resumed ListState entry.");
                }
            }
        } finally {
            resumedReader.close();
        }

        assertTrue(resumedOutput.rows.size() > 0, "expected the remaining ListState elements");
        assertEquals(1L, resumedMetrics.count("cobble.nativeEntriesReadTotal"));
        assertEquals(resumedOutput.rows.size(), resumedMetrics.count("records"));
        assertTrue(resumedMetrics.count("cobble.nativeBytesReadTotal") > 0L);
        for (RowData row : resumedOutput.rows) {
            assertFalse(
                    sameRow(firstOutput.rows.get(0), row),
                    "resumed reader must not duplicate the already-emitted ListState row");
        }
    }

    private Tuple2<String, Long> runStatefulJob() throws Exception {
        Path checkpointRoot = java.nio.file.Paths.get(".tmp/proof/checkpoints");
        Path localState = java.nio.file.Paths.get(".tmp/proof/local-state");
        // Clean up any stale checkpoints from previous test runs — schema format may have changed.
        if (java.nio.file.Files.exists(checkpointRoot)) {
            try (Stream<Path> walk = java.nio.file.Files.walk(checkpointRoot)) {
                walk.sorted(java.util.Comparator.reverseOrder())
                        .forEach(
                                p -> {
                                    try {
                                        java.nio.file.Files.deleteIfExists(p);
                                    } catch (java.io.IOException ignored) {
                                    }
                                });
            }
        }
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

    private static void pollUntilRows(
            CobbleStateSourceReader reader, CollectingOutput output, int expectedRows)
            throws Exception {
        for (int polls = 0; output.rows.size() < expectedRows && polls < 10_000; polls++) {
            InputStatus status = reader.pollNext(output);
            if (status == InputStatus.END_OF_INPUT) {
                break;
            }
        }
        assertEquals(
                expectedRows, output.rows.size(), "reader ended before emitting a ListState row");
    }

    private static CobbleStateSourceSplit findPartialSplit(List<CobbleStateSourceSplit> splits) {
        for (CobbleStateSourceSplit split : splits) {
            if (split.partialEntryKey != null) {
                return split;
            }
        }
        throw new AssertionError("reader did not retain a partially emitted ListState entry");
    }

    private static CobbleStateSourceSplit findSplit(
            List<CobbleStateSourceSplit> splits, String splitId) {
        for (CobbleStateSourceSplit split : splits) {
            if (split.splitId().equals(splitId)) {
                return split;
            }
        }
        throw new AssertionError("missing reader split " + splitId);
    }

    private static boolean sameRow(RowData first, RowData second) {
        return first.getInt(0) == second.getInt(0) && first.getInt(1) == second.getInt(1);
    }

    private static final class TestingReaderContext implements SourceReaderContext {
        private final SourceReaderMetricGroup metricGroup;

        private TestingReaderContext(SourceReaderMetricGroup metricGroup) {
            this.metricGroup = metricGroup;
        }

        @Override
        public SourceReaderMetricGroup metricGroup() {
            return metricGroup;
        }

        @Override
        public Configuration getConfiguration() {
            return new Configuration();
        }

        @Override
        public String getLocalHostName() {
            return "localhost";
        }

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {}

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(getClass().getClassLoader());
        }

        @Override
        public int currentParallelism() {
            return 1;
        }
    }

    private static final class CapturingSourceMetrics {
        private final Map<String, Counter> counters = new HashMap<>();

        private SourceReaderMetricGroup group() {
            OperatorIOMetricGroup ioGroup =
                    (OperatorIOMetricGroup)
                            Proxy.newProxyInstance(
                                    getClass().getClassLoader(),
                                    new Class<?>[] {OperatorIOMetricGroup.class},
                                    (proxy, method, args) -> ioMetric(method.getName()));
            return (SourceReaderMetricGroup)
                    Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[] {SourceReaderMetricGroup.class},
                            (proxy, method, args) -> {
                                if ("getIOMetricGroup".equals(method.getName())) {
                                    return ioGroup;
                                }
                                if ("getNumRecordsInErrorsCounter".equals(method.getName())) {
                                    return counter("errors");
                                }
                                if ("counter".equals(method.getName())) {
                                    return counter((String) args[0]);
                                }
                                return null;
                            });
        }

        private Object ioMetric(String method) {
            if ("getNumRecordsInCounter".equals(method)) {
                return counter("records");
            }
            if ("getNumBytesInCounter".equals(method)) {
                return counter("bytes");
            }
            return null;
        }

        private Counter counter(String name) {
            return counters.computeIfAbsent(name, ignored -> new TestCounter());
        }

        private long count(String name) {
            Counter counter = counters.get(name);
            return counter == null ? 0L : counter.getCount();
        }
    }

    private static final class TestCounter implements Counter {
        private long count;

        @Override
        public void inc() {
            count++;
        }

        @Override
        public void inc(long n) {
            count += n;
        }

        @Override
        public void dec() {
            count--;
        }

        @Override
        public void dec(long n) {
            count -= n;
        }

        @Override
        public long getCount() {
            return count;
        }
    }

    private static final class CollectingOutput implements ReaderOutput<RowData> {
        private final List<RowData> rows = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            rows.add(record);
        }

        @Override
        public void collect(RowData record, long timestamp) {
            rows.add(record);
        }

        @Override
        public void emitWatermark(org.apache.flink.api.common.eventtime.Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public SourceOutput<RowData> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}
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
