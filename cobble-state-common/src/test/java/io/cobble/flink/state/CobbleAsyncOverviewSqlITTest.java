package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.inspect.CobbleInspectClient;
import io.cobble.flink.inspect.InspectOverviewItem;
import io.cobble.flink.inspect.InspectSelection;
import io.cobble.flink.inspect.InspectSession;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/** Runs the Overview's actual DDL against a checkpoint written through Flink async state. */
class CobbleAsyncOverviewSqlITTest {
    private static final int INPUT_COUNT = 3;

    @TempDir private Path tempDir;

    @Test
    void generatedStateSqlScansAndLooksUpAsyncCheckpoint() throws Exception {
        Path checkpointRoot = tempDir.resolve("checkpoints");
        Files.createDirectories(checkpointRoot);
        writeCheckpoint(checkpointRoot, tempDir.resolve("local-state"));

        Path jobRoot = jobCheckpointRoot(checkpointRoot);
        String operatorId = operatorWithInspectSchema(jobRoot);
        String rootUri = jobRoot.toUri().toString();
        try (CobbleInspectClient client = CobbleInspectClient.builder().build();
                InspectSession session =
                        client.open(InspectSelection.latest(rootUri, operatorId))) {
            Map<String, InspectOverviewItem> items = new HashMap<>();
            for (InspectOverviewItem item : session.overview().items()) {
                items.put(item.title(), item);
            }
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            StreamTableEnvironment tables = StreamTableEnvironment.create(env);

            String valueTable = registerGeneratedDdl(tables, items, "async-value", true);
            String mapTable = registerGeneratedDdl(tables, items, "async-map", true);
            String listTable = registerGeneratedDdl(tables, items, "async-list", false);

            Schema probeSchema =
                    Schema.newBuilder()
                            .column("lookup_key", "INT")
                            .column("map_lookup_key", "STRING")
                            .columnByExpression("pt", "PROCTIME()")
                            .build();
            tables.createTemporaryView(
                    "async_probes",
                    tables.fromDataStream(
                            env.fromCollection(
                                    Arrays.asList(
                                            Row.of(1, "tag"),
                                            Row.of(2, "tag"),
                                            Row.of(9, "tag"),
                                            Row.of(1, "missing")),
                                    Types.ROW_NAMED(
                                            new String[] {"lookup_key", "map_lookup_key"},
                                            Types.INT,
                                            Types.STRING)),
                            probeSchema));

            String valueQuery =
                    "SELECT p.lookup_key, d.`value` FROM async_probes AS p "
                            + "LEFT JOIN "
                            + valueTable
                            + " FOR SYSTEM_TIME AS OF p.pt AS d ON p.lookup_key = d.`key`";
            assertTrue(tables.explainSql(valueQuery).contains("LookupJoin"));
            assertEquals(
                    Arrays.asList("1|2", "1|2", "2|1", "9|null"),
                    rows(tables, valueQuery));

            String mapQuery =
                    "SELECT p.lookup_key, p.map_lookup_key, d.map_value "
                            + "FROM async_probes AS p LEFT JOIN "
                            + mapTable
                            + " FOR SYSTEM_TIME AS OF p.pt AS d "
                            + "ON p.lookup_key = d.`key` AND p.map_lookup_key = d.map_key";
            assertTrue(tables.explainSql(mapQuery).contains("LookupJoin"));
            assertEquals(
                    Arrays.asList(
                            "1|missing|null", "1|tag|2", "2|tag|1", "9|tag|null"),
                    rows(tables, mapQuery));
            assertEquals(
                    Arrays.asList("1|2", "2|1"),
                    rows(tables, "SELECT `key`, `value` FROM " + valueTable));
            assertEquals(
                    Arrays.asList("1|tag|2", "2|tag|1"),
                    rows(tables, "SELECT `key`, map_key, map_value FROM " + mapTable));
            assertEquals(
                    Arrays.asList("1|1", "1|1", "2|2"),
                    rows(tables, "SELECT `key`, `value` FROM " + listTable));
        }
    }

    private static String registerGeneratedDdl(
            StreamTableEnvironment tables,
            Map<String, InspectOverviewItem> items,
            String name,
            boolean lookupSupported) {
        InspectOverviewItem item = items.get(name);
        assertNotNull(item, "Overview must contain " + name);
        assertTrue(item.sourceSql().batchScanSupported());
        assertEquals(lookupSupported, item.sourceSql().exactLookupSupported());
        String ddl = item.sourceSql().ddl();
        assertNotNull(ddl);
        tables.executeSql(ddl);
        return ddl.substring("CREATE TABLE ".length(), ddl.indexOf('(', "CREATE TABLE ".length())).trim();
    }

    private static List<String> rows(StreamTableEnvironment tables, String sql) throws Exception {
        List<String> results = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tables.executeSql(sql).collect()) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                StringBuilder result = new StringBuilder();
                for (int index = 0; index < row.getArity(); index++) {
                    if (index > 0) {
                        result.append('|');
                    }
                    result.append(row.getField(index));
                }
                results.add(result.toString());
            }
        }
        Collections.sort(results);
        return results;
    }

    private static void writeCheckpoint(Path checkpointRoot, Path localState) throws Exception {
        Files.createDirectories(localState);
        Configuration clusterConfig = new Configuration();
        clusterConfig.set(
                HighAvailabilityOptions.HA_MODE,
                CobbleHighAvailabilityServicesFactory.class.getName());
        clusterConfig.setString("cobble.ha.delegate.type", "NONE");
        clusterConfig.set(JobManagerOptions.ADDRESS, "localhost");
        clusterConfig.set(JobManagerOptions.PORT, 6123);
        clusterConfig.set(RestOptions.ADDRESS, "localhost");
        clusterConfig.set(RestOptions.PORT, 0);
        clusterConfig.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointRoot.toUri().toString());

        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(clusterConfig)
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(1)
                                .build());
        EmittingSource.finished.set(0);
        AsyncStates.completed.set(0);
        cluster.before();
        try {
            Configuration jobConfig = new Configuration();
            jobConfig.set(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointRoot.toUri().toString());
            jobConfig.set(CobbleOptions.LOCAL_DIRECTORIES, localState.toString());
            jobConfig.set(StateBackendOptions.STATE_BACKEND, CobbleStateBackendFactory.class.getName());
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(jobConfig);
            env.setParallelism(1);
            env.enableCheckpointing(5_000L);
            env.getCheckpointConfig().setCheckpointTimeout(180_000L);
            env.getCheckpointConfig()
                    .setExternalizedCheckpointRetention(
                            ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
            KeyedStream<Integer, Integer> keyed =
                    env.addSource(new EmittingSource())
                    .name("async-overview-source")
                    .uid("async-overview-source")
                    .keyBy(value -> value, BasicTypeInfo.INT_TYPE_INFO);
            keyed.enableAsyncState();
            keyed
                    .flatMap(new AsyncStates())
                    .name("async-overview-states")
                    .uid("async-overview-states")
                    .addSink(new DiscardingSink<>());

            JobGraph graph = env.getStreamGraph().getJobGraph();
            JobID jobId = graph.getJobID();
            cluster.getClusterClient().submitJob(graph).get(30, TimeUnit.SECONDS);
            await(cluster, jobId, () -> EmittingSource.finished.get() == 1, "source emission");
            await(
                    cluster,
                    jobId,
                    () -> AsyncStates.completed.get() == INPUT_COUNT,
                    "async state completions");
            cluster.getMiniCluster().triggerCheckpoint(jobId).get(30, TimeUnit.SECONDS);
            await(
                    cluster,
                    jobId,
                    () -> {
                        try (Stream<Path> files = Files.walk(checkpointRoot)) {
                            return files.anyMatch(
                                    path ->
                                            path.getFileName()
                                                    .toString()
                                                    .startsWith("COBBLE-SNAPSHOT-"));
                        }
                    },
                    "Cobble checkpoint manifest");
            cluster.getClusterClient().cancel(jobId).get(30, TimeUnit.SECONDS);
            await(cluster, jobId, () -> cluster.getMiniCluster().getJobStatus(jobId).get() == JobStatus.CANCELED, "job cancellation");
        } finally {
            cluster.after();
        }
    }

    private static String operatorWithInspectSchema(Path checkpointRoot) throws Exception {
        Path cobble = checkpointRoot.resolve("cobble");
        try (Stream<Path> paths = Files.list(cobble)) {
            return paths.filter(path -> Files.isDirectory(path.resolve("inspect-schema/events")))
                    .map(path -> path.getFileName().toString())
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No async state inspect schema"));
        }
    }

    private static Path jobCheckpointRoot(Path checkpointRoot) throws Exception {
        try (Stream<Path> paths = Files.walk(checkpointRoot)) {
            return paths.filter(
                            path ->
                                    path.getFileName()
                                            .toString()
                                            .startsWith("COBBLE-SNAPSHOT-"))
                    .map(path -> path.getParent().getParent())
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No Cobble checkpoint manifest"));
        }
    }

    private static void await(
            MiniClusterWithClientResource cluster,
            JobID jobId,
            CheckedCondition condition,
            String description)
            throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            AccessExecutionGraph graph = cluster.getMiniCluster().getExecutionGraph(jobId).get();
            if (graph.getState().isGloballyTerminalState()
                    && graph.getState() != JobStatus.CANCELED) {
                throw new AssertionError("Async fixture job ended before " + description + ": " + graph.getState());
            }
            if (condition.test()) {
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("Timed out waiting for " + description);
    }

    @FunctionalInterface
    private interface CheckedCondition {
        boolean test() throws Exception;
    }

    private static final class EmittingSource extends RichParallelSourceFunction<Integer> {
        private static final AtomicInteger finished = new AtomicInteger();
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Integer> context) throws Exception {
            for (int value : new int[] {1, 1, 2}) {
                synchronized (context.getCheckpointLock()) {
                    context.collect(value);
                }
            }
            finished.incrementAndGet();
            while (running) {
                Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class AsyncStates extends RichFlatMapFunction<Integer, Integer> {
        private static final AtomicInteger completed = new AtomicInteger();
        private transient ValueState<Integer> valueState;
        private transient MapState<String, Integer> mapState;
        private transient ListState<Integer> listState;

        @Override
        public void open(OpenContext context) throws Exception {
            StreamingRuntimeContext runtime = (StreamingRuntimeContext) getRuntimeContext();
            valueState =
                    runtime.getValueState(
                            new ValueStateDescriptor<>("async-value", BasicTypeInfo.INT_TYPE_INFO));
            mapState =
                    runtime.getMapState(
                            new MapStateDescriptor<>(
                                    "async-map",
                                    BasicTypeInfo.STRING_TYPE_INFO,
                                    BasicTypeInfo.INT_TYPE_INFO));
            listState =
                    runtime.getListState(
                            new ListStateDescriptor<>("async-list", BasicTypeInfo.INT_TYPE_INFO));
        }

        @Override
        public void flatMap(Integer value, Collector<Integer> output) {
            valueState
                    .asyncValue()
                    .thenCompose(
                            previous -> {
                                int next = previous == null ? 1 : previous + 1;
                                return valueState.asyncUpdate(next).thenApply(ignored -> next);
                            })
                    .thenCompose(next -> mapState.asyncPut("tag", next))
                    .thenCompose(ignored -> listState.asyncAddAll(Collections.singletonList(value)))
                    .thenAccept(
                            ignored -> {
                                completed.incrementAndGet();
                                output.collect(value);
                            });
        }
    }
}
