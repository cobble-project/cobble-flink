package io.cobble.flink.table;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.state.CobbleHighAvailabilityServicesFactory;
import io.cobble.flink.state.CobbleOptions;
import io.cobble.flink.state.CobbleStateBackend;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/** SQL-level proof that Cobble state source DDL reads real Cobble state checkpoints. */
class CobbleStateSourceSqlITTest {

    private static final int PARALLELISM = 4;
    private static final int VALUES_PER_KEY = 3;

    @TempDir private Path tempDir;

    @Test
    void sqlReadsSupportedStateKindsFromCheckpoint() throws Exception {
        CheckpointInfo checkpoint = runStatefulJob();
        StreamTableEnvironment tableEnv = newTableEnv();

        tableEnv.executeSql(
                stateDdl(
                        "state_values_latest",
                        "key INT, `value` INT",
                        checkpoint,
                        "value-state",
                        "value",
                        "latest",
                        false));
        assertEquals(
                expectedSums(), rowSet(tableEnv, "SELECT `key`, `value` FROM state_values_latest"));

        tableEnv.executeSql(
                stateDdl(
                        "state_values_by_id",
                        "key INT, `value` INT",
                        checkpoint,
                        "value-state",
                        "value",
                        Long.toString(checkpoint.checkpointId),
                        true));
        assertEquals(
                expectedSums(), rowSet(tableEnv, "SELECT `key`, `value` FROM state_values_by_id"));

        tableEnv.executeSql(
                stateDdl(
                        "state_list",
                        "key INT, `value` INT",
                        checkpoint,
                        "list-state",
                        "list",
                        "latest",
                        true));
        assertEquals(expectedListRows(), rowSet(tableEnv, "SELECT `key`, `value` FROM state_list"));

        tableEnv.executeSql(
                stateDdl(
                        "state_map",
                        "key INT, map_key INT, map_value STRING",
                        checkpoint,
                        "map-state",
                        "map",
                        "latest",
                        true));
        assertEquals(
                expectedMapRows(),
                rowSet(tableEnv, "SELECT `key`, map_key, map_value FROM state_map"));

        tableEnv.executeSql(
                stateDdl(
                        "state_reducing",
                        "key INT, `value` INT",
                        checkpoint,
                        "reducing-state",
                        "reducing",
                        "latest",
                        true));
        assertEquals(expectedSums(), rowSet(tableEnv, "SELECT `key`, `value` FROM state_reducing"));

        tableEnv.executeSql(
                stateDdl(
                        "state_aggregating",
                        "key INT, `value` INT",
                        checkpoint,
                        "aggregating-state",
                        "aggregating",
                        "latest",
                        true));
        assertEquals(
                expectedSums(), rowSet(tableEnv, "SELECT `key`, `value` FROM state_aggregating"));

        CheckpointInfo namespacedCheckpoint =
                checkpoint.withOperatorId(
                        operatorIdForState(checkpoint.rootUri, "namespaced-value-state"));
        tableEnv.executeSql(
                stateDdl(
                        "state_namespaced_values_latest",
                        "key INT, namespace STRING, `value` INT",
                        namespacedCheckpoint,
                        "namespaced-value-state",
                        "value",
                        "latest",
                        true));
        assertEquals(
                expectedNamespacedRows(),
                rowSet(
                        tableEnv,
                        "SELECT `key`, namespace, `value` FROM state_namespaced_values_latest"));

        tableEnv.executeSql(
                stateDdl(
                        "state_namespaced_values_by_id",
                        "key INT, namespace STRING, `value` INT",
                        namespacedCheckpoint,
                        "namespaced-value-state",
                        "value",
                        Long.toString(namespacedCheckpoint.checkpointId),
                        true));
        assertEquals(
                expectedNamespacedRows(),
                rowSet(
                        tableEnv,
                        "SELECT `key`, namespace, `value` FROM state_namespaced_values_by_id"));
    }

    private CheckpointInfo runStatefulJob() throws Exception {
        Path checkpointRoot = tempDir.resolve("checkpoints");
        Path localState = tempDir.resolve("local-state");
        Files.createDirectories(checkpointRoot);
        Files.createDirectories(localState);

        Configuration clusterConfiguration = new Configuration();
        clusterConfiguration.setString(
                HighAvailabilityOptions.HA_MODE,
                CobbleHighAvailabilityServicesFactory.class.getName());
        clusterConfiguration.setString("cobble.ha.delegate.type", "NONE");
        clusterConfiguration.setString(JobManagerOptions.ADDRESS, "localhost");
        clusterConfiguration.setInteger(JobManagerOptions.PORT, 6123);
        clusterConfiguration.setString(RestOptions.ADDRESS, "localhost");
        clusterConfiguration.setInteger(RestOptions.PORT, 0);
        clusterConfiguration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointRoot.toUri().toString());

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

            env.addSource(new FixedIntegerSource())
                    .name("sql-proof-source")
                    .uid("sql-proof-source")
                    .keyBy(value -> value % PARALLELISM)
                    .map(new StatefulMapper())
                    .name("sql-proof-stateful")
                    .uid("sql-proof-stateful")
                    .keyBy(value -> value % PARALLELISM)
                    .transform(
                            "sql-proof-namespaced-stateful",
                            BasicTypeInfo.INT_TYPE_INFO,
                            new NamespacedStateOperator())
                    .name("sql-proof-namespaced-stateful")
                    .uid("sql-proof-namespaced-stateful")
                    .setMaxParallelism(128);

            JobGraph jobGraph = env.getStreamGraph().getJobGraph();
            JobID jobId = jobGraph.getJobID();
            cluster.getClusterClient().submitJob(jobGraph).get(30, TimeUnit.SECONDS);
            waitForAllTaskRunning(cluster.getMiniCluster(), jobId, false);
            Thread.sleep(1_000L);
            cluster.getMiniCluster().triggerCheckpoint(jobId).get(30, TimeUnit.SECONDS);
            waitForCompletedCheckpoint(cluster.getMiniCluster(), jobId, 1, Duration.ofSeconds(60));
            Thread.sleep(2_000L);
            try {
                cluster.getClusterClient().cancel(jobId).get(30, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // Checkpoint artifacts are already materialized.
            }
        } finally {
            cluster.after();
        }

        return discoverOperatorAndCheckpoint(checkpointRoot);
    }

    private static StreamTableEnvironment newTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        return StreamTableEnvironment.create(env);
    }

    private static String stateDdl(
            String tableName,
            String columns,
            CheckpointInfo checkpoint,
            String stateName,
            String stateKind,
            String checkpointId,
            boolean explicitStateKind) {
        String sourceKindClause = explicitStateKind ? " 'source.kind' = 'state'," : "";
        String stateKindClause = explicitStateKind ? " 'state.kind' = '" + stateKind + "'," : "";
        return "CREATE TABLE "
                + tableName
                + " ("
                + columns
                + ") WITH ("
                + " 'connector' = 'cobble',"
                + sourceKindClause
                + " 'path' = '"
                + checkpoint.rootUri
                + "',"
                + " 'state.operator-id' = '"
                + checkpoint.operatorId
                + "',"
                + " 'state.name' = '"
                + stateName
                + "',"
                + stateKindClause
                + " 'scan.mode' = 'batch',"
                + " 'scan.checkpoint-id' = '"
                + checkpointId
                + "'"
                + ")";
    }

    private static Set<String> rowSet(StreamTableEnvironment tableEnv, String query)
            throws Exception {
        List<Row> rows = collectRows(tableEnv, query, Duration.ofSeconds(60));
        Set<String> encoded = new LinkedHashSet<>();
        for (Row row : rows) {
            encoded.add(encode(row));
        }
        return encoded;
    }

    private static List<Row> collectRows(
            StreamTableEnvironment tableEnv, String query, Duration timeout) throws Exception {
        TableResult result = tableEnv.executeSql(query);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<List<Row>> future =
                executor.submit(
                        () -> {
                            List<Row> rows = new ArrayList<>();
                            try (CloseableIterator<Row> iterator = result.collect()) {
                                while (iterator.hasNext()) {
                                    rows.add(iterator.next());
                                }
                            }
                            return rows;
                        });
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            future.cancel(true);
            executor.shutdownNow();
        }
    }

    private static String encode(Row row) {
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < row.getArity(); index++) {
            if (index > 0) {
                builder.append('|');
            }
            Object value = row.getField(index);
            builder.append(value == null ? "null" : value);
        }
        return builder.toString();
    }

    private static Set<String> expectedSums() {
        Set<String> rows = new LinkedHashSet<>();
        for (int key = 0; key < PARALLELISM; key++) {
            rows.add(key + "|" + expectedSum(key));
        }
        return rows;
    }

    private static Set<String> expectedListRows() {
        Set<String> rows = new LinkedHashSet<>();
        for (int key = 0; key < PARALLELISM; key++) {
            for (int offset = 0; offset < VALUES_PER_KEY; offset++) {
                rows.add(key + "|" + (key + offset * PARALLELISM));
            }
        }
        return rows;
    }

    private static Set<String> expectedMapRows() {
        Set<String> rows = new LinkedHashSet<>();
        for (int key = 0; key < PARALLELISM; key++) {
            rows.add(key + "|" + nullMapKey(key) + "|null");
            for (int offset = 0; offset < VALUES_PER_KEY; offset++) {
                int value = key + offset * PARALLELISM;
                rows.add(key + "|" + value + "|m" + value);
            }
        }
        return rows;
    }

    private static Set<String> expectedNamespacedRows() {
        Set<String> rows = new LinkedHashSet<>();
        for (int key = 0; key < PARALLELISM; key++) {
            rows.add(key + "|ns-a|" + expectedSum(key));
            rows.add(key + "|ns-b|" + (expectedSum(key) + VALUES_PER_KEY * 100));
        }
        return rows;
    }

    private static int expectedSum(int key) {
        int sum = 0;
        for (int offset = 0; offset < VALUES_PER_KEY; offset++) {
            sum += key + offset * PARALLELISM;
        }
        return sum;
    }

    private static int nullMapKey(int key) {
        return -key - 1;
    }

    private static CheckpointInfo discoverOperatorAndCheckpoint(Path checkpointRoot)
            throws Exception {
        String operatorId = null;
        Path root = null;
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
                                root = chkDir.getParent();
                            }
                        } catch (NumberFormatException ignored) {
                        }
                    }
                }
            }
        }
        assertTrue(operatorId != null, "no COBBLE-SNAPSHOT manifest found");
        assertTrue(latestCheckpointId > 0, "no chk-* checkpoint found");
        assertTrue(root != null, "could not resolve checkpoint root");

        operatorId = operatorIdForState(root.toUri().toString(), "value-state");
        long schemaCheckpointId = resolveSchemaCheckpointId(root.toUri().toString(), operatorId);
        assertTrue(schemaCheckpointId > 0, "no inspect-schema event found");
        return new CheckpointInfo(root.toUri().toString(), operatorId, latestCheckpointId);
    }

    private static String operatorIdForState(String checkpointRootUri, String stateName)
            throws Exception {
        Path cobbleRoot = java.nio.file.Paths.get(URI.create(checkpointRootUri)).resolve("cobble");
        assertTrue(Files.isDirectory(cobbleRoot), "no cobble inspect-schema root at " + cobbleRoot);

        try (DirectoryStream<Path> operators = Files.newDirectoryStream(cobbleRoot)) {
            for (Path operatorRoot : operators) {
                if (!Files.isDirectory(operatorRoot)) {
                    continue;
                }
                Path schemaRoot = operatorRoot.resolve("inspect-schema");
                Path eventsDir = schemaRoot.resolve("events");
                Path blobsDir = schemaRoot.resolve("blobs");
                if (!Files.isDirectory(eventsDir) || !Files.isDirectory(blobsDir)) {
                    continue;
                }
                List<InspectSchemaRegistryLayout.SchemaEvent> events = new ArrayList<>();
                try (DirectoryStream<Path> eventFiles = Files.newDirectoryStream(eventsDir)) {
                    for (Path eventFile : eventFiles) {
                        InspectSchemaRegistryLayout.SchemaEvent event =
                                InspectSchemaRegistryLayout.parseEventFileName(
                                        eventFile.getFileName().toString());
                        if (event != null) {
                            events.add(event);
                        }
                    }
                }
                events.sort(
                        Comparator.comparingLong(
                                        InspectSchemaRegistryLayout.SchemaEvent::checkpointId)
                                .reversed());
                for (InspectSchemaRegistryLayout.SchemaEvent event : events) {
                    Path blob =
                            blobsDir.resolve(
                                    InspectSchemaRegistryLayout.blobFileName(event.hash()));
                    if (!Files.isRegularFile(blob)) {
                        continue;
                    }
                    StateInspectSchemaStore store =
                            StateInspectSchemaStore.fromBytes(Files.readAllBytes(blob));
                    if (store.byStateName().containsKey(stateName)) {
                        return operatorRoot.getFileName().toString();
                    }
                }
            }
        }
        throw new AssertionError(
                "Could not find operator inspect schema for state '"
                        + stateName
                        + "' under "
                        + cobbleRoot
                        + ".");
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
        return best;
    }

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

    private static final class FixedIntegerSource extends RichParallelSourceFunction<Integer> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            int key = getRuntimeContext().getIndexOfThisSubtask();
            for (int offset = 0; offset < VALUES_PER_KEY; offset++) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(key + offset * PARALLELISM);
                }
            }
            while (running) {
                Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class StatefulMapper extends RichMapFunction<Integer, Integer> {
        private transient ValueState<Integer> valueState;
        private transient ListState<Integer> listState;
        private transient MapState<Integer, String> mapState;
        private transient ReducingState<Integer> reducingState;
        private transient AggregatingState<Integer, Integer> aggregatingState;

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
            mapState =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "map-state",
                                            BasicTypeInfo.INT_TYPE_INFO,
                                            BasicTypeInfo.STRING_TYPE_INFO));
            reducingState =
                    getRuntimeContext()
                            .getReducingState(
                                    new ReducingStateDescriptor<>(
                                            "reducing-state",
                                            new SumReducer(),
                                            BasicTypeInfo.INT_TYPE_INFO));
            aggregatingState =
                    getRuntimeContext()
                            .getAggregatingState(
                                    new AggregatingStateDescriptor<>(
                                            "aggregating-state",
                                            new SumAggregator(),
                                            BasicTypeInfo.INT_TYPE_INFO));
        }

        @Override
        public Integer map(Integer value) throws Exception {
            Integer current = valueState.value();
            valueState.update(current == null ? value : current + value);
            listState.add(value);
            mapState.put(value, "m" + value);
            if (value < PARALLELISM) {
                mapState.put(nullMapKey(value), null);
            }
            reducingState.add(value);
            aggregatingState.add(value);
            return value;
        }
    }

    private static final class NamespacedStateOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        private transient ValueStateDescriptor<Integer> descriptor;

        @Override
        public void open() throws Exception {
            super.open();
            descriptor =
                    new ValueStateDescriptor<>(
                            "namespaced-value-state", BasicTypeInfo.INT_TYPE_INFO);
        }

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {
            Integer value = element.getValue();
            addToNamespace("ns-a", value);
            addToNamespace("ns-b", value + 100);
            output.collect(element);
        }

        private void addToNamespace(String namespace, int value) throws Exception {
            ValueState<Integer> state =
                    getPartitionedState(namespace, StringSerializer.INSTANCE, descriptor);
            Integer current = state.value();
            state.update(current == null ? value : current + value);
        }
    }

    private static final class SumReducer implements ReduceFunction<Integer> {
        @Override
        public Integer reduce(Integer left, Integer right) {
            return left + right;
        }
    }

    private static final class SumAggregator
            implements AggregateFunction<Integer, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Integer value, Integer accumulator) {
            return accumulator + value;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer left, Integer right) {
            return left + right;
        }
    }

    private static final class CheckpointInfo {
        private final String rootUri;
        private final String operatorId;
        private final long checkpointId;

        private CheckpointInfo(String rootUri, String operatorId, long checkpointId) {
            this.rootUri = rootUri;
            this.operatorId = operatorId;
            this.checkpointId = checkpointId;
        }

        private CheckpointInfo withOperatorId(String operatorId) {
            return new CheckpointInfo(rootUri, operatorId, checkpointId);
        }
    }
}
