package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleEmbeddedCheckpoint;
import io.cobble.flink.common.CobbleMetadataFileIO;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.state.CobbleCheckpointingITSupport;
import io.cobble.flink.state.CobbleHighAvailabilityServicesFactory;
import io.cobble.flink.state.CobbleOptions;
import io.cobble.flink.state.CobbleStateBackend;

import org.apache.flink.api.common.JobID;
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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/** SQL-level proof that Cobble state source DDL reads real Cobble state checkpoints. */
public class CobbleStateSourceSqlITTest {

    private static final int PARALLELISM = 4;
    private static final int VALUES_PER_KEY = 3;

    @TempDir private Path tempDir;

    @Test
    void coldS3StateScanAndLookupWorkInIndependentJvm() throws Exception {
        S3TestConfig s3 = S3TestConfig.fromEnvironment();
        Assumptions.assumeTrue(s3 != null, "S3 fixture environment is not configured");

        String checkpointRoot = s3.root("state-cold-read");
        try {
            Path writerLocalState = tempDir.resolve("writer-local-state");
            Files.createDirectories(writerLocalState);
            CheckpointInfo checkpoint =
                    runStatefulJob(
                            false,
                            checkpointRoot,
                            tempDir.resolve("remote-checkpoint-placeholder"),
                            writerLocalState,
                            s3.flinkConfiguration(),
                            2);

            Path hiddenWriterLocalState = tempDir.resolve("writer-local-state-hidden");
            Files.move(
                    writerLocalState, hiddenWriterLocalState, StandardCopyOption.REPLACE_EXISTING);
            assertTrue(Files.isDirectory(hiddenWriterLocalState));

            String testClasspath = System.getProperty("surefire.test.class.path");
            assertTrue(testClasspath != null && !testClasspath.isEmpty());
            Path childLog = tempDir.resolve("cold-reader.log");
            Process reader =
                    new ProcessBuilder(
                                    java.nio.file.Paths.get(
                                                    System.getProperty("java.home"), "bin", "java")
                                            .toString(),
                                    "-cp",
                                    testClasspath,
                                    CobbleStateSourceSqlITTest.class.getName(),
                                    "s3-cold-reader",
                                    checkpoint.rootUri,
                                    checkpoint.operatorId,
                                    Long.toString(checkpoint.checkpointId))
                            .redirectErrorStream(true)
                            .redirectOutput(childLog.toFile())
                            .start();
            if (!reader.waitFor(180, TimeUnit.SECONDS)) {
                reader.destroyForcibly();
                reader.waitFor(10, TimeUnit.SECONDS);
                throw new AssertionError("Independent S3 state reader timed out.");
            }
            assertEquals(
                    0,
                    reader.exitValue(),
                    "Independent S3 state reader failed: "
                            + new String(Files.readAllBytes(childLog), StandardCharsets.UTF_8));
        } finally {
            try (CobbleMetadataFileIO fileIO =
                    CobbleMetadataFileIO.open(checkpointRoot, s3.connectorOptions())) {
                if (fileIO.exists("")) {
                    fileIO.delete("");
                }
            }
        }
    }

    /** Child-JVM entry point for the opt-in cold S3 reader check. */
    public static void main(String[] args) throws Exception {
        if (args.length == 4 && "s3-cold-reader".equals(args[0])) {
            readS3StateInFreshJvm(args[1], args[2], Long.parseLong(args[3]));
        }
    }

    private static void readS3StateInFreshJvm(
            String checkpointRoot, String operatorId, long checkpointId) throws Exception {
        Configuration global = S3TestConfig.fromEnvironment().flinkConfiguration();
        FileSystem.initialize(global, PluginUtils.createPluginManagerFromRootFolder(global));
        CheckpointInfo checkpoint = new CheckpointInfo(checkpointRoot, operatorId, checkpointId);

        StreamTableEnvironment scanEnv = newTableEnv(global);
        scanEnv.executeSql(
                stateDdl(
                        "cold_s3_state_scan",
                        "key INT, `value` INT",
                        checkpoint,
                        "value-state",
                        "value",
                        Long.toString(checkpointId),
                        true));
        assertEquals(
                expectedSums(), rowSet(scanEnv, "SELECT `key`, `value` FROM cold_s3_state_scan"));

        StreamExecutionEnvironment lookupExecution =
                StreamExecutionEnvironment.getExecutionEnvironment(global);
        lookupExecution.setParallelism(1);
        StreamTableEnvironment lookupEnv = StreamTableEnvironment.create(lookupExecution);
        lookupEnv.executeSql(
                "CREATE TABLE cold_s3_state_lookup ("
                        + "`key` INT, `value` INT, PRIMARY KEY (`key`) NOT ENFORCED) WITH ("
                        + "'connector'='cobble', 'source.kind'='state', 'path'='"
                        + checkpointRoot
                        + "', 'state.operator-id'='"
                        + operatorId
                        + "', 'state.name'='value-state', 'state.kind'='value',"
                        + " 'scan.mode'='batch', 'scan.checkpoint-id'='"
                        + checkpointId
                        + "')");
        List<Row> probes = Arrays.asList(Row.of(0), Row.of(999));
        lookupEnv.createTemporaryView(
                "cold_s3_probes",
                lookupEnv.fromDataStream(
                        lookupExecution.fromCollection(
                                probes, Types.ROW_NAMED(new String[] {"key"}, Types.INT)),
                        Schema.newBuilder()
                                .column("key", DataTypes.INT())
                                .columnByExpression("pt", "PROCTIME()")
                                .build()));
        String query =
                "SELECT p.key, d.`value` FROM cold_s3_probes AS p "
                        + "LEFT JOIN cold_s3_state_lookup FOR SYSTEM_TIME AS OF p.pt AS d "
                        + "ON p.key = d.`key`";
        assertTrue(lookupEnv.explainSql(query).contains("LookupJoin"));
        List<String> actual = new ArrayList<>();
        for (Row row : collectRows(lookupEnv, query, Duration.ofSeconds(60))) {
            actual.add(encode(row));
        }
        Collections.sort(actual);
        assertEquals(Arrays.asList("0|12", "999|null"), actual);
        System.out.println("S3 cold STATE scan and lookup passed in independent JVM.");
    }

    @Test
    void sqlReadsSupportedStateKindsFromCheckpoint() throws Exception {
        CheckpointInfo checkpoint = runStatefulJob(true);
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
        assertEquals(
                expectedValues(), rowList(tableEnv, "SELECT `value` FROM state_values_latest"));

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
        assertTrue(tableEnv.explainSql("SELECT `key` FROM state_list").contains("fields=[key]"));
        assertEquals(expectedListRows(), rowSet(tableEnv, "SELECT `key`, `value` FROM state_list"));
        assertEquals(expectedListKeys(), rowList(tableEnv, "SELECT `key` FROM state_list"));
        assertEquals(
                expectedLiteralRows(PARALLELISM * VALUES_PER_KEY),
                rowList(tableEnv, "SELECT 1 FROM state_list"));

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
        assertEquals(expectedMapValues(), rowList(tableEnv, "SELECT map_value FROM state_map"));
        assertEquals(
                expectedReorderedMapRows(),
                rowList(tableEnv, "SELECT map_value, `key`, map_key, map_value FROM state_map"));
        assertEquals(
                expectedLiteralRows(PARALLELISM * (VALUES_PER_KEY + 1)),
                rowList(tableEnv, "SELECT 1 FROM state_map"));

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
        assertEquals(
                expectedNamespaces(),
                rowList(tableEnv, "SELECT namespace FROM state_namespaced_values_latest"));

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

    @Test
    void sqlReadsEmbeddedCheckpointWithoutHaWrapper() throws Exception {
        CheckpointInfo checkpoint = runStatefulJob(false);
        assertEquals(
                StateSourceConfig.Layout.EMBEDDED_CHECKPOINT,
                CobbleSourceKindDetector.detect(checkpoint.rootUri, CobbleSourceKind.AUTO, false)
                        .stateConfig()
                        .layout());
        StreamTableEnvironment tableEnv = newTableEnv();
        for (String kind : new String[] {"value", "reducing", "aggregating"}) {
            String table = "embedded_" + kind;
            tableEnv.executeSql(
                    stateDdl(
                            table,
                            "key INT, `value` INT",
                            checkpoint,
                            kind + "-state",
                            kind,
                            Long.toString(checkpoint.checkpointId),
                            true));
            assertEquals(expectedSums(), rowSet(tableEnv, "SELECT `key`, `value` FROM " + table));
        }
        assertEquals(expectedValues(), rowList(tableEnv, "SELECT `value` FROM embedded_value"));
    }

    private CheckpointInfo runStatefulJob(boolean haWrapper) throws Exception {
        Path checkpointRoot = tempDir.resolve("checkpoints");
        Path localState = tempDir.resolve("local-state");
        return runStatefulJob(
                haWrapper,
                checkpointRoot.toUri().toString(),
                checkpointRoot,
                localState,
                new Configuration(),
                1);
    }

    private CheckpointInfo runStatefulJob(
            boolean haWrapper,
            String checkpointRootUri,
            Path checkpointRoot,
            Path localState,
            Configuration storageConfiguration,
            int checkpointCount)
            throws Exception {
        if ("file".equalsIgnoreCase(URI.create(checkpointRootUri).getScheme())) {
            Files.createDirectories(checkpointRoot);
        }
        Files.createDirectories(localState);

        Configuration clusterConfiguration = new Configuration();
        storageConfiguration.toMap().forEach(clusterConfiguration::setString);
        if (haWrapper) {
            clusterConfiguration.setString(
                    HighAvailabilityOptions.HA_MODE,
                    CobbleHighAvailabilityServicesFactory.class.getName());
            clusterConfiguration.setString("cobble.ha.delegate.type", "NONE");
        }
        clusterConfiguration.setString(JobManagerOptions.ADDRESS, "localhost");
        clusterConfiguration.setInteger(JobManagerOptions.PORT, 6123);
        clusterConfiguration.setString(RestOptions.ADDRESS, "localhost");
        clusterConfiguration.setInteger(RestOptions.PORT, 0);
        clusterConfiguration.setString(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(), checkpointRootUri);
        FileSystem.initialize(
                clusterConfiguration,
                PluginUtils.createPluginManagerFromRootFolder(clusterConfiguration));

        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(clusterConfiguration)
                                .setNumberTaskManagers(2)
                                .setNumberSlotsPerTaskManager(2)
                                .build());
        cluster.before();
        try {
            FixedIntegerSource.reset();
            Configuration jobConfig = new Configuration();
            storageConfiguration.toMap().forEach(jobConfig::setString);
            jobConfig.setString(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(), checkpointRootUri);
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
            CobbleCheckpointingITSupport.submitJobAndWaitForRunning(cluster, jobGraph);
            CobbleCheckpointingITSupport.waitForJobCondition(
                    cluster.getMiniCluster(),
                    jobId,
                    () -> FixedIntegerSource.finishedSubtasks() == PARALLELISM,
                    Duration.ofSeconds(30),
                    "all SQL source fixtures to finish emitting");
            for (int i = 0; i < checkpointCount; i++) {
                CobbleCheckpointingITSupport.triggerCheckpointAndWait(
                        cluster.getMiniCluster(), jobId);
            }
            if (haWrapper && "file".equalsIgnoreCase(URI.create(checkpointRootUri).getScheme())) {
                CobbleCheckpointingITSupport.waitForCheckpointArtifacts(
                        cluster.getMiniCluster(), jobId, checkpointRoot, Duration.ofSeconds(60));
            }
            CobbleCheckpointingITSupport.cancelJobAndWait(cluster, jobId);
        } finally {
            cluster.after();
        }

        if (haWrapper) {
            return discoverOperatorAndCheckpoint(checkpointRoot);
        }
        CobbleEmbeddedCheckpoint.Location location =
                CobbleEmbeddedCheckpoint.select(
                        new org.apache.flink.core.fs.Path(checkpointRootUri), "latest");
        for (CobbleEmbeddedCheckpoint.OperatorSnapshot operator :
                location.checkpoint().operators().values()) {
            if (operator.schemaStore().byStateName().containsKey("value-state")) {
                assertFalse(operator.volumeDirectories().isEmpty());
                if ("file".equalsIgnoreCase(URI.create(checkpointRootUri).getScheme())) {
                    assertFalse(
                            Files.exists(
                                    java.nio.file.Paths.get(location.checkpointDirectory().toUri())
                                            .resolve(
                                                    "COBBLE-SNAPSHOT-"
                                                            + operator.operatorId()
                                                            + "-MANIFEST")));
                }
                return new CheckpointInfo(
                        location.metadataPath().toString(),
                        operator.operatorId(),
                        location.checkpoint().checkpointId());
            }
        }
        throw new AssertionError("value-state missing from embedded checkpoint metadata");
    }

    private static StreamTableEnvironment newTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        return StreamTableEnvironment.create(env);
    }

    private static StreamTableEnvironment newTableEnv(Configuration configuration) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
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

    private static List<String> rowList(StreamTableEnvironment tableEnv, String query)
            throws Exception {
        List<String> encoded = new ArrayList<>();
        for (Row row : collectRows(tableEnv, query, Duration.ofSeconds(60))) {
            encoded.add(encode(row));
        }
        encoded.sort(String::compareTo);
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

    private static List<String> expectedValues() {
        List<String> rows = new ArrayList<>();
        for (int key = 0; key < PARALLELISM; key++) {
            rows.add(Integer.toString(expectedSum(key)));
        }
        Collections.sort(rows);
        return rows;
    }

    private static List<String> expectedLiteralRows(int rowCount) {
        List<String> rows = new ArrayList<>();
        for (int count = 0; count < rowCount; count++) {
            rows.add("1");
        }
        return rows;
    }

    private static List<String> expectedListKeys() {
        List<String> rows = new ArrayList<>();
        for (int key = 0; key < PARALLELISM; key++) {
            for (int offset = 0; offset < VALUES_PER_KEY; offset++) {
                rows.add(Integer.toString(key));
            }
        }
        rows.sort(String::compareTo);
        return rows;
    }

    private static List<String> expectedMapValues() {
        List<String> rows = new ArrayList<>();
        for (int key = 0; key < PARALLELISM; key++) {
            rows.add("null");
            for (int offset = 0; offset < VALUES_PER_KEY; offset++) {
                rows.add("m" + (key + offset * PARALLELISM));
            }
        }
        rows.sort(String::compareTo);
        return rows;
    }

    private static List<String> expectedReorderedMapRows() {
        List<String> rows = new ArrayList<>();
        for (int key = 0; key < PARALLELISM; key++) {
            rows.add("null|" + key + "|" + nullMapKey(key) + "|null");
            for (int offset = 0; offset < VALUES_PER_KEY; offset++) {
                int value = key + offset * PARALLELISM;
                rows.add("m" + value + "|" + key + "|" + value + "|m" + value);
            }
        }
        Collections.sort(rows);
        return rows;
    }

    private static List<String> expectedNamespaces() {
        List<String> rows = new ArrayList<>();
        for (int key = 0; key < PARALLELISM; key++) {
            rows.add("ns-a");
            rows.add("ns-b");
        }
        Collections.sort(rows);
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

    private static final class FixedIntegerSource extends RichParallelSourceFunction<Integer> {
        private static final AtomicInteger FINISHED_SUBTASKS = new AtomicInteger();
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            int key = getRuntimeContext().getIndexOfThisSubtask();
            for (int offset = 0; offset < VALUES_PER_KEY; offset++) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(key + offset * PARALLELISM);
                }
            }
            FINISHED_SUBTASKS.incrementAndGet();
            while (running) {
                Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        private static void reset() {
            FINISHED_SUBTASKS.set(0);
        }

        private static int finishedSubtasks() {
            return FINISHED_SUBTASKS.get();
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

    private static final class S3TestConfig {
        private final String endpoint;
        private final String bucket;
        private final String accessKey;
        private final String secretKey;

        private S3TestConfig(String endpoint, String bucket, String accessKey, String secretKey) {
            this.endpoint = endpoint;
            this.bucket = bucket;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
        }

        private static S3TestConfig fromEnvironment() throws Exception {
            String endpoint = System.getenv("COBBLE_TEST_S3_ENDPOINT");
            String bucket = System.getenv("COBBLE_TEST_S3_BUCKET");
            String accessKeyFile = System.getenv("COBBLE_TEST_S3_ACCESS_KEY_FILE");
            String secretKeyFile = System.getenv("COBBLE_TEST_S3_SECRET_KEY_FILE");
            if (endpoint == null
                    || bucket == null
                    || accessKeyFile == null
                    || secretKeyFile == null) {
                return null;
            }
            String accessKey =
                    new String(
                                    Files.readAllBytes(java.nio.file.Paths.get(accessKeyFile)),
                                    StandardCharsets.UTF_8)
                            .trim();
            String secretKey =
                    new String(
                                    Files.readAllBytes(java.nio.file.Paths.get(secretKeyFile)),
                                    StandardCharsets.UTF_8)
                            .trim();
            return new S3TestConfig(endpoint, bucket, accessKey, secretKey);
        }

        private Configuration flinkConfiguration() {
            Configuration configuration = new Configuration();
            configuration.setString("s3.endpoint", endpoint);
            configuration.setString("s3.access-key", accessKey);
            configuration.setString("s3.secret-key", secretKey);
            configuration.setString("s3.path.style.access", "true");
            configuration.setString("s3.region", "us-east-1");
            return configuration;
        }

        private CobbleConnectorStorageOptions connectorOptions() {
            Map<String, String> values = new HashMap<>();
            values.put("s3.endpoint", endpoint);
            values.put("s3.access-key", accessKey);
            values.put("s3.secret-key", secretKey);
            values.put("s3.path.style.access", "true");
            values.put("s3.region", "us-east-1");
            return CobbleConnectorStorageOptions.from(values);
        }

        private String root(String scenario) {
            return "s3://"
                    + bucket
                    + "/cobble-flink-validation/"
                    + scenario
                    + "/"
                    + java.util.UUID.randomUUID();
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
