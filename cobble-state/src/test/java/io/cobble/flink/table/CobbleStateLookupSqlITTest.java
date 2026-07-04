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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * SQL-level E2E proof that Cobble state lookup works through the Flink SQL planner's lookup-join
 * path — not just via direct {@link CobbleStateLookupFunction} calls.
 *
 * <p>Each test produces a real Cobble state checkpoint via a MiniCluster job, registers the state
 * as a SQL dimension table with a DDL {@code PRIMARY KEY}, then runs a {@code LEFT JOIN ... FOR
 * SYSTEM_TIME AS OF o.pt} against a bounded probe stream. The explain plan is asserted to contain
 * {@code LookupJoin}, proving the planner chose the lookup source path.
 *
 * <p>Covers value/reducing/aggregating states, MapState (hit/miss/present-null), and non-void
 * namespace isolation — the gap left by the direct-function tests in Steps 3–4.
 */
class CobbleStateLookupSqlITTest {

    private static final int PARALLELISM = 4;
    private static final int VALUES_PER_KEY = 3;

    @TempDir private Path tempDir;

    // ------------------------------------------------------------------------------------------
    //  Tests — value-like states
    // ------------------------------------------------------------------------------------------

    @Test
    void valueStateLookupJoinReturnsHitAndMiss() throws Exception {
        CheckpointInfo checkpoint = runStatefulJob();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = newTableEnv(env);

        tableEnv.executeSql(
                stateLookupDdl(
                        "value_dim",
                        "`key` INT, `value` INT",
                        "PRIMARY KEY (`key`) NOT ENFORCED",
                        checkpoint,
                        "value-state",
                        "value"));
        registerProbe(
                env,
                tableEnv,
                "probes",
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 0),
                        Row.ofKind(RowKind.INSERT, 1),
                        Row.ofKind(RowKind.INSERT, 2),
                        Row.ofKind(RowKind.INSERT, 3),
                        Row.ofKind(RowKind.INSERT, 999)),
                Types.ROW_NAMED(new String[] {"key"}, Types.INT));

        String query =
                "SELECT p.key, d.`value` "
                        + "FROM probes AS p "
                        + "LEFT JOIN value_dim FOR SYSTEM_TIME AS OF p.pt AS d "
                        + "ON p.key = d.`key`";
        assertTrue(
                tableEnv.explainSql(query).contains("LookupJoin"),
                "Expected Flink to plan the value-state dimension as a LookupJoin.");
        assertEquals(
                Arrays.asList("0,12", "1,15", "2,18", "3,21", "999,null"),
                collectRows(tableEnv, query));
    }

    @Test
    void reducingStateLookupJoinReturnsReducedValue() throws Exception {
        CheckpointInfo checkpoint = runStatefulJob();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = newTableEnv(env);

        tableEnv.executeSql(
                stateLookupDdl(
                        "reducing_dim",
                        "`key` INT, `value` INT",
                        "PRIMARY KEY (`key`) NOT ENFORCED",
                        checkpoint,
                        "reducing-state",
                        "reducing"));
        registerProbe(
                env,
                tableEnv,
                "probes",
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 0),
                        Row.ofKind(RowKind.INSERT, 1),
                        Row.ofKind(RowKind.INSERT, 999)),
                Types.ROW_NAMED(new String[] {"key"}, Types.INT));

        String query =
                "SELECT p.key, d.`value` "
                        + "FROM probes AS p "
                        + "LEFT JOIN reducing_dim FOR SYSTEM_TIME AS OF p.pt AS d "
                        + "ON p.key = d.`key`";
        assertTrue(
                tableEnv.explainSql(query).contains("LookupJoin"),
                "Expected Flink to plan the reducing-state dimension as a LookupJoin.");
        assertEquals(Arrays.asList("0,12", "1,15", "999,null"), collectRows(tableEnv, query));
    }

    @Test
    void aggregatingStateLookupJoinReturnsAccumulatorValue() throws Exception {
        CheckpointInfo checkpoint = runStatefulJob();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = newTableEnv(env);

        tableEnv.executeSql(
                stateLookupDdl(
                        "aggregating_dim",
                        "`key` INT, `value` INT",
                        "PRIMARY KEY (`key`) NOT ENFORCED",
                        checkpoint,
                        "aggregating-state",
                        "aggregating"));
        registerProbe(
                env,
                tableEnv,
                "probes",
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 0),
                        Row.ofKind(RowKind.INSERT, 1),
                        Row.ofKind(RowKind.INSERT, 999)),
                Types.ROW_NAMED(new String[] {"key"}, Types.INT));

        String query =
                "SELECT p.key, d.`value` "
                        + "FROM probes AS p "
                        + "LEFT JOIN aggregating_dim FOR SYSTEM_TIME AS OF p.pt AS d "
                        + "ON p.key = d.`key`";
        assertTrue(
                tableEnv.explainSql(query).contains("LookupJoin"),
                "Expected Flink to plan the aggregating-state dimension as a LookupJoin.");
        assertEquals(Arrays.asList("0,12", "1,15", "999,null"), collectRows(tableEnv, query));
    }

    // ------------------------------------------------------------------------------------------
    //  Tests — MapState
    // ------------------------------------------------------------------------------------------

    @Test
    void mapStateLookupJoinReturnsHitAndMiss() throws Exception {
        CheckpointInfo checkpoint = runStatefulJob();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = newTableEnv(env);

        tableEnv.executeSql(
                stateLookupDdl(
                        "map_dim",
                        "`key` INT, map_key INT, map_value INT",
                        "PRIMARY KEY (`key`, map_key) NOT ENFORCED",
                        checkpoint,
                        "map-state",
                        "map"));
        // Probe with (state_key, map_key) pairs. Hits: (0,0),(1,1),(2,2),(3,3) → map_value =
        // map_key*10.
        // Miss: (0, 999) → null dimension row.
        registerProbe(
                env,
                tableEnv,
                "probes",
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 0, 0),
                        Row.ofKind(RowKind.INSERT, 1, 1),
                        Row.ofKind(RowKind.INSERT, 2, 2),
                        Row.ofKind(RowKind.INSERT, 3, 3),
                        Row.ofKind(RowKind.INSERT, 0, 999)),
                Types.ROW_NAMED(new String[] {"key", "map_key"}, Types.INT, Types.INT));

        String query =
                "SELECT p.key, p.map_key, d.map_value "
                        + "FROM probes AS p "
                        + "LEFT JOIN map_dim FOR SYSTEM_TIME AS OF p.pt AS d "
                        + "ON p.key = d.`key` AND p.map_key = d.map_key";
        assertTrue(
                tableEnv.explainSql(query).contains("LookupJoin"),
                "Expected Flink to plan the map-state dimension as a LookupJoin.");
        assertEquals(
                Arrays.asList("0,0,0", "0,999,null", "1,1,10", "2,2,20", "3,3,30"),
                collectRows(tableEnv, query));
    }

    @Test
    void mapStateLookupJoinReturnsPresentNullValue() throws Exception {
        CheckpointInfo checkpoint = runStatefulJob();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = newTableEnv(env);

        tableEnv.executeSql(
                stateLookupDdl(
                        "map_null_dim",
                        "`key` INT, map_key INT, map_value INT",
                        "PRIMARY KEY (`key`, map_key) NOT ENFORCED",
                        checkpoint,
                        "map-null-state",
                        "map"));
        // The map-null-state writes put(value, null) for value < PARALLELISM, so map keys 0..3
        // exist with null values. The LEFT JOIN should still return the row (key and map_key
        // populated) with a null map_value — this distinguishes present-null from a miss.
        registerProbe(
                env,
                tableEnv,
                "probes",
                Arrays.asList(Row.ofKind(RowKind.INSERT, 0, 0), Row.ofKind(RowKind.INSERT, 1, 1)),
                Types.ROW_NAMED(new String[] {"key", "map_key"}, Types.INT, Types.INT));

        String query =
                "SELECT p.key, p.map_key, d.map_value "
                        + "FROM probes AS p "
                        + "LEFT JOIN map_null_dim FOR SYSTEM_TIME AS OF p.pt AS d "
                        + "ON p.key = d.`key` AND p.map_key = d.map_key";
        assertTrue(
                tableEnv.explainSql(query).contains("LookupJoin"),
                "Expected Flink to plan the map-null-state dimension as a LookupJoin.");
        // Present-null: the dimension row exists, so the join produces a row with non-null key
        // columns but null map_value. A miss would produce all-null dimension columns.
        assertEquals(Arrays.asList("0,0,null", "1,1,null"), collectRows(tableEnv, query));
    }

    // ------------------------------------------------------------------------------------------
    //  Tests — non-void namespace (the Step 4 gap)
    // ------------------------------------------------------------------------------------------

    @Test
    void namespacedValueStateLookupJoinIsolatesNamespaces() throws Exception {
        CheckpointInfo checkpoint = runStatefulJob();
        checkpoint =
                checkpoint.withOperatorId(
                        operatorIdForState(checkpoint.rootUri, "namespaced-value-state"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = newTableEnv(env);

        tableEnv.executeSql(
                stateLookupDdl(
                        "ns_value_dim",
                        "`key` INT, namespace STRING, `value` INT",
                        "PRIMARY KEY (`key`, namespace) NOT ENFORCED",
                        checkpoint,
                        "namespaced-value-state",
                        "value"));
        // Probe the same key under two namespaces. ns-a value = 3k+12; ns-b value = 3k+312.
        // This proves the lookup does not cross-read between namespaces.
        registerProbe(
                env,
                tableEnv,
                "probes",
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 0, "ns-a"),
                        Row.ofKind(RowKind.INSERT, 0, "ns-b"),
                        Row.ofKind(RowKind.INSERT, 1, "ns-a"),
                        Row.ofKind(RowKind.INSERT, 1, "ns-b")),
                Types.ROW_NAMED(new String[] {"key", "namespace"}, Types.INT, Types.STRING));

        String query =
                "SELECT p.key, p.namespace, d.`value` "
                        + "FROM probes AS p "
                        + "LEFT JOIN ns_value_dim FOR SYSTEM_TIME AS OF p.pt AS d "
                        + "ON p.key = d.`key` AND p.namespace = d.namespace";
        assertTrue(
                tableEnv.explainSql(query).contains("LookupJoin"),
                "Expected Flink to plan the namespaced-value-state dimension as a LookupJoin.");
        assertEquals(
                Arrays.asList("0,ns-a,12", "0,ns-b,312", "1,ns-a,15", "1,ns-b,315"),
                collectRows(tableEnv, query));
    }

    @Test
    void namespacedMapStateLookupJoinIsolatesNamespaces() throws Exception {
        CheckpointInfo checkpoint = runStatefulJob();
        checkpoint =
                checkpoint.withOperatorId(
                        operatorIdForState(checkpoint.rootUri, "namespaced-map-state"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = newTableEnv(env);

        tableEnv.executeSql(
                stateLookupDdl(
                        "ns_map_dim",
                        "`key` INT, namespace STRING, map_key INT, map_value INT",
                        "PRIMARY KEY (`key`, namespace, map_key) NOT ENFORCED",
                        checkpoint,
                        "namespaced-map-state",
                        "map"));
        // Probe the same (key, map_key) under two namespaces. ns-a map_value = map_key*10 = 40;
        // ns-b map_value = map_key*100 = 400. This proves map lookup isolates namespaces.
        registerProbe(
                env,
                tableEnv,
                "probes",
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 0, "ns-a", 4),
                        Row.ofKind(RowKind.INSERT, 0, "ns-b", 4),
                        Row.ofKind(RowKind.INSERT, 1, "ns-a", 5),
                        Row.ofKind(RowKind.INSERT, 1, "ns-b", 5)),
                Types.ROW_NAMED(
                        new String[] {"key", "namespace", "map_key"},
                        Types.INT,
                        Types.STRING,
                        Types.INT));

        String query =
                "SELECT p.key, p.namespace, p.map_key, d.map_value "
                        + "FROM probes AS p "
                        + "LEFT JOIN ns_map_dim FOR SYSTEM_TIME AS OF p.pt AS d "
                        + "ON p.key = d.`key` AND p.namespace = d.namespace AND p.map_key = d.map_key";
        assertTrue(
                tableEnv.explainSql(query).contains("LookupJoin"),
                "Expected Flink to plan the namespaced-map-state dimension as a LookupJoin.");
        assertEquals(
                Arrays.asList("0,ns-a,4,40", "0,ns-b,4,400", "1,ns-a,5,50", "1,ns-b,5,500"),
                collectRows(tableEnv, query));
    }

    // ------------------------------------------------------------------------------------------
    //  Test — planner pattern confirmation
    // ------------------------------------------------------------------------------------------

    @Test
    void lookupJoinExplainContainsLookupJoin() throws Exception {
        CheckpointInfo checkpoint = runStatefulJob();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = newTableEnv(env);

        tableEnv.executeSql(
                stateLookupDdl(
                        "value_dim",
                        "`key` INT, `value` INT",
                        "PRIMARY KEY (`key`) NOT ENFORCED",
                        checkpoint,
                        "value-state",
                        "value"));
        registerProbe(
                env,
                tableEnv,
                "probes",
                Collections.singletonList(Row.ofKind(RowKind.INSERT, 0)),
                Types.ROW_NAMED(new String[] {"key"}, Types.INT));

        String query =
                "SELECT p.key, d.`value` "
                        + "FROM probes AS p "
                        + "LEFT JOIN value_dim FOR SYSTEM_TIME AS OF p.pt AS d "
                        + "ON p.key = d.`key`";
        String explain = tableEnv.explainSql(query);
        assertTrue(
                explain.contains("LookupJoin"),
                "Expected the explain plan to contain 'LookupJoin'. Actual explain:\n" + explain);
    }

    // ------------------------------------------------------------------------------------------
    //  Tests — order-independent planner key mapping
    //
    // These reuse the same state fixtures and probe rows as the forward tests above, but write the
    // JOIN ON conditions in reverse order relative to the DDL PRIMARY KEY. If the Flink planner
    // passes LookupContext.getKeys() in ON-condition order, the provider's order-independent
    // resolver must remap them. If the planner normalizes to PK order, these tests still serve as
    // regression coverage for the same correct results.
    // ------------------------------------------------------------------------------------------

    @Test
    void mapStateLookupJoinAcceptsReversedOnConditions() throws Exception {
        CheckpointInfo checkpoint = runStatefulJob();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = newTableEnv(env);

        tableEnv.executeSql(
                stateLookupDdl(
                        "map_dim_rev",
                        "`key` INT, map_key INT, map_value INT",
                        "PRIMARY KEY (`key`, map_key) NOT ENFORCED",
                        checkpoint,
                        "map-state",
                        "map"));
        // Same probes and expected results as mapStateLookupJoinReturnsHitAndMiss, but the ON
        // conditions are reversed: map_key first, then key.
        registerProbe(
                env,
                tableEnv,
                "probes",
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 0, 0),
                        Row.ofKind(RowKind.INSERT, 1, 1),
                        Row.ofKind(RowKind.INSERT, 2, 2),
                        Row.ofKind(RowKind.INSERT, 3, 3),
                        Row.ofKind(RowKind.INSERT, 0, 999)),
                Types.ROW_NAMED(new String[] {"key", "map_key"}, Types.INT, Types.INT));

        String query =
                "SELECT p.key, p.map_key, d.map_value "
                        + "FROM probes AS p "
                        + "LEFT JOIN map_dim_rev FOR SYSTEM_TIME AS OF p.pt AS d "
                        + "ON p.map_key = d.map_key AND p.key = d.`key`";
        assertTrue(
                tableEnv.explainSql(query).contains("LookupJoin"),
                "Expected Flink to plan the map-state dimension as a LookupJoin.");
        assertEquals(
                Arrays.asList("0,0,0", "0,999,null", "1,1,10", "2,2,20", "3,3,30"),
                collectRows(tableEnv, query));
    }

    @Test
    void namespacedMapStateLookupJoinAcceptsReversedOnConditions() throws Exception {
        CheckpointInfo checkpoint = runStatefulJob();
        checkpoint =
                checkpoint.withOperatorId(
                        operatorIdForState(checkpoint.rootUri, "namespaced-map-state"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = newTableEnv(env);

        tableEnv.executeSql(
                stateLookupDdl(
                        "ns_map_dim_rev",
                        "`key` INT, namespace STRING, map_key INT, map_value INT",
                        "PRIMARY KEY (`key`, namespace, map_key) NOT ENFORCED",
                        checkpoint,
                        "namespaced-map-state",
                        "map"));
        // Same probes and expected results as namespacedMapStateLookupJoinIsolatesNamespaces, but
        // the ON conditions are fully reversed: map_key, namespace, key.
        registerProbe(
                env,
                tableEnv,
                "probes",
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 0, "ns-a", 4),
                        Row.ofKind(RowKind.INSERT, 0, "ns-b", 4),
                        Row.ofKind(RowKind.INSERT, 1, "ns-a", 5),
                        Row.ofKind(RowKind.INSERT, 1, "ns-b", 5)),
                Types.ROW_NAMED(
                        new String[] {"key", "namespace", "map_key"},
                        Types.INT,
                        Types.STRING,
                        Types.INT));

        String query =
                "SELECT p.key, p.namespace, p.map_key, d.map_value "
                        + "FROM probes AS p "
                        + "LEFT JOIN ns_map_dim_rev FOR SYSTEM_TIME AS OF p.pt AS d "
                        + "ON p.map_key = d.map_key AND p.namespace = d.namespace AND p.key = d.`key`";
        assertTrue(
                tableEnv.explainSql(query).contains("LookupJoin"),
                "Expected Flink to plan the namespaced-map-state dimension as a LookupJoin.");
        assertEquals(
                Arrays.asList("0,ns-a,4,40", "0,ns-b,4,400", "1,ns-a,5,50", "1,ns-b,5,500"),
                collectRows(tableEnv, query));
    }

    // ------------------------------------------------------------------------------------------
    //  MiniCluster fixture
    // ------------------------------------------------------------------------------------------

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
                    .name("lookup-e2e-source")
                    .uid("lookup-e2e-source")
                    .keyBy(value -> value % PARALLELISM)
                    .map(new StatefulMapper())
                    .name("lookup-e2e-stateful")
                    .uid("lookup-e2e-stateful")
                    .keyBy(value -> value % PARALLELISM)
                    .transform(
                            "lookup-e2e-namespaced",
                            BasicTypeInfo.INT_TYPE_INFO,
                            new NamespacedStateOperator())
                    .name("lookup-e2e-namespaced")
                    .uid("lookup-e2e-namespaced")
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

    // ------------------------------------------------------------------------------------------
    //  SQL helpers
    // ------------------------------------------------------------------------------------------

    private static StreamTableEnvironment newTableEnv(StreamExecutionEnvironment env) {
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        return StreamTableEnvironment.create(env);
    }

    private static String stateLookupDdl(
            String tableName,
            String columns,
            String primaryKeyClause,
            CheckpointInfo checkpoint,
            String stateName,
            String stateKind) {
        return "CREATE TABLE "
                + tableName
                + " ("
                + columns
                + ", "
                + primaryKeyClause
                + ") WITH ("
                + " 'connector' = 'cobble',"
                + " 'source.kind' = 'state',"
                + " 'path' = '"
                + checkpoint.rootUri
                + "',"
                + " 'state.operator-id' = '"
                + checkpoint.operatorId
                + "',"
                + " 'state.name' = '"
                + stateName
                + "',"
                + " 'state.kind' = '"
                + stateKind
                + "',"
                + " 'scan.mode' = 'batch',"
                + " 'scan.checkpoint-id' = 'latest'"
                + ")";
    }

    /**
     * Registers a bounded probe view with a {@code PROCTIME()} column named {@code pt}. The probe
     * rows carry the lookup-key columns; the {@code pt} column enables the {@code FOR SYSTEM_TIME
     * AS OF} syntax that triggers the lookup-join planner rule.
     */
    private static void registerProbe(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            String viewName,
            List<Row> rows,
            TypeInformation<Row> rowType) {
        org.apache.flink.api.java.typeutils.RowTypeInfo rowTypeInfo =
                (org.apache.flink.api.java.typeutils.RowTypeInfo) rowType;
        String[] fieldNames = rowTypeInfo.getFieldNames();
        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (int i = 0; i < fieldNames.length; i++) {
            schemaBuilder.column(
                    fieldNames[i],
                    org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType(
                            rowTypeInfo.getTypeAt(i)));
        }
        schemaBuilder.columnByExpression("pt", "PROCTIME()");
        tableEnv.createTemporaryView(
                viewName,
                tableEnv.fromDataStream(env.fromCollection(rows, rowType), schemaBuilder.build()));
    }

    private static List<String> collectRows(StreamTableEnvironment tableEnv, String query)
            throws Exception {
        TableResult result = tableEnv.executeSql(query);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<List<String>> future =
                executor.submit(
                        () -> {
                            List<String> rows = new ArrayList<>();
                            try (CloseableIterator<Row> iterator = result.collect()) {
                                while (iterator.hasNext()) {
                                    Row row = iterator.next();
                                    StringBuilder builder = new StringBuilder();
                                    for (int i = 0; i < row.getArity(); i++) {
                                        if (i > 0) {
                                            builder.append(',');
                                        }
                                        builder.append(row.getField(i));
                                    }
                                    rows.add(builder.toString());
                                }
                            }
                            return rows;
                        });
        try {
            List<String> rows = future.get(60, TimeUnit.SECONDS);
            Collections.sort(rows);
            return rows;
        } finally {
            future.cancel(true);
            executor.shutdownNow();
        }
    }

    // ------------------------------------------------------------------------------------------
    //  Checkpoint discovery helpers
    // ------------------------------------------------------------------------------------------

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

    // ------------------------------------------------------------------------------------------
    //  Stateful mappers
    // ------------------------------------------------------------------------------------------

    /**
     * Bounded source: each parallel subtask {@code s} emits exactly {@code VALUES_PER_KEY} values
     * ({@code s, s+PARALLELISM, s+2*PARALLELISM}) then idles. This makes all state contents
     * deterministic and assertable.
     */
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

    /**
     * Writes void-namespace state for value/reducing/aggregating/map/map-null kinds. Keyed by
     * {@code value % PARALLELISM}, so each key {@code k} receives {@code k, k+4, k+8}.
     */
    private static final class StatefulMapper extends RichMapFunction<Integer, Integer> {
        private transient ValueState<Integer> valueState;
        private transient ReducingState<Integer> reducingState;
        private transient AggregatingState<Integer, Integer> aggregatingState;
        private transient MapState<Integer, Integer> mapState;
        private transient MapState<Integer, Integer> mapNullState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "value-state", BasicTypeInfo.INT_TYPE_INFO));
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
            mapState =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "map-state",
                                            BasicTypeInfo.INT_TYPE_INFO,
                                            BasicTypeInfo.INT_TYPE_INFO));
            mapNullState =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "map-null-state",
                                            BasicTypeInfo.INT_TYPE_INFO,
                                            BasicTypeInfo.INT_TYPE_INFO));
        }

        @Override
        public Integer map(Integer value) throws Exception {
            Integer current = valueState.value();
            valueState.update(current == null ? value : current + value);
            reducingState.add(value);
            aggregatingState.add(value);
            mapState.put(value, value * 10);
            if (value < PARALLELISM) {
                mapNullState.put(value, null);
            }
            return value;
        }
    }

    /**
     * Writes non-void-namespace state (ValueState and MapState) under two String namespaces ({@code
     * ns-a}, {@code ns-b}). Uses {@code getPartitionedState(namespace, ...)} to set the active
     * namespace per write.
     */
    private static final class NamespacedStateOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        private transient ValueStateDescriptor<Integer> valueDescriptor;
        private transient MapStateDescriptor<Integer, Integer> mapDescriptor;

        @Override
        public void open() throws Exception {
            super.open();
            valueDescriptor =
                    new ValueStateDescriptor<>(
                            "namespaced-value-state", BasicTypeInfo.INT_TYPE_INFO);
            mapDescriptor =
                    new MapStateDescriptor<>(
                            "namespaced-map-state",
                            BasicTypeInfo.INT_TYPE_INFO,
                            BasicTypeInfo.INT_TYPE_INFO);
        }

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {
            Integer value = element.getValue();
            // ns-a: value as-is; ns-b: value + 100.
            addToValueNamespace("ns-a", value);
            addToValueNamespace("ns-b", value + 100);
            // ns-a: map value = value*10; ns-b: map value = value*100.
            addToMapNamespace("ns-a", value, value * 10);
            addToMapNamespace("ns-b", value, value * 100);
            output.collect(element);
        }

        private void addToValueNamespace(String namespace, int value) throws Exception {
            ValueState<Integer> state =
                    getPartitionedState(namespace, StringSerializer.INSTANCE, valueDescriptor);
            Integer current = state.value();
            state.update(current == null ? value : current + value);
        }

        private void addToMapNamespace(String namespace, int mapKey, int mapValue)
                throws Exception {
            MapState<Integer, Integer> state =
                    getPartitionedState(namespace, StringSerializer.INSTANCE, mapDescriptor);
            state.put(mapKey, mapValue);
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

    // ------------------------------------------------------------------------------------------
    //  Inner classes
    // ------------------------------------------------------------------------------------------

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
