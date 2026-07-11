package io.cobble.flink.table;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.DescriptorCapability;
import io.cobble.flink.common.inspect.InspectDecoderDescriptorKind;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.StateInspectTypeKind;
import io.cobble.flink.state.CobbleHighAvailabilityServicesFactory;
import io.cobble.flink.state.CobbleOptions;
import io.cobble.flink.state.CobbleStateBackend;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Serializable;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * End-to-end smoke IT test for the snapshot-first state inspect pipeline.
 *
 * <p>Validates the full flow from real Cobble checkpoint through the schema sidecar to the cobble
 * state source scan and lookup — catching regressions when serializer capture policy changes.
 *
 * <p>The stateful job creates four states covering distinct snapshot-derived semantic types:
 *
 * <ul>
 *   <li><b>ValueState&lt;RowData&gt;</b> — ROW with named fields, monitor-portable (snapshot-only).
 *   <li><b>ListState&lt;RowData&gt;</b> — LIST(ROW) with named fields, monitor-portable.
 *   <li><b>MapState&lt;RowData, RowData&gt;</b> — MAP(ROW, ROW), both key and value portable.
 *   <li><b>ValueState&lt;TestOrderPojo&gt;</b> — POJO mapped to ROW with a fully classless
 *       descriptor (snapshot-only).
 * </ul>
 *
 * <p>Phases:
 *
 * <ol>
 *   <li>Run the stateful job on a MiniCluster and trigger a checkpoint.
 *   <li>Read the inspect sidecar and verify semantic schemas carry nested field names and the
 *       RowData and POJO serializers are classless and snapshot-only.
 *   <li>Scan the checkpoint via {@code connector='cobble'} state source SQL and verify decoded
 *       RowData values for value, list, and map states.
 *   <li>Lookup value state and map state via SQL {@code LEFT JOIN ... FOR SYSTEM_TIME AS OF},
 *       verifying the snapshot-only key serializer path for both state kinds.
 * </ol>
 */
class CobbleStateSnapshotPipelineITTest {

    private static final int PARALLELISM = 4;
    private static final int ROWS_PER_KEY = 3;

    private static final String VALUE_ROW_STATE = "value-row-state";
    private static final String LIST_ROW_STATE = "list-row-state";
    private static final String MAP_ROW_ROW_STATE = "map-row-row-state";
    private static final String POJO_STATE = "pojo-state";

    @TempDir private Path tempDir;

    @Test
    void snapshotFirstPipelineEndToEnd() throws Exception {
        // ---- Phase 1: Run stateful job and create a checkpoint ----
        CheckpointInfo checkpoint = runStatefulJob();

        // ---- Phase 2: Verify inspect sidecar semantic schema ----
        StateInspectSchemaStore store = readSchemaStore(checkpoint);

        // 2a: ValueState<RowData> — snapshot-only, ROW(order_id, region)
        StateInspectSchema valueSchema = store.byStateName().get(VALUE_ROW_STATE);
        assertNotNull(valueSchema, "value-row-state must be in the schema store");
        StateInspectSemanticSchema valueSemantic = store.semanticSchema(VALUE_ROW_STATE);
        assertNotNull(valueSemantic, "value-row-state must have a semantic schema");
        assertRowFields(valueSemantic.value(), "order_id", "region");
        assertNull(
                valueSchema.valueSerializer().serializedSerializerBytes(),
                "RowData value serializer should be snapshot-only (portable)");
        assertNotNull(
                valueSchema.valueSerializer().snapshotBytes(),
                "RowData value serializer must have snapshot bytes");

        // 2b: ListState<RowData> — snapshot-only, LIST(ROW(order_id, region))
        StateInspectSchema listSchema = store.byStateName().get(LIST_ROW_STATE);
        assertNotNull(listSchema, "list-row-state must be in the schema store");
        StateInspectSemanticSchema listSemantic = store.semanticSchema(LIST_ROW_STATE);
        assertNotNull(listSemantic, "list-row-state must have a semantic schema");
        assertNotNull(listSemantic.listElement(), "list state must have a list element type");
        assertRowFields(listSemantic.listElement(), "order_id", "region");
        assertNull(
                listSchema.listElementSerializer().serializedSerializerBytes(),
                "RowData list element serializer should be snapshot-only (portable)");

        // 2c: MapState<RowData, RowData> — snapshot-only key+value, MAP(ROW(mk_id, mk_region),
        //     ROW(order_id, region))
        StateInspectSchema mapSchema = store.byStateName().get(MAP_ROW_ROW_STATE);
        assertNotNull(mapSchema, "map-row-row-state must be in the schema store");
        StateInspectSemanticSchema mapSemantic = store.semanticSchema(MAP_ROW_ROW_STATE);
        assertNotNull(mapSemantic, "map-row-row-state must have a semantic schema");
        assertNotNull(mapSemantic.mapUserKey(), "map state must have a map user key type");
        assertRowFields(mapSemantic.mapUserKey(), "mk_id", "mk_region");
        assertRowFields(mapSemantic.mapUserValue(), "order_id", "region");
        assertNull(
                mapSchema.mapUserKeySerializer().serializedSerializerBytes(),
                "RowData map key serializer should be snapshot-only (portable)");
        assertNull(
                mapSchema.mapUserValueSerializer().serializedSerializerBytes(),
                "RowData map value serializer should be snapshot-only (portable)");

        // 2d: ValueState<TestOrderPojo> — fully classless POJO descriptor, ROW(id, name)
        StateInspectSchema pojoSchema = store.byStateName().get(POJO_STATE);
        assertNotNull(pojoSchema, "pojo-state must be in the schema store");
        StateInspectSemanticSchema pojoSemantic = store.semanticSchema(POJO_STATE);
        assertNotNull(pojoSemantic, "pojo-state must have a semantic schema");
        assertRowFields(pojoSemantic.value(), "id", "name");
        assertNotNull(
                pojoSchema.valueSerializer().decoderDescriptor(),
                "POJO value serializer must have a decoder descriptor");
        assertEquals(
                InspectDecoderDescriptorKind.POJO,
                pojoSchema.valueSerializer().decoderDescriptor().kind(),
                "POJO value serializer must have a POJO decoder descriptor");
        assertEquals(
                DescriptorCapability.FULLY_CLASSLESS,
                pojoSchema.valueSerializer().decoderDescriptor().capability(),
                "POJO value serializer descriptor must be fully classless");
        assertNull(
                pojoSchema.valueSerializer().serializedSerializerBytes(),
                "Fully classless POJO serializer should not persist a serialized fallback");
        assertNotNull(
                pojoSchema.valueSerializer().snapshotBytes(),
                "POJO value serializer must have snapshot bytes");
        assertRowFields(pojoSchema.valueSerializer().inspectType(), "id", "name");

        // ---- Phase 3: Scan checkpoint via cobble state source SQL ----
        StreamTableEnvironment tableEnv = newTableEnv();

        // 3a: Value state scan.
        tableEnv.executeSql(
                stateDdl(
                        "row_value_source",
                        "`key` INT, order_id BIGINT, region STRING",
                        checkpoint,
                        VALUE_ROW_STATE,
                        "value"));
        Set<String> valueRows =
                rowSet(tableEnv, "SELECT `key`, order_id, region FROM row_value_source");
        assertEquals(expectedValueRowValues(), valueRows);

        // 3b: List state scan (one row per list element).
        tableEnv.executeSql(
                stateDdl(
                        "row_list_source",
                        "`key` INT, order_id BIGINT, region STRING",
                        checkpoint,
                        LIST_ROW_STATE,
                        "list"));
        Set<String> listRows =
                rowSet(tableEnv, "SELECT `key`, order_id, region FROM row_list_source");
        assertEquals(expectedListRowValues(), listRows);

        // 3c: Map<RowData, RowData> state scan.
        tableEnv.executeSql(
                stateDdl(
                        "row_map_row_source",
                        "`key` INT, mk_id BIGINT, mk_region STRING, order_id BIGINT, region STRING",
                        checkpoint,
                        MAP_ROW_ROW_STATE,
                        "map"));
        Set<String> mapRows =
                rowSet(
                        tableEnv,
                        "SELECT `key`, mk_id, mk_region, order_id, region"
                                + " FROM row_map_row_source");
        assertEquals(expectedMapRowRowValues(), mapRows);

        // ---- Phase 4: Lookup value state and map state via SQL ----
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment lookupEnv = newTableEnv(env);

        // 4a: Value state lookup.
        lookupEnv.executeSql(
                stateLookupDdl(
                        "row_value_dim",
                        "`key` INT, order_id BIGINT, region STRING",
                        "PRIMARY KEY (`key`) NOT ENFORCED",
                        checkpoint,
                        VALUE_ROW_STATE,
                        "value"));
        registerProbe(
                env,
                lookupEnv,
                "value_probes",
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 0),
                        Row.ofKind(RowKind.INSERT, 1),
                        Row.ofKind(RowKind.INSERT, 2),
                        Row.ofKind(RowKind.INSERT, 3),
                        Row.ofKind(RowKind.INSERT, 999)),
                Types.ROW_NAMED(new String[] {"key"}, Types.INT));

        String valueQuery =
                "SELECT p.key, d.order_id, d.region "
                        + "FROM value_probes AS p "
                        + "LEFT JOIN row_value_dim FOR SYSTEM_TIME AS OF p.pt AS d "
                        + "ON p.key = d.`key`";
        assertTrue(
                lookupEnv.explainSql(valueQuery).contains("LookupJoin"),
                "Expected Flink to plan the value-state dimension as a LookupJoin.");
        List<String> valueLookupRows = collectRows(lookupEnv, valueQuery);
        // Keys 0–3 exist; 999 is a miss. The value state keeps the last written RowData per key.
        // For key K, the last order_id is K + (ROWS_PER_KEY-1)*PARALLELISM.
        assertTrue(
                valueLookupRows.contains("0,8,region-0"),
                "key 0 should hit with last order_id: " + valueLookupRows);
        assertTrue(
                valueLookupRows.contains("1,9,region-1"),
                "key 1 should hit with last order_id: " + valueLookupRows);
        assertTrue(
                valueLookupRows.contains("999,null,null"),
                "key 999 should miss: " + valueLookupRows);

        // 4b: Map<RowData, RowData> state lookup — exercises snapshot-only RowData key serializer.
        lookupEnv.executeSql(
                stateLookupDdl(
                        "row_map_row_dim",
                        "`key` INT, mk_id BIGINT, mk_region STRING, order_id BIGINT, region STRING",
                        "PRIMARY KEY (`key`, mk_id, mk_region) NOT ENFORCED",
                        checkpoint,
                        MAP_ROW_ROW_STATE,
                        "map"));
        // Probe with existing (key, mk_id, mk_region) combinations and a miss.
        registerProbe(
                env,
                lookupEnv,
                "map_probes",
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 0, 0L, "region-0"),
                        Row.ofKind(RowKind.INSERT, 0, 4L, "region-0"),
                        Row.ofKind(RowKind.INSERT, 1, 1L, "region-1"),
                        Row.ofKind(RowKind.INSERT, 999, 0L, "region-0")),
                Types.ROW_NAMED(
                        new String[] {"key", "mk_id", "mk_region"},
                        Types.INT,
                        Types.LONG,
                        Types.STRING));

        String mapQuery =
                "SELECT p.key, p.mk_id, d.order_id, d.region "
                        + "FROM map_probes AS p "
                        + "LEFT JOIN row_map_row_dim FOR SYSTEM_TIME AS OF p.pt AS d "
                        + "ON p.key = d.`key` AND p.mk_id = d.mk_id AND p.mk_region = d.mk_region";
        assertTrue(
                lookupEnv.explainSql(mapQuery).contains("LookupJoin"),
                "Expected Flink to plan the map-state dimension as a LookupJoin.");
        List<String> mapLookupRows = collectRows(lookupEnv, mapQuery);
        // Map value = ROW(order_id * 10, region-key). Probe (0, 0) → (0, region-0);
        // (0, 4) → (40, region-0); (1, 1) → (10, region-1); (999, 0) → miss.
        assertTrue(
                mapLookupRows.contains("0,0,0,region-0"),
                "key 0, mk_id 0 should hit: " + mapLookupRows);
        assertTrue(
                mapLookupRows.contains("0,4,40,region-0"),
                "key 0, mk_id 4 should hit: " + mapLookupRows);
        assertTrue(
                mapLookupRows.contains("1,1,10,region-1"),
                "key 1, mk_id 1 should hit: " + mapLookupRows);
        assertTrue(
                mapLookupRows.contains("999,0,null,null"), "key 999 should miss: " + mapLookupRows);
    }

    // ------------------------------------------------------------------------------------------
    //  Assertions
    // ------------------------------------------------------------------------------------------

    private static void assertRowFields(StateInspectType type, String... fieldNames) {
        assertNotNull(type, "semantic type must not be null");
        assertEquals(StateInspectTypeKind.ROW, type.kind(), "expected ROW kind");
        assertEquals(fieldNames.length, type.fields().size(), "field count mismatch");
        for (int i = 0; i < fieldNames.length; i++) {
            assertEquals(
                    fieldNames[i], type.fields().get(i).name(), "field " + i + " name mismatch");
        }
    }

    // ------------------------------------------------------------------------------------------
    //  Expected data
    // ------------------------------------------------------------------------------------------

    /**
     * Expected value-state rows: for each key, the last written RowData. The source emits values
     * 0..(PARALLELISM*ROWS_PER_KEY-1); key = value % PARALLELISM. So the last value for key K is K
     * + (ROWS_PER_KEY-1)*PARALLELISM. Encoded with commas to match {@link #encode}.
     */
    private static Set<String> expectedValueRowValues() {
        Set<String> rows = new LinkedHashSet<>();
        for (int key = 0; key < PARALLELISM; key++) {
            long lastOrderId = key + (long) (ROWS_PER_KEY - 1) * PARALLELISM;
            rows.add(key + "," + lastOrderId + ",region-" + key);
        }
        return rows;
    }

    /**
     * Expected list-state rows: for each key, one row per emitted value (3 per key). Encoded as
     * {@code key,order_id,region}.
     */
    private static Set<String> expectedListRowValues() {
        Set<String> rows = new LinkedHashSet<>();
        for (int key = 0; key < PARALLELISM; key++) {
            for (int offset = 0; offset < ROWS_PER_KEY; offset++) {
                long orderId = key + offset * PARALLELISM;
                rows.add(key + "," + orderId + ",region-" + key);
            }
        }
        return rows;
    }

    /**
     * Expected map-state rows: for each key, one entry per emitted value. Map key =
     * ROW(mk_id=order_id, mk_region), map value = ROW(order_id*10, region). Encoded as {@code
     * key,mk_id,mk_region,order_id,region}.
     */
    private static Set<String> expectedMapRowRowValues() {
        Set<String> rows = new LinkedHashSet<>();
        for (int key = 0; key < PARALLELISM; key++) {
            for (int offset = 0; offset < ROWS_PER_KEY; offset++) {
                long orderId = key + offset * PARALLELISM;
                rows.add(
                        key
                                + ","
                                + orderId
                                + ",region-"
                                + key
                                + ","
                                + (orderId * 10)
                                + ",region-"
                                + key);
            }
        }
        return rows;
    }

    // ------------------------------------------------------------------------------------------
    //  Stateful job
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
                    .name("snapshot-pipeline-source")
                    .uid("snapshot-pipeline-source")
                    .keyBy(value -> value % PARALLELISM)
                    .map(new RowStateMapper())
                    .name("snapshot-pipeline-stateful")
                    .uid("snapshot-pipeline-stateful");

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
    //  Schema store reading
    // ------------------------------------------------------------------------------------------

    private StateInspectSchemaStore readSchemaStore(CheckpointInfo checkpoint) throws Exception {
        String operatorId = operatorIdForState(checkpoint.rootUri, VALUE_ROW_STATE);
        Path cobbleRoot = java.nio.file.Paths.get(URI.create(checkpoint.rootUri)).resolve("cobble");
        Path schemaRoot = cobbleRoot.resolve(operatorId).resolve("inspect-schema");
        Path eventsDir = schemaRoot.resolve("events");
        Path blobsDir = schemaRoot.resolve("blobs");

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
                Comparator.comparingLong(InspectSchemaRegistryLayout.SchemaEvent::checkpointId)
                        .reversed());
        for (InspectSchemaRegistryLayout.SchemaEvent event : events) {
            Path blob = blobsDir.resolve(InspectSchemaRegistryLayout.blobFileName(event.hash()));
            if (Files.isRegularFile(blob)) {
                return StateInspectSchemaStore.fromBytes(Files.readAllBytes(blob));
            }
        }
        throw new AssertionError("Could not read schema store from " + schemaRoot);
    }

    // ------------------------------------------------------------------------------------------
    //  SQL helpers
    // ------------------------------------------------------------------------------------------

    private static StreamTableEnvironment newTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        return StreamTableEnvironment.create(env);
    }

    private static StreamTableEnvironment newTableEnv(StreamExecutionEnvironment env) {
        env.setParallelism(2);
        return StreamTableEnvironment.create(env);
    }

    private static String stateDdl(
            String tableName,
            String columns,
            CheckpointInfo checkpoint,
            String stateName,
            String stateKind) {
        return "CREATE TABLE "
                + tableName
                + " ("
                + columns
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
                + " 'scan.checkpoint-id' = '"
                + checkpoint.checkpointId
                + "'"
                + ")";
    }

    private static String stateLookupDdl(
            String tableName,
            String columns,
            String primaryKey,
            CheckpointInfo checkpoint,
            String stateName,
            String stateKind) {
        return "CREATE TABLE "
                + tableName
                + " ("
                + columns
                + ", "
                + primaryKey
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
                + " 'scan.checkpoint-id' = '"
                + checkpoint.checkpointId
                + "'"
                + ")";
    }

    @SuppressWarnings({"SameParameterValue", "unchecked"})
    private static void registerProbe(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            String tableName,
            List<Row> rows,
            TypeInformation<?> rowType) {
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
                tableName,
                tableEnv.fromDataStream(
                        env.fromCollection(rows, (TypeInformation<Row>) rowType),
                        schemaBuilder.build()));
    }

    private static Set<String> rowSet(StreamTableEnvironment tableEnv, String query)
            throws Exception {
        List<Row> rows = collectRowList(tableEnv, query, Duration.ofSeconds(60));
        Set<String> encoded = new LinkedHashSet<>();
        for (Row row : rows) {
            encoded.add(encode(row));
        }
        return encoded;
    }

    private static List<String> collectRows(StreamTableEnvironment tableEnv, String query)
            throws Exception {
        return collectRows(tableEnv, query, Duration.ofSeconds(60));
    }

    private static List<String> collectRows(
            StreamTableEnvironment tableEnv, String query, Duration timeout) throws Exception {
        List<Row> rows = collectRowList(tableEnv, query, timeout);
        List<String> encoded = new ArrayList<>(rows.size());
        for (Row row : rows) {
            encoded.add(encode(row));
        }
        return encoded;
    }

    private static List<Row> collectRowList(
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
                builder.append(',');
            }
            Object value = row.getField(index);
            builder.append(value == null ? "null" : value);
        }
        return builder.toString();
    }

    // ------------------------------------------------------------------------------------------
    //  Checkpoint discovery
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

        operatorId = operatorIdForState(root.toUri().toString(), VALUE_ROW_STATE);
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
    //  Test fixtures
    // ------------------------------------------------------------------------------------------

    /** RowData type for value/list/map-value: ROW(order_id BIGINT, region VARCHAR). */
    private static final InternalTypeInfo<RowData> RECORD_TYPE =
            InternalTypeInfo.of(
                    org.apache.flink.table.types.logical.RowType.of(
                            new org.apache.flink.table.types.logical.LogicalType[] {
                                new BigIntType(false), VarCharType.STRING_TYPE
                            },
                            new String[] {"order_id", "region"}));

    /** RowData type for map keys: ROW(mk_id BIGINT, mk_region VARCHAR). */
    private static final InternalTypeInfo<RowData> MAP_KEY_TYPE =
            InternalTypeInfo.of(
                    org.apache.flink.table.types.logical.RowType.of(
                            new org.apache.flink.table.types.logical.LogicalType[] {
                                new BigIntType(false), VarCharType.STRING_TYPE
                            },
                            new String[] {"mk_id", "mk_region"}));

    private static final class FixedIntegerSource extends RichParallelSourceFunction<Integer> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            int key = getRuntimeContext().getIndexOfThisSubtask();
            for (int offset = 0; offset < ROWS_PER_KEY; offset++) {
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
     * Writes into four states covering distinct snapshot-derived semantic types:
     *
     * <ul>
     *   <li>ValueState&lt;RowData&gt; — keeps last RowData per key.
     *   <li>ListState&lt;RowData&gt; — accumulates all RowData per key.
     *   <li>MapState&lt;RowData, RowData&gt; — keys on ROW(mk_id, mk_region), value =
     *       ROW(order_id*10, region).
     *   <li>ValueState&lt;TestOrderPojo&gt; — keeps last POJO per key (non-portable serializer).
     * </ul>
     */
    private static final class RowStateMapper extends RichMapFunction<Integer, Integer> {
        private transient ValueState<RowData> valueRowState;
        private transient ListState<RowData> listRowState;
        private transient MapState<RowData, RowData> mapRowRowState;
        private transient ValueState<TestOrderPojo> pojoState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueRowState =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>(VALUE_ROW_STATE, RECORD_TYPE));
            listRowState =
                    getRuntimeContext()
                            .getListState(new ListStateDescriptor<>(LIST_ROW_STATE, RECORD_TYPE));
            mapRowRowState =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            MAP_ROW_ROW_STATE, MAP_KEY_TYPE, RECORD_TYPE));
            pojoState =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            POJO_STATE, TypeInformation.of(TestOrderPojo.class)));
        }

        @Override
        public Integer map(Integer value) throws Exception {
            int key = value % PARALLELISM;
            String region = "region-" + key;
            RowData record = GenericRowData.of((long) value, StringData.fromString(region));
            RowData mapKey = GenericRowData.of((long) value, StringData.fromString(region));
            RowData mapValue = GenericRowData.of((long) value * 10, StringData.fromString(region));

            valueRowState.update(record);
            listRowState.add(record);
            mapRowRowState.put(mapKey, mapValue);
            pojoState.update(new TestOrderPojo(value, region));
            return value;
        }
    }

    /** A simple POJO with two fields, used to exercise the non-portable serializer capture path. */
    public static final class TestOrderPojo implements Serializable {
        private static final long serialVersionUID = 1L;

        public int id;
        public String name;

        public TestOrderPojo() {}

        TestOrderPojo(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestOrderPojo)) {
                return false;
            }
            TestOrderPojo that = (TestOrderPojo) o;
            return id == that.id && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }

        @Override
        public String toString() {
            return id + "," + name;
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
    }
}
