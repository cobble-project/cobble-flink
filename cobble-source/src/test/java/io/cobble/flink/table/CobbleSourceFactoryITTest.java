package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Factory wiring tests proving source-kind detection drives validation: sink roots keep the
 * existing behavior; state roots resolve and validate the DDL during planning, then fail only when
 * the (unimplemented) scan runtime is requested.
 */
class CobbleSourceFactoryITTest {

    @TempDir private Path tempDir;

    @Test
    void sinkDdlWithoutSourceKindStillCreatesSinkSource() throws Exception {
        Path root = sinkRoot("sink-default");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(sinkDdl("t_sink_default", root, null));

        assertDoesNotThrow(() -> tableEnv.explainSql("SELECT * FROM t_sink_default"));
    }

    @Test
    void sinkDdlWithExplicitSinkKindCreatesSinkSource() throws Exception {
        Path root = sinkRoot("sink-explicit");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(sinkDdl("t_sink_explicit", root, "sink"));

        assertDoesNotThrow(() -> tableEnv.explainSql("SELECT * FROM t_sink_explicit"));
    }

    @Test
    void sinkDdlWithExplicitAutoKindCreatesSinkSource() throws Exception {
        Path root = sinkRoot("sink-auto");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(sinkDdl("t_sink_auto", root, "auto"));

        assertDoesNotThrow(() -> tableEnv.explainSql("SELECT * FROM t_sink_auto"));
    }

    @Test
    void autoSinkShapedDdlWithoutInspectSchemaStillCreatesSinkSource() throws Exception {
        // A sink table written without an inspect-schema sidecar: only snapshot/CURRENT exists.
        // The sink-shaped DDL (PK + non-PK column) must keep working under default 'auto'.
        Path root = ambiguousRoot("sink-no-sidecar");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(sinkDdl("t_sink_no_sidecar", root, null));

        assertDoesNotThrow(() -> tableEnv.explainSql("SELECT * FROM t_sink_no_sidecar"));
    }

    @Test
    void sidecarBackedSinkDdlPlansWhenDdlMatchesPersistedSchema() throws Exception {
        Path root = sinkRootWithSidecar("sink-sidecar-valid", "name", "VARCHAR(2147483647)");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(sinkDdl("t_sink_sidecar_valid", root, "sink"));

        assertDoesNotThrow(() -> tableEnv.explainSql("SELECT * FROM t_sink_sidecar_valid"));
    }

    @Test
    void sidecarBackedSinkDdlPlansWhenPrimaryKeyIsNotFirstPhysicalColumn() throws Exception {
        Path root =
                sinkRootWithSidecar(
                        "sink-sidecar-non-leading-pk",
                        "id",
                        1,
                        "name",
                        "VARCHAR(2147483647)",
                        0,
                        0);
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(
                "CREATE TABLE t_sink_sidecar_non_leading_pk ("
                        + " name STRING,"
                        + " id BIGINT,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'source.kind' = 'sink',"
                        + " 'path' = '"
                        + escape(root)
                        + "'"
                        + ")");

        assertDoesNotThrow(
                () -> tableEnv.explainSql("SELECT * FROM t_sink_sidecar_non_leading_pk"));
    }

    @Test
    void sidecarBackedSinkDdlWithReorderedColumnsFailsDuringPlanning() throws Exception {
        Path root = sinkRootWithSidecar("sink-sidecar-reorder", "name", "VARCHAR(2147483647)");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(
                "CREATE TABLE t_sink_sidecar_reorder ("
                        + " name STRING,"
                        + " id BIGINT,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'source.kind' = 'sink',"
                        + " 'path' = '"
                        + escape(root)
                        + "'"
                        + ")");

        Exception error =
                assertThrows(
                        Exception.class,
                        () -> tableEnv.explainSql("SELECT * FROM t_sink_sidecar_reorder"));
        assertTrue(
                messageChain(error).contains("inspect schema expects 'id'"),
                "expected sidecar reorder message but got: " + messageChain(error));
    }

    @Test
    void sidecarBackedSinkDdlWithWrongPrimaryKeyFailsDuringPlanning() throws Exception {
        Path root = sinkRootWithSidecar("sink-sidecar-wrong-pk", "name", "VARCHAR(2147483647)");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(
                "CREATE TABLE t_sink_sidecar_wrong_pk ("
                        + " id BIGINT,"
                        + " name STRING,"
                        + " PRIMARY KEY (name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'source.kind' = 'sink',"
                        + " 'path' = '"
                        + escape(root)
                        + "'"
                        + ")");

        Exception error =
                assertThrows(
                        Exception.class,
                        () -> tableEnv.explainSql("SELECT * FROM t_sink_sidecar_wrong_pk"));
        assertTrue(
                messageChain(error).contains("PRIMARY KEY column at position 0"),
                "expected sidecar PK-order message but got: " + messageChain(error));
    }

    @Test
    void sidecarLessSinkDdlStillPlansWithDdlDerivedSchema() throws Exception {
        Path root = ambiguousRoot("sink-no-sidecar-explicit");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(sinkDdl("t_sink_no_sidecar_explicit", root, "sink"));

        assertDoesNotThrow(() -> tableEnv.explainSql("SELECT * FROM t_sink_no_sidecar_explicit"));
    }

    @Test
    void sinkDdlWithStateOptionIsRejected() throws Exception {
        Path root = sinkRoot("sink-state-option");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(
                "CREATE TABLE t_sink_state_opt ("
                        + " id BIGINT,"
                        + " name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'bucket' = '2',"
                        + " 'state.name' = 'orders',"
                        + " 'path' = '"
                        + escape(root)
                        + "'"
                        + ")");

        Exception error =
                assertThrows(
                        Exception.class,
                        () -> tableEnv.explainSql("SELECT * FROM t_sink_state_opt"));
        assertTrue(
                messageChain(error).contains("'state.name' is only valid when source.kind='state'"),
                "expected state-option rejection but got: " + messageChain(error));
    }

    @Test
    void validStateDdlPlans() throws Exception {
        Path root = stateCheckpointRoot("state-valid");
        StreamTableEnvironment tableEnv = newTableEnv();
        // No PRIMARY KEY: state mode must not require a sink primary key.
        tableEnv.executeSql(
                "CREATE TABLE t_state_valid ("
                        + " `key` INT,"
                        + " `value` INT"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'path' = '"
                        + escape(root)
                        + "',"
                        + " 'source.kind' = 'state',"
                        + " 'state.name' = 'orders'"
                        + ")");

        assertDoesNotThrow(() -> tableEnv.explainSql("SELECT * FROM t_state_valid"));
    }

    @Test
    void stateScanWithPrimaryKeyPlans() throws Exception {
        Path root = stateCheckpointRoot("state-pk-scan");
        StreamTableEnvironment tableEnv = newTableEnv();
        // A DDL PRIMARY KEY is now an optional lookup contract; scan must still plan.
        tableEnv.executeSql(
                "CREATE TABLE t_state_pk_scan ("
                        + " `key` INT,"
                        + " `value` INT,"
                        + " PRIMARY KEY (`key`) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'path' = '"
                        + escape(root)
                        + "',"
                        + " 'source.kind' = 'state',"
                        + " 'state.name' = 'orders'"
                        + ")");

        assertDoesNotThrow(() -> tableEnv.explainSql("SELECT * FROM t_state_pk_scan"));
    }

    @Test
    void stateScanWithPrimaryKeyStillRejectsReorderedPhysicalColumns() throws Exception {
        Path root = stateCheckpointRoot("state-pk-reorder");
        StreamTableEnvironment tableEnv = newTableEnv();
        // Even with a correct PK, physical columns must stay in semantic order.
        tableEnv.executeSql(
                "CREATE TABLE t_state_pk_reorder ("
                        + " `value` INT,"
                        + " `key` INT,"
                        + " PRIMARY KEY (`key`) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'path' = '"
                        + escape(root)
                        + "',"
                        + " 'source.kind' = 'state',"
                        + " 'state.name' = 'orders'"
                        + ")");

        Exception error =
                assertThrows(
                        Exception.class,
                        () -> tableEnv.explainSql("SELECT * FROM t_state_pk_reorder"));
        assertTrue(
                messageChain(error).contains("column at position 0")
                        && messageChain(error).contains("expects 'key'"),
                "expected position-order message but got: " + messageChain(error));
    }

    @Test
    void autoDetectedStateWithValidDdlReachesStateBranch() throws Exception {
        Path root = stateCheckpointRoot("state-auto");
        StreamTableEnvironment tableEnv = newTableEnv();
        // No source.kind: the checkpoint-root layout must be auto-detected as state.
        tableEnv.executeSql(
                "CREATE TABLE t_state_auto ("
                        + " `key` INT,"
                        + " `value` INT"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'path' = '"
                        + escape(root)
                        + "',"
                        + " 'state.name' = 'orders'"
                        + ")");

        assertDoesNotThrow(() -> tableEnv.explainSql("SELECT * FROM t_state_auto"));
    }

    @Test
    void invalidStateDdlFailsDuringPlanning() throws Exception {
        Path root = stateCheckpointRoot("state-invalid");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(
                "CREATE TABLE t_state_invalid ("
                        + " key INT,"
                        + " wrong INT"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'path' = '"
                        + escape(root)
                        + "',"
                        + " 'source.kind' = 'state',"
                        + " 'state.name' = 'orders'"
                        + ")");

        Exception error =
                assertThrows(
                        Exception.class,
                        () -> tableEnv.explainSql("SELECT * FROM t_state_invalid"));
        assertTrue(
                messageChain(error).contains("column at position 1")
                        && messageChain(error).contains("expects 'value'"),
                "expected schema-validation message but got: " + messageChain(error));
    }

    @Test
    void stateNameMissingFailsDuringPlanning() throws Exception {
        Path root = stateCheckpointRoot("state-no-name");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(
                "CREATE TABLE t_state_no_name ("
                        + " `key` INT,"
                        + " `value` INT"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'path' = '"
                        + escape(root)
                        + "',"
                        + " 'source.kind' = 'state'"
                        + ")");

        Exception error =
                assertThrows(
                        Exception.class,
                        () -> tableEnv.explainSql("SELECT * FROM t_state_no_name"));
        assertTrue(
                messageChain(error).contains("'state.name' is required"),
                "expected required-state-name message but got: " + messageChain(error));
    }

    @Test
    void stateDdlWithReorderedColumnsFailsDuringPlanning() throws Exception {
        Path root = stateCheckpointRoot("state-reorder");
        StreamTableEnvironment tableEnv = newTableEnv();
        // Semantic output order is key, value. Swapping them must fail at planning time: the
        // runtime emits in semantic order while Flink reads in DDL order, so a reordered DDL
        // would silently swap columns.
        tableEnv.executeSql(
                "CREATE TABLE t_state_reorder ("
                        + " `value` INT,"
                        + " `key` INT"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'path' = '"
                        + escape(root)
                        + "',"
                        + " 'source.kind' = 'state',"
                        + " 'state.name' = 'orders'"
                        + ")");

        Exception error =
                assertThrows(
                        Exception.class,
                        () -> tableEnv.explainSql("SELECT * FROM t_state_reorder"));
        assertTrue(
                messageChain(error).contains("column at position 0")
                        && messageChain(error).contains("expects 'key'"),
                "expected position-order message but got: " + messageChain(error));
    }

    @Test
    void stateStreamingScanModeFailsClearly() throws Exception {
        Path root = stateCheckpointRoot("state-streaming");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(
                "CREATE TABLE t_state_streaming ("
                        + " `key` INT,"
                        + " `value` INT"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'path' = '"
                        + escape(root)
                        + "',"
                        + " 'source.kind' = 'state',"
                        + " 'scan.mode' = 'streaming',"
                        + " 'state.name' = 'orders'"
                        + ")");

        Exception error =
                assertThrows(
                        Exception.class,
                        () -> tableEnv.explainSql("SELECT * FROM t_state_streaming"));
        assertTrue(
                messageChain(error).contains("supports only scan.mode='batch'"),
                "expected streaming-unsupported message but got: " + messageChain(error));
    }

    @Test
    void stateLookupRuntimeFailsClearly() {
        // No DDL PRIMARY KEY => contract absent => lookup must fail with the "requires PRIMARY KEY"
        // message, not the not-implemented boundary.
        StateSourceConfig config =
                new StateSourceConfig(
                        "file:///tmp/checkpoints",
                        StateSourceConfig.Layout.CHECKPOINT_ROOT,
                        "operator-1",
                        "orders",
                        "value",
                        "latest",
                        "batch",
                        7L,
                        -1,
                        0L,
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)));
        CobbleStateDynamicTableSource source =
                new CobbleStateDynamicTableSource(config, "default_catalog.default_database.t");

        Exception error =
                assertThrows(Exception.class, () -> source.getLookupRuntimeProvider(null));
        assertTrue(
                messageChain(error).contains("requires a DDL PRIMARY KEY"),
                "expected requires-PK message but got: " + messageChain(error));
    }

    @Test
    void stateLookupWithValuePrimaryKeyReturnsProvider() {
        // Contract present (value state PK = key), LookupContext provides the single key column at
        // physical position 0 => validation passes => provider returns a lookup function.
        StateSourceConfig config =
                stateConfigWithContract(
                        "orders",
                        "value",
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        new int[] {0});
        CobbleStateDynamicTableSource source =
                new CobbleStateDynamicTableSource(config, "default_catalog.default_database.t");

        LookupTableSource.LookupRuntimeProvider provider =
                assertDoesNotThrow(
                        () -> source.getLookupRuntimeProvider(lookupContext(new int[] {0})));
        assertNotNull(provider, "expected a non-null LookupRuntimeProvider for value state lookup");
    }

    @Test
    void stateLookupWithMapPrimaryKeyReturnsProvider() {
        // Map state with a full-entry PK (key + map_key) => provider returns a lookup function.
        StateSourceConfig config =
                stateConfigWithContract(
                        "orders",
                        "map",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0),
                                new StateSourceField(
                                        "map_value", "INT", StateSourceField.Group.MAP_VALUE, 0)),
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0)),
                        new int[] {0, 1});
        CobbleStateDynamicTableSource source =
                new CobbleStateDynamicTableSource(config, "default_catalog.default_database.t");

        LookupTableSource.LookupRuntimeProvider provider =
                assertDoesNotThrow(
                        () ->
                                source.getLookupRuntimeProvider(
                                        lookupContext(new int[] {0}, new int[] {1})));
        assertNotNull(provider, "expected a non-null LookupRuntimeProvider for map state lookup");
    }

    @Test
    void stateLookupWithPartialLookupContextFailsBeforeNotImplemented() {
        // Contract requires one key column at position 0, but the planner provides a different
        // position => must fail validation, not reach the not-implemented boundary.
        StateSourceConfig config =
                stateConfigWithContract(
                        "orders",
                        "value",
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        new int[] {0});
        CobbleStateDynamicTableSource source =
                new CobbleStateDynamicTableSource(config, "default_catalog.default_database.t");

        Exception error =
                assertThrows(
                        Exception.class,
                        () -> source.getLookupRuntimeProvider(lookupContext(new int[] {1})));
        assertTrue(
                messageChain(error).contains("lookup key at position 0")
                        && messageChain(error).contains("requires physical column 0"),
                "expected lookup-key mismatch message but got: " + messageChain(error));
    }

    @Test
    void stateLookupWithWrongKeyCountFailsBeforeNotImplemented() {
        StateSourceConfig config =
                stateConfigWithContract(
                        "orders",
                        "value",
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        new int[] {0});
        CobbleStateDynamicTableSource source =
                new CobbleStateDynamicTableSource(config, "default_catalog.default_database.t");

        Exception error =
                assertThrows(
                        Exception.class,
                        () ->
                                source.getLookupRuntimeProvider(
                                        lookupContext(new int[] {0}, new int[] {1})));
        assertTrue(
                messageChain(error).contains("equality conditions for all 1 PRIMARY KEY column(s)"),
                "expected key-count mismatch message but got: " + messageChain(error));
    }

    @Test
    void stateLookupForListFailsAsUnsupportedEvenWithContract() {
        StateSourceConfig config =
                stateConfigWithContract(
                        "orders",
                        "list",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "value", "INT", StateSourceField.Group.LIST_ELEMENT, 0)),
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        new int[] {0});
        CobbleStateDynamicTableSource source =
                new CobbleStateDynamicTableSource(config, "default_catalog.default_database.t");

        Exception error =
                assertThrows(
                        Exception.class,
                        () -> source.getLookupRuntimeProvider(lookupContext(new int[] {0})));
        assertTrue(
                messageChain(error).contains("list lookup is not supported"),
                "expected list-lookup unsupported message but got: " + messageChain(error));
    }

    @Test
    void stateLookupForTimerFailsAsUnsupported() {
        StateSourceConfig config =
                stateConfigWithContract(
                        "orders",
                        "timer",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "timestamp",
                                        "BIGINT",
                                        StateSourceField.Group.TIMER_TIMESTAMP,
                                        0)),
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        new int[] {0});
        CobbleStateDynamicTableSource source =
                new CobbleStateDynamicTableSource(config, "default_catalog.default_database.t");

        Exception error =
                assertThrows(
                        Exception.class,
                        () -> source.getLookupRuntimeProvider(lookupContext(new int[] {0})));
        assertTrue(
                messageChain(error).contains("timer lookup is not supported"),
                "expected timer-lookup unsupported message but got: " + messageChain(error));
    }

    @Test
    void invalidSourceKindFailsDuringPlanning() throws Exception {
        Path root = sinkRoot("invalid-kind");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(sinkDdl("t_invalid_kind", root, "bogus"));

        Exception error =
                assertThrows(
                        Exception.class, () -> tableEnv.explainSql("SELECT * FROM t_invalid_kind"));
        assertTrue(
                messageChain(error).contains("auto, sink, state, raw"),
                "expected valid-values message but got: " + messageChain(error));
    }

    // ------------------------------------------------------------------------------------------
    //  Raw source factory tests
    // ------------------------------------------------------------------------------------------

    @Test
    void rawSourcePlansOnTableRoot() throws Exception {
        Path root = ambiguousRoot("raw-planning");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(rawDdl("t_raw_plan", root, "0,1"));

        assertDoesNotThrow(() -> tableEnv.explainSql("SELECT * FROM t_raw_plan"));
    }

    @Test
    void rawSourceMissingColumnsFailsDuringPlanning() throws Exception {
        Path root = ambiguousRoot("raw-no-columns");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(rawDdl("t_raw_no_cols", root, null));

        Exception error =
                assertThrows(
                        Exception.class, () -> tableEnv.explainSql("SELECT * FROM t_raw_no_cols"));
        assertTrue(
                messageChain(error).contains("'raw.columns' is required"),
                "expected required message but got: " + messageChain(error));
    }

    @Test
    void sinkSourceWithRawColumnsFailsDuringPlanning() throws Exception {
        Path root = sinkRoot("sink-with-raw-columns");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(
                "CREATE TABLE t_sink_raw_opt ("
                        + " id BIGINT,"
                        + " name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'source.kind' = 'sink',"
                        + " 'bucket' = '2',"
                        + " 'raw.columns' = '0,1',"
                        + " 'path' = '"
                        + escape(root)
                        + "'"
                        + ")");

        Exception error =
                assertThrows(
                        Exception.class, () -> tableEnv.explainSql("SELECT * FROM t_sink_raw_opt"));
        assertTrue(
                messageChain(error).contains("'raw.columns' is only valid when source.kind='raw'"),
                "expected raw-option rejection but got: " + messageChain(error));
    }

    @Test
    void stateSourceWithRawColumnsFailsDuringPlanning() throws Exception {
        Path root = stateCheckpointRoot("state-with-raw-columns");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(
                "CREATE TABLE t_state_raw_opt ("
                        + " `key` INT,"
                        + " `value` INT"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'path' = '"
                        + escape(root)
                        + "',"
                        + " 'source.kind' = 'state',"
                        + " 'state.name' = 'orders',"
                        + " 'raw.columns' = '0'"
                        + ")");

        Exception error =
                assertThrows(
                        Exception.class,
                        () -> tableEnv.explainSql("SELECT * FROM t_state_raw_opt"));
        assertTrue(
                messageChain(error).contains("'raw.columns' is only valid when source.kind='raw'"),
                "expected raw-option rejection but got: " + messageChain(error));
    }

    @Test
    void rawSourceRejectsPrimaryKey() throws Exception {
        Path root = ambiguousRoot("raw-with-pk");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(
                "CREATE TABLE t_raw_pk ("
                        + " `key` BYTES,"
                        + " `columns` ARRAY<BYTES>,"
                        + " PRIMARY KEY (`key`) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'source.kind' = 'raw',"
                        + " 'raw.columns' = '0',"
                        + " 'path' = '"
                        + escape(root)
                        + "'"
                        + ")");

        Exception error =
                assertThrows(Exception.class, () -> tableEnv.explainSql("SELECT * FROM t_raw_pk"));
        assertTrue(
                messageChain(error).contains("does not support a PRIMARY KEY"),
                "expected PK rejection but got: " + messageChain(error));
    }

    @Test
    void rawSourceRejectsWrongColumnCount() throws Exception {
        Path root = ambiguousRoot("raw-wrong-cols");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(
                "CREATE TABLE t_raw_wrong_cols ("
                        + " `key` BYTES"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'source.kind' = 'raw',"
                        + " 'raw.columns' = '0',"
                        + " 'path' = '"
                        + escape(root)
                        + "'"
                        + ")");

        Exception error =
                assertThrows(
                        Exception.class,
                        () -> tableEnv.explainSql("SELECT * FROM t_raw_wrong_cols"));
        assertTrue(
                messageChain(error).contains("two columns"),
                "expected column-count message but got: " + messageChain(error));
    }

    // ------------------------------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------------------------------

    private static StreamTableEnvironment newTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return StreamTableEnvironment.create(env);
    }

    /** Builds a resolved state config carrying an explicit present lookup contract. */
    private static StateSourceConfig stateConfigWithContract(
            String stateName,
            String stateKind,
            List<StateSourceField> outputFields,
            List<StateSourceField> requiredFields,
            int[] requiredPositions) {
        StateSourceLookupKeyContract contract =
                StateSourceLookupKeyContract.present(requiredFields, requiredPositions);
        return new StateSourceConfig(
                "file:///tmp/checkpoints",
                StateSourceConfig.Layout.CHECKPOINT_ROOT,
                "operator-1",
                stateName,
                stateKind,
                "latest",
                "batch",
                7L,
                -1,
                0L,
                outputFields,
                contract);
    }

    /** Minimal {@link LookupTableSource.LookupContext} exposing explicit physical key positions. */
    private static LookupTableSource.LookupContext lookupContext(int[]... keys) {
        return new TestLookupContext(keys);
    }

    private static final class TestLookupContext implements LookupTableSource.LookupContext {
        private final int[][] keys;

        private TestLookupContext(int[][] keys) {
            this.keys = keys;
        }

        @Override
        public int[][] getKeys() {
            return keys;
        }

        @Override
        public <T> org.apache.flink.api.common.typeinfo.TypeInformation<T> createTypeInformation(
                org.apache.flink.table.types.DataType dataType) {
            throw new UnsupportedOperationException("not used by lookup-context tests");
        }

        @Override
        public <T> org.apache.flink.api.common.typeinfo.TypeInformation<T> createTypeInformation(
                org.apache.flink.table.types.logical.LogicalType logicalType) {
            throw new UnsupportedOperationException("not used by lookup-context tests");
        }

        @Override
        public DynamicTableSource.DataStructureConverter createDataStructureConverter(
                org.apache.flink.table.types.DataType dataType) {
            throw new UnsupportedOperationException("not used by lookup-context tests");
        }
    }

    private static String sinkDdl(String tableName, Path root, String sourceKind) {
        String sourceKindClause =
                sourceKind == null ? "" : " 'source.kind' = '" + sourceKind + "',";
        return "CREATE TABLE "
                + tableName
                + " ("
                + " id BIGINT,"
                + " name STRING,"
                + " PRIMARY KEY (id) NOT ENFORCED"
                + ") WITH ("
                + " 'connector' = 'cobble',"
                + sourceKindClause
                + " 'bucket' = '2',"
                + " 'path' = '"
                + escape(root)
                + "'"
                + ")";
    }

    private static String rawDdl(String tableName, Path root, String rawColumns) {
        String rawColumnsClause =
                rawColumns == null ? "" : " 'raw.columns' = '" + rawColumns + "',";
        return "CREATE TABLE "
                + tableName
                + " ("
                + " `key` BYTES,"
                + " `columns` ARRAY<BYTES>"
                + ") WITH ("
                + " 'connector' = 'cobble',"
                + " 'source.kind' = 'raw',"
                + rawColumnsClause
                + " 'path' = '"
                + escape(root)
                + "'"
                + ")";
    }

    private Path sinkRoot(String name) throws Exception {
        Path root = tempDir.resolve(name);
        byte[] blob = new SinkInspectSchemaStore(null).toBytes();
        String hash = InspectSchemaRegistryLayout.sha256(blob);
        write(
                root.resolve("inspect-schema")
                        .resolve("blobs")
                        .resolve(InspectSchemaRegistryLayout.blobFileName(hash)),
                blob);
        write(root.resolve("snapshot").resolve("CURRENT"), new byte[] {1});
        return root;
    }

    private Path sinkRootWithSidecar(String name, String valueName, String valueLogicalType)
            throws Exception {
        return sinkRootWithSidecar(name, "id", 0, valueName, valueLogicalType, 1, 0);
    }

    private Path sinkRootWithSidecar(
            String name,
            String keyName,
            int keyRowIndex,
            String valueName,
            String valueLogicalType,
            int valueRowIndex,
            int valueStructuredColumnIndex)
            throws Exception {
        Path root = tempDir.resolve(name);
        SinkInspectSchemaStore store =
                SinkInspectSchemaStore.of(
                        new SinkInspectSchema(
                                Collections.singletonList(
                                        SinkInspectField.key(keyName, "BIGINT", keyRowIndex, -1)),
                                Collections.singletonList(
                                        SinkInspectField.value(
                                                valueName,
                                                valueLogicalType,
                                                valueRowIndex,
                                                valueStructuredColumnIndex))));
        byte[] bytes = store.toBytes();
        String hash = InspectSchemaRegistryLayout.sha256(bytes);
        Path base = root.resolve("inspect-schema");
        write(base.resolve("blobs").resolve(InspectSchemaRegistryLayout.blobFileName(hash)), bytes);
        write(
                base.resolve("events").resolve(InspectSchemaRegistryLayout.eventFileName(7L, hash)),
                new byte[0]);
        write(root.resolve("snapshot").resolve("CURRENT"), new byte[] {1});
        return root;
    }

    private Path ambiguousRoot(String name) throws Exception {
        Path root = tempDir.resolve(name);
        // Sink data without an inspect-schema sidecar: only the weak snapshot/CURRENT signal.
        write(root.resolve("snapshot").resolve("CURRENT"), new byte[] {1});
        return root;
    }

    /**
     * Builds a checkpoint root that the detector recognizes as state (chk-* with _metadata and a
     * Cobble manifest) and that also carries an inspect-schema registry holding a single value
     * state named {@code orders} (key INT, value INT, void namespace).
     */
    private Path stateCheckpointRoot(String name) throws Exception {
        Path root = tempDir.resolve(name);
        Path chk = root.resolve("chk-7");
        write(chk.resolve("_metadata"), new byte[] {0});
        write(chk.resolve("COBBLE-SNAPSHOT-operator-1-MANIFEST"), new byte[] {0});

        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "orders",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        StateInspectSemanticSchema semantic =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("INT"));
        StateInspectSchemaStore store =
                new StateInspectSchemaStore(
                        Collections.singletonList(schema),
                        Collections.singletonMap("orders", semantic));

        byte[] bytes = store.toBytes();
        String hash = InspectSchemaRegistryLayout.sha256(bytes);
        Path base = root.resolve("cobble").resolve("operator-1").resolve("inspect-schema");
        write(base.resolve("blobs").resolve(InspectSchemaRegistryLayout.blobFileName(hash)), bytes);
        write(
                base.resolve("events").resolve(InspectSchemaRegistryLayout.eventFileName(7L, hash)),
                new byte[0]);
        return root;
    }

    private static void write(Path path, byte[] bytes) throws Exception {
        Files.createDirectories(path.getParent());
        Files.write(path, bytes);
    }

    private static String escape(Path path) {
        return path.toUri().toString();
    }

    private static String messageChain(Throwable error) {
        StringBuilder builder = new StringBuilder();
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current.getMessage() != null) {
                builder.append(current.getMessage()).append('\n');
            }
            if (current.getCause() == current) {
                break;
            }
        }
        return builder.toString();
    }
}
