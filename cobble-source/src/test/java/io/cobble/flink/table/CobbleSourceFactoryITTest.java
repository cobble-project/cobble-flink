package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

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
    void validStateDdlPlansAndFailsOnlyAtRuntime() throws Exception {
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

        Exception error =
                assertThrows(
                        Exception.class, () -> tableEnv.explainSql("SELECT * FROM t_state_valid"));
        assertTrue(
                messageChain(error).contains("state source runtime is not implemented yet"),
                "expected not-implemented message but got: " + messageChain(error));
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

        Exception error =
                assertThrows(
                        Exception.class, () -> tableEnv.explainSql("SELECT * FROM t_state_auto"));
        assertTrue(
                messageChain(error).contains("state source runtime is not implemented yet"),
                "expected not-implemented message but got: " + messageChain(error));
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
    void invalidSourceKindFailsDuringPlanning() throws Exception {
        Path root = sinkRoot("invalid-kind");
        StreamTableEnvironment tableEnv = newTableEnv();
        tableEnv.executeSql(sinkDdl("t_invalid_kind", root, "bogus"));

        Exception error =
                assertThrows(
                        Exception.class, () -> tableEnv.explainSql("SELECT * FROM t_invalid_kind"));
        assertTrue(
                messageChain(error).contains("auto, sink, state"),
                "expected valid-values message but got: " + messageChain(error));
    }

    // ------------------------------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------------------------------

    private static StreamTableEnvironment newTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return StreamTableEnvironment.create(env);
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
