package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Factory wiring tests proving that source-kind detection drives validation: sink roots keep the
 * existing behavior, state roots fail in the factory, and invalid options fail during planning.
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
    void stateRootWithStateKindFailsWithNotImplementedAndIgnoresMissingPrimaryKey()
            throws Exception {
        Path root = checkpointRoot("state-no-pk");
        StreamTableEnvironment tableEnv = newTableEnv();
        // No PRIMARY KEY declared: state mode must not require a sink primary key.
        tableEnv.executeSql(
                "CREATE TABLE t_state ("
                        + " k STRING,"
                        + " v STRING"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'path' = '"
                        + escape(root)
                        + "',"
                        + " 'source.kind' = 'state'"
                        + ")");

        Exception error =
                assertThrows(Exception.class, () -> tableEnv.explainSql("SELECT * FROM t_state"));
        assertTrue(
                messageChain(error).contains("state source runtime is not implemented"),
                "expected not-implemented message but got: " + messageChain(error));
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

    private Path checkpointRoot(String name) throws Exception {
        Path root = tempDir.resolve(name);
        Path chk = root.resolve("chk-7");
        write(chk.resolve("_metadata"), new byte[] {0});
        write(chk.resolve("COBBLE-SNAPSHOT-operator-1-MANIFEST"), new byte[] {0});
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
