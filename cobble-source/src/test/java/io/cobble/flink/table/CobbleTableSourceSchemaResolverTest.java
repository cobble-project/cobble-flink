package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

class CobbleTableSourceSchemaResolverTest {

    @TempDir private Path tempDir;

    @Test
    void matchingNativeTableSchemaIsAccepted() throws Exception {
        Path root = tempDir.resolve("matching");
        CobbleTableSourceTestData.write(
                root,
                2,
                CobbleTableSourceTestData.idNameSchema(),
                CobbleTableSourceTestData.idNameRows());

        assertDoesNotThrow(() -> CobbleTableSourceSchemaResolver.validate(config(root)));
    }

    @Test
    void mismatchedDdlSchemaIsRejected() throws Exception {
        Path root = tempDir.resolve("mismatch");
        CobbleTableSourceTestData.write(
                root,
                2,
                CobbleTableSourceTestData.idNameSchema(),
                CobbleTableSourceTestData.idNameRows());
        CobbleDynamicTableSource.SerializableConfig mismatch =
                new CobbleDynamicTableSource.SerializableConfig(
                        root.toUri().toString(),
                        2,
                        "latest",
                        "batch",
                        50L,
                        0L,
                        Collections.singletonList(
                                new CobbleDynamicTableSource.SerializableField(
                                        "name", "VARCHAR(2147483647)", 0, -1)),
                        Collections.singletonList(
                                new CobbleDynamicTableSource.SerializableField(
                                        "id", "BIGINT", 1, 0)));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () -> CobbleTableSourceSchemaResolver.validate(mismatch));
        assertTrue(error.getMessage().contains("does not match"));
    }

    @Test
    void missingCommittedSnapshotIsRejected() {
        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                CobbleTableSourceSchemaResolver.validate(
                                        config(tempDir.resolve("missing"))));
        assertTrue(messageChain(error).contains("could not find a committed table snapshot"));
    }

    @Test
    void legacySinkSidecarIsRejectedClearly() throws Exception {
        Path root = tempDir.resolve("legacy");
        Path event = root.resolve("inspect-schema/events/CSNK-1-deadbeef");
        Files.createDirectories(event.getParent());
        Files.write(event, new byte[0]);

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () -> CobbleTableSourceSchemaResolver.validate(config(root)));
        assertTrue(error.getMessage().contains("pre-Table Cobble sink format"));
    }

    private static CobbleDynamicTableSource.SerializableConfig config(Path root) {
        return new CobbleDynamicTableSource.SerializableConfig(
                root.toUri().toString(),
                2,
                "latest",
                "batch",
                50L,
                0L,
                Collections.singletonList(
                        new CobbleDynamicTableSource.SerializableField("id", "BIGINT", 0, -1)),
                Collections.singletonList(
                        new CobbleDynamicTableSource.SerializableField(
                                "name", "VARCHAR(2147483647)", 1, 0)));
    }

    private static String messageChain(Throwable error) {
        StringBuilder message = new StringBuilder();
        Throwable current = error;
        while (current != null) {
            if (current.getMessage() != null) {
                message.append(current.getMessage()).append('\n');
            }
            current = current.getCause();
        }
        return message.toString();
    }
}
