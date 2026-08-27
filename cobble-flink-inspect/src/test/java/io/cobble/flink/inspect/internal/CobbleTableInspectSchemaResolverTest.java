package io.cobble.flink.inspect.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.GlobalSnapshot;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.table.Value;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

class CobbleTableInspectSchemaResolverTest {
    @TempDir private Path tempDir;

    @Test
    void resolvesPersistedNativeTableSchema() throws Exception {
        Path root = tempDir.resolve("table");
        GlobalSnapshot snapshot =
                CobbleTableInspectTestData.write(
                        root,
                        1,
                        7L,
                        CobbleTableInspectTestData.stringKeyValueSchema(),
                        Collections.singletonList(
                                Arrays.asList(Value.string("id"), Value.string("value"))));

        TableInspectSchema schema =
                CobbleTableInspectSchemaResolver.resolve(
                        root.toString(), snapshot, CobbleConnectorStorageOptions.empty());

        assertEquals("id", schema.keyFields.get(0).name());
        assertEquals("payload", schema.valueFields.get(0).name());
    }

    @Test
    void rejectsRecognizedLegacySinkFormat() throws Exception {
        Path root = tempDir.resolve("legacy");
        Path event = root.resolve("inspect-schema/events/CSNK-1-deadbeef");
        Files.createDirectories(event.getParent());
        Files.write(event, new byte[0]);
        GlobalSnapshot snapshot = new GlobalSnapshot();
        snapshot.totalBuckets = 1;
        snapshot.shardSnapshots = Collections.emptyList();

        InspectInputException error =
                assertThrows(
                        InspectInputException.class,
                        () ->
                                CobbleTableInspectSchemaResolver.resolve(
                                        root.toString(),
                                        snapshot,
                                        CobbleConnectorStorageOptions.empty()));
        assertTrue(error.getMessage().contains("pre-Table Cobble sink format"));
    }
}
