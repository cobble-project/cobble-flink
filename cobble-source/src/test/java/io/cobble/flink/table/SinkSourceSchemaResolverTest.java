package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Tests for sink source sidecar schema resolution and DDL validation. */
class SinkSourceSchemaResolverTest {

    @TempDir private Path tempDir;

    @Test
    void absentSidecarReturnsAbsent() {
        SinkSourceResolvedSchema resolved =
                SinkSourceSchemaResolver.resolve(uri(tempDir), "latest", ddl("id", "name"));

        assertFalse(resolved.present());
    }

    @Test
    void latestEventSelectsHighestSnapshot() throws Exception {
        writeRegistry(tempDir, 10L, store("name", "VARCHAR(2147483647)", 1, 0));
        writeRegistry(tempDir, 20L, store("score", "INT", 1, 0));

        SinkSourceResolvedSchema resolved =
                SinkSourceSchemaResolver.resolve(
                        uri(tempDir),
                        "latest",
                        schema(
                                Arrays.asList(
                                        Column.physical("id", DataTypes.BIGINT()),
                                        Column.physical("score", DataTypes.INT())),
                                Collections.singletonList("id")));

        assertTrue(resolved.present());
        assertEquals(20L, resolved.schemaSnapshotId());
        assertEquals("score", resolved.valueFields().get(0).name);
        assertEquals("INT", resolved.valueFields().get(0).logicalType);
    }

    @Test
    void numericCheckpointSelectsLatestEventAtOrBeforeIt() throws Exception {
        writeRegistry(tempDir, 10L, store("name", "VARCHAR(2147483647)", 1, 0));
        writeRegistry(tempDir, 20L, store("score", "INT", 1, 0));

        SinkSourceResolvedSchema resolved =
                SinkSourceSchemaResolver.resolve(uri(tempDir), "15", ddl("id", "name"));

        assertTrue(resolved.present());
        assertEquals(10L, resolved.schemaSnapshotId());
        assertEquals("name", resolved.valueFields().get(0).name);
    }

    @Test
    void numericCheckpointBeforeFirstEventFails() throws Exception {
        writeRegistry(tempDir, 10L, store("name", "VARCHAR(2147483647)", 1, 0));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                SinkSourceSchemaResolver.resolve(
                                        uri(tempDir), "9", ddl("id", "name")));
        assertTrue(error.getMessage().contains("snapshot id <= 9"), error.getMessage());
    }

    @Test
    void malformedEventNameIsIgnored() throws Exception {
        Path events = tempDir.resolve("inspect-schema").resolve("events");
        Files.createDirectories(events);
        Files.write(events.resolve("SCHEMA-not-a-valid-event.ref"), new byte[0]);

        SinkSourceResolvedSchema resolved =
                SinkSourceSchemaResolver.resolve(uri(tempDir), "latest", ddl("id", "name"));

        assertFalse(resolved.present());
    }

    @Test
    void missingBlobForSelectedEventFails() throws Exception {
        Path events = tempDir.resolve("inspect-schema").resolve("events");
        Files.createDirectories(events);
        String hash = repeat('a', InspectSchemaRegistryLayout.SHA256_HEX_LENGTH);
        Files.write(
                events.resolve(InspectSchemaRegistryLayout.eventFileName(7L, hash)), new byte[0]);

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                SinkSourceSchemaResolver.resolve(
                                        uri(tempDir), "latest", ddl("id", "name")));
        assertTrue(error.getMessage().contains("blob is missing"), error.getMessage());
    }

    @Test
    void emptyStoreWithEventFails() throws Exception {
        writeRegistry(tempDir, 7L, new SinkInspectSchemaStore(null));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                SinkSourceSchemaResolver.resolve(
                                        uri(tempDir), "latest", ddl("id", "name")));
        assertTrue(error.getMessage().contains("empty store"), error.getMessage());
    }

    @Test
    void matchingDdlDerivesFieldMappingsFromSidecar() throws Exception {
        writeRegistry(tempDir, 7L, store("name", "VARCHAR(2147483647)", 1, 0));

        SinkSourceResolvedSchema resolved =
                SinkSourceSchemaResolver.resolve(uri(tempDir), "latest", ddl("id", "name"));

        assertTrue(resolved.present());
        assertEquals(0, resolved.keyFields().get(0).rowIndex);
        assertEquals(-1, resolved.keyFields().get(0).structuredColumnIndex);
        assertEquals(1, resolved.valueFields().get(0).rowIndex);
        assertEquals(0, resolved.valueFields().get(0).structuredColumnIndex);
    }

    @Test
    void physicalOrderUsesSidecarRowIndexNotKeyThenValueOrder() throws Exception {
        writeRegistry(
                tempDir,
                7L,
                SinkInspectSchemaStore.of(
                        new SinkInspectSchema(
                                Collections.singletonList(
                                        SinkInspectField.key("id", "BIGINT", 1, -1)),
                                Collections.singletonList(
                                        SinkInspectField.value(
                                                "name", "VARCHAR(2147483647)", 0, 0)))));

        SinkSourceResolvedSchema resolved =
                SinkSourceSchemaResolver.resolve(uri(tempDir), "latest", ddl("name", "id"));

        assertTrue(resolved.present());
        assertEquals(1, resolved.keyFields().get(0).rowIndex);
        assertEquals(0, resolved.valueFields().get(0).rowIndex);
    }

    @Test
    void sidecarRowIndexOutsideDdlPhysicalFieldsFails() throws Exception {
        writeRegistry(tempDir, 7L, store("name", "VARCHAR(2147483647)", 3, 0));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                SinkSourceSchemaResolver.resolve(
                                        uri(tempDir), "latest", ddl("id", "name")));
        assertTrue(error.getMessage().contains("rowIndex 3"), error.getMessage());
    }

    @Test
    void sidecarRowIndexMustCoverEveryPhysicalColumnExactlyOnce() throws Exception {
        writeRegistry(
                tempDir,
                7L,
                SinkInspectSchemaStore.of(
                        new SinkInspectSchema(
                                Collections.singletonList(
                                        SinkInspectField.key("id", "BIGINT", 0, -1)),
                                Collections.singletonList(
                                        SinkInspectField.value(
                                                "name", "VARCHAR(2147483647)", 0, 0)))));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                SinkSourceSchemaResolver.resolve(
                                        uri(tempDir), "latest", ddl("id", "name")));
        assertTrue(error.getMessage().contains("duplicate rowIndex 0"), error.getMessage());
    }

    @Test
    void valueStructuredColumnIndexMustBeDenseAndUnique() throws Exception {
        writeRegistry(tempDir, 7L, store("name", "VARCHAR(2147483647)", 1, 1));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                SinkSourceSchemaResolver.resolve(
                                        uri(tempDir), "latest", ddl("id", "name")));
        assertTrue(error.getMessage().contains("structuredColumnIndex 1"), error.getMessage());
    }

    @Test
    void duplicateValueStructuredColumnIndexFails() throws Exception {
        writeRegistry(
                tempDir,
                7L,
                SinkInspectSchemaStore.of(
                        new SinkInspectSchema(
                                Collections.singletonList(
                                        SinkInspectField.key("id", "BIGINT", 0, -1)),
                                Arrays.asList(
                                        SinkInspectField.value("name", "VARCHAR(2147483647)", 1, 0),
                                        SinkInspectField.value(
                                                "city", "VARCHAR(2147483647)", 2, 0)))));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                SinkSourceSchemaResolver.resolve(
                                        uri(tempDir), "latest", ddl("id", "name", "city")));
        assertTrue(
                error.getMessage().contains("duplicate value structuredColumnIndex 0"),
                error.getMessage());
    }

    @Test
    void reorderedDdlColumnsFail() throws Exception {
        writeRegistry(tempDir, 7L, store("name", "VARCHAR(2147483647)", 1, 0));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                SinkSourceSchemaResolver.resolve(
                                        uri(tempDir), "latest", ddl("name", "id")));
        assertTrue(error.getMessage().contains("column at position 0"), error.getMessage());
        assertTrue(error.getMessage().contains("expects 'id'"), error.getMessage());
    }

    @Test
    void primaryKeyOrderMustMatchSidecarKeyOrder() throws Exception {
        writeRegistry(
                tempDir,
                7L,
                SinkInspectSchemaStore.of(
                        new SinkInspectSchema(
                                Arrays.asList(
                                        SinkInspectField.key("id", "BIGINT", 0, -1),
                                        SinkInspectField.key(
                                                "region", "VARCHAR(2147483647)", 1, -1)),
                                Collections.singletonList(
                                        SinkInspectField.value(
                                                "name", "VARCHAR(2147483647)", 2, 0)))));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                SinkSourceSchemaResolver.resolve(
                                        uri(tempDir),
                                        "latest",
                                        schema(
                                                columns("id", "region", "name"),
                                                Arrays.asList("region", "id"))));
        assertTrue(
                error.getMessage().contains("PRIMARY KEY column at position 0"),
                error.getMessage());
    }

    @Test
    void typeMismatchFails() throws Exception {
        writeRegistry(tempDir, 7L, store("score", "INT", 1, 0));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                SinkSourceSchemaResolver.resolve(
                                        uri(tempDir),
                                        "latest",
                                        schema(
                                                Arrays.asList(
                                                        Column.physical("id", DataTypes.BIGINT()),
                                                        Column.physical(
                                                                "score", DataTypes.STRING())),
                                                Collections.singletonList("id"))));
        assertTrue(error.getMessage().contains("expects INT"), error.getMessage());
    }

    @Test
    void varcharSidecarAcceptsStringDdl() throws Exception {
        writeRegistry(tempDir, 7L, store("name", "VARCHAR(32)", 1, 0));

        SinkSourceResolvedSchema resolved =
                SinkSourceSchemaResolver.resolve(uri(tempDir), "latest", ddl("id", "name"));

        assertTrue(resolved.present());
        assertEquals("VARCHAR(32)", resolved.valueFields().get(0).logicalType);
    }

    private static SinkInspectSchemaStore store(
            String valueName, String valueLogicalType, int rowIndex, int structuredColumnIndex) {
        return SinkInspectSchemaStore.of(
                new SinkInspectSchema(
                        Collections.singletonList(SinkInspectField.key("id", "BIGINT", 0, -1)),
                        Collections.singletonList(
                                SinkInspectField.value(
                                        valueName,
                                        valueLogicalType,
                                        rowIndex,
                                        structuredColumnIndex))));
    }

    private static ResolvedSchema ddl(String... columns) {
        return schema(columns(columns), Collections.singletonList("id"));
    }

    private static List<Column> columns(String... names) {
        List<Column> columns = new ArrayList<>();
        for (String name : names) {
            DataType type = "id".equals(name) ? DataTypes.BIGINT() : DataTypes.STRING();
            columns.add(Column.physical(name, type));
        }
        return columns;
    }

    private static ResolvedSchema schema(List<Column> columns, List<String> pkColumns) {
        return new ResolvedSchema(
                columns, Collections.emptyList(), UniqueConstraint.primaryKey("pk", pkColumns));
    }

    private static void writeRegistry(Path root, long snapshotId, SinkInspectSchemaStore store)
            throws Exception {
        byte[] bytes = store.toBytes();
        String hash = InspectSchemaRegistryLayout.sha256(bytes);
        Path base = root.resolve("inspect-schema");
        Path events = base.resolve("events");
        Path blobs = base.resolve("blobs");
        Files.createDirectories(events);
        Files.createDirectories(blobs);
        Files.write(blobs.resolve(InspectSchemaRegistryLayout.blobFileName(hash)), bytes);
        Files.write(
                events.resolve(InspectSchemaRegistryLayout.eventFileName(snapshotId, hash)),
                new byte[0]);
    }

    private static String repeat(char c, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
    }

    private static String uri(Path path) {
        return path.toUri().toString();
    }
}
