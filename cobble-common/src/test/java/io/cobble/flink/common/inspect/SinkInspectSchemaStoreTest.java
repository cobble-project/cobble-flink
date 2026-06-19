package io.cobble.flink.common.inspect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

class SinkInspectSchemaStoreTest {

    @Test
    void schemaStoreRoundTrips() throws IOException {
        SinkInspectSchema schema = schema();
        SinkInspectSchemaStore restored = SinkInspectSchemaStore.fromBytes(store(schema).toBytes());

        assertFalse(restored.isEmpty());
        assertEquals(schema, restored.schema());
        assertEquals(2, restored.schema().keyFields().size());
        assertEquals(3, restored.schema().valueFields().size());
        assertEquals("BIGINT", restored.schema().keyFields().get(0).logicalType());
        assertEquals(1, restored.schema().valueFields().get(1).structuredColumnIndex());
    }

    @Test
    void emptyAndNonMagicBytesProduceEmptyStore() throws IOException {
        assertTrue(SinkInspectSchemaStore.fromBytes(null).isEmpty());
        assertTrue(SinkInspectSchemaStore.fromBytes(new byte[0]).isEmpty());
        assertTrue(
                SinkInspectSchemaStore.read(
                                new ByteArrayInputStream(new byte[] {'n', 'o', 'p', 'e'}))
                        .isEmpty());
    }

    @Test
    void unknownVersionFailsClearly() throws IOException {
        byte[] bytes = store(schema()).toBytes();
        bytes[7] = 2;

        IOException error =
                assertThrows(IOException.class, () -> SinkInspectSchemaStore.fromBytes(bytes));
        assertTrue(error.getMessage().contains("Unsupported Cobble sink inspect schema version"));
    }

    @Test
    void hashStableForSameSchemaBytes() throws IOException {
        String left = InspectSchemaRegistryLayout.sha256(store(schema()).toBytes());
        String right = InspectSchemaRegistryLayout.sha256(store(schema()).toBytes());
        String changed =
                InspectSchemaRegistryLayout.sha256(
                        store(
                                        new SinkInspectSchema(
                                                Arrays.asList(
                                                        SinkInspectField.key(
                                                                "id", "BIGINT", 0, -1)),
                                                Arrays.asList(
                                                        SinkInspectField.value(
                                                                "payload", "STRING", 1, 0))))
                                .toBytes());

        assertEquals(left, right);
        assertFalse(left.equals(changed));
    }

    @Test
    void rejectsFieldsWithWrongRole() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SinkInspectSchema(
                                Arrays.asList(SinkInspectField.value("id", "BIGINT", 0, 0)),
                                Arrays.asList(SinkInspectField.value("payload", "STRING", 1, 0))));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SinkInspectSchema(
                                Arrays.asList(SinkInspectField.key("id", "BIGINT", 0, -1)),
                                Arrays.asList(SinkInspectField.key("payload", "STRING", 1, -1))));
    }

    private static SinkInspectSchemaStore store(SinkInspectSchema schema) {
        return SinkInspectSchemaStore.of(schema);
    }

    private static SinkInspectSchema schema() {
        return new SinkInspectSchema(
                Arrays.asList(
                        SinkInspectField.key("id", "BIGINT", 0, -1),
                        SinkInspectField.key("region", "STRING", 1, -1)),
                Arrays.asList(
                        SinkInspectField.value("score", "INT", 2, 0),
                        SinkInspectField.value("payload", "STRING", 3, 1),
                        SinkInspectField.value("enabled", "BOOLEAN", 4, 2)));
    }
}
