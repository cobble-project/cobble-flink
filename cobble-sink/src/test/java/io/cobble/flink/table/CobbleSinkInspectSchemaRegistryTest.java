package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectSchema;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class CobbleSinkInspectSchemaRegistryTest {

    @TempDir private Path tempDir;

    @Test
    void writesContentAddressedSchemaAndSnapshotEvents() throws Exception {
        CobbleDynamicTableSink.SerializableConfig config =
                configWithValues(
                        new CobbleDynamicTableSink.SerializableField(
                                "name", "VARCHAR(2147483647)", 1, 0),
                        new CobbleDynamicTableSink.SerializableField("score", "INT", 2, 1));

        String hash7 = CobbleSinkInspectSchemaRegistry.writeForSnapshot(config, 7L);
        String hash7Again = CobbleSinkInspectSchemaRegistry.writeForSnapshot(config, 7L);
        String hash8 = CobbleSinkInspectSchemaRegistry.writeForSnapshot(config, 8L);

        assertEquals(hash7, hash7Again);
        assertEquals(hash7, hash8);
        assertTrue(InspectSchemaRegistryLayout.isValidSha256(hash7));

        Path schemaRoot = tempDir.resolve(CobbleSinkInspectSchemaRegistry.SCHEMA_DIR);
        Path blobsDir = schemaRoot.resolve(CobbleSinkInspectSchemaRegistry.BLOBS_DIR);
        Path eventsDir = schemaRoot.resolve(CobbleSinkInspectSchemaRegistry.EVENTS_DIR);

        List<Path> blobs = listFiles(blobsDir);
        List<Path> events = listFiles(eventsDir);
        assertEquals(1, blobs.size());
        assertEquals(2, events.size());
        assertEquals(
                InspectSchemaRegistryLayout.blobFileName(hash7),
                blobs.get(0).getFileName().toString());
        assertTrue(
                events.stream()
                        .anyMatch(
                                path ->
                                        path.getFileName()
                                                .toString()
                                                .equals(
                                                        InspectSchemaRegistryLayout.eventFileName(
                                                                7L, hash7))));
        assertTrue(
                events.stream()
                        .anyMatch(
                                path ->
                                        path.getFileName()
                                                .toString()
                                                .equals(
                                                        InspectSchemaRegistryLayout.eventFileName(
                                                                8L, hash7))));

        SinkInspectSchema schema =
                SinkInspectSchemaStore.fromBytes(Files.readAllBytes(blobs.get(0))).schema();
        assertEquals("id", schema.keyFields().get(0).name());
        assertEquals("BIGINT", schema.keyFields().get(0).logicalType());
        assertEquals("name", schema.valueFields().get(0).name());
        assertEquals("VARCHAR(2147483647)", schema.valueFields().get(0).logicalType());
        assertEquals(0, schema.valueFields().get(0).structuredColumnIndex());
        assertEquals("score", schema.valueFields().get(1).name());
        assertEquals("INT", schema.valueFields().get(1).logicalType());
        assertEquals(1, schema.valueFields().get(1).structuredColumnIndex());
        assertFalse(Files.exists(tempDir.resolve(InspectSchemaRegistryLayout.blobFileName(hash7))));
    }

    @Test
    void schemaChangeCreatesNewBlobAndEvent() throws Exception {
        CobbleDynamicTableSink.SerializableConfig first =
                configWithValues(
                        new CobbleDynamicTableSink.SerializableField(
                                "name", "VARCHAR(2147483647)", 1, 0));
        CobbleDynamicTableSink.SerializableConfig second =
                configWithValues(
                        new CobbleDynamicTableSink.SerializableField(
                                "name", "VARCHAR(2147483647)", 1, 0),
                        new CobbleDynamicTableSink.SerializableField("active", "BOOLEAN", 2, 1));

        String firstHash = CobbleSinkInspectSchemaRegistry.writeForSnapshot(first, 1L);
        String secondHash = CobbleSinkInspectSchemaRegistry.writeForSnapshot(second, 2L);

        assertFalse(firstHash.equals(secondHash));
        Path schemaRoot = tempDir.resolve(CobbleSinkInspectSchemaRegistry.SCHEMA_DIR);
        List<Path> blobs = listFiles(schemaRoot.resolve(CobbleSinkInspectSchemaRegistry.BLOBS_DIR));
        List<Path> events =
                listFiles(schemaRoot.resolve(CobbleSinkInspectSchemaRegistry.EVENTS_DIR));

        assertEquals(2, blobs.size());
        assertEquals(2, events.size());
        assertTrue(
                events.stream()
                        .anyMatch(
                                path ->
                                        path.getFileName()
                                                .toString()
                                                .equals(
                                                        InspectSchemaRegistryLayout.eventFileName(
                                                                1L, firstHash))));
        assertTrue(
                events.stream()
                        .anyMatch(
                                path ->
                                        path.getFileName()
                                                .toString()
                                                .equals(
                                                        InspectSchemaRegistryLayout.eventFileName(
                                                                2L, secondHash))));
    }

    private CobbleDynamicTableSink.SerializableConfig configWithValues(
            CobbleDynamicTableSink.SerializableField... valueFields) {
        return new CobbleDynamicTableSink.SerializableConfig(
                tempDir.toUri().toString(),
                4,
                2,
                1,
                false,
                16L * 1024L * 1024L,
                Collections.singletonList(
                        new CobbleDynamicTableSink.SerializableField("id", "BIGINT", 0, -1)),
                Arrays.asList(valueFields));
    }

    private static List<Path> listFiles(Path directory) throws IOException {
        try (Stream<Path> stream = Files.list(directory)) {
            return stream.sorted().collect(Collectors.toList());
        }
    }
}
