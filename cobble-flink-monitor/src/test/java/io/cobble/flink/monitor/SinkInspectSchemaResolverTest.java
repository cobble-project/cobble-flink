package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.GlobalSnapshot;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class SinkInspectSchemaResolverTest {

    @Test
    void resolvesLatestSinkSchemaAtOrBeforeSnapshot(@TempDir Path tempDir) throws Exception {
        SinkInspectSchemaStore schema100 = store("name", "VARCHAR(2147483647)");
        SinkInspectSchemaStore schema200 = store("active", "BOOLEAN");
        writeRegistry(tempDir, 100L, schema100);
        writeRegistry(tempDir, 200L, schema200);

        SinkSchemaResolveResult result =
                SinkInspectSchemaResolver.resolve(tempDir.toUri().toString(), 150L);

        assertEquals(SinkSchemaResolveResult.STATUS_AVAILABLE, result.status);
        assertEquals(100L, result.schemaSnapshotId);
        assertEquals("name", result.store.schema().valueFields().get(0).name());
    }

    @Test
    void sinkTargetExposesKeyAndValueFields(@TempDir Path tempDir) throws Exception {
        SinkInspectSchemaStore store = store("score", "INT");
        writeRegistry(tempDir, 7L, store);
        SinkSchemaResolveResult result =
                SinkInspectSchemaResolver.resolve(tempDir.toUri().toString(), 7L);

        GlobalSnapshot snapshot = new GlobalSnapshot();
        snapshot.columnFamilyIds = new LinkedHashMap<>();
        snapshot.columnFamilyIds.put("default", 0);
        List<InspectTarget> targets =
                StateInspectTargetBuilder.build(
                        snapshot, SchemaResolveResult.missing("none"), result);

        assertEquals(1, targets.size());
        Map<String, Object> json = targets.get(0).toJson();
        assertEquals("sink", json.get("kind"));
        assertEquals(Boolean.TRUE, json.get("allows_columns"));

        List<Map<String, Object>> keyFields = fieldList(json.get("key_fields"));
        List<Map<String, Object>> valueFields = fieldList(json.get("value_fields"));
        assertEquals("id", keyFields.get(0).get("name"));
        assertEquals("BIGINT", keyFields.get(0).get("logical_type"));
        assertEquals("score", valueFields.get(0).get("name"));
        assertEquals("INT", valueFields.get(0).get("logical_type"));
        assertEquals(0, valueFields.get(0).get("structured_column_index"));
    }

    @Test
    void missingSinkRegistryFallsBackToRawTarget(@TempDir Path tempDir) {
        SinkSchemaResolveResult result =
                SinkInspectSchemaResolver.resolve(tempDir.toUri().toString(), 7L);

        GlobalSnapshot snapshot = new GlobalSnapshot();
        snapshot.columnFamilyIds = new LinkedHashMap<>();
        snapshot.columnFamilyIds.put("default", 0);
        List<InspectTarget> targets =
                StateInspectTargetBuilder.build(
                        snapshot, SchemaResolveResult.missing("none"), result);

        assertEquals(1, targets.size());
        assertEquals("sink", targets.get(0).kind);
        assertTrue(!targets.get(0).toJson().containsKey("key_fields"));
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

    private static SinkInspectSchemaStore store(String valueName, String valueLogicalType) {
        return SinkInspectSchemaStore.of(
                new SinkInspectSchema(
                        java.util.Collections.singletonList(
                                SinkInspectField.key("id", "BIGINT", 0, -1)),
                        java.util.Collections.singletonList(
                                SinkInspectField.value(valueName, valueLogicalType, 1, 0))));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> fieldList(Object value) {
        return (List<Map<String, Object>>) value;
    }
}
