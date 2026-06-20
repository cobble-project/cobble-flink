package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.GlobalSnapshot;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class MonitorInspectSchemaResolverTest {

    @Test
    void resolvesExactCheckpointEvent(@TempDir Path tempDir) throws Exception {
        Path root = tempDir.resolve("checkpoints");
        StateInspectSchemaStore store = store("state-100", "cf-100");
        writeRegistry(root, "op-a", 100L, store);

        SchemaResolveResult result =
                MonitorInspectSchemaResolver.resolve(
                        checkpoint(root, 100L), operator(root, "op-a"));

        assertEquals(SchemaResolveResult.STATUS_AVAILABLE, result.status);
        assertEquals(100L, result.schemaCheckpointId);
        assertEquals("state-100", result.store.schemas().get(0).stateName());
    }

    @Test
    void resolvesPreviousCheckpointEvent(@TempDir Path tempDir) throws Exception {
        Path root = tempDir.resolve("checkpoints");
        writeRegistry(root, "op-a", 100L, store("state-100", "cf-100"));
        writeRegistry(root, "op-a", 200L, store("state-200", "cf-200"));

        SchemaResolveResult result =
                MonitorInspectSchemaResolver.resolve(
                        checkpoint(root, 150L), operator(root, "op-a"));

        assertEquals(SchemaResolveResult.STATUS_AVAILABLE, result.status);
        assertEquals(100L, result.schemaCheckpointId);
        assertEquals("state-100", result.store.schemas().get(0).stateName());
    }

    @Test
    void ignoresMalformedEventsAndChoosesValidEvent(@TempDir Path tempDir) throws Exception {
        Path root = tempDir.resolve("checkpoints");
        writeRegistry(root, "op-a", 100L, store("state-100", "cf-100"));
        Path events = schemaRoot(root, "op-a").resolve("events");
        Files.write(events.resolve("SCHEMA-00000000000000000150-bad.ref"), new byte[0]);

        SchemaResolveResult result =
                MonitorInspectSchemaResolver.resolve(
                        checkpoint(root, 160L), operator(root, "op-a"));

        assertEquals(SchemaResolveResult.STATUS_AVAILABLE, result.status);
        assertEquals(100L, result.schemaCheckpointId);
    }

    @Test
    void missingBlobReturnsInvalidEmptyStore(@TempDir Path tempDir) throws Exception {
        Path root = tempDir.resolve("checkpoints");
        Path events = schemaRoot(root, "op-a").resolve("events");
        Files.createDirectories(events);
        String hash = InspectSchemaRegistryLayout.sha256(new byte[] {1, 2, 3});
        Files.write(
                events.resolve(InspectSchemaRegistryLayout.eventFileName(100L, hash)), new byte[0]);

        SchemaResolveResult result =
                MonitorInspectSchemaResolver.resolve(
                        checkpoint(root, 100L), operator(root, "op-a"));

        assertEquals(SchemaResolveResult.STATUS_INVALID, result.status);
        assertTrue(result.store.isEmpty());
    }

    @Test
    void corruptBlobReturnsInvalidEmptyStore(@TempDir Path tempDir) throws Exception {
        Path root = tempDir.resolve("checkpoints");
        Path base = schemaRoot(root, "op-a");
        Path events = base.resolve("events");
        Path blobs = base.resolve("blobs");
        Files.createDirectories(events);
        Files.createDirectories(blobs);
        byte[] corrupt = "not-a-schema".getBytes(StandardCharsets.UTF_8);
        String hash = InspectSchemaRegistryLayout.sha256(corrupt);
        Files.write(
                events.resolve(InspectSchemaRegistryLayout.eventFileName(100L, hash)), new byte[0]);
        Files.write(blobs.resolve(InspectSchemaRegistryLayout.blobFileName(hash)), corrupt);

        SchemaResolveResult result =
                MonitorInspectSchemaResolver.resolve(
                        checkpoint(root, 100L), operator(root, "op-a"));

        assertEquals(SchemaResolveResult.STATUS_INVALID, result.status);
        assertTrue(result.store.isEmpty());
    }

    @Test
    void schemaTargetsUseStateNamesAndHideColumnFamily() {
        GlobalSnapshot snapshot = new GlobalSnapshot();
        snapshot.columnFamilyIds = new LinkedHashMap<>();
        snapshot.columnFamilyIds.put("default", 0);
        snapshot.columnFamilyIds.put("cf-value", 1);
        snapshot.columnFamilyIds.put("__cobble_timer__timer-state", 2);
        StateInspectSchemaStore store = store("value-state", "cf-value");

        List<InspectTarget> targets =
                StateInspectTargetBuilder.build(
                        snapshot,
                        SchemaResolveResult.available(store, "event", "blob", "hash", 100L));

        assertEquals("value-state", targets.get(0).id);
        assertEquals("VALUE", targets.get(0).stateKind);
        assertFalse(targets.get(0).allowsColumns);
        assertEquals("timer:timer-state", targets.get(1).id);
    }

    @Test
    void schemaTargetsExposeSemanticParts() {
        GlobalSnapshot snapshot = new GlobalSnapshot();
        snapshot.columnFamilyIds = new LinkedHashMap<>();
        snapshot.columnFamilyIds.put("default", 0);
        snapshot.columnFamilyIds.put("cf-value", 1);
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.row(
                                Collections.singletonList(
                                        new StateInspectField(
                                                "f0", StateInspectType.scalar("BIGINT")))),
                        StateInspectType.unknown(),
                        StateInspectType.row(
                                Collections.singletonList(
                                        new StateInspectField(
                                                "order_id", StateInspectType.scalar("BIGINT")))));
        StateInspectSchemaStore store =
                new StateInspectSchemaStore(
                        Collections.singletonList(
                                StateInspectSchema.forValue(
                                        "value-state",
                                        "cf-value",
                                        false,
                                        IntSerializer.INSTANCE,
                                        StringSerializer.INSTANCE,
                                        StringSerializer.INSTANCE)),
                        Collections.singletonMap("value-state", semanticSchema));

        InspectTarget target =
                StateInspectTargetBuilder.build(
                                snapshot,
                                SchemaResolveResult.available(store, "event", "blob", "hash", 100L))
                        .get(0);
        Map<String, Object> targetJson = target.toJson();

        @SuppressWarnings("unchecked")
        Map<String, Object> parts = (Map<String, Object>) targetJson.get("semantic_parts");
        assertEquals("ROW", ((Map<?, ?>) parts.get("state_key")).get("kind"));
        assertEquals("UNKNOWN", ((Map<?, ?>) parts.get("namespace")).get("kind"));
        assertEquals("ROW", ((Map<?, ?>) parts.get("value")).get("kind"));
    }

    @Test
    void rawTargetsRemainWhenSchemaMissing() {
        GlobalSnapshot snapshot = new GlobalSnapshot();
        snapshot.columnFamilyIds = new LinkedHashMap<>();
        snapshot.columnFamilyIds.put("default", 0);
        snapshot.columnFamilyIds.put("raw-cf", 1);

        List<InspectTarget> targets =
                StateInspectTargetBuilder.build(snapshot, SchemaResolveResult.missing("none"));

        assertEquals("raw-cf", targets.get(0).id);
        assertNull(targets.get(0).stateKind);
    }

    private static CheckpointEntry checkpoint(Path root, long checkpointId) throws Exception {
        Path checkpointDir = root.resolve("chk-" + checkpointId);
        Files.createDirectories(checkpointDir);
        return new CheckpointEntry(
                checkpointId,
                checkpointDir.toUri().toString(),
                Collections.singletonList(operator(root, "op-a")));
    }

    private static OperatorEntry operator(Path root, String operatorId) {
        String operatorDirectory = root.resolve("cobble").resolve(operatorId).toUri().toString();
        return new OperatorEntry(
                operatorId,
                null,
                operatorDirectory,
                Collections.singletonList(operatorDirectory),
                true);
    }

    private static void writeRegistry(
            Path root, String operatorId, long checkpointId, StateInspectSchemaStore store)
            throws Exception {
        byte[] bytes = store.toBytes();
        String hash = InspectSchemaRegistryLayout.sha256(bytes);
        Path base = schemaRoot(root, operatorId);
        Path events = base.resolve("events");
        Path blobs = base.resolve("blobs");
        Files.createDirectories(events);
        Files.createDirectories(blobs);
        Files.write(blobs.resolve(InspectSchemaRegistryLayout.blobFileName(hash)), bytes);
        Files.write(
                events.resolve(InspectSchemaRegistryLayout.eventFileName(checkpointId, hash)),
                eventContent(checkpointId, operatorId, hash).getBytes(StandardCharsets.UTF_8));
    }

    private static String eventContent(long checkpointId, String operatorId, String hash) {
        return "version=1\n"
                + "checkpoint_id="
                + checkpointId
                + "\noperator_id="
                + operatorId
                + "\nschema_sha256="
                + hash
                + "\n";
    }

    private static Path schemaRoot(Path root, String operatorId) {
        return root.resolve("cobble").resolve(operatorId).resolve("inspect-schema");
    }

    private static StateInspectSchemaStore store(String stateName, String columnFamily) {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        stateName,
                        columnFamily,
                        false,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        return new StateInspectSchemaStore(Collections.singletonList(schema));
    }
}
