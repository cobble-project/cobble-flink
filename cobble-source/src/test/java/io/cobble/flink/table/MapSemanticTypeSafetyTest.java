package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Verifies that a nested {@link StateInspectType.Kind#MAP} value type (a {@code Map<K,V>} wrapper
 * serializer used as a MapState value) is safely rejected by the source schema resolver and lookup
 * key encoder with clear errors, rather than causing an NPE or silent data loss.
 *
 * <p>MAP here is the value-type shape from a nested {@link MapSerializer}, not Flink {@code
 * MapState}. The resolver and encoder cannot flatten a nested MAP into SQL columns, so they must
 * reject it explicitly.
 */
class MapSemanticTypeSafetyTest {

    @TempDir private Path tempDir;

    @Test
    void resolverRejectsNestedMapValueTypeWithValidationException() throws Exception {
        // A map-state whose user value is itself Map<String, Integer>.
        StateInspectType nestedMapValue =
                StateInspectType.map(
                        StateInspectType.scalar("VARCHAR(2147483647)"),
                        StateInspectType.scalar("INT"));
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "nested-map-state",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        new MapSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE));
        StateInspectSemanticSchema semantic =
                StateInspectSemanticSchema.forMap(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("INT"),
                        nestedMapValue);

        writeRegistry(tempDir, "op-a", 100L, store(schema, "nested-map-state", semantic));

        StateSourceOptions opts = opts("nested-map-state", "op-a", "map");
        ValidationException ex =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(tempDir),
                                        opts,
                                        "latest",
                                        schema(
                                                physical("key", DataTypes.INT()),
                                                physical("map_key", DataTypes.INT()),
                                                physical(
                                                        "map_value",
                                                        DataTypes.MAP(
                                                                DataTypes.STRING(),
                                                                DataTypes.INT())))));
        assertTrue(
                ex.getMessage().contains("MAP"),
                "error should mention MAP, got: " + ex.getMessage());
    }

    @Test
    void lookupEncoderRejectsNestedMapKeyTypeWithIOException() {
        // A map-state whose user key is itself Map<String, Integer> — the lookup encoder cannot
        // flatten a nested MAP key into lookup key bytes.
        StateInspectType nestedMapKey =
                StateInspectType.map(
                        StateInspectType.scalar("VARCHAR(2147483647)"),
                        StateInspectType.scalar("INT"));
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "nested-map-key-state",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new MapSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE),
                        IntSerializer.INSTANCE);
        StateInspectSemanticSchema semantic =
                StateInspectSemanticSchema.forMap(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        nestedMapKey,
                        StateInspectType.scalar("INT"));

        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                new CobbleStateSourceRuntime.RuntimeSchema(schema, semantic);

        StateSourceConfig config =
                new StateSourceConfig(
                        "file:///tmp/checkpoints",
                        StateSourceConfig.Layout.CHECKPOINT_ROOT,
                        "op-a",
                        "nested-map-key-state",
                        "map",
                        "latest",
                        "batch",
                        7L,
                        -1,
                        0L,
                        // output fields don't matter — construction should fail before encoding
                        Collections.emptyList(),
                        StateSourceLookupKeyContract.present(Collections.emptyList(), new int[0]));

        // GroupLookupEncoder.create calls flattenedLogicalTypes which throws IOException for MAP;
        // the constructor wraps it in IllegalArgumentException.
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[0]));
        Throwable cause = ex.getCause();
        assertTrue(cause != null, "expected a cause");
        String message = cause.getMessage();
        assertTrue(
                message != null && message.contains("unsupported"),
                "error message should contain 'unsupported', got: " + message);
    }

    // ---- helpers (mirrors StateSourceSchemaResolverTest) ----

    private static StateInspectSchemaStore store(
            StateInspectSchema schema, String stateName, StateInspectSemanticSchema semantic) {
        Map<String, StateInspectSemanticSchema> semanticSchemas = new LinkedHashMap<>();
        semanticSchemas.put(stateName, semantic);
        return new StateInspectSchemaStore(Collections.singletonList(schema), semanticSchemas);
    }

    private static StateSourceOptions opts(String name, String operatorId, String kind) {
        Configuration config = new Configuration();
        config.set(CobbleSourceTableOptions.STATE_NAME, name);
        config.set(CobbleSourceTableOptions.STATE_OPERATOR_ID, operatorId);
        config.set(CobbleSourceTableOptions.STATE_KIND, kind);
        return StateSourceOptions.parseForState(config);
    }

    private static Column physical(String name, DataType type) {
        return Column.physical(name, type);
    }

    private static ResolvedSchema schema(Column... columns) {
        return ResolvedSchema.of(columns);
    }

    private static String uri(Path path) {
        return path.toUri().toString();
    }

    private static Path schemaRoot(Path root, String operatorId) {
        return root.resolve("cobble").resolve(operatorId).resolve("inspect-schema");
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
                new byte[0]);
    }
}
