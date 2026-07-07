package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Verifies that a nested {@link StateInspectType.Kind#MAP} value type flows through the monitor's
 * JSON rendering paths without NPE and is rejected with a clear error by the decoded-value path.
 *
 * <p>MAP here refers to the value-type shape (a {@code Map<K,V>} wrapper serializer), which is
 * distinct from Flink {@code MapState} (whose key/value roles are separate semantic parts). A
 * nested MAP can appear as a map-user-value when the value serializer is itself a {@code
 * MapSerializer}.
 */
class MapSemanticTypeSafetyTest {

    @Test
    void inspectTargetJsonRendersMapKeyTypeAndValueTypeWithoutNpe() {
        // A map-state whose user value is itself Map<String, Integer> (nested map value type).
        StateInspectType nestedMapType =
                StateInspectType.map(
                        StateInspectType.scalar("VARCHAR(2147483647)"),
                        StateInspectType.scalar("INT"));
        StateInspectSemanticSchema semantic =
                StateInspectSemanticSchema.forMap(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("INT"),
                        nestedMapType);

        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "nested-map-state",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        new MapSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE));

        InspectTarget target =
                new InspectTarget(
                        schema.stateName(),
                        schema.stateName(),
                        "state",
                        schema.columnFamily(),
                        false,
                        schema.stateKind().name(),
                        java.util.Collections.emptyMap(),
                        schema,
                        semantic,
                        null);

        // Should not throw NPE.
        Map<String, Object> json = target.toJson();
        assertTrue(json.containsKey("semantic_parts"));

        @SuppressWarnings("unchecked")
        Map<String, Object> semanticParts = (Map<String, Object>) json.get("semantic_parts");
        assertTrue(semanticParts.containsKey("map_value"));

        @SuppressWarnings("unchecked")
        Map<String, Object> mapValueJson = (Map<String, Object>) semanticParts.get("map_value");
        assertEquals("MAP", mapValueJson.get("kind"));
        // key_type and value_type must be present — no silent data loss.
        assertTrue(mapValueJson.containsKey("key_type"), "key_type must be rendered for MAP");
        assertTrue(mapValueJson.containsKey("value_type"), "value_type must be rendered for MAP");

        @SuppressWarnings("unchecked")
        Map<String, Object> keyTypeJson = (Map<String, Object>) mapValueJson.get("key_type");
        assertEquals("SCALAR", keyTypeJson.get("kind"));
        assertEquals("VARCHAR(2147483647)", keyTypeJson.get("logical_type"));

        @SuppressWarnings("unchecked")
        Map<String, Object> valueTypeJson = (Map<String, Object>) mapValueJson.get("value_type");
        assertEquals("SCALAR", valueTypeJson.get("kind"));
        assertEquals("INT", valueTypeJson.get("logical_type"));
    }

    @Test
    void semanticValueToJsonRejectsMapWithClearError() throws Exception {
        StateInspectType mapType =
                StateInspectType.map(
                        StateInspectType.scalar("VARCHAR(2147483647)"),
                        StateInspectType.scalar("INT"));

        // semanticValueToJson is package-private static; invoke via reflection.
        Method method =
                StateInspectDecoder.class.getDeclaredMethod(
                        "semanticValueToJson", StateInspectType.class, Object.class);
        method.setAccessible(true);

        // Method.invoke wraps the original IOException in InvocationTargetException.
        java.lang.reflect.InvocationTargetException ex =
                assertThrows(
                        java.lang.reflect.InvocationTargetException.class,
                        () -> method.invoke(null, mapType, null));
        Throwable cause = ex.getCause();
        assertTrue(cause instanceof IOException, "expected IOException, got " + cause);
        assertTrue(
                cause.getMessage().contains("MAP"),
                "error message should mention MAP, got: " + cause.getMessage());
    }
}
