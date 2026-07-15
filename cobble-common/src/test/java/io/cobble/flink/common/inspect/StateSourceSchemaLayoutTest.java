package io.cobble.flink.common.inspect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class StateSourceSchemaLayoutTest {
    @Test
    void derivesNamespacedMapColumnsInSourceOrder() {
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "orders",
                        "orders",
                        false,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        StateInspectSemanticSchema semantic =
                StateInspectSemanticSchema.forMap(
                        StateInspectType.scalar("INT"),
                        StateInspectType.scalar("VARCHAR"),
                        StateInspectType.scalar("VARCHAR"),
                        StateInspectType.scalar("INT"));

        List<StateSourceSchemaLayout.Field> fields =
                StateSourceSchemaLayout.derive(schema, semantic);
        assertEquals(Arrays.asList("key", "namespace", "map_key", "map_value"), names(fields));
        assertEquals(StateSourceSchemaLayout.Group.STATE_KEY, fields.get(0).group());
        assertEquals(StateSourceSchemaLayout.Group.MAP_KEY, fields.get(2).group());
    }

    @Test
    void listUsesValueColumnAndVoidNamespaceIsOmitted() {
        StateInspectSchema schema =
                StateInspectSchema.forList(
                        "items",
                        "items",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        StateInspectSemanticSchema semantic =
                StateInspectSemanticSchema.forList(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("VARCHAR"));

        assertEquals(
                Arrays.asList("key", "value"),
                names(StateSourceSchemaLayout.derive(schema, semantic)));
    }

    @Test
    void rejectsNestedAndDuplicateStructuredFields() {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "bad",
                        "bad",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        StateInspectType nested =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField(
                                        "nested",
                                        StateInspectType.list(StateInspectType.scalar("INT")))));
        IllegalArgumentException nestedError =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                StateSourceSchemaLayout.derive(
                                        schema,
                                        StateInspectSemanticSchema.forValue(
                                                StateInspectType.scalar("INT"),
                                                StateInspectType.unknown(),
                                                nested)));
        assertTrue(nestedError.getMessage().contains("nested non-scalar"));

        StateInspectType duplicate =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("key", StateInspectType.scalar("INT"))));
        IllegalArgumentException duplicateError =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                StateSourceSchemaLayout.derive(
                                        schema,
                                        StateInspectSemanticSchema.forValue(
                                                StateInspectType.scalar("INT"),
                                                StateInspectType.unknown(),
                                                duplicate)));
        assertTrue(duplicateError.getMessage().contains("duplicate output column"));
    }

    private static List<String> names(List<StateSourceSchemaLayout.Field> fields) {
        java.util.ArrayList<String> names = new java.util.ArrayList<>();
        for (StateSourceSchemaLayout.Field field : fields) {
            names.add(field.name());
        }
        return names;
    }
}
