package io.cobble.flink.inspect.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.inspect.FieldValue;
import io.cobble.flink.inspect.InspectErrorCode;
import io.cobble.flink.inspect.InspectException;
import io.cobble.flink.inspect.ScanFilter;
import io.cobble.flink.inspect.StateKey;
import io.cobble.flink.inspect.TypedValue;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class InspectTypedStateFilterTest {
    @Test
    void enforcesStructuredStateNamespaceAndMapCompletenessChain() {
        InspectTarget target = structuredMapTarget();

        InspectSessionImpl.StateFilterValues statePrefix =
                InspectSessionImpl.validateStateFilter(
                        target, stateKey(fields(field("tenant", 7)), none(), none()), false);
        assertFalse(statePrefix.stateExact);
        assertFalse(statePrefix.namespaceExact);

        assertInvalid(
                () ->
                        InspectSessionImpl.validateStateFilter(
                                target,
                                stateKey(
                                        fields(field("tenant", 7)),
                                        fields(field("scope", 3)),
                                        none()),
                                false));

        assertInvalid(
                () ->
                        InspectSessionImpl.validateStateFilter(
                                target,
                                stateKey(
                                        fullState(),
                                        fields(field("scope", 3)),
                                        fields(field("attribute", 5))),
                                false));

        InspectSessionImpl.StateFilterValues mapPrefix =
                InspectSessionImpl.validateStateFilter(
                        target,
                        stateKey(fullState(), fullNamespace(), fields(field("attribute", 5))),
                        false);
        assertTrue(mapPrefix.stateExact);
        assertTrue(mapPrefix.namespaceExact);
        assertEquals(Collections.singletonList("5"), mapPrefix.mapValues);

        assertInvalid(
                () ->
                        InspectSessionImpl.validateStateFilter(
                                target,
                                stateKey(fields(field("tenant", 7)), none(), none()),
                                true));
    }

    @Test
    void rejectsNoOpAndNullTypedInputsWithStableInvalidInput() {
        InspectTarget target = structuredMapTarget();
        assertInvalid(
                () ->
                        InspectSessionImpl.validateStateFilter(
                                target, stateKey(none(), none(), none()), false));
        assertInvalid(() -> InspectSessionImpl.validateFilterShape(ScanFilter.sink(none())));
        assertInvalid(
                () ->
                        TypedInputs.semantic(
                                Collections.<FieldValue>singletonList(null),
                                StateInspectType.scalar("INT"),
                                "key",
                                true,
                                "state key"));
        assertInvalid(
                () ->
                        TypedInputs.semantic(
                                Collections.singletonList(new FieldValue("key", null)),
                                StateInspectType.scalar("INT"),
                                "key",
                                true,
                                "state key"));
    }

    @Test
    void validatesFloatingAndDecimalKindsWithoutConflation() {
        assertEquals(
                Collections.singletonList("1.25"),
                TypedInputs.semantic(
                        Collections.singletonList(new FieldValue("key", TypedValue.floating(1.25))),
                        StateInspectType.scalar("DOUBLE"),
                        "key",
                        true,
                        "state key"));
        assertEquals(
                Collections.singletonList("1.25"),
                TypedInputs.semantic(
                        Collections.singletonList(
                                new FieldValue("key", TypedValue.decimal("1.25"))),
                        StateInspectType.scalar("DECIMAL(10, 2)"),
                        "key",
                        true,
                        "state key"));
        assertInvalid(
                () ->
                        TypedInputs.semantic(
                                Collections.singletonList(
                                        new FieldValue("key", TypedValue.decimal("1.25"))),
                                StateInspectType.scalar("DOUBLE"),
                                "key",
                                true,
                                "state key"));
        assertInvalid(
                () ->
                        TypedInputs.semantic(
                                Collections.singletonList(
                                        new FieldValue("key", TypedValue.floating(1.25))),
                                StateInspectType.scalar("DECIMAL(10, 2)"),
                                "key",
                                true,
                                "state key"));
    }

    private static InspectTarget structuredMapTarget() {
        RowDataSerializer serializer =
                new RowDataSerializer(new IntType(), VarCharType.STRING_TYPE);
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "attributes",
                        "attributes",
                        false,
                        serializer,
                        serializer,
                        serializer,
                        StringSerializer.INSTANCE);
        StateInspectType structured =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("tenant", StateInspectType.scalar("INT")),
                                new StateInspectField(
                                        "entity", StateInspectType.scalar("VARCHAR"))));
        StateInspectType namespace =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("scope", StateInspectType.scalar("INT")),
                                new StateInspectField(
                                        "window", StateInspectType.scalar("VARCHAR"))));
        StateInspectType mapKey =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("attribute", StateInspectType.scalar("INT")),
                                new StateInspectField(
                                        "locale", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semantic =
                StateInspectSemanticSchema.forMap(
                        structured, namespace, mapKey, StateInspectType.scalar("VARCHAR"));
        return new InspectTarget(
                "attributes",
                "attributes",
                "state",
                "attributes",
                false,
                "MAP",
                Collections.<String, String>emptyMap(),
                schema,
                semantic,
                null);
    }

    private static StateKey stateKey(
            List<FieldValue> state, List<FieldValue> namespace, List<FieldValue> mapKey) {
        return new StateKey(state, namespace, mapKey);
    }

    private static List<FieldValue> fullState() {
        return fields(field("tenant", 7), field("entity", "customer-9"));
    }

    private static List<FieldValue> fullNamespace() {
        return fields(field("scope", 3), field("window", "daily"));
    }

    private static FieldValue field(String name, int value) {
        return new FieldValue(name, TypedValue.integer(value));
    }

    private static FieldValue field(String name, String value) {
        return new FieldValue(name, TypedValue.string(value));
    }

    private static List<FieldValue> fields(FieldValue... values) {
        return Arrays.asList(values);
    }

    private static List<FieldValue> none() {
        return Collections.emptyList();
    }

    private static void assertInvalid(Runnable operation) {
        InspectException error = assertThrows(InspectException.class, operation::run);
        assertEquals(InspectErrorCode.INVALID_INPUT, error.errorCode());
    }
}
