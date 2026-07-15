package io.cobble.flink.inspect.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.inspect.DecodedValue;
import io.cobble.flink.inspect.InspectTargetKind;
import io.cobble.flink.inspect.SemanticType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

class PublicInspectModelsTest {

    @Test
    void preservesRecursiveTupleAndMapSemanticTypes() {
        StateInspectType tuple =
                StateInspectType.tuple(
                        Arrays.asList(
                                new StateInspectField("f0", StateInspectType.scalar("BIGINT")),
                                new StateInspectField("f1", StateInspectType.scalar("VARCHAR"))));
        StateInspectType value = StateInspectType.map(StateInspectType.scalar("VARCHAR"), tuple);
        InspectTarget internal =
                new InspectTarget(
                        "state",
                        "state",
                        "state",
                        "state",
                        false,
                        "VALUE",
                        Collections.<String, String>emptyMap(),
                        null,
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("BIGINT"), null, value),
                        null);

        io.cobble.flink.inspect.InspectTarget target = PublicInspectModels.target(internal);
        assertEquals(InspectTargetKind.STATE, target.kind());
        SemanticType semantic = target.semanticSchema().get("value");
        assertEquals(SemanticType.Kind.MAP, semantic.kind());
        assertEquals(SemanticType.Kind.TUPLE, semantic.valueType().kind());
        assertEquals("f1", semantic.valueType().fields().get(1).name());
    }

    @Test
    void convertsStructuredMapWithoutLeakingInternalTypes() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("kind", "ROW");
        Map<String, Object> field = new LinkedHashMap<>();
        field.put("name", "count");
        field.put("value", DisplayLong.forJson(Long.MAX_VALUE));
        row.put("fields", Collections.singletonList(field));

        DecodedValue decoded = PublicInspectModels.decoded(row);
        assertEquals(DecodedValue.Kind.ROW, decoded.kind());
        assertEquals("count", decoded.fields().get(0).name());
        assertTrue(decoded.fields().get(0).value().scalar() instanceof Long);
    }

    @Test
    void userClassLoaderScopeAlwaysRestoresTheCallingThread() {
        Thread thread = Thread.currentThread();
        ClassLoader original = thread.getContextClassLoader();
        ClassLoader userLoader = new ClassLoader(original) {};

        assertThrows(
                IllegalStateException.class,
                () ->
                        UserClassLoaderScope.call(
                                userLoader,
                                () -> {
                                    assertSame(userLoader, thread.getContextClassLoader());
                                    throw new IllegalStateException("expected");
                                }));
        assertSame(original, thread.getContextClassLoader());
    }
}
