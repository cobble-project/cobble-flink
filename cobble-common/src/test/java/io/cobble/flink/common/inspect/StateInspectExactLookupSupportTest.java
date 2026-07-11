package io.cobble.flink.common.inspect;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/** Regression coverage for shared state exact-lookup eligibility. */
class StateInspectExactLookupSupportTest {

    @Test
    void voidNamespaceWithScalarValueKeySupportsExactLookup() {
        StateInspectExactLookupSupport.Result result =
                StateInspectExactLookupSupport.evaluate(
                        StateInspectSchema.forValue(
                                "orders",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT")));

        assertTrue(result.supported(), result.reason());
    }

    @Test
    void unknownNonVoidNamespaceDoesNotSupportExactLookup() {
        StateInspectExactLookupSupport.Result result =
                StateInspectExactLookupSupport.evaluate(
                        StateInspectSchema.forValue(
                                "orders",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT")));

        assertFalse(result.supported());
        assertTrue(result.reason().contains("namespace semantic type is unavailable"));
    }

    @Test
    void portableRowDataMapKeySupportsExactLookup() {
        StateInspectExactLookupSupport.Result result =
                StateInspectExactLookupSupport.evaluate(
                        mapSchema(new RowDataSerializer(new IntType(), VarCharType.STRING_TYPE)),
                        mapSemantic(structuredRow()));

        assertTrue(result.supported(), result.reason());
    }

    @Test
    void classlessPojoAndAvroMapKeysDoNotSupportExactLookup() {
        TypeSerializer<ExactLookupPojo> pojoSerializer =
                TypeInformation.of(ExactLookupPojo.class).createSerializer(new ExecutionConfig());
        StateInspectExactLookupSupport.Result pojo =
                StateInspectExactLookupSupport.evaluate(
                        mapSchema(pojoSerializer), mapSemantic(structuredRow()));
        assertFalse(pojo.supported());
        assertTrue(pojo.reason().contains("classless POJO map key"));

        Schema avroSchema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"MapKey\","
                                        + "\"fields\":[{\"name\":\"itemId\",\"type\":\"int\"},"
                                        + "{\"name\":\"label\",\"type\":\"string\"}]}");
        StateInspectExactLookupSupport.Result avro =
                StateInspectExactLookupSupport.evaluate(
                        mapSchema(new AvroSerializer<>(GenericRecord.class, avroSchema)),
                        mapSemantic(structuredRow()));
        assertFalse(avro.supported());
        assertTrue(avro.reason().contains("classless Avro map key"));
    }

    @Test
    void tupleStateKeyDoesNotSupportExactLookup() {
        StateInspectType tuple =
                StateInspectType.tuple(
                        Arrays.asList(
                                new StateInspectField("f0", StateInspectType.scalar("INT")),
                                new StateInspectField("f1", StateInspectType.scalar("VARCHAR"))));
        StateInspectExactLookupSupport.Result result =
                StateInspectExactLookupSupport.evaluate(
                        StateInspectSchema.forValue(
                                "orders",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forValue(
                                tuple, StateInspectType.unknown(), StateInspectType.scalar("INT")));

        assertFalse(result.supported());
        assertTrue(result.reason().contains("state key Tuple reconstruction is not supported"));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static StateInspectSchema mapSchema(TypeSerializer<?> mapKeySerializer) {
        return StateInspectSchema.forMap(
                "orders",
                "cf",
                false,
                IntSerializer.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                (TypeSerializer) mapKeySerializer,
                IntSerializer.INSTANCE);
    }

    private static StateInspectSemanticSchema mapSemantic(StateInspectType mapKey) {
        return StateInspectSemanticSchema.forMap(
                StateInspectType.scalar("INT"),
                StateInspectType.unknown(),
                mapKey,
                StateInspectType.scalar("INT"));
    }

    private static StateInspectType structuredRow() {
        return StateInspectType.row(
                Arrays.asList(
                        new StateInspectField("itemId", StateInspectType.scalar("INT")),
                        new StateInspectField("label", StateInspectType.scalar("VARCHAR"))));
    }

    public static final class ExactLookupPojo {
        public int itemId;
        public String label;

        public ExactLookupPojo() {}
    }
}
