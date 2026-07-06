package io.cobble.flink.common.inspect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.BigDecSerializer;
import org.apache.flink.api.common.typeutils.base.BigIntSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.CharSerializer;
import org.apache.flink.api.common.typeutils.base.DateSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.InstantSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LocalDateSerializer;
import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer;
import org.apache.flink.api.common.typeutils.base.LocalTimeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.SqlDateSerializer;
import org.apache.flink.api.common.typeutils.base.SqlTimeSerializer;
import org.apache.flink.api.common.typeutils.base.SqlTimestampSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.EitherSerializer;
import org.apache.flink.api.java.typeutils.runtime.JavaEitherSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** Tests for {@link SerializerSnapshotInspectTypeExtractor} across all required type families. */
class SerializerSnapshotInspectTypeExtractorTest {

    // ---- Primitive / wrapper snapshots ----

    @Test
    void primitiveSnapshotsMapToScalarLogicalTypes() {
        assertScalar(IntSerializer.INSTANCE, "INT");
        assertScalar(LongSerializer.INSTANCE, "BIGINT");
        assertScalar(ShortSerializer.INSTANCE, "SMALLINT");
        assertScalar(ByteSerializer.INSTANCE, "TINYINT");
        assertScalar(BooleanSerializer.INSTANCE, "BOOLEAN");
        assertScalar(FloatSerializer.INSTANCE, "FLOAT");
        assertScalar(DoubleSerializer.INSTANCE, "DOUBLE");
        assertScalar(CharSerializer.INSTANCE, "CHAR");
        assertScalar(StringSerializer.INSTANCE, "VARCHAR");
        assertScalar(VoidSerializer.INSTANCE, "NULL");
    }

    @Test
    void bigNumbersUseConservativeDecimalDefaults() {
        assertScalar(BigIntSerializer.INSTANCE, "DECIMAL(38, 0)");
        // BigDec snapshot carries no precision/scale, so the conservative default is used.
        assertScalar(BigDecSerializer.INSTANCE, "DECIMAL(38, 18)");
    }

    @Test
    void dateAndTimeSnapshotsMapByJavaType() {
        // java.util.Date has a time component, so it maps to TIMESTAMP, not DATE.
        assertScalar(DateSerializer.INSTANCE, "TIMESTAMP(9)");
        assertScalar(SqlDateSerializer.INSTANCE, "DATE");
        // java.sql.Time has no fractional seconds.
        assertScalar(SqlTimeSerializer.INSTANCE, "TIME(0)");
        assertScalar(SqlTimestampSerializer.INSTANCE, "TIMESTAMP(9)");
        // java.time.* — LocalTime has nanosecond precision.
        assertScalar(LocalDateSerializer.INSTANCE, "DATE");
        assertScalar(LocalTimeSerializer.INSTANCE, "TIME(9)");
        assertScalar(LocalDateTimeSerializer.INSTANCE, "TIMESTAMP(9)");
        // Instant is a point on the global timeline.
        assertScalar(InstantSerializer.INSTANCE, "TIMESTAMP(9) WITH LOCAL TIME ZONE");
    }

    @Test
    void valueWrapperSnapshotsMapToSameLogicalTypesAsPrimitives() {
        assertScalar(org.apache.flink.api.common.typeutils.base.IntValueSerializer.INSTANCE, "INT");
        assertScalar(
                org.apache.flink.api.common.typeutils.base.LongValueSerializer.INSTANCE, "BIGINT");
        assertScalar(
                org.apache.flink.api.common.typeutils.base.BooleanValueSerializer.INSTANCE,
                "BOOLEAN");
        assertScalar(
                org.apache.flink.api.common.typeutils.base.StringValueSerializer.INSTANCE,
                "VARCHAR");
    }

    // ---- Composite snapshots: List, Map, Tuple ----

    @Test
    void listSerializerSnapshotExtractsElementType() {
        ListSerializer<Integer> listSerializer = new ListSerializer<>(IntSerializer.INSTANCE);
        StateInspectType type = extract(listSerializer);
        assertEquals(StateInspectTypeKind.LIST, type.kind());
        assertScalar(type.elementType(), "INT");
    }

    @Test
    void mapSerializerSnapshotExtractsKeyAndValueTypes() {
        MapSerializer<Integer, String> mapSerializer =
                new MapSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE);
        StateInspectType type = extract(mapSerializer);
        assertEquals(StateInspectTypeKind.MAP, type.kind());
        assertScalar(type.keyType(), "INT");
        assertScalar(type.valueType(), "VARCHAR");
    }

    @Test
    void tupleSerializerSnapshotExtractsFieldsAsTuple() {
        @SuppressWarnings("unchecked")
        TypeSerializer<Tuple2<Integer, String>> tupleSerializer =
                (TypeSerializer<Tuple2<Integer, String>>)
                        TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {})
                                .createSerializer(new ExecutionConfig());
        StateInspectType type = extract(tupleSerializer);
        assertEquals(StateInspectTypeKind.TUPLE, type.kind());
        assertEquals(2, type.fields().size());
        assertEquals("f0", type.fields().get(0).name());
        assertScalar(type.fields().get(0).type(), "INT");
        assertEquals("f1", type.fields().get(1).name());
        assertScalar(type.fields().get(1).type(), "VARCHAR");
    }

    @Test
    void nestedListInsideListExtractsRecursively() {
        ListSerializer<List<Integer>> nestedList =
                new ListSerializer<>(new ListSerializer<>(IntSerializer.INSTANCE));
        StateInspectType type = extract(nestedList);
        assertEquals(StateInspectTypeKind.LIST, type.kind());
        assertEquals(StateInspectTypeKind.LIST, type.elementType().kind());
        assertScalar(type.elementType().elementType(), "INT");
    }

    @Test
    void nestedMapInsideListExtractsRecursively() {
        ListSerializer<Map<Integer, Long>> listWithMap =
                new ListSerializer<>(
                        new MapSerializer<>(IntSerializer.INSTANCE, LongSerializer.INSTANCE));
        StateInspectType type = extract(listWithMap);
        assertEquals(StateInspectTypeKind.LIST, type.kind());
        assertEquals(StateInspectTypeKind.MAP, type.elementType().kind());
        assertScalar(type.elementType().keyType(), "INT");
        assertScalar(type.elementType().valueType(), "BIGINT");
    }

    @Test
    void unknownCompositeSnapshotReturnsUnknownNotTuple() {
        // JavaEitherSerializerSnapshot extends CompositeTypeSerializerSnapshot but is not
        // List/Map/Tuple, so it must return UNKNOWN rather than guessing a tuple shape.
        EitherSerializer<Integer, String> eitherSerializer =
                new EitherSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE);
        TypeSerializerSnapshot<?> snapshot = eitherSerializer.snapshotConfiguration();
        // Confirm it is indeed a CompositeTypeSerializerSnapshot subclass.
        assertTrue(snapshot instanceof JavaEitherSerializerSnapshot);
        StateInspectType type = SerializerSnapshotInspectTypeExtractor.extract(snapshot);
        assertEquals(StateInspectTypeKind.UNKNOWN, type.kind());
    }

    // ---- RowData (flink-table-runtime on test classpath) ----

    @Test
    void rowDataSerializerSnapshotExtractsRowFromLogicalTypes() {
        org.apache.flink.table.runtime.typeutils.RowDataSerializer rowDataSerializer =
                new org.apache.flink.table.runtime.typeutils.RowDataSerializer(
                        new org.apache.flink.table.types.logical.IntType(),
                        new org.apache.flink.table.types.logical.VarCharType(100));
        StateInspectType type = extract(rowDataSerializer);
        assertEquals(StateInspectTypeKind.ROW, type.kind());
        assertEquals(2, type.fields().size());
        assertEquals("f0", type.fields().get(0).name());
        assertScalar(type.fields().get(0).type(), "INT");
        assertEquals("f1", type.fields().get(1).name());
        // VarCharType(100).asSerializableString() → "VARCHAR(100)"
        assertScalar(type.fields().get(1).type(), "VARCHAR(100)");
    }

    // ---- POJO (flink-core on classpath) ----

    @Test
    void pojoSerializerSnapshotExtractsNamedFields() {
        TypeSerializer<TestPojo> pojoSerializer =
                TypeInformation.of(TestPojo.class).createSerializer(new ExecutionConfig());
        StateInspectType type = extract(pojoSerializer);
        assertEquals(StateInspectTypeKind.ROW, type.kind());
        assertEquals(2, type.fields().size());
        assertEquals("id", type.fields().get(0).name());
        assertScalar(type.fields().get(0).type(), "INT");
        assertEquals("name", type.fields().get(1).name());
        assertScalar(type.fields().get(1).type(), "VARCHAR");
    }

    /** Simple POJO for serializer snapshot extraction testing. */
    public static final class TestPojo implements Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public String name;
    }

    // ---- Avro (flink-avro on test classpath) ----

    @Test
    void avroSerializerSnapshotExtractsRowFromWriterSchema() {
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("TestRecord")
                        .fields()
                        .name("name")
                        .type()
                        .stringType()
                        .noDefault()
                        .name("age")
                        .type()
                        .intType()
                        .noDefault()
                        .name("active")
                        .type()
                        .booleanType()
                        .noDefault()
                        .endRecord();
        org.apache.flink.formats.avro.typeutils.AvroSerializer<
                        org.apache.avro.generic.GenericRecord>
                avroSerializer =
                        new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                                org.apache.avro.generic.GenericRecord.class, schema);
        StateInspectType type = extract(avroSerializer);
        assertEquals(StateInspectTypeKind.ROW, type.kind());
        assertEquals(3, type.fields().size());
        assertEquals("name", type.fields().get(0).name());
        assertScalar(type.fields().get(0).type(), "VARCHAR");
        assertEquals("age", type.fields().get(1).name());
        assertScalar(type.fields().get(1).type(), "INT");
        assertEquals("active", type.fields().get(2).name());
        assertScalar(type.fields().get(2).type(), "BOOLEAN");
    }

    @Test
    void avroUnionWithNullResolvesToNonNullMember() {
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("NullableString")
                        .fields()
                        .name("value")
                        .type()
                        .unionOf()
                        .nullType()
                        .and()
                        .stringType()
                        .endUnion()
                        .noDefault()
                        .endRecord();
        org.apache.flink.formats.avro.typeutils.AvroSerializer<
                        org.apache.avro.generic.GenericRecord>
                avroSerializer =
                        new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                                org.apache.avro.generic.GenericRecord.class, schema);
        StateInspectType type = extract(avroSerializer);
        assertEquals(StateInspectTypeKind.ROW, type.kind());
        assertEquals(1, type.fields().size());
        assertEquals("value", type.fields().get(0).name());
        // Union of null + string should resolve to VARCHAR.
        assertScalar(type.fields().get(0).type(), "VARCHAR");
    }

    @Test
    void avroNestedRecordExtractsRecursiveRow() {
        // A record containing a nested record field — the nested field must be a ROW, not a
        // scalar placeholder.
        org.apache.avro.Schema innerSchema =
                org.apache.avro.SchemaBuilder.record("Inner")
                        .fields()
                        .name("x")
                        .type()
                        .intType()
                        .noDefault()
                        .endRecord();
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("Outer")
                        .fields()
                        .name("inner")
                        .type(innerSchema)
                        .noDefault()
                        .endRecord();
        org.apache.flink.formats.avro.typeutils.AvroSerializer<
                        org.apache.avro.generic.GenericRecord>
                avroSerializer =
                        new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                                org.apache.avro.generic.GenericRecord.class, schema);
        StateInspectType type = extract(avroSerializer);
        assertEquals(StateInspectTypeKind.ROW, type.kind());
        assertEquals(1, type.fields().size());
        assertEquals("inner", type.fields().get(0).name());
        // The nested record field should be a ROW, not SCALAR("ROW").
        StateInspectType innerType = type.fields().get(0).type();
        assertEquals(StateInspectTypeKind.ROW, innerType.kind());
        assertEquals(1, innerType.fields().size());
        assertEquals("x", innerType.fields().get(0).name());
        assertScalar(innerType.fields().get(0).type(), "INT");
    }

    @Test
    void avroArrayFieldExtractsList() {
        // array<int> should extract as LIST(INT), not SCALAR("ARRAY").
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("WithArray")
                        .fields()
                        .name("tags")
                        .type()
                        .array()
                        .items()
                        .intType()
                        .noDefault()
                        .endRecord();
        org.apache.flink.formats.avro.typeutils.AvroSerializer<
                        org.apache.avro.generic.GenericRecord>
                avroSerializer =
                        new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                                org.apache.avro.generic.GenericRecord.class, schema);
        StateInspectType type = extract(avroSerializer);
        assertEquals(StateInspectTypeKind.ROW, type.kind());
        assertEquals("tags", type.fields().get(0).name());
        StateInspectType arrayType = type.fields().get(0).type();
        assertEquals(StateInspectTypeKind.LIST, arrayType.kind());
        assertScalar(arrayType.elementType(), "INT");
    }

    @Test
    void avroMapFieldExtractsMapWithStringKey() {
        // map<long> should extract as MAP(VARCHAR, BIGINT) — Avro map keys are always strings.
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("WithMap")
                        .fields()
                        .name("counts")
                        .type()
                        .map()
                        .values()
                        .longType()
                        .noDefault()
                        .endRecord();
        org.apache.flink.formats.avro.typeutils.AvroSerializer<
                        org.apache.avro.generic.GenericRecord>
                avroSerializer =
                        new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                                org.apache.avro.generic.GenericRecord.class, schema);
        StateInspectType type = extract(avroSerializer);
        assertEquals(StateInspectTypeKind.ROW, type.kind());
        assertEquals("counts", type.fields().get(0).name());
        StateInspectType mapType = type.fields().get(0).type();
        assertEquals(StateInspectTypeKind.MAP, mapType.kind());
        // Avro map keys are strings → VARCHAR.
        assertScalar(mapType.keyType(), "VARCHAR");
        assertScalar(mapType.valueType(), "BIGINT");
    }

    @Test
    void avroComplexUnionReturnsUnknownNotScalarPlaceholder() {
        // A union with multiple non-null members has no single SQL type — must return UNKNOWN
        // (StateInspectTypeKind.UNKNOWN), not SCALAR("UNKNOWN").
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("ComplexUnion")
                        .fields()
                        .name("value")
                        .type()
                        .unionOf()
                        .intType()
                        .and()
                        .stringType()
                        .endUnion()
                        .noDefault()
                        .endRecord();
        org.apache.flink.formats.avro.typeutils.AvroSerializer<
                        org.apache.avro.generic.GenericRecord>
                avroSerializer =
                        new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                                org.apache.avro.generic.GenericRecord.class, schema);
        StateInspectType type = extract(avroSerializer);
        assertEquals(StateInspectTypeKind.ROW, type.kind());
        assertEquals("value", type.fields().get(0).name());
        StateInspectType unionType = type.fields().get(0).type();
        // Must be UNKNOWN kind, not a scalar with logical_type "UNKNOWN".
        assertEquals(StateInspectTypeKind.UNKNOWN, unionType.kind());
        assertNull(unionType.logicalType());
    }

    // ---- Edge cases ----

    @Test
    void unknownSnapshotReturnsUnknown() {
        TypeSerializerSnapshot<String> customSnapshot = new CustomSnapshot();
        StateInspectType type = SerializerSnapshotInspectTypeExtractor.extract(customSnapshot);
        assertEquals(StateInspectTypeKind.UNKNOWN, type.kind());
    }

    @Test
    void nullSnapshotReturnsUnknown() {
        StateInspectType type = SerializerSnapshotInspectTypeExtractor.extract(null);
        assertEquals(StateInspectTypeKind.UNKNOWN, type.kind());
    }

    @Test
    void mapTypeRoundTripsThroughSerialization() throws IOException {
        StateInspectType original = StateInspectType.map(scalar("INT"), scalar("VARCHAR"));
        DataOutputSerializer out = new DataOutputSerializer(128);
        original.write(out);
        StateInspectType restored =
                StateInspectType.read(new DataInputDeserializer(out.getCopyOfBuffer()));
        assertEquals(original, restored);
        assertEquals(StateInspectTypeKind.MAP, restored.kind());
        assertScalar(restored.keyType(), "INT");
        assertScalar(restored.valueType(), "VARCHAR");
    }

    @Test
    void nestedMapTypeRoundTripsThroughSerialization() throws IOException {
        StateInspectType original =
                StateInspectType.map(scalar("BIGINT"), StateInspectType.list(scalar("INT")));
        DataOutputSerializer out = new DataOutputSerializer(128);
        original.write(out);
        StateInspectType restored =
                StateInspectType.read(new DataInputDeserializer(out.getCopyOfBuffer()));
        assertEquals(original, restored);
        assertEquals(StateInspectTypeKind.MAP, restored.kind());
        assertScalar(restored.keyType(), "BIGINT");
        assertEquals(StateInspectTypeKind.LIST, restored.valueType().kind());
        assertScalar(restored.valueType().elementType(), "INT");
    }

    // ---- Helpers ----

    private static StateInspectType scalar(String logicalType) {
        return StateInspectType.scalar(logicalType);
    }

    private static StateInspectType extract(TypeSerializer<?> serializer) {
        return SerializerSnapshotInspectTypeExtractor.extract(serializer.snapshotConfiguration());
    }

    private static void assertScalar(TypeSerializer<?> serializer, String expectedLogicalType) {
        StateInspectType type = extract(serializer);
        assertScalar(type, expectedLogicalType);
    }

    private static void assertScalar(StateInspectType type, String expectedLogicalType) {
        assertEquals(StateInspectTypeKind.SCALAR, type.kind(), "Expected SCALAR kind");
        assertEquals(expectedLogicalType, type.logicalType());
    }

    /** A custom TypeSerializerSnapshot that the extractor cannot recognize. */
    private static final class CustomSnapshot implements TypeSerializerSnapshot<String> {
        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {}

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {}

        @Override
        public TypeSerializer<String> restoreSerializer() {
            return StringSerializer.INSTANCE;
        }

        @Override
        public TypeSerializerSchemaCompatibility<String> resolveSchemaCompatibility(
                TypeSerializer<String> newSerializer) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }
    }
}
