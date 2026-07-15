package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.DescriptorCapability;
import io.cobble.flink.common.inspect.InspectDecoderDescriptor;
import io.cobble.flink.common.inspect.SerializerInspectSchema;
import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.decode.AvroClasslessDecoder;
import io.cobble.flink.inspect.internal.*;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for classless Avro decoding in the monitor. These tests build real Avro
 * serializer schemas (with AVRO descriptors), serialize actual GenericRecord/SpecificRecord values
 * via Flink's AvroSerializer (test-scoped), then decode them through {@link StateInspectDecoder}'s
 * semantic path.
 *
 * <p>The classless path is exercised because {@link SerializerInspectSchema#fromSerializer} builds
 * an AVRO descriptor for AvroSerializer. The descriptor is FULLY_CLASSLESS for standard schemas,
 * meaning the monitor decodes via {@link AvroClasslessDecoder} without loading user classes.
 */
class AvroClasslessDecoderIntegrationTest {

    // ---- GenericRecord ValueState ----

    @Test
    void decodesGenericRecordValueStateWithoutUserJar() throws Exception {
        org.apache.avro.Schema avroSchema =
                org.apache.avro.SchemaBuilder.record("UserRecord")
                        .fields()
                        .name("userId")
                        .type()
                        .intType()
                        .noDefault()
                        .name("userName")
                        .type()
                        .stringType()
                        .noDefault()
                        .endRecord();
        TypeSerializer<org.apache.avro.generic.GenericRecord> avroSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, avroSchema);

        // Verify the descriptor is FULLY_CLASSLESS AVRO.
        SerializerInspectSchema valueSchema =
                SerializerInspectSchema.fromSerializer(avroSerializer);
        assertNotNull(valueSchema.decoderDescriptor());
        assertTrue(valueSchema.decoderDescriptor().isAvro());
        assertEquals(
                DescriptorCapability.FULLY_CLASSLESS, valueSchema.decoderDescriptor().capability());
        // No live serializer fallback for FULLY_CLASSLESS.
        assertNull(valueSchema.serializedSerializerBytes());

        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "avro-generic",
                        "cf-avro-g",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        avroSerializer);

        StateInspectType rowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("userId", StateInspectType.scalar("INT")),
                                new StateInspectField(
                                        "userName", StateInspectType.scalar("VARCHAR"))));
        InspectTarget target =
                semanticTarget(
                        schema,
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                rowType));

        org.apache.avro.generic.GenericRecord record =
                new org.apache.avro.generic.GenericData.Record(avroSchema);
        record.put("userId", 55);
        record.put("userName", "carol");
        byte[] valueBytes = serialize(avroSerializer, record);
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(target, rowKey, new byte[][] {valueBytes});

        // Semantic value should decode correctly.
        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        assertEquals("ROW", valuePart.get("kind"));
        List<?> fields = (List<?>) valuePart.get("fields");
        assertEquals(2, fields.size());
        assertEquals("userId", ((Map<?, ?>) fields.get(0)).get("name"));
        assertEquals(55, ((Map<?, ?>) fields.get(0)).get("value"));
        assertEquals("userName", ((Map<?, ?>) fields.get(1)).get("name"));
        assertEquals("carol", ((Map<?, ?>) fields.get(1)).get("value"));
    }

    // ---- SpecificRecord produces identical output to GenericRecord ----

    @Test
    void specificRecordProducesIdenticalSemanticJsonAsGenericRecord() throws Exception {
        org.apache.avro.Schema avroSchema = TestSpecificEvent.SCHEMA$;

        // Generic and Specific records share the exact writer schema and field values.
        TypeSerializer<org.apache.avro.generic.GenericRecord> genericSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, avroSchema);
        org.apache.avro.generic.GenericRecord genericRecord =
                new org.apache.avro.generic.GenericData.Record(avroSchema);
        genericRecord.put("id", 88);
        genericRecord.put("label", "click");
        genericRecord.put("active", true);

        // Specific using AvroSerializer(TestSpecificEvent.class) - a real SpecificRecord class
        // with a static SCHEMA$ field. This exercises the actual SpecificRecord serialization
        // path through Flink's DataOutputEncoder.
        TypeSerializer<TestSpecificEvent> specificSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        TestSpecificEvent.class);
        TestSpecificEvent specificRecord = new TestSpecificEvent();
        specificRecord.setId(88);
        specificRecord.setLabel("click");
        specificRecord.setActive(true);

        StateInspectType rowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("label", StateInspectType.scalar("VARCHAR")),
                                new StateInspectField(
                                        "active", StateInspectType.scalar("BOOLEAN"))));

        // Decode both via the monitor's classless path.
        Map<?, ?> genericValuePart =
                (Map<?, ?>)
                        decodeAvroValueStateWithSchema(
                                        genericSerializer, avroSchema, genericRecord, rowType)
                                .decodedParts
                                .get("value");
        Map<?, ?> specificValuePart =
                (Map<?, ?>)
                        decodeAvroValueStateWithSchema(
                                        specificSerializer, avroSchema, specificRecord, rowType)
                                .decodedParts
                                .get("value");

        // Both should produce identical semantic JSON.
        assertEquals(genericValuePart, specificValuePart);
        List<?> fields = (List<?>) genericValuePart.get("fields");
        assertEquals("id", ((Map<?, ?>) fields.get(0)).get("name"));
        assertEquals("label", ((Map<?, ?>) fields.get(1)).get("name"));
        assertEquals("active", ((Map<?, ?>) fields.get(2)).get("name"));
        assertEquals(88, semanticFieldValue(genericValuePart, 0));
        assertEquals("click", semanticFieldValue(genericValuePart, 1));
        assertEquals(true, semanticFieldValue(genericValuePart, 2));
    }

    // ---- Nested record ----

    @Test
    void decodesNestedRecord() throws Exception {
        org.apache.avro.Schema innerSchema =
                org.apache.avro.SchemaBuilder.record("Inner")
                        .fields()
                        .name("x")
                        .type()
                        .intType()
                        .noDefault()
                        .endRecord();
        org.apache.avro.Schema outerSchema =
                org.apache.avro.SchemaBuilder.record("Outer")
                        .fields()
                        .name("id")
                        .type()
                        .intType()
                        .noDefault()
                        .name("inner")
                        .type(innerSchema)
                        .noDefault()
                        .endRecord();
        TypeSerializer<org.apache.avro.generic.GenericRecord> avroSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, outerSchema);

        org.apache.avro.generic.GenericRecord innerRecord =
                new org.apache.avro.generic.GenericData.Record(innerSchema);
        innerRecord.put("x", 42);
        org.apache.avro.generic.GenericRecord outerRecord =
                new org.apache.avro.generic.GenericData.Record(outerSchema);
        outerRecord.put("id", 7);
        outerRecord.put("inner", innerRecord);

        StateInspectType innerRowType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField("x", StateInspectType.scalar("INT"))));
        StateInspectType outerRowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("inner", innerRowType)));

        StateInspectDecoder.DecodedRow row =
                decodeAvroValueStateWithSchema(
                        avroSerializer, outerSchema, outerRecord, outerRowType);

        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        Map<String, Object> byName = semanticFieldsByName((List<?>) valuePart.get("fields"));
        assertEquals(7, ((Map<?, ?>) byName.get("id")).get("value"));
        Map<?, ?> innerPart = (Map<?, ?>) byName.get("inner");
        assertEquals("ROW", innerPart.get("kind"));
        assertEquals(42, semanticFieldValue(innerPart, 0));
    }

    // ---- Array field ----

    @Test
    void decodesRecordWithArrayField() throws Exception {
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("WithArray")
                        .fields()
                        .name("tags")
                        .type()
                        .array()
                        .items()
                        .stringType()
                        .noDefault()
                        .endRecord();
        TypeSerializer<org.apache.avro.generic.GenericRecord> avroSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, schema);

        org.apache.avro.generic.GenericRecord record =
                new org.apache.avro.generic.GenericData.Record(schema);
        record.put("tags", Arrays.asList("a", "b", "c"));

        StateInspectType rowType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField(
                                        "tags",
                                        StateInspectType.list(
                                                StateInspectType.scalar("VARCHAR")))));

        StateInspectDecoder.DecodedRow row =
                decodeAvroValueStateWithSchema(avroSerializer, schema, record, rowType);

        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        Map<?, ?> tagsPart = (Map<?, ?>) ((List<?>) valuePart.get("fields")).get(0);
        assertEquals("LIST", tagsPart.get("kind"));
        List<?> values = (List<?>) tagsPart.get("values");
        assertEquals(3, values.size());
        assertEquals("a", ((Map<?, ?>) values.get(0)).get("value"));
        assertEquals("c", ((Map<?, ?>) values.get(2)).get("value"));
    }

    // ---- Map field ----

    @Test
    void decodesRecordWithMapField() throws Exception {
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("WithMap")
                        .fields()
                        .name("counts")
                        .type()
                        .map()
                        .values()
                        .intType()
                        .noDefault()
                        .endRecord();
        TypeSerializer<org.apache.avro.generic.GenericRecord> avroSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, schema);

        java.util.HashMap<java.lang.CharSequence, Integer> counts = new java.util.HashMap<>();
        counts.put("a", 1);
        counts.put("b", 2);
        org.apache.avro.generic.GenericRecord record =
                new org.apache.avro.generic.GenericData.Record(schema);
        record.put("counts", counts);

        StateInspectType rowType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField(
                                        "counts",
                                        StateInspectType.map(
                                                StateInspectType.scalar("VARCHAR"),
                                                StateInspectType.scalar("INT")))));

        StateInspectDecoder.DecodedRow row =
                decodeAvroValueStateWithSchema(avroSerializer, schema, record, rowType);

        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        Map<?, ?> countsPart = (Map<?, ?>) ((List<?>) valuePart.get("fields")).get(0);
        assertEquals("MAP", countsPart.get("kind"));
        List<?> entries = (List<?>) countsPart.get("entries");
        assertEquals(2, entries.size());
    }

    // ---- Enum field ----

    @Test
    void decodesRecordWithEnumField() throws Exception {
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("WithEnum")
                        .fields()
                        .name("status")
                        .type()
                        .enumeration("Status")
                        .symbols("ACTIVE", "INACTIVE")
                        .noDefault()
                        .endRecord();
        TypeSerializer<org.apache.avro.generic.GenericRecord> avroSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, schema);

        org.apache.avro.generic.GenericRecord record =
                new org.apache.avro.generic.GenericData.Record(schema);
        record.put(
                "status",
                new org.apache.avro.generic.GenericData.EnumSymbol(
                        schema.getField("status").schema(), "ACTIVE"));

        StateInspectType rowType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField(
                                        "status", StateInspectType.scalar("VARCHAR"))));

        StateInspectDecoder.DecodedRow row =
                decodeAvroValueStateWithSchema(avroSerializer, schema, record, rowType);

        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        assertEquals("ACTIVE", semanticFieldValue(valuePart, 0));
    }

    // ---- Fixed/bytes field ----

    @Test
    void decodesRecordWithFixedAndBytes() throws Exception {
        org.apache.avro.Schema fixedSchema =
                org.apache.avro.SchemaBuilder.builder().fixed("Hash").size(4);
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("WithFixed")
                        .fields()
                        .name("hash")
                        .type(fixedSchema)
                        .noDefault()
                        .name("data")
                        .type()
                        .bytesType()
                        .noDefault()
                        .endRecord();
        TypeSerializer<org.apache.avro.generic.GenericRecord> avroSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, schema);

        org.apache.avro.generic.GenericRecord record =
                new org.apache.avro.generic.GenericData.Record(schema);
        record.put(
                "hash",
                new org.apache.avro.generic.GenericData.Fixed(
                        fixedSchema, new byte[] {1, 2, 3, 4}));
        record.put("data", java.nio.ByteBuffer.wrap(new byte[] {5, 6, 7}));

        StateInspectType rowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("hash", StateInspectType.scalar("VARBINARY")),
                                new StateInspectField(
                                        "data", StateInspectType.scalar("VARBINARY"))));

        StateInspectDecoder.DecodedRow row =
                decodeAvroValueStateWithSchema(avroSerializer, schema, record, rowType);

        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        Map<String, Object> byName = semanticFieldsByName((List<?>) valuePart.get("fields"));
        assertNotNull(byName.get("hash"));
        assertNotNull(byName.get("data"));
    }

    // ---- Nullable union field ----

    @Test
    void decodesNullableUnionField() throws Exception {
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("WithNullable")
                        .fields()
                        .name("label")
                        .type()
                        .unionOf()
                        .nullType()
                        .and()
                        .stringType()
                        .endUnion()
                        .noDefault()
                        .endRecord();
        TypeSerializer<org.apache.avro.generic.GenericRecord> avroSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, schema);

        org.apache.avro.generic.GenericRecord record =
                new org.apache.avro.generic.GenericData.Record(schema);
        record.put("label", "present");

        StateInspectType rowType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField(
                                        "label", StateInspectType.scalar("VARCHAR"))));

        StateInspectDecoder.DecodedRow row =
                decodeAvroValueStateWithSchema(avroSerializer, schema, record, rowType);

        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        assertEquals("present", semanticFieldValue(valuePart, 0));
    }

    // ---- ListState of Avro records ----

    @Test
    void decodesListStateOfAvroRecords() throws Exception {
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("Item")
                        .fields()
                        .name("sku")
                        .type()
                        .intType()
                        .noDefault()
                        .endRecord();
        TypeSerializer<org.apache.avro.generic.GenericRecord> avroSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, schema);

        org.apache.avro.generic.GenericRecord r1 =
                new org.apache.avro.generic.GenericData.Record(schema);
        r1.put("sku", 10);
        org.apache.avro.generic.GenericRecord r2 =
                new org.apache.avro.generic.GenericData.Record(schema);
        r2.put("sku", 20);

        StateInspectSchema schemaObj =
                StateInspectSchema.forList(
                        "avro-list",
                        "cf-avro-list",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        avroSerializer);

        StateInspectType elementType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField("sku", StateInspectType.scalar("INT"))));
        InspectTarget target =
                semanticTarget(
                        schemaObj,
                        StateInspectSemanticSchema.forList(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                elementType));

        // Build list payload: element, delimiter, element, delimiter.
        java.io.ByteArrayOutputStream listOut = new java.io.ByteArrayOutputStream();
        listOut.write(serialize(avroSerializer, r1));
        listOut.write(',');
        listOut.write(serialize(avroSerializer, r2));
        listOut.write(',');
        byte[] listBytes = listOut.toByteArray();
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(target, rowKey, new byte[][] {listBytes});

        Map<?, ?> listPart = (Map<?, ?>) row.decodedParts.get("value");
        assertEquals("LIST", listPart.get("kind"));
        List<?> values = (List<?>) listPart.get("values");
        assertEquals(2, values.size());
        // Each element is a ROW with sku field.
        Map<?, ?> elem0 = (Map<?, ?>) values.get(0);
        assertEquals("ROW", elem0.get("kind"));
        assertEquals(10, semanticFieldValue(elem0, 0));
        Map<?, ?> elem1 = (Map<?, ?>) values.get(1);
        assertEquals(20, semanticFieldValue(elem1, 0));
    }

    // ---- MapState with Avro key and Avro value ----

    @Test
    void decodesMapStateWithAvroKeyAndValue() throws Exception {
        org.apache.avro.Schema keySchema =
                org.apache.avro.SchemaBuilder.record("KeyRec")
                        .fields()
                        .name("region")
                        .type()
                        .stringType()
                        .noDefault()
                        .endRecord();
        org.apache.avro.Schema valueSchema =
                org.apache.avro.SchemaBuilder.record("ValRec")
                        .fields()
                        .name("count")
                        .type()
                        .intType()
                        .noDefault()
                        .endRecord();
        TypeSerializer<org.apache.avro.generic.GenericRecord> keySerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, keySchema);
        TypeSerializer<org.apache.avro.generic.GenericRecord> valueSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, valueSchema);

        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "avro-map-kv",
                        "cf-avro-map-kv",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        keySerializer,
                        valueSerializer);

        StateInspectType keyType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField(
                                        "region", StateInspectType.scalar("VARCHAR"))));
        StateInspectType valueType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField("count", StateInspectType.scalar("INT"))));
        InspectTarget target =
                semanticTarget(
                        schema,
                        StateInspectSemanticSchema.forMap(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                keyType,
                                valueType));

        org.apache.avro.generic.GenericRecord keyRecord =
                new org.apache.avro.generic.GenericData.Record(keySchema);
        keyRecord.put("region", "us-west");
        org.apache.avro.generic.GenericRecord valueRecord =
                new org.apache.avro.generic.GenericData.Record(valueSchema);
        valueRecord.put("count", 99);

        byte[] mapKeyBytes = serialize(keySerializer, keyRecord);
        byte[] rowKey =
                mapKeyWithVoidNamespace(schema, serialize(IntSerializer.INSTANCE, 1), mapKeyBytes);
        byte[] mapValueColumn = mapValueBytes(valueSerializer, valueRecord);

        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(target, rowKey, new byte[][] {mapValueColumn});

        // Both map_key and map_value should decode as ROW.
        Map<?, ?> keyPart = (Map<?, ?>) row.decodedParts.get("map_key");
        assertEquals("ROW", keyPart.get("kind"));
        assertEquals("us-west", semanticFieldValue(keyPart, 0));

        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("map_value");
        assertEquals("ROW", valuePart.get("kind"));
        assertEquals(99, semanticFieldValue(valuePart, 0));
    }

    // ---- Complex union is PARTIALLY_CLASSLESS with fallback ----

    @Test
    void complexUnionIsPartiallyClasslessAndFallsBack() throws Exception {
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("ComplexUnion")
                        .fields()
                        .name("value")
                        .type()
                        .unionOf()
                        .stringType()
                        .and()
                        .intType()
                        .endUnion()
                        .noDefault()
                        .endRecord();
        TypeSerializer<org.apache.avro.generic.GenericRecord> avroSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, schema);

        // Verify descriptor is PARTIALLY_CLASSLESS.
        SerializerInspectSchema valueSchema =
                SerializerInspectSchema.fromSerializer(avroSerializer);
        assertNotNull(valueSchema.decoderDescriptor());
        assertTrue(valueSchema.decoderDescriptor().isAvro());
        assertEquals(
                DescriptorCapability.PARTIALLY_CLASSLESS,
                valueSchema.decoderDescriptor().capability());
        // Live serializer fallback retained.
        assertNotNull(valueSchema.serializedSerializerBytes());

        // Decode should still work via the restored serializer fallback (flink-avro is
        // test-scoped).
        org.apache.avro.generic.GenericRecord record =
                new org.apache.avro.generic.GenericData.Record(schema);
        record.put("value", "hello");

        StateInspectType rowType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField(
                                        "value", StateInspectType.scalar("VARCHAR"))));

        StateInspectDecoder.DecodedRow row =
                decodeAvroValueStateWithSchema(avroSerializer, schema, record, rowType);

        // The classless path may fail for complex union, but the restored serializer fallback
        // should produce the correct result.
        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        assertNotNull(valuePart);
    }

    // ---- Recursive schema is PARTIALLY_CLASSLESS ----

    @Test
    void recursiveSchemaIsPartiallyClassless() throws Exception {
        // Build a recursive schema manually (SchemaBuilder can't self-reference).
        org.apache.avro.Schema nodeSchema =
                org.apache.avro.Schema.createRecord("Node", null, null, false);
        org.apache.avro.Schema nextSchema =
                org.apache.avro.Schema.createUnion(
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                        nodeSchema);
        nodeSchema.setFields(
                java.util.Arrays.asList(
                        new org.apache.avro.Schema.Field(
                                "value",
                                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
                                null,
                                null),
                        new org.apache.avro.Schema.Field("next", nextSchema, null, null)));
        TypeSerializer<org.apache.avro.generic.GenericRecord> avroSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, nodeSchema);

        // Verify descriptor is PARTIALLY_CLASSLESS.
        SerializerInspectSchema valueSchema =
                SerializerInspectSchema.fromSerializer(avroSerializer);
        assertNotNull(valueSchema.decoderDescriptor());
        assertTrue(valueSchema.decoderDescriptor().isAvro());
        assertEquals(
                DescriptorCapability.PARTIALLY_CLASSLESS,
                valueSchema.decoderDescriptor().capability());
    }

    // ---- PARTIALLY_CLASSLESS without live serializer: classless may still succeed ----

    @Test
    void partiallyClasslessWithoutLiveSerializerMayStillDecode() throws Exception {
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("ComplexUnion2")
                        .fields()
                        .name("value")
                        .type()
                        .unionOf()
                        .stringType()
                        .and()
                        .intType()
                        .endUnion()
                        .noDefault()
                        .endRecord();
        TypeSerializer<org.apache.avro.generic.GenericRecord> avroSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, schema);

        // Build the state schema normally (with live serializer), then strip the live serializer
        // bytes to simulate a no-user-jar environment.
        StateInspectSchema stateSchema =
                StateInspectSchema.forValue(
                        "complex-no-jar",
                        "cf-complex-no-jar",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        avroSerializer);
        // Replace the value serializer with a descriptor-only version (no live serializer bytes).
        SerializerInspectSchema originalValueSchema = stateSchema.valueSerializer();
        SerializerInspectSchema descriptorOnlySchema = descriptorOnly(originalValueSchema);
        StateInspectSchema strippedSchema = withValueSerializer(stateSchema, descriptorOnlySchema);

        StateInspectType rowType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField(
                                        "value", StateInspectType.scalar("VARCHAR"))));
        InspectTarget target =
                semanticTarget(
                        strippedSchema,
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                rowType));

        org.apache.avro.generic.GenericRecord record =
                new org.apache.avro.generic.GenericData.Record(schema);
        record.put("value", "hello");
        byte[] valueBytes = serialize(avroSerializer, record);
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(target, rowKey, new byte[][] {valueBytes});

        // The classless path may succeed even for PARTIALLY_CLASSLESS schemas (GenericDatumReader
        // can handle complex unions). If it succeeds, the result should be correct with no
        // decode_error and the value should be present. If it fails, the error must reference the
        // value part (row-level fallback) since there is no live serializer.
        if (row.decodeError == null) {
            Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
            assertNotNull(valuePart, "value part should be present when decodeError is null");
        } else {
            assertTrue(
                    row.decodeError.contains("value"),
                    "decode_error should reference the value part: " + row.decodeError);
        }
    }

    // ---- FULLY_CLASSLESS success coexists with unavailable legacy preview ----

    @Test
    void fullyClasslessAvroSuccessHasNoDecodeError() throws Exception {
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("SimpleRecord")
                        .fields()
                        .name("name")
                        .type()
                        .stringType()
                        .noDefault()
                        .endRecord();
        TypeSerializer<org.apache.avro.generic.GenericRecord> avroSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, schema);

        StateInspectSchema stateSchema =
                StateInspectSchema.forValue(
                        "simple-avro",
                        "cf-simple-avro",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        avroSerializer);

        StateInspectType rowType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        InspectTarget target =
                semanticTarget(
                        stateSchema,
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                rowType));

        org.apache.avro.generic.GenericRecord record =
                new org.apache.avro.generic.GenericData.Record(schema);
        record.put("name", "alice");
        byte[] valueBytes = serialize(avroSerializer, record);
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(target, rowKey, new byte[][] {valueBytes});

        // Semantic Avro output should succeed. No false decode_error from the legacy preview
        // (which is suppressed for FULLY_CLASSLESS Avro).
        assertNull(row.decodeError);
        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        assertNotNull(valuePart);
        assertEquals("alice", semanticFieldValue(valuePart, 0));
        // Legacy decodedValue should be null (suppressed for FULLY_CLASSLESS).
        assertNull(row.decodedValue);
    }

    // ---- MapState present-null value ----

    @Test
    void mapStatePresentNullAvroValueDecodesAsNull() throws Exception {
        org.apache.avro.Schema keySchema =
                org.apache.avro.SchemaBuilder.record("KeyRec2")
                        .fields()
                        .name("region")
                        .type()
                        .stringType()
                        .noDefault()
                        .endRecord();
        org.apache.avro.Schema valueSchema =
                org.apache.avro.SchemaBuilder.record("ValRec2")
                        .fields()
                        .name("count")
                        .type()
                        .intType()
                        .noDefault()
                        .endRecord();
        TypeSerializer<org.apache.avro.generic.GenericRecord> keySerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, keySchema);
        TypeSerializer<org.apache.avro.generic.GenericRecord> valueSerializer =
                new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                        org.apache.avro.generic.GenericRecord.class, valueSchema);

        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "avro-map-null",
                        "cf-avro-map-null",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        keySerializer,
                        valueSerializer);

        StateInspectType keyType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField(
                                        "region", StateInspectType.scalar("VARCHAR"))));
        StateInspectType valueType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField("count", StateInspectType.scalar("INT"))));
        InspectTarget target =
                semanticTarget(
                        schema,
                        StateInspectSemanticSchema.forMap(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                keyType,
                                valueType));

        org.apache.avro.generic.GenericRecord keyRecord =
                new org.apache.avro.generic.GenericData.Record(keySchema);
        keyRecord.put("region", "us-east");
        byte[] mapKeyBytes = serialize(keySerializer, keyRecord);
        byte[] rowKey =
                mapKeyWithVoidNamespace(schema, serialize(IntSerializer.INSTANCE, 1), mapKeyBytes);
        // Present-null: column exists with 0x01 marker.
        byte[] presentNullColumn = new byte[] {0x01};

        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(target, rowKey, new byte[][] {presentNullColumn});

        // map_key should still decode; map_value should be null (present-null).
        Map<?, ?> keyPart = (Map<?, ?>) row.decodedParts.get("map_key");
        assertNotNull(keyPart);
        assertEquals("us-east", semanticFieldValue(keyPart, 0));
        assertTrue(row.decodedParts.containsKey("map_value"));
        assertNull(row.decodedParts.get("map_value"));
    }

    @Test
    void oversizedSchemaIsNotRetainedByTheCache() throws Exception {
        clearSchemaCache();
        StringBuilder doc = new StringBuilder(4 * 1024 * 1024 + 1);
        for (int index = 0; index <= 4 * 1024 * 1024; index++) {
            doc.append('x');
        }
        String schemaJson =
                "{\"type\":\"record\",\"name\":\"OversizedDoc\",\"doc\":\""
                        + doc
                        + "\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}";
        InspectDecoderDescriptor descriptor =
                InspectDecoderDescriptor.avro(
                        schemaJson,
                        InspectDecoderDescriptor.AVRO_WIRE_FORMAT_FLINK_DATA_INPUT_V1,
                        DescriptorCapability.FULLY_CLASSLESS);
        DataOutputSerializer output = new DataOutputSerializer(8);
        output.writeInt(1);

        AvroClasslessDecoder.decode(descriptor, output.getCopyOfBuffer());

        assertEquals(0, schemaCacheSize());
    }

    // ---- Regression: portable RowData still works ----

    @Test
    void portableTupleRegressionStillWorks() throws Exception {
        @SuppressWarnings("unchecked")
        TypeSerializer<org.apache.flink.api.java.tuple.Tuple2<Integer, String>> tupleSerializer =
                (TypeSerializer<org.apache.flink.api.java.tuple.Tuple2<Integer, String>>)
                        new org.apache.flink.api.java.typeutils.TupleTypeInfo(
                                        org.apache.flink.api.common.typeinfo.Types.INT,
                                        org.apache.flink.api.common.typeinfo.Types.STRING)
                                .createSerializer(
                                        new org.apache.flink.api.common.ExecutionConfig());

        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "tuple-regression",
                        "cf-tuple",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        tupleSerializer);

        StateInspectType tupleType =
                StateInspectType.tuple(
                        Arrays.asList(
                                new StateInspectField("f0", StateInspectType.scalar("INT")),
                                new StateInspectField("f1", StateInspectType.scalar("VARCHAR"))));
        InspectTarget target =
                semanticTarget(
                        schema,
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                tupleType));

        byte[] valueBytes =
                serialize(tupleSerializer, org.apache.flink.api.java.tuple.Tuple2.of(30, "bob"));
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(target, rowKey, new byte[][] {valueBytes});

        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        assertEquals("TUPLE", valuePart.get("kind"));
        assertEquals(30, semanticFieldValue(valuePart, 0));
        assertEquals("bob", semanticFieldValue(valuePart, 1));
    }

    // ---- Helpers ----

    private static <T> Map<?, ?> decodeAvroValueState(
            TypeSerializer<T> avroSerializer, org.apache.avro.Schema schema, T record)
            throws Exception {
        StateInspectType rowType =
                StateInspectType.row(
                        schema.getFields().stream()
                                .map(
                                        f ->
                                                new StateInspectField(
                                                        f.name(),
                                                        StateInspectType.scalar("VARCHAR")))
                                .collect(java.util.stream.Collectors.toList()));
        StateInspectDecoder.DecodedRow row =
                decodeAvroValueStateWithSchema(avroSerializer, schema, record, rowType);
        return (Map<?, ?>) row.decodedParts.get("value");
    }

    @SuppressWarnings("unchecked")
    private static void clearSchemaCache() throws Exception {
        Field lockField = AvroClasslessDecoder.class.getDeclaredField("SCHEMA_CACHE_LOCK");
        Field cacheField = AvroClasslessDecoder.class.getDeclaredField("SCHEMA_CACHE");
        Field byteCountField =
                AvroClasslessDecoder.class.getDeclaredField("cachedSchemaSourceBytes");
        lockField.setAccessible(true);
        cacheField.setAccessible(true);
        byteCountField.setAccessible(true);
        Object lock = lockField.get(null);
        synchronized (lock) {
            ((Map<Object, Object>) cacheField.get(null)).clear();
            byteCountField.setInt(null, 0);
        }
    }

    private static int schemaCacheSize() throws Exception {
        Field lockField = AvroClasslessDecoder.class.getDeclaredField("SCHEMA_CACHE_LOCK");
        Field cacheField = AvroClasslessDecoder.class.getDeclaredField("SCHEMA_CACHE");
        lockField.setAccessible(true);
        cacheField.setAccessible(true);
        Object lock = lockField.get(null);
        synchronized (lock) {
            return ((Map<?, ?>) cacheField.get(null)).size();
        }
    }

    private static <T> StateInspectDecoder.DecodedRow decodeAvroValueStateWithSchema(
            TypeSerializer<T> avroSerializer,
            org.apache.avro.Schema schema,
            T record,
            StateInspectType rowType)
            throws Exception {
        StateInspectSchema stateSchema =
                StateInspectSchema.forValue(
                        "avro-test",
                        "cf-avro-test",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        avroSerializer);
        InspectTarget target =
                semanticTarget(
                        stateSchema,
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                rowType));
        byte[] valueBytes = serialize(avroSerializer, record);
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));
        return StateInspectDecoder.decode(target, rowKey, new byte[][] {valueBytes});
    }

    private static <T> byte[] serialize(TypeSerializer<T> serializer, T value) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);
        return out.getCopyOfBuffer();
    }

    private static byte[] keyWithVoidNamespace(byte[] stateKeyBytes) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(stateKeyBytes.length + 1);
        output.write(stateKeyBytes);
        output.writeByte(0); // void namespace marker
        return output.getCopyOfBuffer();
    }

    private static byte[] mapKeyWithVoidNamespace(
            StateInspectSchema schema, byte[] stateKeyBytes, byte[] mapKeyBytes) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(128);
        output.write(stateKeyBytes);
        output.writeByte(0); // void namespace marker
        output.writeByte(0); // void map namespace marker
        output.write(mapKeyBytes);
        if (schema.mapKeyLengthStored()) {
            output.writeInt(stateKeyBytes.length);
        }
        if (schema.mapNamespaceLengthStored()) {
            output.writeInt(1);
        }
        return output.getCopyOfBuffer();
    }

    private static <T> byte[] mapValueBytes(TypeSerializer<T> serializer, T value)
            throws Exception {
        byte[] payload = serialize(serializer, value);
        byte[] row = new byte[payload.length + 1];
        row[0] = 0x00; // not null
        System.arraycopy(payload, 0, row, 1, payload.length);
        return row;
    }

    private static InspectTarget semanticTarget(
            StateInspectSchema schema, StateInspectSemanticSchema semanticSchema) {
        return new InspectTarget(
                schema.stateName(),
                schema.stateName(),
                "state",
                schema.columnFamily(),
                false,
                schema.stateKind().name(),
                java.util.Collections.emptyMap(),
                schema,
                semanticSchema,
                null);
    }

    private static Map<String, Object> semanticFieldsByName(List<?> fields) {
        Map<String, Object> byName = new LinkedHashMap<>();
        for (Object f : fields) {
            Map<?, ?> m = (Map<?, ?>) f;
            byName.put((String) m.get("name"), m);
        }
        return byName;
    }

    private static Object semanticFieldValue(Map<?, ?> part, int fieldIndex) {
        List<?> fields = (List<?>) part.get("fields");
        return ((Map<?, ?>) fields.get(fieldIndex)).get("value");
    }

    private static StateInspectSchema withValueSerializer(
            StateInspectSchema schema, SerializerInspectSchema newValueSerializer)
            throws Exception {
        java.lang.reflect.Field field =
                StateInspectSchema.class.getDeclaredField("valueSerializer");
        field.setAccessible(true);
        field.set(schema, newValueSerializer);
        return schema;
    }

    private static SerializerInspectSchema descriptorOnly(SerializerInspectSchema original)
            throws Exception {
        java.lang.reflect.Constructor<SerializerInspectSchema> constructor =
                SerializerInspectSchema.class.getDeclaredConstructor(
                        String.class,
                        int.class,
                        String.class,
                        byte[].class,
                        StateInspectType.class,
                        byte[].class,
                        io.cobble.flink.common.inspect.InspectDecoderDescriptor.class);
        constructor.setAccessible(true);
        return constructor.newInstance(
                original.serializerClassName(),
                original.lengthTag(),
                original.snapshotClassName(),
                original.snapshotBytes(),
                original.inspectType(),
                null, // strip serialized serializer bytes
                original.decoderDescriptor());
    }
}
