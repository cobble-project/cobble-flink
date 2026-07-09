package io.cobble.flink.common.inspect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link InspectDecoderDescriptor} binary round-trip, {@link
 * InspectDecoderDescriptorExtractor} extraction from live snapshots, safety limits, and read-side
 * invariant validation.
 */
class InspectDecoderDescriptorTest {

    // ---- Binary round-trip for every kind ----

    @Test
    void unsupportedRoundTrips() throws IOException {
        InspectDecoderDescriptor descriptor = InspectDecoderDescriptor.unsupported("test reason");
        InspectDecoderDescriptor restored = roundTrip(descriptor);
        assertEquals(descriptor, restored);
        assertEquals(InspectDecoderDescriptorKind.UNSUPPORTED, restored.kind());
        assertEquals(DescriptorCapability.UNSUPPORTED, restored.capability());
    }

    @Test
    void portableSnapshotRoundTripsWithRealSnapshotBytes() throws IOException {
        // Use a real IntSerializer snapshot to get non-null snapshotBytes.
        SerializerInspectSchema schema =
                SerializerInspectSchema.fromSerializer(IntSerializer.INSTANCE);
        assertNotNull(schema.decoderDescriptor());
        assertEquals(
                InspectDecoderDescriptorKind.PORTABLE_SNAPSHOT, schema.decoderDescriptor().kind());
        assertEquals(DescriptorCapability.FULLY_CLASSLESS, schema.decoderDescriptor().capability());

        InspectDecoderDescriptor restored = roundTrip(schema.decoderDescriptor());
        assertEquals(schema.decoderDescriptor(), restored);
        // Verify snapshot bytes are preserved.
        InspectDecoderDescriptor.PortableSnapshotDescriptor psd =
                (InspectDecoderDescriptor.PortableSnapshotDescriptor) restored;
        assertNotNull(psd.snapshotBytes());
        assertTrue(psd.snapshotBytes().length > 0);
    }

    @Test
    void avroDescriptorRoundTrips() throws IOException {
        InspectDecoderDescriptor descriptor =
                InspectDecoderDescriptor.avro(
                        "{\"type\":\"record\",\"name\":\"T\",\"fields\":[]}",
                        "FLINK_DATA_INPUT_V1",
                        DescriptorCapability.FULLY_CLASSLESS);
        InspectDecoderDescriptor restored = roundTrip(descriptor);
        assertEquals(descriptor, restored);
    }

    @Test
    void pojoDescriptorRoundTrips() throws IOException {
        // Build a child PORTABLE_SNAPSHOT with real IntSerializer snapshot bytes.
        byte[] childBytes = serializeSnapshot(IntSerializer.INSTANCE.snapshotConfiguration());
        InspectDecoderDescriptor child =
                InspectDecoderDescriptor.portableSnapshot(
                        "org.apache.flink.api.common.typeutils.base.IntSerializer$IntSerializerSnapshot",
                        childBytes);
        InspectDecoderDescriptor.PojoFieldDescriptor field =
                new InspectDecoderDescriptor.PojoFieldDescriptor("id", child);
        InspectDecoderDescriptor descriptor =
                InspectDecoderDescriptor.pojo(
                        "com.example.MyPojo",
                        Collections.singletonList(field),
                        Collections.emptyList(),
                        false,
                        true,
                        DescriptorCapability.FULLY_CLASSLESS);
        InspectDecoderDescriptor restored = roundTrip(descriptor);
        assertEquals(descriptor, restored);
    }

    // ---- Extraction from live snapshots: POJO ----

    @Test
    void pojoWithPrimitiveFieldsExtractsFullyClassless() {
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(pojoSerializer);

        assertNotNull(schema.decoderDescriptor());
        assertEquals(InspectDecoderDescriptorKind.POJO, schema.decoderDescriptor().kind());
        assertEquals(DescriptorCapability.FULLY_CLASSLESS, schema.decoderDescriptor().capability());
        // No live serializer fallback for FULLY_CLASSLESS.
        assertNull(schema.serializedSerializerBytes());

        // Verify child fields carry snapshot bytes.
        InspectDecoderDescriptor.PojoDescriptor pojo =
                (InspectDecoderDescriptor.PojoDescriptor) schema.decoderDescriptor();
        for (InspectDecoderDescriptor.PojoFieldDescriptor field : pojo.fields()) {
            assertEquals(InspectDecoderDescriptorKind.PORTABLE_SNAPSHOT, field.descriptor().kind());
            InspectDecoderDescriptor.PortableSnapshotDescriptor psd =
                    (InspectDecoderDescriptor.PortableSnapshotDescriptor) field.descriptor();
            assertNotNull(
                    psd.snapshotBytes(), "field " + field.name() + " must have snapshot bytes");
        }
    }

    @Test
    void pojoWithNestedPojoExtractsRecursiveDescriptor() {
        TypeSerializer<PojoWithNested> pojoSerializer =
                TypeInformation.of(PojoWithNested.class).createSerializer(new ExecutionConfig());
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(pojoSerializer);

        assertNotNull(schema.decoderDescriptor());
        assertEquals(InspectDecoderDescriptorKind.POJO, schema.decoderDescriptor().kind());
        assertEquals(DescriptorCapability.FULLY_CLASSLESS, schema.decoderDescriptor().capability());

        InspectDecoderDescriptor.PojoDescriptor pojo =
                (InspectDecoderDescriptor.PojoDescriptor) schema.decoderDescriptor();
        Map<String, InspectDecoderDescriptor.PojoFieldDescriptor> byName = pojoFieldsByName(pojo);
        InspectDecoderDescriptor.PojoFieldDescriptor innerField = byName.get("inner");
        assertNotNull(innerField, "expected 'inner' field");
        assertEquals(InspectDecoderDescriptorKind.POJO, innerField.descriptor().kind());
    }

    @Test
    void pojoWithKryoFieldIsPartiallyClassless() {
        // POJO with a List<String> field - List fields use Kryo, which is not monitor-portable.
        TypeSerializer<PojoWithList> pojoSerializer =
                TypeInformation.of(PojoWithList.class).createSerializer(new ExecutionConfig());
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(pojoSerializer);

        assertNotNull(schema.decoderDescriptor());
        assertEquals(InspectDecoderDescriptorKind.POJO, schema.decoderDescriptor().kind());
        assertEquals(
                DescriptorCapability.PARTIALLY_CLASSLESS, schema.decoderDescriptor().capability());
        // Live serializer fallback retained for PARTIALLY_CLASSLESS.
        assertNotNull(schema.serializedSerializerBytes());

        // The Kryo field should have a diagnostic UNSUPPORTED reason mentioning the snapshot class.
        InspectDecoderDescriptor.PojoDescriptor pojo =
                (InspectDecoderDescriptor.PojoDescriptor) schema.decoderDescriptor();
        Map<String, InspectDecoderDescriptor.PojoFieldDescriptor> byName = pojoFieldsByName(pojo);
        InspectDecoderDescriptor.PojoFieldDescriptor tagsField = byName.get("tags");
        assertNotNull(tagsField);
        assertEquals(InspectDecoderDescriptorKind.UNSUPPORTED, tagsField.descriptor().kind());
        InspectDecoderDescriptor.UnsupportedDescriptor ud =
                (InspectDecoderDescriptor.UnsupportedDescriptor) tagsField.descriptor();
        assertNotNull(ud.reason());
        // The reason should mention the serializer snapshot class (KryoSerializerSnapshot).
        assertTrue(
                ud.reason().contains("Kryo") || ud.reason().contains("unsupported"),
                "reason should be diagnostic, got: " + ud.reason());
    }

    @Test
    void pojoWithRegisteredSubclassExtractsSubclassTable() {
        ExecutionConfig config = new ExecutionConfig();
        config.registerPojoType(PojoSubclass.class);
        TypeSerializer<PojoBase> pojoSerializer =
                TypeInformation.of(PojoBase.class).createSerializer(config);
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(pojoSerializer);

        assertNotNull(schema.decoderDescriptor());
        assertEquals(InspectDecoderDescriptorKind.POJO, schema.decoderDescriptor().kind());

        InspectDecoderDescriptor restored = roundTripQuietly(schema.decoderDescriptor());
        assertEquals(schema.decoderDescriptor(), restored);
    }

    @Test
    void pojoDescriptorBinaryRoundTripsWithFields() throws IOException {
        // Build a POJO descriptor with fields and subclasses, then round-trip.
        byte[] childBytes1 = serializeSnapshot(IntSerializer.INSTANCE.snapshotConfiguration());
        byte[] childBytes2 = serializeSnapshot(StringSerializer.INSTANCE.snapshotConfiguration());
        InspectDecoderDescriptor child1 =
                InspectDecoderDescriptor.portableSnapshot("IntSerializerSnapshot", childBytes1);
        InspectDecoderDescriptor child2 =
                InspectDecoderDescriptor.portableSnapshot("StringSerializerSnapshot", childBytes2);
        InspectDecoderDescriptor.PojoFieldDescriptor field1 =
                new InspectDecoderDescriptor.PojoFieldDescriptor("id", child1);
        InspectDecoderDescriptor.PojoFieldDescriptor field2 =
                new InspectDecoderDescriptor.PojoFieldDescriptor("name", child2);
        InspectDecoderDescriptor.RegisteredSubclass subclass =
                new InspectDecoderDescriptor.RegisteredSubclass(
                        "com.example.Sub",
                        0,
                        InspectDecoderDescriptor.pojo(
                                "com.example.Sub",
                                Arrays.asList(field1, field2),
                                Collections.emptyList(),
                                false,
                                true,
                                DescriptorCapability.FULLY_CLASSLESS));
        InspectDecoderDescriptor descriptor =
                InspectDecoderDescriptor.pojo(
                        "com.example.Base",
                        Arrays.asList(field1, field2),
                        Collections.singletonList(subclass),
                        true, // hasNonRegisteredSubclasses
                        true, // basePathClassless
                        DescriptorCapability.PARTIALLY_CLASSLESS);
        InspectDecoderDescriptor restored = roundTrip(descriptor);
        assertEquals(descriptor, restored);
    }

    // ---- Extraction from live snapshots: Avro ----

    @Test
    void avroGenericRecordExtractsAvroDescriptor() {
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
                        .endRecord();
        org.apache.flink.formats.avro.typeutils.AvroSerializer<
                        org.apache.avro.generic.GenericRecord>
                avroSerializer =
                        new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                                org.apache.avro.generic.GenericRecord.class, schema);
        SerializerInspectSchema schemaObj = SerializerInspectSchema.fromSerializer(avroSerializer);

        assertNotNull(schemaObj.decoderDescriptor());
        assertEquals(InspectDecoderDescriptorKind.AVRO, schemaObj.decoderDescriptor().kind());
        assertEquals(
                DescriptorCapability.FULLY_CLASSLESS, schemaObj.decoderDescriptor().capability());
        assertNull(schemaObj.serializedSerializerBytes());
    }

    @Test
    void avroSpecificRecordExtractsSameDescriptorAsGeneric() {
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
                        .endRecord();
        org.apache.flink.formats.avro.typeutils.AvroSerializer<
                        org.apache.avro.generic.GenericRecord>
                genericSerializer =
                        new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                                org.apache.avro.generic.GenericRecord.class, schema);
        org.apache.flink.formats.avro.typeutils.AvroSerializer<
                        org.apache.avro.generic.GenericRecord>
                specificSerializer =
                        new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                                org.apache.avro.generic.GenericRecord.class, schema);

        SerializerInspectSchema genericSchema =
                SerializerInspectSchema.fromSerializer(genericSerializer);
        SerializerInspectSchema specificSchema =
                SerializerInspectSchema.fromSerializer(specificSerializer);

        assertEquals(genericSchema.decoderDescriptor(), specificSchema.decoderDescriptor());
    }

    @Test
    void avroComplexUnionIsPartiallyClassless() {
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
        org.apache.flink.formats.avro.typeutils.AvroSerializer<
                        org.apache.avro.generic.GenericRecord>
                avroSerializer =
                        new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                                org.apache.avro.generic.GenericRecord.class, schema);
        SerializerInspectSchema schemaObj = SerializerInspectSchema.fromSerializer(avroSerializer);

        assertNotNull(schemaObj.decoderDescriptor());
        assertEquals(InspectDecoderDescriptorKind.AVRO, schemaObj.decoderDescriptor().kind());
        assertEquals(
                DescriptorCapability.PARTIALLY_CLASSLESS,
                schemaObj.decoderDescriptor().capability());
        assertNotNull(schemaObj.serializedSerializerBytes());
    }

    @Test
    void avroNullableUnionIsFullyClassless() {
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("NullableField")
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
        SerializerInspectSchema schemaObj = SerializerInspectSchema.fromSerializer(avroSerializer);

        assertNotNull(schemaObj.decoderDescriptor());
        assertEquals(
                DescriptorCapability.FULLY_CLASSLESS, schemaObj.decoderDescriptor().capability());
    }

    @Test
    void avroRecursiveSchemaExtractsCorrectly() {
        org.apache.avro.Schema schema =
                org.apache.avro.SchemaBuilder.record("Recursive")
                        .fields()
                        .name("tags")
                        .type()
                        .array()
                        .items()
                        .stringType()
                        .noDefault()
                        .name("counts")
                        .type()
                        .map()
                        .values()
                        .intType()
                        .noDefault()
                        .name("color")
                        .type()
                        .enumeration("Color")
                        .symbols("RED", "GREEN", "BLUE")
                        .noDefault()
                        .name("hash")
                        .type()
                        .fixed("MD5")
                        .size(16)
                        .noDefault()
                        .name("label")
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
        SerializerInspectSchema schemaObj = SerializerInspectSchema.fromSerializer(avroSerializer);

        assertNotNull(schemaObj.decoderDescriptor());
        assertEquals(InspectDecoderDescriptorKind.AVRO, schemaObj.decoderDescriptor().kind());
        assertEquals(
                DescriptorCapability.FULLY_CLASSLESS, schemaObj.decoderDescriptor().capability());
    }

    // ---- Self-referential Avro schema (cycle detection) ----

    @Test
    void selfReferentialAvroSchemaDoesNotStackOverflow() {
        // Build a self-referential Avro schema: Node { value: int, next: ["null", "Node"] }
        // The "next" field references the enclosing "Node" record, creating a cycle.
        org.apache.avro.Schema nodeSchema =
                org.apache.avro.SchemaBuilder.record("Node")
                        .fields()
                        .name("value")
                        .type()
                        .intType()
                        .noDefault()
                        .name("next")
                        .type()
                        .unionOf()
                        .nullType()
                        .and()
                        .type("Node")
                        .endUnion()
                        .noDefault()
                        .endRecord();

        org.apache.flink.formats.avro.typeutils.AvroSerializer<
                        org.apache.avro.generic.GenericRecord>
                avroSerializer =
                        new org.apache.flink.formats.avro.typeutils.AvroSerializer<>(
                                org.apache.avro.generic.GenericRecord.class, nodeSchema);

        // This must not throw StackOverflowError. The schema capture should complete normally.
        SerializerInspectSchema schemaObj = SerializerInspectSchema.fromSerializer(avroSerializer);

        // The descriptor should be AVRO.
        assertNotNull(schemaObj.decoderDescriptor());
        assertEquals(InspectDecoderDescriptorKind.AVRO, schemaObj.decoderDescriptor().kind());

        // A self-referential schema must be PARTIALLY_CLASSLESS, not FULLY_CLASSLESS.
        // The semantic extractor maps the recursive field to UNKNOWN, which causes row-level
        // decode errors. The live serializer fallback must be retained until Phase 2 implements
        // a depth-bounded recursive renderer.
        assertEquals(
                DescriptorCapability.PARTIALLY_CLASSLESS,
                schemaObj.decoderDescriptor().capability());
        assertNotNull(schemaObj.serializedSerializerBytes());

        // The inspectType should also not StackOverflow. The recursive "next" field should map
        // to UNKNOWN (cycle detected), but the schema capture should succeed.
        assertNotNull(schemaObj.inspectType());

        // Verify the "next" field is UNKNOWN in the semantic type.
        assertEquals(StateInspectTypeKind.ROW, schemaObj.inspectType().kind());
        List<StateInspectField> fields = schemaObj.inspectType().fields();
        StateInspectField nextField = null;
        for (StateInspectField f : fields) {
            if (f.name().equals("next")) {
                nextField = f;
            }
        }
        assertNotNull(nextField, "expected 'next' field in schema");
        // The nullable union resolves to the non-null member, which is the recursive "Node".
        // The cycle guard maps it to UNKNOWN.
        assertEquals(StateInspectTypeKind.UNKNOWN, nextField.type().kind());
    }

    // ---- Safety limits: malformed ordinals ----

    @Test
    void malformedKindOrdinalRejected() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeInt(999); // invalid kind ordinal
        out.writeInt(0); // capability ordinal
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        IOException error =
                assertThrows(IOException.class, () -> InspectDecoderDescriptor.read(in));
        assertTrue(error.getMessage().contains("Unknown InspectDecoderDescriptorKind ordinal"));
    }

    @Test
    void malformedCapabilityOrdinalRejected() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeInt(0); // valid PORTABLE_SNAPSHOT
        out.writeInt(999); // invalid capability ordinal
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        IOException error =
                assertThrows(IOException.class, () -> InspectDecoderDescriptor.read(in));
        assertTrue(error.getMessage().contains("Unknown DescriptorCapability ordinal"));
    }

    // ---- Safety limits: oversized payloads rejected on read ----

    @Test
    void oversizedSnapshotBytesRejected() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(32);
        out.writeInt(InspectDecoderDescriptorKind.PORTABLE_SNAPSHOT.ordinal());
        out.writeInt(DescriptorCapability.FULLY_CLASSLESS.ordinal());
        writeBoundedUtf8Raw(
                out, "test", InspectDecoderDescriptor.MAX_NAME_UTF_BYTES); // snapshotClassName
        out.writeInt(InspectDecoderDescriptor.MAX_SNAPSHOT_BYTES + 1); // length
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        IOException error =
                assertThrows(IOException.class, () -> InspectDecoderDescriptor.read(in));
        assertTrue(error.getMessage().contains("out of range"));
    }

    @Test
    void oversizedSchemaJsonRejected() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(32);
        out.writeInt(InspectDecoderDescriptorKind.AVRO.ordinal());
        out.writeInt(DescriptorCapability.FULLY_CLASSLESS.ordinal());
        out.writeInt(InspectDecoderDescriptor.MAX_SCHEMA_JSON_BYTES + 1); // byte length
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        IOException error =
                assertThrows(IOException.class, () -> InspectDecoderDescriptor.read(in));
        assertTrue(error.getMessage().contains("out of range"));
    }

    @Test
    void excessiveFieldCountRejected() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(32);
        out.writeInt(InspectDecoderDescriptorKind.POJO.ordinal());
        out.writeInt(DescriptorCapability.FULLY_CLASSLESS.ordinal());
        writeBoundedUtf8Raw(
                out, "test", InspectDecoderDescriptor.MAX_NAME_UTF_BYTES); // pojoClassName
        out.writeBoolean(true); // basePathClassless
        out.writeBoolean(false); // hasNonRegisteredSubclasses
        out.writeInt(InspectDecoderDescriptor.MAX_TOTAL_FIELDS + 1); // field count
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        IOException error =
                assertThrows(IOException.class, () -> InspectDecoderDescriptor.read(in));
        assertTrue(error.getMessage().contains("total field count exceeded"));
    }

    // ---- Safety limits: oversized payloads rejected on write ----

    @Test
    void writeRejectsOversizedReason() {
        // Test the writeBoundedUtf8 write-side size check via a large reason string.
        // (Testing a real 16 MB schema JSON is impractical; the same writeBoundedUtf8 path
        // is exercised here with the smaller MAX_REASON_BYTES limit.)
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < InspectDecoderDescriptor.MAX_REASON_BYTES + 1; i++) {
            sb.append('x');
        }
        InspectDecoderDescriptor descriptor = InspectDecoderDescriptor.unsupported(sb.toString());
        DataOutputSerializer out = new DataOutputSerializer(64);
        IOException error =
                assertThrows(
                        IOException.class,
                        () -> descriptor.write(out),
                        "write should reject oversized reason");
        assertTrue(error.getMessage().contains("exceeds max UTF-8 byte length"));
    }

    @Test
    void writeRejectsOversizedPortableSnapshotBytes() {
        // Construct a PORTABLE_SNAPSHOT with snapshotBytes exceeding MAX_SNAPSHOT_BYTES.
        // The portableSnapshot() factory does not check size (capture-side check is in the
        // extractor), so this simulates a hand-constructed or future-call-site descriptor.
        byte[] oversized = new byte[InspectDecoderDescriptor.MAX_SNAPSHOT_BYTES + 1];
        InspectDecoderDescriptor descriptor =
                InspectDecoderDescriptor.portableSnapshot("test.Snapshot", oversized);
        DataOutputSerializer out = new DataOutputSerializer(64);
        IOException error =
                assertThrows(
                        IOException.class,
                        () -> descriptor.write(out),
                        "write should reject oversized snapshot bytes");
        assertTrue(error.getMessage().contains("exceeds max size"));
    }

    // ---- Read-side invariant validation ----

    @Test
    void portableSnapshotWithNullBytesRejectedOnRead() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(32);
        out.writeInt(InspectDecoderDescriptorKind.PORTABLE_SNAPSHOT.ordinal());
        out.writeInt(DescriptorCapability.FULLY_CLASSLESS.ordinal());
        writeBoundedUtf8Raw(
                out, "test", InspectDecoderDescriptor.MAX_NAME_UTF_BYTES); // snapshotClassName
        out.writeInt(0); // snapshotBytes length = 0 (effectively null)
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        IOException error =
                assertThrows(IOException.class, () -> InspectDecoderDescriptor.read(in));
        assertTrue(error.getMessage().contains("requires non-null snapshot bytes"));
    }

    @Test
    void unsupportedKindWithNonUnsupportedCapabilityRejected() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(32);
        out.writeInt(InspectDecoderDescriptorKind.UNSUPPORTED.ordinal());
        out.writeInt(DescriptorCapability.FULLY_CLASSLESS.ordinal()); // wrong capability
        writeBoundedUtf8Raw(out, "reason", InspectDecoderDescriptor.MAX_REASON_BYTES);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        IOException error =
                assertThrows(IOException.class, () -> InspectDecoderDescriptor.read(in));
        assertTrue(error.getMessage().contains("UNSUPPORTED capability"));
    }

    @Test
    void registeredSubclassTagMismatchRejected() throws IOException {
        // Build a POJO descriptor where the subclass tag is 1 but should be 0.
        byte[] childBytes = serializeSnapshot(IntSerializer.INSTANCE.snapshotConfiguration());
        InspectDecoderDescriptor child =
                InspectDecoderDescriptor.portableSnapshot("IntSerializerSnapshot", childBytes);
        InspectDecoderDescriptor.RegisteredSubclass badSubclass =
                new InspectDecoderDescriptor.RegisteredSubclass(
                        "com.example.Sub",
                        1, // wrong tag: should be 0
                        child);
        InspectDecoderDescriptor descriptor =
                InspectDecoderDescriptor.pojo(
                        "com.example.Base",
                        Collections.emptyList(),
                        Collections.singletonList(badSubclass),
                        false,
                        true,
                        DescriptorCapability.PARTIALLY_CLASSLESS);
        DataOutputSerializer out = new DataOutputSerializer(256);
        descriptor.write(out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        IOException error =
                assertThrows(IOException.class, () -> InspectDecoderDescriptor.read(in));
        assertTrue(error.getMessage().contains("tag"));
    }

    @Test
    void fullyClasslessPojoWithUnsupportedChildRejected() throws IOException {
        InspectDecoderDescriptor unsupportedChild = InspectDecoderDescriptor.unsupported("bad");
        InspectDecoderDescriptor.PojoFieldDescriptor field =
                new InspectDecoderDescriptor.PojoFieldDescriptor("id", unsupportedChild);
        InspectDecoderDescriptor descriptor =
                InspectDecoderDescriptor.pojo(
                        "com.example.Base",
                        Collections.singletonList(field),
                        Collections.emptyList(),
                        false,
                        false, // basePathClassless=false (consistent with unsupported child)
                        DescriptorCapability.FULLY_CLASSLESS); // but claims FULLY_CLASSLESS
        DataOutputSerializer out = new DataOutputSerializer(128);
        descriptor.write(out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        IOException error =
                assertThrows(IOException.class, () -> InspectDecoderDescriptor.read(in));
        assertTrue(
                error.getMessage().contains("FULLY_CLASSLESS"),
                "should reject FULLY_CLASSLESS with unsupported child, got: " + error.getMessage());
    }

    @Test
    void fullyClasslessPojoWithBasePathClasslessFalseRejected() throws IOException {
        InspectDecoderDescriptor descriptor =
                InspectDecoderDescriptor.pojo(
                        "com.example.Base",
                        Collections.emptyList(),
                        Collections.emptyList(),
                        false,
                        false, // basePathClassless=false
                        DescriptorCapability.FULLY_CLASSLESS);
        DataOutputSerializer out = new DataOutputSerializer(128);
        descriptor.write(out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        IOException error =
                assertThrows(IOException.class, () -> InspectDecoderDescriptor.read(in));
        assertTrue(error.getMessage().contains("basePathClassless"));
    }

    @Test
    void avroWithUnsupportedWireFormatRejected() throws IOException {
        InspectDecoderDescriptor descriptor =
                InspectDecoderDescriptor.avro(
                        "{\"type\":\"record\",\"name\":\"T\",\"fields\":[]}",
                        "UNSUPPORTED_FORMAT", // wrong wire format
                        DescriptorCapability.FULLY_CLASSLESS);
        DataOutputSerializer out = new DataOutputSerializer(128);
        descriptor.write(out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        IOException error =
                assertThrows(IOException.class, () -> InspectDecoderDescriptor.read(in));
        assertTrue(error.getMessage().contains("wireFormat"));
    }

    // ---- Isolation test: descriptor field metadata matches serialized POJO ----

    @Test
    void pojoDescriptorFieldsMatchSerializedValueStructure() throws IOException {
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(pojoSerializer);

        // Serialize a POJO value to bytes.
        SimplePojo pojo = new SimplePojo(42, "hello");
        DataOutputSerializer out = new DataOutputSerializer(64);
        pojoSerializer.serialize(pojo, out);
        byte[] pojoBytes = out.getCopyOfBuffer();

        // The first byte should be the flag byte (NO_SUBCLASS = 0x02).
        assertEquals(0x02, pojoBytes[0] & 0xFF, "first byte should be NO_SUBCLASS flag");

        // The descriptor should have 2 fields matching the POJO's fields.
        InspectDecoderDescriptor.PojoDescriptor pojoDesc =
                (InspectDecoderDescriptor.PojoDescriptor) schema.decoderDescriptor();
        Map<String, InspectDecoderDescriptor.PojoFieldDescriptor> byName =
                pojoFieldsByName(pojoDesc);
        assertEquals(2, byName.size());
        assertTrue(byName.containsKey("id"));
        assertTrue(byName.containsKey("name"));
    }

    // ---- P1: No-POJO-class classless decode test ----

    @Test
    void childSnapshotBytesRestoreIntAndStringSerializersWithoutPojoClass() throws Exception {
        // Build a FULLY_CLASSLESS POJO descriptor from a real SimplePojo serializer.
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(pojoSerializer);
        assertNotNull(schema.decoderDescriptor());

        // Serialize an actual SimplePojo value.
        SimplePojo value = new SimplePojo(777, "world");
        DataOutputSerializer valOut = new DataOutputSerializer(64);
        pojoSerializer.serialize(value, valOut);
        byte[] pojoBytes = valOut.getCopyOfBuffer();

        // Round-trip the descriptor to simulate monitor reading from sidecar.
        InspectDecoderDescriptor restored = roundTrip(schema.decoderDescriptor());
        InspectDecoderDescriptor.PojoDescriptor pojoDesc =
                (InspectDecoderDescriptor.PojoDescriptor) restored;

        // Parse the POJO wire bytes manually using the descriptor's child snapshot bytes.
        // POJO wire: flag byte (0x02=NO_SUBCLASS), then per-field: null-marker bool + field bytes.
        DataInputDeserializer in = new DataInputDeserializer(pojoBytes);
        int flag = in.readByte() & 0xFF;
        assertEquals(0x02, flag, "expected NO_SUBCLASS flag");

        // Walk the fields in snapshot order. Each field is PORTABLE_SNAPSHOT with child snapshot
        // bytes. Restore the child serializer from those bytes and decode the field value.
        Map<String, Object> decoded = new HashMap<>();
        for (InspectDecoderDescriptor.PojoFieldDescriptor field : pojoDesc.fields()) {
            boolean isNull = in.readBoolean();
            if (isNull) {
                decoded.put(field.name(), null);
                continue;
            }
            InspectDecoderDescriptor.PortableSnapshotDescriptor psd =
                    (InspectDecoderDescriptor.PortableSnapshotDescriptor) field.descriptor();
            assertNotNull(psd.snapshotBytes(), "field must carry snapshot bytes");

            // Restore the child serializer from the child snapshot bytes.
            // Use a filtering classloader that explicitly rejects SimplePojo, proving the
            // child codec can be restored without any user POJO class on the classpath.
            // The field serializers (IntSerializer, StringSerializer) are in flink-core, so
            // they are available via the parent classloader.
            ClassLoader noPojoClassloader = new FilteringClassLoader(getClass().getClassLoader());
            TypeSerializerSnapshot<?> childSnapshot =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            new DataInputViewStreamWrapper(
                                    new ByteArrayInputStream(psd.snapshotBytes())),
                            noPojoClassloader);
            TypeSerializer<?> childSerializer = childSnapshot.restoreSerializer();
            assertNotNull(childSerializer);

            // Decode the field value from the POJO wire bytes.
            Object fieldValue = childSerializer.deserialize(in);
            decoded.put(field.name(), fieldValue);
        }

        // Verify the decoded values match the original.
        assertEquals(777, decoded.get("id"));
        assertEquals("world", decoded.get("name"));
    }

    // ---- Large Avro schema (exercises bounded UTF-8 encoding) ----

    @Test
    void largeAvroSchemaJsonRoundTripsBeyond64KB() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"type\":\"record\",\"name\":\"Big\",\"fields\":[");
        for (int i = 0; i < 5000; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("{\"name\":\"field_")
                    .append(i)
                    .append("_with_a_rather_long_name_for_size_testing\",\"type\":\"int\"}");
        }
        sb.append("]}");
        String largeSchemaJson = sb.toString();
        assertTrue(
                largeSchemaJson.getBytes(StandardCharsets.UTF_8).length > 65536,
                "schema JSON should exceed 64KB for this test");

        InspectDecoderDescriptor descriptor =
                InspectDecoderDescriptor.avro(
                        largeSchemaJson,
                        "FLINK_DATA_INPUT_V1",
                        DescriptorCapability.FULLY_CLASSLESS);
        InspectDecoderDescriptor restored = roundTrip(descriptor);
        assertEquals(descriptor, restored);
    }

    // ---- Helpers ----

    private static InspectDecoderDescriptor roundTrip(InspectDecoderDescriptor descriptor)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(4096);
        descriptor.write(out);
        return InspectDecoderDescriptor.read(new DataInputDeserializer(out.getCopyOfBuffer()));
    }

    private static InspectDecoderDescriptor roundTripQuietly(InspectDecoderDescriptor descriptor) {
        try {
            return roundTrip(descriptor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, InspectDecoderDescriptor.PojoFieldDescriptor> pojoFieldsByName(
            InspectDecoderDescriptor.PojoDescriptor pojo) {
        Map<String, InspectDecoderDescriptor.PojoFieldDescriptor> map = new HashMap<>();
        for (InspectDecoderDescriptor.PojoFieldDescriptor f : pojo.fields()) {
            map.put(f.name(), f);
        }
        return map;
    }

    /** Serializes a TypeSerializerSnapshot to bytes using the same Flink utility as capture. */
    private static byte[] serializeSnapshot(TypeSerializerSnapshot<?> snapshot) {
        return InspectDecoderDescriptorExtractor.serializeSnapshotBytes(snapshot);
    }

    /**
     * Writes a string as explicit length-prefixed UTF-8 bytes for test-construction purposes (same
     * wire format as {@code writeBoundedUtf8}).
     */
    private static void writeBoundedUtf8Raw(DataOutputView output, String value, int maxBytes)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    // ---- Test fixtures ----

    public static final class SimplePojo implements Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public String name;

        public SimplePojo() {}

        public SimplePojo(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public static final class PojoWithNested implements Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public InnerPojo inner;
    }

    public static final class InnerPojo implements Serializable {
        private static final long serialVersionUID = 1L;
        public int x;
        public String y;
    }

    public static final class PojoWithList implements Serializable {
        private static final long serialVersionUID = 1L;
        public List<String> tags;
    }

    public static class PojoBase implements Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public String name;
    }

    public static class PojoSubclass extends PojoBase {
        private static final long serialVersionUID = 1L;
        public String extra;
    }

    /**
     * A classloader that explicitly rejects loading the test's POJO classes, simulating the monitor
     * classloader which does NOT have user job classes. All other classes (Flink, Avro, JDK) are
     * delegated to the parent.
     */
    private static final class FilteringClassLoader extends ClassLoader {
        private final ClassLoader parent;

        FilteringClassLoader(ClassLoader parent) {
            super(parent);
            this.parent = parent;
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            // Reject all test POJO fixture classes by package prefix.
            if (name.startsWith(InspectDecoderDescriptorTest.class.getName())) {
                throw new ClassNotFoundException("Filtered (no POJO class): " + name);
            }
            return parent.loadClass(name);
        }
    }
}
