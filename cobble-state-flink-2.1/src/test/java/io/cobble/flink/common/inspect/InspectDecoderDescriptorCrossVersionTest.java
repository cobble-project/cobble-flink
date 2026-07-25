package io.cobble.flink.common.inspect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Cross-version test for {@link InspectDecoderDescriptorExtractor} running against the actual Flink
 * 2.1 classes. Verifies that the reflection-based extraction of POJO field names, field order, and
 * registered-subclass tag ordering works correctly with Flink 2.1's {@code SerializerConfig}.
 *
 * <p>This test lives in the {@code io.cobble.flink.common.inspect} package to access the
 * package-private {@link InspectDecoderDescriptorExtractor} and concrete descriptor subclasses.
 */
class InspectDecoderDescriptorCrossVersionTest {

    @Test
    void pojoWithPrimitiveFieldsExtractsFullyClassless() {
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new SerializerConfigImpl());
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(pojoSerializer);

        assertNotNull(schema.decoderDescriptor());
        assertEquals(InspectDecoderDescriptorKind.POJO, schema.decoderDescriptor().kind());
        assertEquals(DescriptorCapability.FULLY_CLASSLESS, schema.decoderDescriptor().capability());
        assertNull(schema.serializedSerializerBytes());

        Map<String, InspectDecoderDescriptor.PojoFieldDescriptor> byName = pojoFieldsByName(schema);
        assertEquals(2, byName.size());
        assertNotNull(byName.get("id"));
        assertNotNull(byName.get("name"));
    }

    @Test
    void pojoWithNestedPojoExtractsRecursiveDescriptor() {
        TypeSerializer<PojoWithNested> pojoSerializer =
                TypeInformation.of(PojoWithNested.class)
                        .createSerializer(new SerializerConfigImpl());
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(pojoSerializer);

        assertNotNull(schema.decoderDescriptor());
        assertEquals(InspectDecoderDescriptorKind.POJO, schema.decoderDescriptor().kind());
        assertEquals(DescriptorCapability.FULLY_CLASSLESS, schema.decoderDescriptor().capability());

        Map<String, InspectDecoderDescriptor.PojoFieldDescriptor> byName = pojoFieldsByName(schema);
        InspectDecoderDescriptor.PojoFieldDescriptor innerField = byName.get("inner");
        assertNotNull(innerField);
        assertEquals(InspectDecoderDescriptorKind.POJO, innerField.descriptor().kind());
    }

    @Test
    void pojoWithTwoRegisteredSubclassesHasCorrectTagOrdering() {
        // Register two subclasses in a specific order.
        SerializerConfigImpl config = new SerializerConfigImpl();
        config.registerPojoType(PojoSubclassA.class);
        config.registerPojoType(PojoSubclassB.class);
        TypeSerializer<PojoBase> pojoSerializer =
                TypeInformation.of(PojoBase.class).createSerializer(config);
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(pojoSerializer);

        assertNotNull(schema.decoderDescriptor());
        assertEquals(InspectDecoderDescriptorKind.POJO, schema.decoderDescriptor().kind());

        InspectDecoderDescriptor.PojoDescriptor pojo =
                (InspectDecoderDescriptor.PojoDescriptor) schema.decoderDescriptor();
        List<InspectDecoderDescriptor.RegisteredSubclass> subclasses = pojo.registeredSubclasses();

        // Assert exactly two subclasses with correct tag ordering.
        assertEquals(2, subclasses.size(), "expected exactly 2 registered subclasses");

        // Tag 0 = first registered subclass (PojoSubclassA).
        assertEquals(0, subclasses.get(0).tag());
        assertEquals(PojoSubclassA.class.getName(), subclasses.get(0).className());
        assertEquals(InspectDecoderDescriptorKind.POJO, subclasses.get(0).descriptor().kind());

        // Tag 1 = second registered subclass (PojoSubclassB).
        assertEquals(1, subclasses.get(1).tag());
        assertEquals(PojoSubclassB.class.getName(), subclasses.get(1).className());
        assertEquals(InspectDecoderDescriptorKind.POJO, subclasses.get(1).descriptor().kind());

        // Verify the subclass fields are in snapshot order. PojoSubclassA extends PojoBase with
        // one extra field; its POJO descriptor should have the base fields plus the extra.
        InspectDecoderDescriptor.PojoDescriptor sub0Pojo =
                (InspectDecoderDescriptor.PojoDescriptor) subclasses.get(0).descriptor();
        Map<String, InspectDecoderDescriptor.PojoFieldDescriptor> sub0Fields = new HashMap<>();
        for (InspectDecoderDescriptor.PojoFieldDescriptor f : sub0Pojo.fields()) {
            sub0Fields.put(f.name(), f);
        }
        assertNotNull(sub0Fields.get("id"), "subclass A should have 'id' field");
        assertNotNull(sub0Fields.get("name"), "subclass A should have 'name' field");
        assertNotNull(sub0Fields.get("extraA"), "subclass A should have 'extraA' field");
    }

    @Test
    void pojoDescriptorBinaryRoundTrips() {
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new SerializerConfigImpl());
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(pojoSerializer);
        InspectDecoderDescriptor restored = roundTripQuietly(schema.decoderDescriptor());
        assertEquals(schema.decoderDescriptor(), restored);
    }

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

    // ---- Helpers ----

    private static InspectDecoderDescriptor roundTripQuietly(InspectDecoderDescriptor descriptor) {
        try {
            DataOutputSerializer out = new DataOutputSerializer(4096);
            descriptor.write(out);
            return InspectDecoderDescriptor.read(new DataInputDeserializer(out.getCopyOfBuffer()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, InspectDecoderDescriptor.PojoFieldDescriptor> pojoFieldsByName(
            SerializerInspectSchema schema) {
        InspectDecoderDescriptor.PojoDescriptor pojo =
                (InspectDecoderDescriptor.PojoDescriptor) schema.decoderDescriptor();
        Map<String, InspectDecoderDescriptor.PojoFieldDescriptor> map = new HashMap<>();
        for (InspectDecoderDescriptor.PojoFieldDescriptor f : pojo.fields()) {
            map.put(f.name(), f);
        }
        return map;
    }

    // ---- Fixtures ----

    public static final class SimplePojo implements Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public String name;
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

    public static class PojoBase implements Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public String name;
    }

    public static class PojoSubclassA extends PojoBase {
        private static final long serialVersionUID = 1L;
        public String extraA;
    }

    public static class PojoSubclassB extends PojoBase {
        private static final long serialVersionUID = 1L;
        public int extraB;
    }
}
