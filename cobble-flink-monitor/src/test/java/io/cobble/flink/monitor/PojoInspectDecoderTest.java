package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.DescriptorCapability;
import io.cobble.flink.common.inspect.InspectDecoderDescriptor;
import io.cobble.flink.common.inspect.InspectDecoderDescriptorKind;
import io.cobble.flink.common.inspect.SerializerInspectSchema;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link PojoInspectDecoder} using real Flink {@code PojoSerializer} bytes. Each
 * test first verifies the actual child descriptor kind captured by {@code SerializerInspectSchema}
 * before asserting classless decode behavior.
 */
class PojoInspectDecoderTest {

    // ---- Test fixtures ----

    public static final class SimplePojo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public String name;

        public SimplePojo() {}

        public SimplePojo(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public static final class PojoWithNested implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public InnerPojo inner;

        public PojoWithNested() {}

        public PojoWithNested(int id, InnerPojo inner) {
            this.id = id;
            this.inner = inner;
        }
    }

    public static final class InnerPojo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int x;
        public String y;

        public InnerPojo() {}

        public InnerPojo(int x, String y) {
            this.x = x;
            this.y = y;
        }
    }

    public static final class PojoWithTuple implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public Tuple2<Integer, String> tuple;
    }

    public static final class PojoWithList implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public List<String> tags;
    }

    public static class PojoBase implements java.io.Serializable {
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

    // ---- Tests: base POJO ----

    @Test
    void basePojoDecodesFields() throws Exception {
        TypeSerializer<SimplePojo> serializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(serializer);
        InspectDecoderDescriptor descriptor = schema.decoderDescriptor();

        // Verify: POJO descriptor, FULLY_CLASSLESS, children are PORTABLE_SNAPSHOT.
        assertEquals(InspectDecoderDescriptorKind.POJO, descriptor.kind());
        assertEquals(DescriptorCapability.FULLY_CLASSLESS, descriptor.capability());
        assertEquals(2, descriptor.pojoFields().size());
        for (InspectDecoderDescriptor.PojoFieldDescriptor field : descriptor.pojoFields()) {
            assertEquals(
                    InspectDecoderDescriptorKind.PORTABLE_SNAPSHOT,
                    field.descriptor().kind(),
                    "field " + field.name());
        }

        SimplePojo pojo = new SimplePojo(42, "hello");
        byte[] bytes = serialize(serializer, pojo);

        Object result = PojoInspectDecoder.decode(descriptor, bytes);
        assertTrue(result instanceof ClasslessPojoValue);
        ClasslessPojoValue value = (ClasslessPojoValue) result;
        assertEquals(42, value.fields().get("id"));
        assertEquals("hello", value.fields().get("name"));
    }

    @Test
    void nullPojoReturnsNullValue() throws Exception {
        TypeSerializer<SimplePojo> serializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        InspectDecoderDescriptor descriptor =
                SerializerInspectSchema.fromSerializer(serializer).decoderDescriptor();

        byte[] bytes = serialize(serializer, null);
        assertEquals(1, bytes.length, "null POJO should be just the IS_NULL flag");
        assertEquals(0x01, bytes[0] & 0xFF);

        Object result = PojoInspectDecoder.decode(descriptor, bytes);
        assertTrue(result instanceof ClasslessPojoValue);
        assertTrue(((ClasslessPojoValue) result).isNull());
    }

    @Test
    void nullFieldIsPreservedAsNull() throws Exception {
        TypeSerializer<SimplePojo> serializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        InspectDecoderDescriptor descriptor =
                SerializerInspectSchema.fromSerializer(serializer).decoderDescriptor();

        SimplePojo pojo = new SimplePojo(99, null);
        byte[] bytes = serialize(serializer, pojo);

        Object result = PojoInspectDecoder.decode(descriptor, bytes);
        ClasslessPojoValue value = (ClasslessPojoValue) result;
        assertEquals(99, value.fields().get("id"));
        assertNull(value.fields().get("name"));
    }

    // ---- Tests: nested POJO ----

    @Test
    void nestedPojoDecodesAsClasslessPojoValue() throws Exception {
        TypeSerializer<PojoWithNested> serializer =
                TypeInformation.of(PojoWithNested.class).createSerializer(new ExecutionConfig());
        InspectDecoderDescriptor descriptor =
                SerializerInspectSchema.fromSerializer(serializer).decoderDescriptor();

        // Verify: the inner field is a POJO descriptor.
        InspectDecoderDescriptor.PojoFieldDescriptor innerField = findField(descriptor, "inner");
        assertEquals(InspectDecoderDescriptorKind.POJO, innerField.descriptor().kind());

        PojoWithNested pojo = new PojoWithNested(7, new InnerPojo(3, "nested"));
        byte[] bytes = serialize(serializer, pojo);

        Object result = PojoInspectDecoder.decode(descriptor, bytes);
        ClasslessPojoValue value = (ClasslessPojoValue) result;
        assertEquals(7, value.fields().get("id"));
        Object inner = value.fields().get("inner");
        assertTrue(inner instanceof ClasslessPojoValue, "inner should be ClasslessPojoValue");
        ClasslessPojoValue innerValue = (ClasslessPojoValue) inner;
        assertEquals(3, innerValue.fields().get("x"));
        assertEquals("nested", innerValue.fields().get("y"));
    }

    // ---- Tests: POJO with Tuple field ----

    @Test
    void pojoWithTupleFieldDecodesViaPortableSnapshot() throws Exception {
        TypeSerializer<PojoWithTuple> serializer =
                TypeInformation.of(PojoWithTuple.class).createSerializer(new ExecutionConfig());
        InspectDecoderDescriptor descriptor =
                SerializerInspectSchema.fromSerializer(serializer).decoderDescriptor();

        // Verify: the tuple field is PORTABLE_SNAPSHOT (Tuple is portable).
        InspectDecoderDescriptor.PojoFieldDescriptor tupleField = findField(descriptor, "tuple");
        assertEquals(
                InspectDecoderDescriptorKind.PORTABLE_SNAPSHOT,
                tupleField.descriptor().kind(),
                "Tuple field should be PORTABLE_SNAPSHOT");

        PojoWithTuple pojo = new PojoWithTuple();
        pojo.id = 5;
        pojo.tuple = new Tuple2<>(10, "tuple-val");
        byte[] bytes = serialize(serializer, pojo);

        Object result = PojoInspectDecoder.decode(descriptor, bytes);
        ClasslessPojoValue value = (ClasslessPojoValue) result;
        assertEquals(5, value.fields().get("id"));
        Object tuple = value.fields().get("tuple");
        assertNotNull(tuple, "tuple should be decoded");
        assertTrue(tuple instanceof Tuple2, "tuple should be Tuple2");
        assertEquals(10, ((Tuple2<?, ?>) tuple).f0);
        assertEquals("tuple-val", ((Tuple2<?, ?>) tuple).f1);
    }

    // ---- Tests: POJO with List field (Kryo / UNSUPPORTED) ----

    @Test
    void pojoWithListFieldIsPartiallyClassless() throws Exception {
        TypeSerializer<PojoWithList> serializer =
                TypeInformation.of(PojoWithList.class).createSerializer(new ExecutionConfig());
        InspectDecoderDescriptor descriptor =
                SerializerInspectSchema.fromSerializer(serializer).decoderDescriptor();

        // Verify: the tags field is UNSUPPORTED (Kryo-backed List is not portable).
        InspectDecoderDescriptor.PojoFieldDescriptor tagsField = findField(descriptor, "tags");
        assertEquals(
                InspectDecoderDescriptorKind.UNSUPPORTED,
                tagsField.descriptor().kind(),
                "List<String> field should be UNSUPPORTED (Kryo)");

        // The POJO should be PARTIALLY_CLASSLESS.
        assertEquals(DescriptorCapability.PARTIALLY_CLASSLESS, descriptor.capability());

        PojoWithList pojo = new PojoWithList();
        pojo.id = 1;
        pojo.tags = Arrays.asList("a", "b");
        byte[] bytes = serialize(serializer, pojo);

        // Classless decode should fail because basePathClassless is false.
        IOException ex =
                assertThrows(IOException.class, () -> PojoInspectDecoder.decode(descriptor, bytes));
        assertTrue(
                ex.getMessage().contains("base path is not classless"),
                "error should mention base path: " + ex.getMessage());
    }

    // ---- Tests: registered subclasses ----

    @Test
    void registeredSubclassesDecodeWithTagDispatch() throws Exception {
        ExecutionConfig config = new ExecutionConfig();
        config.registerPojoType(PojoSubclassA.class);
        config.registerPojoType(PojoSubclassB.class);
        TypeSerializer<PojoBase> serializer =
                TypeInformation.of(PojoBase.class).createSerializer(config);
        InspectDecoderDescriptor descriptor =
                SerializerInspectSchema.fromSerializer(serializer).decoderDescriptor();

        // Verify: two registered subclasses with tags 0 and 1.
        List<InspectDecoderDescriptor.RegisteredSubclass> subclasses =
                descriptor.registeredPojoSubclasses();
        assertEquals(2, subclasses.size());
        assertEquals(0, subclasses.get(0).tag());
        assertEquals(1, subclasses.get(1).tag());

        // Serialize a PojoSubclassA instance (tag 0).
        PojoSubclassA a = new PojoSubclassA();
        a.id = 1;
        a.name = "base";
        a.extraA = "extra";
        byte[] bytesA = serialize(serializer, a);

        Object resultA = PojoInspectDecoder.decode(descriptor, bytesA);
        // Tagged subclass returns the child descriptor's result as-is.
        assertTrue(resultA instanceof ClasslessPojoValue, "subclass A should decode as POJO");
        ClasslessPojoValue valueA = (ClasslessPojoValue) resultA;
        // The subclass descriptor includes base + extra fields.
        assertEquals(1, valueA.fields().get("id"));
        assertEquals("base", valueA.fields().get("name"));
        assertEquals("extra", valueA.fields().get("extraA"));

        // Serialize a PojoSubclassB instance (tag 1).
        PojoSubclassB b = new PojoSubclassB();
        b.id = 2;
        b.name = "base2";
        b.extraB = 99;
        byte[] bytesB = serialize(serializer, b);

        Object resultB = PojoInspectDecoder.decode(descriptor, bytesB);
        assertTrue(resultB instanceof ClasslessPojoValue);
        ClasslessPojoValue valueB = (ClasslessPojoValue) resultB;
        assertEquals(2, valueB.fields().get("id"));
        assertEquals("base2", valueB.fields().get("name"));
        assertEquals(99, valueB.fields().get("extraB"));
    }

    // ---- Tests: malformed input ----

    @Test
    void malformedFlagZeroRejected() throws Exception {
        TypeSerializer<SimplePojo> serializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        InspectDecoderDescriptor descriptor =
                SerializerInspectSchema.fromSerializer(serializer).decoderDescriptor();

        IOException ex =
                assertThrows(
                        IOException.class,
                        () -> PojoInspectDecoder.decode(descriptor, new byte[] {0x00}));
        assertTrue(ex.getMessage().contains("Malformed POJO flag"), ex.getMessage());
    }

    @Test
    void malformedFlagCombinationRejected() throws Exception {
        TypeSerializer<SimplePojo> serializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        InspectDecoderDescriptor descriptor =
                SerializerInspectSchema.fromSerializer(serializer).decoderDescriptor();

        // 0x03 = IS_NULL | NO_SUBCLASS - invalid combination.
        IOException ex =
                assertThrows(
                        IOException.class,
                        () -> PojoInspectDecoder.decode(descriptor, new byte[] {0x03}));
        assertTrue(ex.getMessage().contains("Malformed POJO flag"), ex.getMessage());
    }

    @Test
    void malformedFlagFFRejected() throws Exception {
        TypeSerializer<SimplePojo> serializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        InspectDecoderDescriptor descriptor =
                SerializerInspectSchema.fromSerializer(serializer).decoderDescriptor();

        IOException ex =
                assertThrows(
                        IOException.class,
                        () -> PojoInspectDecoder.decode(descriptor, new byte[] {(byte) 0xFF}));
        assertTrue(ex.getMessage().contains("Malformed POJO flag"), ex.getMessage());
    }

    @Test
    void isSubclassFlagRejected() throws Exception {
        TypeSerializer<SimplePojo> serializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        InspectDecoderDescriptor descriptor =
                SerializerInspectSchema.fromSerializer(serializer).decoderDescriptor();

        // 0x04 = IS_SUBCLASS - not classlessly decodable.
        IOException ex =
                assertThrows(
                        IOException.class,
                        () -> PojoInspectDecoder.decode(descriptor, new byte[] {0x04, 0x00, 0x00}));
        assertTrue(
                ex.getMessage().contains("Non-registered subclass"),
                "error should mention non-registered subclass: " + ex.getMessage());
    }

    @Test
    void invalidTagRejected() throws Exception {
        ExecutionConfig config = new ExecutionConfig();
        config.registerPojoType(PojoSubclassA.class);
        TypeSerializer<PojoBase> serializer =
                TypeInformation.of(PojoBase.class).createSerializer(config);
        InspectDecoderDescriptor descriptor =
                SerializerInspectSchema.fromSerializer(serializer).decoderDescriptor();

        // Only 1 registered subclass (tag 0); tag 1 is invalid.
        byte[] bytes = new byte[] {0x08, 0x01};
        IOException ex =
                assertThrows(IOException.class, () -> PojoInspectDecoder.decode(descriptor, bytes));
        assertTrue(
                ex.getMessage().contains("Invalid POJO subclass tag"),
                "error should mention invalid tag: " + ex.getMessage());
    }

    @Test
    void trailingBytesRejected() throws Exception {
        TypeSerializer<SimplePojo> serializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        InspectDecoderDescriptor descriptor =
                SerializerInspectSchema.fromSerializer(serializer).decoderDescriptor();

        SimplePojo pojo = new SimplePojo(1, "x");
        byte[] bytes = serialize(serializer, pojo);
        // Append an extra byte.
        byte[] withTrailing = Arrays.copyOf(bytes, bytes.length + 1);

        IOException ex =
                assertThrows(
                        IOException.class,
                        () -> PojoInspectDecoder.decode(descriptor, withTrailing));
        assertTrue(
                ex.getMessage().contains("Trailing bytes"),
                "error should mention trailing bytes: " + ex.getMessage());
    }

    @Test
    void truncatedPayloadRejected() throws Exception {
        TypeSerializer<SimplePojo> serializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        InspectDecoderDescriptor descriptor =
                SerializerInspectSchema.fromSerializer(serializer).decoderDescriptor();

        SimplePojo pojo = new SimplePojo(1, "hello");
        byte[] bytes = serialize(serializer, pojo);
        // Truncate after the flag + first field null boolean.
        byte[] truncated = Arrays.copyOf(bytes, 2);

        assertThrows(IOException.class, () -> PojoInspectDecoder.decode(descriptor, truncated));
    }

    // ---- Helpers ----

    private static InspectDecoderDescriptor.PojoFieldDescriptor findField(
            InspectDecoderDescriptor descriptor, String name) {
        for (InspectDecoderDescriptor.PojoFieldDescriptor field : descriptor.pojoFields()) {
            if (field.name().equals(name)) {
                return field;
            }
        }
        throw new AssertionError("Field not found: " + name);
    }

    private static <T> byte[] serialize(TypeSerializer<T> serializer, T value) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);
        return out.getCopyOfBuffer();
    }
}
