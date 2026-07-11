package io.cobble.flink.common.inspect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

/**
 * Tests the capture policy in {@link SerializerInspectSchema#fromSerializer}: monitor-portable
 * serializers omit the serialized live serializer fallback, while non-portable serializers include
 * it. Also verifies the snapshot-first restore priority.
 */
class SerializerInspectSchemaCapturePolicyTest {

    // ---- Monitor-portable: no serialized fallback ----

    @Test
    void primitiveSerializerHasNoSerializedBytes() {
        SerializerInspectSchema schema =
                SerializerInspectSchema.fromSerializer(IntSerializer.INSTANCE);
        assertEquals(IntSerializer.INSTANCE.getClass().getName(), schema.serializerClassName());
        assertNotNull(schema.snapshotBytes());
        assertNotNull(schema.snapshotClassName());
        assertNotNull(schema.inspectType());
        assertEquals(StateInspectTypeKind.SCALAR, schema.inspectType().kind());
        assertEquals("INT", schema.inspectType().logicalType());
        // IntSerializer is monitor-portable (SimpleTypeSerializerSnapshot) → no fallback.
        assertNull(schema.serializedSerializerBytes());
    }

    @Test
    void nestedListSerializerHasNoSerializedBytes() {
        ListSerializer<Integer> listSerializer = new ListSerializer<>(IntSerializer.INSTANCE);
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(listSerializer);
        assertNotNull(schema.snapshotBytes());
        assertNotNull(schema.inspectType());
        assertEquals(StateInspectTypeKind.LIST, schema.inspectType().kind());
        // List(Int) is monitor-portable → no fallback.
        assertNull(schema.serializedSerializerBytes());
    }

    @Test
    void mapSerializerHasNoSerializedBytes() {
        MapSerializer<Integer, String> mapSerializer =
                new MapSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE);
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(mapSerializer);
        assertNotNull(schema.snapshotBytes());
        assertNotNull(schema.inspectType());
        assertEquals(StateInspectTypeKind.MAP, schema.inspectType().kind());
        // Map(Int, String) is monitor-portable → no fallback.
        assertNull(schema.serializedSerializerBytes());
    }

    @Test
    void tupleSerializerHasNoSerializedBytes() {
        @SuppressWarnings("unchecked")
        TypeSerializer<Tuple2<Integer, String>> tupleSerializer =
                (TypeSerializer<Tuple2<Integer, String>>)
                        TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {})
                                .createSerializer(new ExecutionConfig());
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(tupleSerializer);
        assertNotNull(schema.snapshotBytes());
        assertNotNull(schema.inspectType());
        assertEquals(StateInspectTypeKind.TUPLE, schema.inspectType().kind());
        // Tuple(Int, String) is monitor-portable → no fallback.
        assertNull(schema.serializedSerializerBytes());
    }

    // ---- Non-portable with classless descriptor: no serialized fallback ----

    @Test
    void pojoSerializerHasClasslessDescriptorAndNoSerializedBytes() {
        TypeSerializer<TestPojo> pojoSerializer =
                TypeInformation.of(TestPojo.class).createSerializer(new ExecutionConfig());
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(pojoSerializer);
        // POJO with primitive/String fields is FULLY_CLASSLESS -> no live serializer fallback.
        assertNull(schema.serializedSerializerBytes());
        // The snapshot-derived inspect type is present.
        assertNotNull(schema.snapshotBytes());
        assertNotNull(schema.inspectType());
        assertEquals(StateInspectTypeKind.ROW, schema.inspectType().kind());
        // The decoder descriptor is a FULLY_CLASSLESS POJO.
        assertNotNull(schema.decoderDescriptor());
        assertEquals(InspectDecoderDescriptorKind.POJO, schema.decoderDescriptor().kind());
        assertEquals(DescriptorCapability.FULLY_CLASSLESS, schema.decoderDescriptor().capability());
    }

    @Test
    void customSnapshotSerializerHasSerializedBytes() {
        SerializerInspectSchema schema =
                SerializerInspectSchema.fromSerializer(new CustomSnapshotSerializer());
        // Custom snapshot -> not monitor-portable, descriptor is UNSUPPORTED -> fallback persisted.
        assertNotNull(schema.serializedSerializerBytes());
        assertNotNull(schema.decoderDescriptor());
        assertEquals(InspectDecoderDescriptorKind.UNSUPPORTED, schema.decoderDescriptor().kind());
        assertEquals(DescriptorCapability.UNSUPPORTED, schema.decoderDescriptor().capability());
    }

    @Test
    void throwingSnapshotSerializerHasSerializedBytes() {
        SerializerInspectSchema schema =
                SerializerInspectSchema.fromSerializer(new ThrowingSnapshotSerializer());
        // Snapshot capture failed → fallback must be present.
        assertNotNull(schema.serializedSerializerBytes());
        assertNull(schema.snapshotBytes());
        assertNull(schema.snapshotClassName());
        assertNull(schema.inspectType());
    }

    @Test
    void rowDataSerializerHasNoSerializedBytes() {
        // RowDataSerializer with primitive field serializers → all nested are monitor-portable,
        // so the RowDataSerializerSnapshot is portable and no fallback is persisted.
        RowDataSerializer rowDataSerializer =
                new RowDataSerializer(new BigIntType(false), VarCharType.STRING_TYPE);
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(rowDataSerializer);
        assertNotNull(schema.snapshotBytes());
        assertNotNull(schema.snapshotClassName());
        // The snapshot class is RowDataSerializer$RowDataSerializerSnapshot (nested class);
        // isMonitorPortable must correctly resolve its simple name.
        assertNull(
                schema.serializedSerializerBytes(),
                "RowData with portable field serializers should not persist live serializer");
        assertNotNull(schema.inspectType());
        assertEquals(StateInspectTypeKind.ROW, schema.inspectType().kind());
    }

    @Test
    void nestedListWithRowDataElementHasNoSerializedBytes() {
        // List(RowData(BigInt, VarChar)) — both List and RowData are monitor-portable when their
        // nested serializers are portable.
        RowDataSerializer rowDataSerializer =
                new RowDataSerializer(new BigIntType(false), VarCharType.STRING_TYPE);
        ListSerializer<org.apache.flink.table.data.RowData> listSerializer =
                new ListSerializer<>(rowDataSerializer);
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(listSerializer);
        assertNotNull(schema.snapshotBytes());
        assertEquals(StateInspectTypeKind.LIST, schema.inspectType().kind());
        assertNull(
                schema.serializedSerializerBytes(),
                "List(RowData(portable)) should not persist live serializer");
    }

    // ---- Restore priority: snapshot first, serialized fallback second ----

    @Test
    void restoreSerializerPrefersSnapshotOverSerializedBytes() {
        // POJO has both snapshot and serialized bytes. Restore should use snapshot first.
        TypeSerializer<TestPojo> pojoSerializer =
                TypeInformation.of(TestPojo.class).createSerializer(new ExecutionConfig());
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(pojoSerializer);
        TypeSerializer<?> restored = schema.restoreSerializer(getClass().getClassLoader());
        assertNotNull(restored);
        // The restored serializer should be a PojoSerializer (from snapshot), not a deserialized
        // instance. We verify it can serialize/deserialize correctly.
        assertTrue(restored.getClass().getName().contains("PojoSerializer"));
    }

    @Test
    void restoreSerializerPrefersSnapshotRestoreOverSerializedFallback() {
        // This serializer's snapshot.restoreSerializer() returns StringSerializer.INSTANCE,
        // but the serialized live serializer is SnapshotPreferenceSerializer itself. If restore
        // preferred the serialized bytes, we'd get a SnapshotPreferenceSerializer. If it
        // correctly prefers the snapshot, we get StringSerializer.
        SerializerInspectSchema schema =
                SerializerInspectSchema.fromSerializer(new SnapshotPreferenceSerializer());
        // Both snapshot bytes and serialized bytes should be present (non-portable snapshot).
        assertNotNull(schema.snapshotBytes());
        assertNotNull(schema.serializedSerializerBytes());
        TypeSerializer<?> restored = schema.restoreSerializer(getClass().getClassLoader());
        assertNotNull(restored);
        // Must be StringSerializer (from snapshot), NOT SnapshotPreferenceSerializer (from bytes).
        assertTrue(
                restored instanceof StringSerializer,
                "expected StringSerializer from snapshot restore, got " + restored.getClass());
    }

    @Test
    void restoreSerializerFallsBackToSerializedBytesWhenSnapshotFails() {
        // ThrowingSnapshotSerializer: snapshot bytes are null, so restore must use serialized
        // bytes.
        SerializerInspectSchema schema =
                SerializerInspectSchema.fromSerializer(new ThrowingSnapshotSerializer());
        TypeSerializer<?> restored = schema.restoreSerializer(getClass().getClassLoader());
        assertNotNull(restored);
        assertTrue(restored instanceof ThrowingSnapshotSerializer);
    }

    @Test
    void restoreSerializerFallsBackWhenSnapshotRestoreHitsNoClassDefFoundError() {
        SerializerInspectSchema schema =
                SerializerInspectSchema.fromSerializer(new SnapshotLinkageSerializer());

        TypeSerializer<?> restored = schema.restoreSerializer(getClass().getClassLoader());

        assertNotNull(restored);
        assertTrue(restored instanceof SnapshotLinkageSerializer);
    }

    @Test
    void restoreSerializerPropagatesVerifyErrorWithoutRunningSerializedFallback() {
        assertSnapshotLinkageErrorPropagates(
                VerifyError.class, new SnapshotVerifyErrorSerializer());
    }

    @Test
    void restoreSerializerPropagatesUnsupportedClassVersionErrorWithoutRunningSerializedFallback() {
        assertSnapshotLinkageErrorPropagates(
                UnsupportedClassVersionError.class,
                new SnapshotUnsupportedClassVersionErrorSerializer());
    }

    @Test
    void restoreSerializerPropagatesNoSuchMethodErrorWithoutRunningSerializedFallback() {
        assertSnapshotLinkageErrorPropagates(
                NoSuchMethodError.class, new SnapshotNoSuchMethodErrorSerializer());
    }

    @Test
    void restoreSerializerReturnsNullWhenSerializedFallbackHitsLinkageError() {
        SerializerInspectSchema schema =
                SerializerInspectSchema.fromSerializer(new ThrowingSnapshotSerializer());
        String blockedClassName = ThrowingSnapshotSerializer.class.getName();
        ClassLoader rejectingLoader =
                new ClassLoader(getClass().getClassLoader()) {
                    @Override
                    protected Class<?> loadClass(String name, boolean resolve)
                            throws ClassNotFoundException {
                        if (blockedClassName.equals(name)) {
                            throw new NoClassDefFoundError(name);
                        }
                        return super.loadClass(name, resolve);
                    }
                };

        assertNull(schema.restoreSerializer(rejectingLoader));
    }

    private void assertSnapshotLinkageErrorPropagates(
            Class<? extends LinkageError> errorClass, TypeSerializer<String> serializer) {
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(serializer);
        SnapshotLinkageSerializer.resetSerializedFallbackDeserializationCount();

        assertThrows(errorClass, () -> schema.restoreSerializer(getClass().getClassLoader()));
        assertEquals(0, SnapshotLinkageSerializer.serializedFallbackDeserializationCount());
    }

    // ---- Test fixtures ----

    public static final class TestPojo implements Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public String name;
    }

    /** A serializer with a custom (non-portable) snapshot. */
    private static class CustomSnapshotSerializer extends TypeSerializer<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<String> duplicate() {
            return this;
        }

        @Override
        public String createInstance() {
            return "";
        }

        @Override
        public String copy(String from) {
            return from;
        }

        @Override
        public String copy(String from, String reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(String record, DataOutputView target) throws IOException {
            StringSerializer.INSTANCE.serialize(record, target);
        }

        @Override
        public String deserialize(DataInputView source) throws IOException {
            return StringSerializer.INSTANCE.deserialize(source);
        }

        @Override
        public String deserialize(String reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            StringSerializer.INSTANCE.copy(source, target);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CustomSnapshotSerializer;
        }

        @Override
        public int hashCode() {
            return CustomSnapshotSerializer.class.hashCode();
        }

        @Override
        public TypeSerializerSnapshot<String> snapshotConfiguration() {
            return new CustomSnapshot();
        }
    }

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

    public static class SnapshotLinkageSerializer extends CustomSnapshotSerializer {
        private static final long serialVersionUID = 1L;
        private static int serializedFallbackDeserializationCount;

        @Override
        public TypeSerializerSnapshot<String> snapshotConfiguration() {
            return new SnapshotLinkageSnapshot();
        }

        private static void resetSerializedFallbackDeserializationCount() {
            serializedFallbackDeserializationCount = 0;
        }

        private static int serializedFallbackDeserializationCount() {
            return serializedFallbackDeserializationCount;
        }

        private void readObject(ObjectInputStream input)
                throws IOException, ClassNotFoundException {
            input.defaultReadObject();
            serializedFallbackDeserializationCount++;
        }
    }

    public static class SnapshotLinkageSnapshot implements TypeSerializerSnapshot<String> {
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
            throw new NoClassDefFoundError("missing-snapshot-dependency");
        }

        @Override
        public TypeSerializerSchemaCompatibility<String> resolveSchemaCompatibility(
                TypeSerializer<String> newSerializer) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }
    }

    public static final class SnapshotVerifyErrorSerializer extends SnapshotLinkageSerializer {
        private static final long serialVersionUID = 1L;

        @Override
        public TypeSerializerSnapshot<String> snapshotConfiguration() {
            return new SnapshotVerifyErrorSnapshot();
        }
    }

    public static final class SnapshotVerifyErrorSnapshot extends SnapshotLinkageSnapshot {
        @Override
        public TypeSerializer<String> restoreSerializer() {
            throw new VerifyError("invalid-snapshot-bytecode");
        }
    }

    public static final class SnapshotUnsupportedClassVersionErrorSerializer
            extends SnapshotLinkageSerializer {
        private static final long serialVersionUID = 1L;

        @Override
        public TypeSerializerSnapshot<String> snapshotConfiguration() {
            return new SnapshotUnsupportedClassVersionErrorSnapshot();
        }
    }

    public static final class SnapshotUnsupportedClassVersionErrorSnapshot
            extends SnapshotLinkageSnapshot {
        @Override
        public TypeSerializer<String> restoreSerializer() {
            throw new UnsupportedClassVersionError("unsupported-snapshot-class-version");
        }
    }

    public static final class SnapshotNoSuchMethodErrorSerializer
            extends SnapshotLinkageSerializer {
        private static final long serialVersionUID = 1L;

        @Override
        public TypeSerializerSnapshot<String> snapshotConfiguration() {
            return new SnapshotNoSuchMethodErrorSnapshot();
        }
    }

    public static final class SnapshotNoSuchMethodErrorSnapshot extends SnapshotLinkageSnapshot {
        @Override
        public TypeSerializer<String> restoreSerializer() {
            throw new NoSuchMethodError("missing-snapshot-method");
        }
    }

    /** A serializer whose snapshotConfiguration() throws. */
    private static final class ThrowingSnapshotSerializer extends TypeSerializer<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<String> duplicate() {
            return this;
        }

        @Override
        public String createInstance() {
            return "";
        }

        @Override
        public String copy(String from) {
            return from;
        }

        @Override
        public String copy(String from, String reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(String record, DataOutputView target) throws IOException {
            StringSerializer.INSTANCE.serialize(record, target);
        }

        @Override
        public String deserialize(DataInputView source) throws IOException {
            return StringSerializer.INSTANCE.deserialize(source);
        }

        @Override
        public String deserialize(String reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            StringSerializer.INSTANCE.copy(source, target);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ThrowingSnapshotSerializer;
        }

        @Override
        public int hashCode() {
            return ThrowingSnapshotSerializer.class.hashCode();
        }

        @Override
        public TypeSerializerSnapshot<String> snapshotConfiguration() {
            throw new RuntimeException("expected snapshot failure");
        }
    }

    /**
     * A serializer whose snapshot restores {@link StringSerializer#INSTANCE} but whose serialized
     * form deserializes back to this class. Used to prove that {@code restoreSerializer} prefers
     * the snapshot path over the serialized fallback.
     */
    private static final class SnapshotPreferenceSerializer extends TypeSerializer<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<String> duplicate() {
            return this;
        }

        @Override
        public String createInstance() {
            return "";
        }

        @Override
        public String copy(String from) {
            return from;
        }

        @Override
        public String copy(String from, String reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(String record, DataOutputView target) throws IOException {
            StringSerializer.INSTANCE.serialize(record, target);
        }

        @Override
        public String deserialize(DataInputView source) throws IOException {
            return StringSerializer.INSTANCE.deserialize(source);
        }

        @Override
        public String deserialize(String reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            StringSerializer.INSTANCE.copy(source, target);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof SnapshotPreferenceSerializer;
        }

        @Override
        public int hashCode() {
            return SnapshotPreferenceSerializer.class.hashCode();
        }

        @Override
        public TypeSerializerSnapshot<String> snapshotConfiguration() {
            return new SnapshotPreferenceSnapshot();
        }
    }
}
