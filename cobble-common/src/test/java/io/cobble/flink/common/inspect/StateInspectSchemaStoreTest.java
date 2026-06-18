package io.cobble.flink.common.inspect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

/** Round-trip and restore tests for the inspect schema model and store. */
class StateInspectSchemaStoreTest {

    @TempDir private Path tempDir;

    @Test
    void emptyStoreRoundTrips() throws IOException {
        StateInspectSchemaStore store = new StateInspectSchemaStore(Collections.emptyList());
        byte[] bytes = store.toBytes();
        assertEquals(StateInspectSchemaStore.MAGIC, readInt(bytes, 0));

        StateInspectSchemaStore restored = StateInspectSchemaStore.fromBytes(bytes);
        assertTrue(restored.isEmpty());
    }

    @Test
    void emptyBytesProduceEmptyStore() throws IOException {
        StateInspectSchemaStore restored = StateInspectSchemaStore.fromBytes(new byte[0]);
        assertTrue(restored.isEmpty());
        StateInspectSchemaStore fromNull = StateInspectSchemaStore.fromBytes(null);
        assertTrue(fromNull.isEmpty());
    }

    @Test
    void nonMagicBytesProduceEmptyStore() throws IOException {
        byte[] notSchema = new byte[] {'h', 'e', 'l', 'l', 'o'};
        StateInspectSchemaStore restored =
                StateInspectSchemaStore.read(new ByteArrayInputStream(notSchema));
        assertTrue(restored.isEmpty());
    }

    @Test
    void fileInputStreamsDoNotNeedMarkReset() throws IOException {
        Path emptyFile = tempDir.resolve("empty-schema.bin");
        java.nio.file.Files.write(emptyFile, new byte[0]);
        try (FileInputStream input = new FileInputStream(emptyFile.toFile())) {
            assertTrue(StateInspectSchemaStore.read(input).isEmpty());
        }

        Path nonMagicFile = tempDir.resolve("not-schema.bin");
        try (FileOutputStream output = new FileOutputStream(nonMagicFile.toFile())) {
            output.write(new byte[] {'n', 'o', 'p', 'e'});
        }
        try (FileInputStream input = new FileInputStream(nonMagicFile.toFile())) {
            assertTrue(StateInspectSchemaStore.read(input).isEmpty());
        }
    }

    @Test
    void validSidecarReadsBackFromFileInputStream() throws IOException {
        // The monitor opens the sidecar via FSDataInputStream/FileInputStream, which do NOT support
        // mark/reset. A complete (magic + version + schema) store written to a file must read back
        // correctly through such a stream — this is the real monitor path, not just a byte[] path.
        StateInspectSchemaStore store =
                new StateInspectSchemaStore(
                        Arrays.asList(
                                StateInspectSchema.forValue(
                                        "value-state",
                                        "value-state",
                                        false,
                                        IntSerializer.INSTANCE,
                                        StringSerializer.INSTANCE,
                                        StringSerializer.INSTANCE)));
        Path sidecar = tempDir.resolve("COBBLE-SCHEMA-deadbeef");
        java.nio.file.Files.write(sidecar, store.toBytes());

        try (FileInputStream input = new FileInputStream(sidecar.toFile())) {
            StateInspectSchemaStore restored = StateInspectSchemaStore.read(input);
            assertEquals(1, restored.schemas().size());
            assertEquals("value-state", restored.schemas().get(0).stateName());
            assertEquals(StateKind.VALUE, restored.schemas().get(0).stateKind());
        }
    }

    @Test
    void valueStateSchemaRoundTrips() throws IOException {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "value-state",
                        "value-state",
                        false,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        StateInspectSchemaStore restored =
                roundTrip(new StateInspectSchemaStore(Arrays.asList(schema)));

        assertEquals(1, restored.schemas().size());
        StateInspectSchema out = restored.schemas().get(0);
        assertEquals("value-state", out.stateName());
        assertEquals(StateKind.VALUE, out.stateKind());
        assertFalse(out.ttlEnabled());

        // IntSerializer is fixed-length (4), StringSerializer is variable-length (-1). Key length
        // is
        // therefore not stored (both parts must be variable for the key+namespace trailer).
        assertTrue(out.keyFixedLength());
        assertFalse(out.namespaceFixedLength());
        assertFalse(out.keyLengthStored());

        assertEquals("value-state", out.columnFamily());
        assertEquals(
                IntSerializer.INSTANCE.getClass().getName(),
                out.keySerializer().serializerClassName());
        assertEquals(4, out.keySerializer().lengthTag());
        assertEquals(-1, out.valueSerializer().lengthTag());

        // Restored serializer should be functional.
        TypeSerializer<String> valueSerializer =
                out.valueSerializer().restoreSerializer(getClass().getClassLoader());
        assertNotNull(valueSerializer);
        assertEquals("hello", roundTripString(valueSerializer, "hello"));
    }

    @Test
    void listStateSchemaRoundTrips() throws IOException {
        StateInspectSchema schema =
                StateInspectSchema.forList(
                        "list-state",
                        "list-state",
                        true,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        StateInspectSchemaStore restored =
                roundTrip(new StateInspectSchemaStore(Arrays.asList(schema)));

        StateInspectSchema out = restored.schemas().get(0);
        assertEquals(StateKind.LIST, out.stateKind());
        assertTrue(out.ttlEnabled());
        // key+namespace both variable-length strings -> key length is stored.
        assertFalse(out.keyFixedLength());
        assertFalse(out.namespaceFixedLength());
        assertTrue(out.keyLengthStored());
        assertNotNull(out.listElementSerializer());
        assertNull(out.valueSerializer());
        assertNull(out.mapUserKeySerializer());
    }

    @Test
    void mapStateSchemaRoundTrips() throws IOException {
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "map-state",
                        "map-state",
                        false,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        StateInspectSchemaStore restored =
                roundTrip(new StateInspectSchemaStore(Arrays.asList(schema)));

        StateInspectSchema out = restored.schemas().get(0);
        assertEquals(StateKind.MAP, out.stateKind());
        // All three of key/namespace/user-key are fixed-length IntSerializer, so no trailer lengths
        // are stored.
        assertTrue(out.keyFixedLength());
        assertTrue(out.namespaceFixedLength());
        assertFalse(out.keyLengthStored());
        assertTrue(out.mapUserKeyFixedLength());
        assertFalse(out.mapKeyLengthStored());
        assertFalse(out.mapNamespaceLengthStored());
        assertNotNull(out.mapUserKeySerializer());
        assertNotNull(out.mapUserValueSerializer());
        assertNull(out.valueSerializer());
        assertNull(out.listElementSerializer());
    }

    @Test
    void mapStateMixedLengthsStoreKeyLength() throws IOException {
        // key=Int(fixed), namespace=String(var), userKey=String(var): 2 variable parts -> exactly
        // one
        // length persisted. key is fixed so it must not be the persisted one; namespace length is.
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "map-mixed",
                        "map-mixed",
                        false,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        StateInspectSchemaStore restored =
                roundTrip(new StateInspectSchemaStore(Arrays.asList(schema)));
        StateInspectSchema out = restored.schemas().get(0);
        assertFalse(out.mapKeyLengthStored());
        assertTrue(out.mapNamespaceLengthStored());
    }

    @Test
    void multipleSchemasIndexedByName() throws IOException {
        StateInspectSchema value =
                StateInspectSchema.forValue(
                        "v",
                        "v",
                        false,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        StateInspectSchema list =
                StateInspectSchema.forList(
                        "l",
                        "l",
                        false,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        StateInspectSchemaStore restored =
                roundTrip(new StateInspectSchemaStore(Arrays.asList(value, list)));
        assertEquals(2, restored.schemas().size());
        assertEquals(StateKind.VALUE, restored.byStateName().get("v").stateKind());
        assertEquals(StateKind.LIST, restored.byStateName().get("l").stateKind());
    }

    @Test
    void flinkListAndMapSerializersRoundTripThroughSnapshot() throws IOException {
        // Sanity-check that the Flink collection serializers the backend uses can be captured and
        // restored, mirroring the backend's createListState / createMapState serializer wiring.
        ListSerializer<Integer> flinkList = new ListSerializer<>(IntSerializer.INSTANCE);
        MapSerializer<Integer, String> flinkMap =
                new MapSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE);

        SerializerInspectSchema listSchema = SerializerInspectSchema.fromSerializer(flinkList);
        SerializerInspectSchema mapSchema = SerializerInspectSchema.fromSerializer(flinkMap);

        TypeSerializer<?> restoredList = listSchema.restoreSerializer(getClass().getClassLoader());
        TypeSerializer<?> restoredMap = mapSchema.restoreSerializer(getClass().getClassLoader());
        assertNotNull(restoredList);
        assertNotNull(restoredMap);
        // ListSerializer restores as a ListSerializer whose element serializer is IntSerializer:
        // round-tripping an Integer element confirms the restored serializer is live.
        assertTrue(restoredList instanceof ListSerializer);
        TypeSerializer<Integer> restoredElement =
                ((ListSerializer<Integer>) restoredList).getElementSerializer();
        assertEquals(42, roundTripInt(restoredElement, 42));
    }

    @Test
    void serializerSnapshotFailuresDegradeToRawInspectMetadata() {
        SerializerInspectSchema schema =
                SerializerInspectSchema.fromSerializer(new ThrowingSnapshotSerializer());
        assertEquals(ThrowingSnapshotSerializer.class.getName(), schema.serializerClassName());
        assertEquals(-1, schema.lengthTag());
        assertNull(schema.snapshotBytes());
        assertNotNull(schema.serializedSerializerBytes());
    }

    @Test
    void unknownVersionFailsClearly() throws IOException {
        // Build bytes with magic + a bogus version.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(StateInspectSchemaStore.MAGIC >> 24);
        out.write(StateInspectSchemaStore.MAGIC >> 16);
        out.write(StateInspectSchemaStore.MAGIC >> 8);
        out.write(StateInspectSchemaStore.MAGIC);
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(99); // version 99
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(0); // count 0
        byte[] bytes = out.toByteArray();

        IOException error =
                assertThrows(
                        IOException.class,
                        () -> StateInspectSchemaStore.read(new ByteArrayInputStream(bytes)));
        assertTrue(error.getMessage().contains("Unsupported Cobble inspect schema version"));
    }

    @Test
    void schemaEqualsAndHashCodeRoundTrip() throws IOException {
        StateInspectSchema original =
                StateInspectSchema.forValue(
                        "eq-test",
                        "cf-eq-test",
                        true,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        StateInspectSchemaStore restored =
                roundTrip(new StateInspectSchemaStore(Arrays.asList(original)));
        assertEquals(original, restored.schemas().get(0));
        assertEquals(original.hashCode(), restored.schemas().get(0).hashCode());
    }

    @Test
    void columnFamilyParticipatesInEquals() {
        StateInspectSchema a =
                StateInspectSchema.forValue(
                        "s",
                        "cf-a",
                        false,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        StateInspectSchema b =
                StateInspectSchema.forValue(
                        "s",
                        "cf-b",
                        false,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        assertFalse(a.equals(b));
    }

    @Test
    void serializerInspectSchemaEqualsAndHashCodeRoundTrip() throws IOException {
        SerializerInspectSchema original =
                SerializerInspectSchema.fromSerializer(IntSerializer.INSTANCE);
        SerializerInspectSchema restored = roundTripSerializerInspectSchema(original);
        assertEquals(original, restored);
        assertEquals(original.hashCode(), restored.hashCode());
    }

    private static StateInspectSchemaStore roundTrip(StateInspectSchemaStore store)
            throws IOException {
        return StateInspectSchemaStore.fromBytes(store.toBytes());
    }

    private static String roundTripString(TypeSerializer<String> serializer, String value)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(32);
        serializer.serialize(value, out);
        return serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
    }

    @SuppressWarnings("unchecked")
    private static int roundTripInt(TypeSerializer<Integer> serializer, int value)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(16);
        serializer.serialize(value, out);
        TypeSerializer<Integer> typed = (TypeSerializer<Integer>) serializer;
        return typed.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }

    private static SerializerInspectSchema roundTripSerializerInspectSchema(
            SerializerInspectSchema schema) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        schema.write(out);
        return SerializerInspectSchema.read(new DataInputDeserializer(out.getCopyOfBuffer()));
    }

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
}
