package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Pure byte-level tests for {@link CobbleCanonicalKeyEncoder}. Cobble row keys are built with
 * {@link CobbleStateKeySerializer.ReusableSerializedKeyBuilder}, converted to canonical keys with
 * the encoder, then decoded back with {@link CompositeKeySerializationUtils} to verify round-trip
 * correctness. No DB or backend is involved.
 */
class CobbleCanonicalKeyEncoderTest {

    private static final int TOTAL_KEY_GROUPS = 16;
    private static final String STATE_NAME = "test-state";

    @Test
    void simpleKeyFixedLengthSerializers() throws Exception {
        TypeSerializer<Integer> keySer = IntSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> nsSer = VoidNamespaceSerializer.INSTANCE;
        int key = 42;
        int keyGroup = 7;

        // Build Cobble row key: key + namespace (VoidNamespace = 1 byte 0x00), no length suffix
        // because IntSerializer is fixed-length.
        byte[] cobbleRowKey = buildSimpleKey(keySer, key, nsSer, VoidNamespace.INSTANCE);

        // Encode to canonical.
        CobbleCanonicalKeyEncoder encoder = new CobbleCanonicalKeyEncoder(TOTAL_KEY_GROUPS);
        byte[] canonicalKey =
                encoder.encodeSimpleKey(keyGroup, cobbleRowKey, keySer, nsSer, STATE_NAME);

        // Decode canonical key back.
        DataInputDeserializer input = new DataInputDeserializer(canonicalKey);
        int decodedKeyGroup =
                CompositeKeySerializationUtils.readKeyGroup(
                        CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                                TOTAL_KEY_GROUPS),
                        input);
        assertEquals(keyGroup, decodedKeyGroup, "key group mismatch");

        boolean ambiguous = CompositeKeySerializationUtils.isAmbiguousKeyPossible(keySer, nsSer);
        Integer decodedKey = CompositeKeySerializationUtils.readKey(keySer, input, ambiguous);
        assertEquals(key, decodedKey, "key mismatch");
        VoidNamespace decodedNs =
                CompositeKeySerializationUtils.readNamespace(nsSer, input, ambiguous);
        assertEquals(VoidNamespace.INSTANCE, decodedNs, "namespace mismatch");
        assertEquals(0, input.available(), "no trailing bytes expected");
    }

    @Test
    void simpleKeyVariableLengthSerializers() throws Exception {
        TypeSerializer<String> keySer = StringSerializer.INSTANCE;
        TypeSerializer<String> nsSer = StringSerializer.INSTANCE;
        String key = "my-key";
        String namespace = "my-namespace";
        int keyGroup = 3;

        byte[] cobbleRowKey = buildSimpleKey(keySer, key, nsSer, namespace);

        CobbleCanonicalKeyEncoder encoder = new CobbleCanonicalKeyEncoder(TOTAL_KEY_GROUPS);
        byte[] canonicalKey =
                encoder.encodeSimpleKey(keyGroup, cobbleRowKey, keySer, nsSer, STATE_NAME);

        // Decode.
        DataInputDeserializer input = new DataInputDeserializer(canonicalKey);
        int decodedKeyGroup =
                CompositeKeySerializationUtils.readKeyGroup(
                        CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                                TOTAL_KEY_GROUPS),
                        input);
        assertEquals(keyGroup, decodedKeyGroup);

        boolean ambiguous = CompositeKeySerializationUtils.isAmbiguousKeyPossible(keySer, nsSer);
        String decodedKey = CompositeKeySerializationUtils.readKey(keySer, input, ambiguous);
        assertEquals(key, decodedKey, "key mismatch");
        String decodedNs = CompositeKeySerializationUtils.readNamespace(nsSer, input, ambiguous);
        assertEquals(namespace, decodedNs, "namespace mismatch");
        assertEquals(0, input.available(), "no trailing bytes expected");
    }

    @Test
    void mapKeyAllFixedLength() throws Exception {
        TypeSerializer<Integer> keySer = IntSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> nsSer = VoidNamespaceSerializer.INSTANCE;
        TypeSerializer<Integer> ukSer = IntSerializer.INSTANCE;
        int key = 42;
        int mapKey = 99;
        int keyGroup = 5;

        byte[] cobbleRowKey =
                buildMapKey(keySer, key, nsSer, VoidNamespace.INSTANCE, ukSer, mapKey);

        CobbleCanonicalKeyEncoder encoder = new CobbleCanonicalKeyEncoder(TOTAL_KEY_GROUPS);
        byte[] canonicalKey =
                encoder.encodeMapKey(keyGroup, cobbleRowKey, keySer, nsSer, ukSer, STATE_NAME);

        // Decode canonical key.
        DataInputDeserializer input = new DataInputDeserializer(canonicalKey);
        int decodedKeyGroup =
                CompositeKeySerializationUtils.readKeyGroup(
                        CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                                TOTAL_KEY_GROUPS),
                        input);
        assertEquals(keyGroup, decodedKeyGroup);

        boolean ambiguous = CompositeKeySerializationUtils.isAmbiguousKeyPossible(keySer, nsSer);
        Integer decodedKey = CompositeKeySerializationUtils.readKey(keySer, input, ambiguous);
        assertEquals(key, decodedKey, "key mismatch");
        VoidNamespace decodedNs =
                CompositeKeySerializationUtils.readNamespace(nsSer, input, ambiguous);
        assertEquals(VoidNamespace.INSTANCE, decodedNs, "namespace mismatch");
        Integer decodedMapKey = ukSer.deserialize(input);
        assertEquals(mapKey, decodedMapKey, "map user key mismatch");
        assertEquals(0, input.available(), "no trailing bytes expected");
    }

    @Test
    void mapKeyAllVariableLength() throws Exception {
        TypeSerializer<String> keySer = StringSerializer.INSTANCE;
        TypeSerializer<String> nsSer = StringSerializer.INSTANCE;
        TypeSerializer<String> ukSer = StringSerializer.INSTANCE;
        String key = "state-key";
        String namespace = "ns-1";
        String mapKey = "map-user-key";
        int keyGroup = 11;

        byte[] cobbleRowKey = buildMapKey(keySer, key, nsSer, namespace, ukSer, mapKey);

        CobbleCanonicalKeyEncoder encoder = new CobbleCanonicalKeyEncoder(TOTAL_KEY_GROUPS);
        byte[] canonicalKey =
                encoder.encodeMapKey(keyGroup, cobbleRowKey, keySer, nsSer, ukSer, STATE_NAME);

        // Decode canonical key.
        DataInputDeserializer input = new DataInputDeserializer(canonicalKey);
        int decodedKeyGroup =
                CompositeKeySerializationUtils.readKeyGroup(
                        CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                                TOTAL_KEY_GROUPS),
                        input);
        assertEquals(keyGroup, decodedKeyGroup);

        boolean ambiguous = CompositeKeySerializationUtils.isAmbiguousKeyPossible(keySer, nsSer);
        String decodedKey = CompositeKeySerializationUtils.readKey(keySer, input, ambiguous);
        assertEquals(key, decodedKey, "key mismatch");
        String decodedNs = CompositeKeySerializationUtils.readNamespace(nsSer, input, ambiguous);
        assertEquals(namespace, decodedNs, "namespace mismatch");
        String decodedMapKey = ukSer.deserialize(input);
        assertEquals(mapKey, decodedMapKey, "map user key mismatch");
        assertEquals(0, input.available(), "no trailing bytes expected");
    }

    @Test
    void mapKeyVoidNamespaceWithVariableMapKey() throws Exception {
        TypeSerializer<Integer> keySer = IntSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> nsSer = VoidNamespaceSerializer.INSTANCE;
        TypeSerializer<String> ukSer = StringSerializer.INSTANCE;
        int key = 7;
        String mapKey = "variable-map-key";
        int keyGroup = 0;

        byte[] cobbleRowKey =
                buildMapKey(keySer, key, nsSer, VoidNamespace.INSTANCE, ukSer, mapKey);

        CobbleCanonicalKeyEncoder encoder = new CobbleCanonicalKeyEncoder(TOTAL_KEY_GROUPS);
        byte[] canonicalKey =
                encoder.encodeMapKey(keyGroup, cobbleRowKey, keySer, nsSer, ukSer, STATE_NAME);

        DataInputDeserializer input = new DataInputDeserializer(canonicalKey);
        int decodedKeyGroup =
                CompositeKeySerializationUtils.readKeyGroup(
                        CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                                TOTAL_KEY_GROUPS),
                        input);
        assertEquals(keyGroup, decodedKeyGroup);

        boolean ambiguous = CompositeKeySerializationUtils.isAmbiguousKeyPossible(keySer, nsSer);
        Integer decodedKey = CompositeKeySerializationUtils.readKey(keySer, input, ambiguous);
        assertEquals(key, decodedKey, "key mismatch");
        VoidNamespace decodedNs =
                CompositeKeySerializationUtils.readNamespace(nsSer, input, ambiguous);
        assertEquals(VoidNamespace.INSTANCE, decodedNs, "namespace mismatch");
        String decodedMapKey = ukSer.deserialize(input);
        assertEquals(mapKey, decodedMapKey, "map user key mismatch");
        assertEquals(0, input.available(), "no trailing bytes expected");
    }

    @Test
    void mapKeyNonVoidNamespace() throws Exception {
        TypeSerializer<Integer> keySer = IntSerializer.INSTANCE;
        TypeSerializer<String> nsSer = StringSerializer.INSTANCE;
        TypeSerializer<Integer> ukSer = IntSerializer.INSTANCE;
        int key = 15;
        String namespace = "custom-ns";
        int mapKey = 200;
        int keyGroup = 9;

        byte[] cobbleRowKey = buildMapKey(keySer, key, nsSer, namespace, ukSer, mapKey);

        CobbleCanonicalKeyEncoder encoder = new CobbleCanonicalKeyEncoder(TOTAL_KEY_GROUPS);
        byte[] canonicalKey =
                encoder.encodeMapKey(keyGroup, cobbleRowKey, keySer, nsSer, ukSer, STATE_NAME);

        DataInputDeserializer input = new DataInputDeserializer(canonicalKey);
        int decodedKeyGroup =
                CompositeKeySerializationUtils.readKeyGroup(
                        CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                                TOTAL_KEY_GROUPS),
                        input);
        assertEquals(keyGroup, decodedKeyGroup);

        boolean ambiguous = CompositeKeySerializationUtils.isAmbiguousKeyPossible(keySer, nsSer);
        Integer decodedKey = CompositeKeySerializationUtils.readKey(keySer, input, ambiguous);
        assertEquals(key, decodedKey, "key mismatch");
        String decodedNs = CompositeKeySerializationUtils.readNamespace(nsSer, input, ambiguous);
        assertEquals(namespace, decodedNs, "namespace mismatch");
        Integer decodedMapKey = ukSer.deserialize(input);
        assertEquals(mapKey, decodedMapKey, "map user key mismatch");
        assertEquals(0, input.available(), "no trailing bytes expected");
    }

    @Test
    void timerKeyRoundTrip() throws Exception {
        int keyGroup = 13;
        byte[] timerBytes = {1, 2, 3, 4, 5, 6, 7, 8};

        CobbleCanonicalKeyEncoder encoder = new CobbleCanonicalKeyEncoder(TOTAL_KEY_GROUPS);
        byte[] canonicalKey = encoder.encodeTimerKey(keyGroup, timerBytes);

        DataInputDeserializer input = new DataInputDeserializer(canonicalKey);
        int decodedKeyGroup =
                CompositeKeySerializationUtils.readKeyGroup(
                        CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                                TOTAL_KEY_GROUPS),
                        input);
        assertEquals(keyGroup, decodedKeyGroup, "key group mismatch");

        // The remaining bytes should be the timer element bytes verbatim.
        byte[] remaining = new byte[input.available()];
        input.readFully(remaining);
        assertArrayEquals(
                timerBytes, remaining, "timer bytes should be verbatim after key-group prefix");
    }

    @Test
    void multipleKeyGroups() throws Exception {
        TypeSerializer<Integer> keySer = IntSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> nsSer = VoidNamespaceSerializer.INSTANCE;
        int key = 100;

        int[] keyGroups = {0, 7, 15};
        for (int keyGroup : keyGroups) {
            byte[] cobbleRowKey = buildSimpleKey(keySer, key, nsSer, VoidNamespace.INSTANCE);

            CobbleCanonicalKeyEncoder encoder = new CobbleCanonicalKeyEncoder(TOTAL_KEY_GROUPS);
            byte[] canonicalKey =
                    encoder.encodeSimpleKey(keyGroup, cobbleRowKey, keySer, nsSer, STATE_NAME);

            DataInputDeserializer input = new DataInputDeserializer(canonicalKey);
            int decodedKeyGroup =
                    CompositeKeySerializationUtils.readKeyGroup(
                            CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                                    TOTAL_KEY_GROUPS),
                            input);
            assertEquals(keyGroup, decodedKeyGroup, "key group " + keyGroup + " mismatch");
        }
    }

    // ------------------------------------------------------------------------------------------
    //  Null-namespace rejection (P1)
    // ------------------------------------------------------------------------------------------

    /**
     * A simple-key row with a null namespace (zero namespace bytes) must be rejected with a clear
     * error referencing the state name, because the Flink canonical format has no null-namespace
     * representation.
     */
    @Test
    void simpleKeyNullNamespaceIsRejected() throws Exception {
        TypeSerializer<Integer> keySer = IntSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> nsSer = VoidNamespaceSerializer.INSTANCE;
        int key = 42;
        int keyGroup = 7;

        // Build a Cobble row key with null namespace — the builder encodes null as zero bytes.
        byte[] cobbleRowKey = buildSimpleKey(keySer, key, nsSer, null);

        CobbleCanonicalKeyEncoder encoder = new CobbleCanonicalKeyEncoder(TOTAL_KEY_GROUPS);
        IOException error =
                assertThrows(
                        IOException.class,
                        () ->
                                encoder.encodeSimpleKey(
                                        keyGroup, cobbleRowKey, keySer, nsSer, STATE_NAME));
        assertTrue(
                error.getMessage().contains("null (zero-byte) namespace"),
                "expected null-namespace rejection but got: " + error.getMessage());
        assertTrue(
                error.getMessage().contains(STATE_NAME),
                "error should reference the state name: " + error.getMessage());
        assertTrue(
                error.getMessage().contains(String.valueOf(keyGroup)),
                "error should reference the key group: " + error.getMessage());
    }

    /**
     * A map-key row with a null namespace must not silently produce wrong bytes. For map keys with
     * a fixed-length namespace serializer (e.g. VoidNamespaceSerializer), a null namespace causes
     * the 0x00 separator to be consumed as the namespace, which then causes the user key
     * deserialization to fail. The important contract is that the encoder throws a clear error
     * referencing the state name rather than silently producing a corrupted canonical key.
     */
    @Test
    void mapKeyNullNamespaceIsRejected() throws Exception {
        TypeSerializer<Integer> keySer = IntSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> nsSer = VoidNamespaceSerializer.INSTANCE;
        TypeSerializer<Integer> ukSer = IntSerializer.INSTANCE;
        int key = 42;
        int mapKey = 99;
        int keyGroup = 5;

        byte[] cobbleRowKey = buildMapKey(keySer, key, nsSer, null, ukSer, mapKey);

        CobbleCanonicalKeyEncoder encoder = new CobbleCanonicalKeyEncoder(TOTAL_KEY_GROUPS);
        IOException error =
                assertThrows(
                        IOException.class,
                        () ->
                                encoder.encodeMapKey(
                                        keyGroup, cobbleRowKey, keySer, nsSer, ukSer, STATE_NAME));
        assertNotNull(error.getMessage(), "error message should not be null");
        assertTrue(
                error.getMessage().contains(STATE_NAME),
                "error should reference the state name: " + error.getMessage());
    }

    /**
     * A simple-key row with a null namespace and variable-length key+namespace serializers must be
     * rejected. This is the critical layout the previous implementation missed: when both key and
     * namespace are variable-length, the Cobble row key carries a 4-byte {@code keyLength} trailer,
     * so {@code remaining == 4} (not 0) after deserializing the key. The encoder must compute the
     * namespace byte range from the trailer, detect that it is zero, and reject the row.
     */
    @Test
    void simpleKeyNullNamespaceWithVariableLengthSerializersIsRejected() throws Exception {
        TypeSerializer<String> keySer = StringSerializer.INSTANCE;
        TypeSerializer<String> nsSer = StringSerializer.INSTANCE;
        String key = "my-key";
        int keyGroup = 3;

        // Build a Cobble row key with null namespace — the builder encodes null as zero bytes.
        // Layout: [key bytes][keyLength trailer(4)] — namespace is zero bytes.
        byte[] cobbleRowKey = buildSimpleKey(keySer, key, nsSer, null);

        CobbleCanonicalKeyEncoder encoder = new CobbleCanonicalKeyEncoder(TOTAL_KEY_GROUPS);
        IOException error =
                assertThrows(
                        IOException.class,
                        () ->
                                encoder.encodeSimpleKey(
                                        keyGroup, cobbleRowKey, keySer, nsSer, STATE_NAME));
        assertTrue(
                error.getMessage().contains("null (zero-byte) namespace"),
                "expected null-namespace rejection but got: " + error.getMessage());
        assertTrue(
                error.getMessage().contains(STATE_NAME),
                "error should reference the state name: " + error.getMessage());
    }

    /**
     * A map-key row with a null namespace and all variable-length serializers must be rejected.
     * This layout stores both {@code keyLength} and {@code namespaceLength} trailers — the
     * namespaceLength trailer reads 0, so the encoder can detect the null namespace directly.
     */
    @Test
    void mapKeyNullNamespaceWithStoredNamespaceLengthIsRejected() throws Exception {
        TypeSerializer<String> keySer = StringSerializer.INSTANCE;
        TypeSerializer<String> nsSer = StringSerializer.INSTANCE;
        TypeSerializer<String> ukSer = StringSerializer.INSTANCE;
        String key = "my-key";
        String mapKey = "my-map-key";
        int keyGroup = 3;

        // Build a Cobble map row key with null namespace.
        // Layout: [key bytes][0x00 separator][userKey bytes][keyLen(4)][nsLen(4)] — namespace is
        // zero bytes, so nsLen trailer = 0.
        byte[] cobbleRowKey = buildMapKey(keySer, key, nsSer, null, ukSer, mapKey);

        CobbleCanonicalKeyEncoder encoder = new CobbleCanonicalKeyEncoder(TOTAL_KEY_GROUPS);
        IOException error =
                assertThrows(
                        IOException.class,
                        () ->
                                encoder.encodeMapKey(
                                        keyGroup, cobbleRowKey, keySer, nsSer, ukSer, STATE_NAME));
        assertTrue(
                error.getMessage().contains("null (zero-byte) namespace"),
                "expected null-namespace rejection but got: " + error.getMessage());
        assertTrue(
                error.getMessage().contains(STATE_NAME),
                "error should reference the state name: " + error.getMessage());
    }

    // ------------------------------------------------------------------------------------------
    //  Malformed key rejection (P2)
    // ------------------------------------------------------------------------------------------

    /**
     * A map row key with trailing bytes after the user key (within the user-key slice) must be
     * rejected. This catches corruption where extra bytes are inserted between the user key and the
     * trailer.
     */
    @Test
    void mapKeyTrailingBytesAfterUserKeyAreRejected() throws Exception {
        TypeSerializer<Integer> keySer = IntSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> nsSer = VoidNamespaceSerializer.INSTANCE;
        TypeSerializer<Integer> ukSer = IntSerializer.INSTANCE;
        int key = 42;
        int mapKey = 99;
        int keyGroup = 5;

        // Build a valid map row key, then append one extra byte. All serializers are fixed-length,
        // so trailerBytes = 0 and the extra byte falls inside the user-key slice.
        byte[] validKey = buildMapKey(keySer, key, nsSer, VoidNamespace.INSTANCE, ukSer, mapKey);
        byte[] malformedKey = new byte[validKey.length + 1];
        System.arraycopy(validKey, 0, malformedKey, 0, validKey.length);
        malformedKey[validKey.length] = 0x77; // arbitrary trailing byte

        CobbleCanonicalKeyEncoder encoder = new CobbleCanonicalKeyEncoder(TOTAL_KEY_GROUPS);
        IOException error =
                assertThrows(
                        IOException.class,
                        () ->
                                encoder.encodeMapKey(
                                        keyGroup, malformedKey, keySer, nsSer, ukSer, STATE_NAME));
        assertTrue(
                error.getMessage().contains("trailing byte"),
                "expected trailing-bytes rejection but got: " + error.getMessage());
    }

    /**
     * A map row key missing the 0x00 separator must be rejected with a clear error. We use a
     * variable-length namespace serializer (StringSerializer) so the namespace bytes are non-zero
     * and the separator check is meaningful — with VoidNamespace (which serializes to 0x00), the
     * separator byte is indistinguishable from the namespace byte. The user key value is chosen so
     * its first serialized byte is non-zero (avoiding a false separator match).
     */
    @Test
    void mapKeyMissingSeparatorIsRejected() throws Exception {
        TypeSerializer<Integer> keySer = IntSerializer.INSTANCE;
        TypeSerializer<String> nsSer = StringSerializer.INSTANCE;
        TypeSerializer<Integer> ukSer = IntSerializer.INSTANCE;
        int key = 42;
        String namespace = "ns";
        int mapKey = 0x01020304; // first byte is 0x01, not 0x00
        int keyGroup = 5;

        // Manually build: key(4) + ns("ns") + uk(4) — no 0x00 separator.
        DataOutputSerializer out = new DataOutputSerializer(32);
        keySer.serialize(key, out);
        nsSer.serialize(namespace, out);
        ukSer.serialize(mapKey, out);
        byte[] malformedKey = out.getCopyOfBuffer();

        CobbleCanonicalKeyEncoder encoder = new CobbleCanonicalKeyEncoder(TOTAL_KEY_GROUPS);
        IOException error =
                assertThrows(
                        IOException.class,
                        () ->
                                encoder.encodeMapKey(
                                        keyGroup, malformedKey, keySer, nsSer, ukSer, STATE_NAME));
        assertTrue(
                error.getMessage().contains("0x00 separator"),
                "expected separator rejection but got: " + error.getMessage());
    }

    // ------------------------------------------------------------------------------------------
    //  Helpers — build Cobble row keys using the same CobbleStateKeySerializer logic
    // ------------------------------------------------------------------------------------------

    private static <K, N> byte[] buildSimpleKey(
            TypeSerializer<K> keySer, K key, TypeSerializer<N> nsSer, N namespace)
            throws IOException {
        CobbleStateKeySerializer.ReusableSerializedKeyBuilder<K, N> builder =
                new CobbleStateKeySerializer.ReusableSerializedKeyBuilder<>(keySer, nsSer, 64);
        return builder.buildKeyAndNamespace(key, namespace);
    }

    private static <K, N, UK> byte[] buildMapKey(
            TypeSerializer<K> keySer,
            K key,
            TypeSerializer<N> nsSer,
            N namespace,
            TypeSerializer<UK> ukSer,
            UK mapKey)
            throws IOException {
        CobbleStateKeySerializer.ReusableSerializedKeyBuilder<K, N> builder =
                new CobbleStateKeySerializer.ReusableSerializedKeyBuilder<>(keySer, nsSer, 64);
        return builder.buildMapKeyNamespaceAndUserKey(key, ukSer, mapKey, namespace);
    }
}
