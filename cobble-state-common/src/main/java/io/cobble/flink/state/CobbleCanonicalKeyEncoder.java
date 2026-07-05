package io.cobble.flink.state;

import io.cobble.flink.common.inspect.StateRowKeyLayout;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;

import java.io.IOException;

/**
 * Converts Cobble row keys into Flink canonical composite key bytes for savepoint export.
 *
 * <p>Cobble row keys omit the key-group prefix (the Cobble bucket encodes that). The canonical
 * savepoint key requires the prefix. This encoder prepends it and reassembles the key into Flink's
 * canonical layout via {@link CompositeKeySerializationUtils}, ensuring the output is byte-for-byte
 * compatible with what {@code CanonicalSavepointRestoreOperation} expects on restore.
 *
 * <h2>Null namespace contract</h2>
 *
 * Cobble encodes a null namespace as zero bytes (see {@code
 * CobbleStateKeySerializer.ReusableSerializedKeyBuilder.ensureNamespaceSerialized}). The Flink
 * canonical savepoint format does not have a representation for null namespaces — keyed state
 * always has at least {@code VoidNamespace} (which serializes to 1 byte). The encoder detects
 * zero-byte namespaces by computing the namespace byte range from the Cobble row-key layout
 * (trailing length fields, fixed serializer lengths, or the 0x00 separator position) before
 * deserializing. When the namespace byte length is zero, it throws a clear {@link IOException}
 * referencing the state name and key group. This is a v1 decision: null namespaces are rejected
 * rather than silently exported with wrong bytes.
 *
 * <h2>Simple keys (VALUE / LIST / REDUCING / AGGREGATING)</h2>
 *
 * The Cobble row key is {@code key + namespace [+ keyLength]}. The canonical key is {@code
 * writeKeyGroup + writeKey + writeNameSpace}. When both key and namespace are variable-length, the
 * Cobble row key has a trailing 4-byte {@code keyLength} suffix that the canonical format does not
 * use (it relies on {@code ambiguousKeyPossible} instead). The encoder deserializes key and
 * namespace from the row key (serializers know their own boundaries), then reassembles.
 *
 * <h2>Map keys</h2>
 *
 * The Cobble map row key is {@code key + namespace + 0x00 + userKey [+ keyLength][+
 * namespaceLength]}. The canonical key is {@code writeKeyGroup + writeKey + writeNameSpace +
 * userKey} (no separator, no trailing lengths). The encoder deserializes key and namespace, then
 * reads the user key between the {@code 0x00} separator and the trailer, with a strict check that
 * the user key is fully consumed.
 *
 * <h2>Timer keys</h2>
 *
 * The Cobble timer key is the raw serialized timer element. The canonical key is {@code
 * writeKeyGroup + cobbleTimerKey}.
 */
final class CobbleCanonicalKeyEncoder {

    private final int keyGroupPrefixBytes;
    private final DataOutputSerializer output;

    /**
     * @param totalKeyGroups the total number of key groups in the backend (used to compute the
     *     key-group prefix width)
     */
    CobbleCanonicalKeyEncoder(int totalKeyGroups) {
        this.keyGroupPrefixBytes =
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(totalKeyGroups);
        this.output = new DataOutputSerializer(128);
    }

    /**
     * Encodes a simple (non-map) Cobble row key into a canonical savepoint key.
     *
     * @param keyGroup the Flink key group (Cobble bucket)
     * @param cobbleRowKey the Cobble row key bytes ({@code key + namespace [+ keyLength]})
     * @param keySer the key serializer
     * @param namespaceSer the namespace serializer
     * @param stateName the state name (for error messages)
     */
    <K, N> byte[] encodeSimpleKey(
            int keyGroup,
            byte[] cobbleRowKey,
            TypeSerializer<K> keySer,
            TypeSerializer<N> namespaceSer,
            String stateName)
            throws IOException {
        int keyLen = keySer.getLength();
        int nsLen = namespaceSer.getLength();
        boolean keyLengthStored = StateRowKeyLayout.shouldStoreKeyLengthForKeyNamespace(keyLen, nsLen);
        int trailerBytes = keyLengthStored ? 4 : 0;

        DataInputDeserializer input = new DataInputDeserializer(cobbleRowKey);
        K key = deserializeOrThrow(keySer, input, stateName, keyGroup, "key");
        int afterKeyPosition = input.getPosition();

        // Compute the namespace byte range from the row-key layout.
        int nsByteLength = computeSimpleKeyNamespaceByteLength(cobbleRowKey, afterKeyPosition, trailerBytes);
        rejectNullNamespace(nsByteLength, namespaceSer, stateName, keyGroup);

        N namespace = deserializeNamespaceOrThrow(namespaceSer, input, stateName, keyGroup);
        int nsEnd = input.getPosition();

        // The remaining bytes (if any) are the optional keyLength suffix. We don't need to
        // validate them — they are not part of the canonical key.

        // Reassemble canonical key.
        output.clear();
        CompositeKeySerializationUtils.writeKeyGroup(keyGroup, keyGroupPrefixBytes, output);
        boolean ambiguousKeyPossible =
                CompositeKeySerializationUtils.isAmbiguousKeyPossible(keySer, namespaceSer);
        CompositeKeySerializationUtils.writeKey(key, keySer, output, ambiguousKeyPossible);
        CompositeKeySerializationUtils.writeNameSpace(
                namespace, namespaceSer, output, ambiguousKeyPossible);
        return output.getCopyOfBuffer();
    }

    /**
     * Encodes a Cobble map row key into a canonical savepoint key.
     *
     * @param keyGroup the Flink key group (Cobble bucket)
     * @param cobbleRowKey the Cobble map row key bytes
     * @param keySer the key serializer
     * @param namespaceSer the namespace serializer
     * @param userKeySer the map user-key serializer
     * @param stateName the state name (for error messages)
     */
    <K, N, UK> byte[] encodeMapKey(
            int keyGroup,
            byte[] cobbleRowKey,
            TypeSerializer<K> keySer,
            TypeSerializer<N> namespaceSer,
            TypeSerializer<UK> userKeySer,
            String stateName)
            throws IOException {
        int keyLen = keySer.getLength();
        int nsLen = namespaceSer.getLength();
        int ukLen = userKeySer.getLength();

        boolean mapKeyLengthStored = StateRowKeyLayout.shouldStoreMapKeyLength(keyLen, nsLen, ukLen);
        boolean mapNsLengthStored =
                StateRowKeyLayout.shouldStoreMapNamespaceLength(keyLen, nsLen, ukLen);
        int trailerBytes = (mapKeyLengthStored ? 4 : 0) + (mapNsLengthStored ? 4 : 0);
        int trailerStart = cobbleRowKey.length - trailerBytes;

        DataInputDeserializer input = new DataInputDeserializer(cobbleRowKey);
        K key = deserializeOrThrow(keySer, input, stateName, keyGroup, "key");
        int afterKeyPosition = input.getPosition();

        // Compute the namespace byte range from the row-key layout.
        int nsByteLength =
                computeMapKeyNamespaceByteLength(
                        cobbleRowKey,
                        nsLen,
                        mapNsLengthStored);
        rejectNullNamespace(nsByteLength, namespaceSer, stateName, keyGroup);

        N namespace = deserializeNamespaceOrThrow(namespaceSer, input, stateName, keyGroup);
        int nsEnd = input.getPosition();

        // Verify the 0x00 separator.
        if (nsEnd >= cobbleRowKey.length || cobbleRowKey[nsEnd] != 0) {
            throw new IOException(
                    "Malformed Cobble map row key for state '"
                            + stateName
                            + "' key group "
                            + keyGroup
                            + ": expected 0x00 separator at position "
                            + nsEnd
                            + " but found "
                            + (nsEnd < cobbleRowKey.length
                                    ? String.format("0x%02x", cobbleRowKey[nsEnd])
                                    : "end of key")
                            + ".");
        }

        int userKeyStart = nsEnd + 1;
        int userKeyEnd = trailerStart;
        if (userKeyEnd < userKeyStart) {
            throw new IOException(
                    "Malformed Cobble map row key for state '"
                            + stateName
                            + "' key group "
                            + keyGroup
                            + ": userKeyEnd ("
                            + userKeyEnd
                            + ") < userKeyStart ("
                            + userKeyStart
                            + ").");
        }
        DataInputDeserializer ukInput =
                new DataInputDeserializer(cobbleRowKey, userKeyStart, userKeyEnd - userKeyStart);
        UK userKey = deserializeOrThrow(userKeySer, ukInput, stateName, keyGroup, "user key");
        // Strict check: the user key must be fully consumed — no trailing bytes in the slice.
        if (ukInput.available() > 0) {
            throw new IOException(
                    "Malformed Cobble map row key for state '"
                            + stateName
                            + "' key group "
                            + keyGroup
                            + ": user key deserialization left "
                            + ukInput.available()
                            + " trailing byte(s).");
        }

        // Reassemble canonical key: writeKeyGroup + writeKey + writeNameSpace + userKey
        output.clear();
        CompositeKeySerializationUtils.writeKeyGroup(keyGroup, keyGroupPrefixBytes, output);
        boolean ambiguousKeyPossible =
                CompositeKeySerializationUtils.isAmbiguousKeyPossible(keySer, namespaceSer);
        CompositeKeySerializationUtils.writeKey(key, keySer, output, ambiguousKeyPossible);
        CompositeKeySerializationUtils.writeNameSpace(
                namespace, namespaceSer, output, ambiguousKeyPossible);
        userKeySer.serialize(userKey, output);
        return output.getCopyOfBuffer();
    }

    /**
     * Encodes a Cobble timer key into a canonical savepoint key.
     *
     * @param keyGroup the Flink key group (Cobble bucket)
     * @param cobbleTimerKey the raw serialized timer element bytes
     * @return the canonical key bytes ({@code keyGroupPrefix + cobbleTimerKey})
     */
    byte[] encodeTimerKey(int keyGroup, byte[] cobbleTimerKey) throws IOException {
        output.clear();
        CompositeKeySerializationUtils.writeKeyGroup(keyGroup, keyGroupPrefixBytes, output);
        output.write(cobbleTimerKey);
        return output.getCopyOfBuffer();
    }

    // ------------------------------------------------------------------------------------------
    //  Namespace byte-range computation
    // ------------------------------------------------------------------------------------------

    /**
     * Computes the namespace byte length for a simple-key row from the Cobble row-key layout.
     *
     * <p>Simple key layout: {@code [key][namespace][?keyLength(4)]}. When {@code keyLengthStored}
     * is true (both key and namespace are variable-length), the trailing 4-byte {@code keyLength}
     * gives the key byte length, so the namespace length is {@code (totalLen - 4) - keyLength}.
     * When no trailer is present, the namespace length is simply {@code totalLen -
     * afterKeyPosition}.
     *
     * @return the namespace byte length
     */
    private static int computeSimpleKeyNamespaceByteLength(
            byte[] cobbleRowKey, int afterKeyPosition, int trailerBytes) {
        int totalLen = cobbleRowKey.length;
        int nsStart = afterKeyPosition;
        int nsEnd = totalLen - trailerBytes;
        return Math.max(nsEnd - nsStart, 0);
    }

    /**
     * Computes the namespace byte length for a map-key row from the Cobble row-key layout.
     *
     * <p>Map key layout: {@code [key][namespace][0x00][userKey][?keyLen(4)][?nsLen(4)]}. When
     * {@code mapNsLengthStored} is true, the trailing {@code nsLen} int directly gives the
     * namespace byte length. When not stored but the namespace serializer has a positive fixed
     * length, the namespace byte length equals {@code nsLen}. When neither applies (variable-length
     * namespace, or {@code VoidNamespaceSerializer} whose {@code getLength()} returns 0 but
     * {@code serialize()} writes 1 byte), returns {@code -1} — the caller must rely on
     * deserialization and separator verification.
     *
     * <p>Note: {@code VoidNamespaceSerializer.getLength()} returns 0 despite {@code serialize()}
     * writing 1 byte. A fixed-length namespace with {@code nsLen == 0} is ambiguous — it could be
     * a null namespace (0 bytes) or a {@code VoidNamespace} (1 byte). We return {@code -1} in this
     * case so the caller falls back to deserialization + separator check, which correctly
     * distinguishes the two.
     *
     * @return the namespace byte length, or {@code -1} if it cannot be determined
     */
    private static int computeMapKeyNamespaceByteLength(
            byte[] cobbleRowKey,
            int nsLen,
            boolean mapNsLengthStored) {
        if (mapNsLengthStored) {
            // The namespace length is the last trailing int. Trailer layout: [?keyLen(4)][nsLen(4)]
            // — nsLen is always the last 4 bytes regardless of whether keyLen is also stored.
            int nsLengthOffset = cobbleRowKey.length - 4;
            return readInt(cobbleRowKey, nsLengthOffset);
        }
        if (nsLen > 0) {
            // Fixed-length namespace serializer with a positive length. The namespace occupies
            // exactly nsLen bytes.
            return nsLen;
        }
        // Variable-length namespace (nsLen < 0) with no stored length, or VoidNamespaceSerializer
        // (nsLen == 0 but serialize() writes 1 byte). Cannot determine byte length from layout
        // alone — must rely on deserialization + separator check.
        return -1;
    }

    /**
     * Rejects null-namespace rows (zero-byte namespace) with a clear error.
     *
     * <p>When {@code nsByteLength == 0}, the Cobble row was written with a null namespace (encoded
     * as zero bytes). The Flink canonical savepoint format does not support null namespaces —
     * {@code VoidNamespaceSerializer} writes 1 byte, and other serializers always write their
     * serialized form.
     *
     * <p>When {@code nsByteLength == -1}, the namespace byte length could not be determined from
     * the layout (variable-length namespace with no trailer). The null-namespace check is skipped
     * — the caller relies on deserialization and separator verification to detect malformed keys.
     */
    private static void rejectNullNamespace(
            int nsByteLength,
            TypeSerializer<?> namespaceSer,
            String stateName,
            int keyGroup)
            throws IOException {
        if (nsByteLength == 0) {
            throw new IOException(
                    "Cannot export state '"
                            + stateName
                            + "' key group "
                            + keyGroup
                            + " to canonical savepoint: row has a null (zero-byte) namespace."
                            + " The Flink canonical savepoint format does not support null"
                            + " namespaces (namespace serializer: "
                            + namespaceSer.getClass().getName()
                            + ").");
        }
    }

    // ------------------------------------------------------------------------------------------
    //  Deserialization helpers
    // ------------------------------------------------------------------------------------------

    /**
     * Deserializes a value, wrapping EOF errors with state name and key group context.
     */
    private static <T> T deserializeOrThrow(
            TypeSerializer<T> serializer,
            DataInputDeserializer input,
            String stateName,
            int keyGroup,
            String role)
            throws IOException {
        try {
            return serializer.deserialize(input);
        } catch (IOException e) {
            throw new IOException(
                    "Malformed Cobble row key for state '"
                            + stateName
                            + "' key group "
                            + keyGroup
                            + ": failed to deserialize "
                            + role
                            + " ("
                            + e.getMessage()
                            + ").",
                    e);
        }
    }

    /**
     * Deserializes the namespace, wrapping errors with state name and key group context.
     */
    private static <N> N deserializeNamespaceOrThrow(
            TypeSerializer<N> namespaceSer,
            DataInputDeserializer input,
            String stateName,
            int keyGroup)
            throws IOException {
        try {
            return namespaceSer.deserialize(input);
        } catch (IOException e) {
            throw new IOException(
                    "Malformed Cobble row key for state '"
                            + stateName
                            + "' key group "
                            + keyGroup
                            + ": failed to deserialize namespace ("
                            + e.getMessage()
                            + ").",
                    e);
        }
    }

    /** Reads a big-endian int from the given byte array at the given offset. */
    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }
}
