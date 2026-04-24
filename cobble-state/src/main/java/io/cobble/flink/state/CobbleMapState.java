package io.cobble.flink.state;

import io.cobble.structured.Db;
import io.cobble.structured.Row;
import io.cobble.structured.ScanCursor;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/** Cobble-backed {@link org.apache.flink.api.common.state.MapState}. */
final class CobbleMapState<K, N, UK, UV> extends AbstractCobbleState<K, N, Map<UK, UV>>
        implements InternalMapState<K, N, UK, UV> {

    private final TypeSerializer<UK> userKeySerializer;
    private final TypeSerializer<UV> userValueSerializer;
    private final DataInputDeserializer userKeyInputView;

    CobbleMapState(
            CobbleKeyedStateBackend<K> backend,
            Db db,
            String columnFamily,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            MapSerializer<UK, UV> mapSerializer,
            StateTtlConfig ttlConfig) {
        super(
                backend,
                db,
                columnFamily,
                keySerializer,
                namespaceSerializer,
                mapSerializer,
                ttlConfig);
        this.userKeySerializer = mapSerializer.getKeySerializer();
        this.userValueSerializer = mapSerializer.getValueSerializer();
        this.userKeyInputView = new DataInputDeserializer();
    }

    @Override
    public UV get(UK userKey) throws IOException {
        Preconditions.checkNotNull(userKey, "MapState user key must not be null.");
        byte[] stored = getCurrentEntryBytes(userKey);
        return stored == null ? null : deserializeValue(userValueSerializer, stored);
    }

    @Override
    public void put(UK userKey, UV userValue) throws IOException {
        Preconditions.checkNotNull(userKey, "MapState user key must not be null.");
        putBytes(
                currentBucket(),
                entryRowKey(currentKey(), currentNamespace(), userKey),
                serializeValue(userValueSerializer, userValue));
    }

    @Override
    public void putAll(Map<UK, UV> value) throws IOException {
        Preconditions.checkNotNull(value, "MapState putAll value must not be null.");
        for (Map.Entry<UK, UV> entry : value.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void remove(UK userKey) throws IOException {
        Preconditions.checkNotNull(userKey, "MapState user key must not be null.");
        delete(currentBucket(), entryRowKey(currentKey(), currentNamespace(), userKey));
    }

    @Override
    public boolean contains(UK userKey) throws IOException {
        Preconditions.checkNotNull(userKey, "MapState user key must not be null.");
        return getCurrentEntryBytes(userKey) != null;
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws IOException {
        return readCurrentEntries().entrySet();
    }

    @Override
    public Iterable<UK> keys() throws IOException {
        return readCurrentEntries().keySet();
    }

    @Override
    public Iterable<UV> values() throws IOException {
        return readCurrentEntries().values();
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws IOException {
        return readCurrentEntries().entrySet().iterator();
    }

    @Override
    public boolean isEmpty() throws IOException {
        return readCurrentEntries().isEmpty();
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<Map<UK, UV>> safeValueSerializer)
            throws Exception {
        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);
        MapSerializer<UK, UV> safeMapSerializer = (MapSerializer<UK, UV>) safeValueSerializer;
        Map<UK, UV> entries =
                readEntries(
                        keyAndNamespace.f0,
                        bucketForKey(keyAndNamespace.f0),
                        keyAndNamespace.f1,
                        safeMapSerializer.getKeySerializer(),
                        safeMapSerializer.getValueSerializer());
        return entries.isEmpty()
                ? null
                : KvStateSerializer.serializeMap(
                        entries.entrySet(),
                        safeMapSerializer.getKeySerializer(),
                        safeMapSerializer.getValueSerializer());
    }

    @Override
    protected void clearState(K key, N namespace) throws IOException {
        int bucket = currentBucket();
        for (byte[] rowKey : scanMatchingRowKeys(bucket, key, namespace)) {
            delete(bucket, rowKey);
        }
    }

    private N currentNamespace() {
        return Preconditions.checkNotNull(
                currentNamespace,
                "Current namespace is not set for Cobble state '%s'.",
                columnFamily);
    }

    private Map<UK, UV> readCurrentEntries() throws IOException {
        return readEntries(
                currentKey(),
                currentBucket(),
                currentNamespace(),
                userKeySerializer,
                userValueSerializer);
    }

    /**
     * Rebuilds the visible map by scanning the key-specific KV range and filtering the namespace.
     */
    private <DUK, DUV> Map<DUK, DUV> readEntries(
            K key,
            int bucket,
            N namespace,
            TypeSerializer<DUK> deserializedUserKeySerializer,
            TypeSerializer<DUV> deserializedUserValueSerializer)
            throws IOException {
        byte[] keyPrefix = keyPrefix(key);
        byte[] namespaceBytes = serializeValue(namespaceSerializer, namespace);
        LinkedHashMap<DUK, DUV> entries = new LinkedHashMap<>();

        try (ScanCursor cursor = scanRows(bucket, scanStartKey(keyPrefix), scanEndKey(keyPrefix))) {
            for (Row row : cursor) {
                byte[] rowKey = row.getKey();
                if (!startsWithMapKeyPrefix(rowKey, keyPrefix)) {
                    break;
                }
                if (!matchesNamespace(rowKey, keyPrefix.length, namespaceBytes)) {
                    continue;
                }
                DUK userKey =
                        deserializeUserKey(deserializedUserKeySerializer, rowKey, keyPrefix.length);
                entries.put(
                        userKey,
                        deserializeValue(
                                deserializedUserValueSerializer, row.getBytes(STATE_COLUMN_INDEX)));
            }
        }

        return entries.isEmpty() ? Collections.emptyMap() : entries;
    }

    /** Collects the raw entry row keys for the current logical map so clear() can delete them. */
    private ArrayList<byte[]> scanMatchingRowKeys(int bucket, K key, N namespace)
            throws IOException {
        byte[] keyPrefix = keyPrefix(key);
        byte[] namespaceBytes = serializeValue(namespaceSerializer, namespace);
        ArrayList<byte[]> rowKeys = new ArrayList<>();

        try (ScanCursor cursor = scanRows(bucket, scanStartKey(keyPrefix), scanEndKey(keyPrefix))) {
            for (Row row : cursor) {
                byte[] rowKey = row.getKey();
                if (!startsWithMapKeyPrefix(rowKey, keyPrefix)) {
                    break;
                }
                if (matchesNamespace(rowKey, keyPrefix.length, namespaceBytes)) {
                    rowKeys.add(Arrays.copyOf(rowKey, rowKey.length));
                }
            }
        }

        return rowKeys;
    }

    private byte[] getCurrentEntryBytes(UK userKey) throws IOException {
        return getBytes(currentBucket(), entryRowKey(currentKey(), currentNamespace(), userKey));
    }

    private byte[] getEntryBytes(K key, N namespace, UK userKey) throws IOException {
        return getBytes(bucketForKey(key), entryRowKey(key, namespace, userKey));
    }

    private byte[] entryRowKey(K key, N namespace, UK userKey) throws IOException {
        return mapEntryRowKey(key, namespace, userKeySerializer, userKey);
    }

    private <DUK> DUK deserializeUserKey(
            TypeSerializer<DUK> deserializedUserKeySerializer, byte[] rowKey, int keyPrefixLength)
            throws IOException {
        int userKeyLength = readTrailingInt(rowKey, Integer.BYTES);
        userKeyInputView.setBuffer(rowKey, keyPrefixLength + 1, userKeyLength);
        return deserializedUserKeySerializer.deserialize(userKeyInputView);
    }

    /** Checks whether a scanned row belongs to the current map-key range. */
    private static boolean startsWithMapKeyPrefix(byte[] bytes, byte[] keyPrefix) {
        if (bytes.length <= keyPrefix.length || bytes[keyPrefix.length] != 0) {
            return false;
        }
        for (int index = 0; index < keyPrefix.length; index++) {
            if (bytes[index] != keyPrefix[index]) {
                return false;
            }
        }
        return true;
    }

    /** Validates the namespace suffix embedded after the user-key segment. */
    private static boolean matchesNamespace(
            byte[] rowKey, int keyPrefixLength, byte[] namespaceBytes) {
        if (rowKey.length < keyPrefixLength + 1 + namespaceBytes.length + (Integer.BYTES * 2)) {
            return false;
        }

        int keyLength = readTrailingInt(rowKey, Integer.BYTES * 2);
        int userKeyLength = readTrailingInt(rowKey, Integer.BYTES);
        if (keyLength != keyPrefixLength || userKeyLength < 0) {
            return false;
        }

        int namespaceStart = keyPrefixLength + 1 + userKeyLength;
        int namespaceLength = rowKey.length - namespaceStart - (Integer.BYTES * 2);
        if (namespaceLength != namespaceBytes.length) {
            return false;
        }

        for (int index = 0; index < namespaceLength; index++) {
            if (rowKey[namespaceStart + index] != namespaceBytes[index]) {
                return false;
            }
        }
        return true;
    }

    private static int readTrailingInt(byte[] bytes, int fromEnd) {
        int start = bytes.length - fromEnd;
        return ((bytes[start] & 0xFF) << 24)
                | ((bytes[start + 1] & 0xFF) << 16)
                | ((bytes[start + 2] & 0xFF) << 8)
                | (bytes[start + 3] & 0xFF);
    }

    /** The map-entry scan starts at key + 0x00 so it skips any unrelated prefixes. */
    private static byte[] scanStartKey(byte[] keyPrefix) {
        return appendByte(keyPrefix, (byte) 0x00);
    }

    /** The map-entry scan stops before key + 0xFF, which bounds the whole map-key range. */
    private static byte[] scanEndKey(byte[] keyPrefix) {
        return appendByte(keyPrefix, (byte) 0xFF);
    }

    private static byte[] appendByte(byte[] bytes, byte suffix) {
        byte[] result = Arrays.copyOf(bytes, bytes.length + 1);
        result[bytes.length] = suffix;
        return result;
    }
}
