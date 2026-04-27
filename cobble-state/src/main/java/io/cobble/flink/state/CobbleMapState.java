package io.cobble.flink.state;

import io.cobble.structured.Db;
import io.cobble.structured.DirectScanBatch;
import io.cobble.structured.DirectScanCursor;
import io.cobble.structured.DirectScanRow;
import io.cobble.structured.Row;
import io.cobble.structured.ScanCursor;
import io.cobble.structured.ScanOptions;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
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
    private final ScanOptions emptyCheckScanOptions;
    private ByteBuffer directScanStartBuffer;
    private ByteBuffer directScanEndBuffer;

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
        this.emptyCheckScanOptions = ScanOptions.defaults().columnFamily(columnFamily).batchSize(1);
        this.directScanStartBuffer = ByteBuffer.allocateDirect(128);
        this.directScanEndBuffer = ByteBuffer.allocateDirect(128);
    }

    @Override
    public UV get(UK userKey) throws IOException {
        Preconditions.checkNotNull(userKey, "MapState user key must not be null.");
        return getEntryValueDirect(currentKey(), currentNamespace(), userKey);
    }

    @Override
    public void put(UK userKey, UV userValue) throws IOException {
        Preconditions.checkNotNull(userKey, "MapState user key must not be null.");
        Preconditions.checkNotNull(userValue, "MapState user value must not be null.");
        CobbleStateKeySerializer.DirectBufferSlice directKey =
                directMapEntryRowKey(currentKey(), currentNamespace(), userKeySerializer, userKey);
        CobbleStateKeySerializer.DirectBufferSlice directValue =
                directValueSerializer.serialize(userValueSerializer, userValue);
        db.putDirectWithOptions(
                currentBucket(),
                directKey.buffer(),
                directKey.length(),
                STATE_COLUMN_INDEX,
                directValue.buffer(),
                directValue.length(),
                writeOptions);
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
        return getEntryValueDirect(currentKey(), currentNamespace(), userKey) != null;
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
        return !hasAnyEntry(currentKey(), currentBucket(), currentNamespace());
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

    private boolean hasAnyEntry(K key, int bucket, N namespace) throws IOException {
        byte[] keyNamespacePrefix = mapKeyNamespacePrefix(key, namespace);
        DirectScanBounds directScanBounds = prepareDirectScanBounds(keyNamespacePrefix);
        try (DirectScanCursor cursor =
                scanDirectRowsForEmptyCheck(
                        bucket,
                        directScanBounds.startKeyBuffer,
                        directScanBounds.startKeyLength,
                        directScanBounds.endKeyBuffer,
                        directScanBounds.endKeyLength)) {
            while (true) {
                DirectScanBatch batch = cursor.nextBatch();
                int size = batch.size();
                if (size == 0) {
                    return false;
                }
                for (int index = 0; index < size; index++) {
                    DirectScanRow row = batch.getRow(index);
                    ByteBuffer rowKey = row.getKey();
                    if (!startsWithMapKeyNamespacePrefix(rowKey, keyNamespacePrefix)) {
                        return false;
                    }
                    return true;
                }
            }
        }
    }

    @Override
    public void close() {
        try {
            emptyCheckScanOptions.close();
        } finally {
            super.close();
        }
    }

    /** Rebuilds the visible map by scanning the key + namespace-specific KV range. */
    private <DUK, DUV> Map<DUK, DUV> readEntries(
            K key,
            int bucket,
            N namespace,
            TypeSerializer<DUK> deserializedUserKeySerializer,
            TypeSerializer<DUV> deserializedUserValueSerializer)
            throws IOException {
        byte[] keyNamespacePrefix = mapKeyNamespacePrefix(key, namespace);
        LinkedHashMap<DUK, DUV> entries = new LinkedHashMap<>();

        DirectScanBounds directScanBounds = prepareDirectScanBounds(keyNamespacePrefix);
        try (DirectScanCursor cursor =
                scanDirectRows(
                        bucket,
                        directScanBounds.startKeyBuffer,
                        directScanBounds.startKeyLength,
                        directScanBounds.endKeyBuffer,
                        directScanBounds.endKeyLength)) {
            for (DirectScanRow row : cursor) {
                ByteBuffer rowKey = row.getKey();
                if (!startsWithMapKeyNamespacePrefix(rowKey, keyNamespacePrefix)) {
                    break;
                }
                DUK userKey =
                        deserializeUserKey(
                                deserializedUserKeySerializer, rowKey, keyNamespacePrefix.length);
                ByteBuffer directValue = row.getBytes(STATE_COLUMN_INDEX);
                entries.put(
                        userKey,
                        directValue == null
                                ? null
                                : deserializeValue(deserializedUserValueSerializer, directValue));
            }
        }

        return entries.isEmpty() ? Collections.emptyMap() : entries;
    }

    /** Collects the raw entry row keys for the current logical map so clear() can delete them. */
    private ArrayList<byte[]> scanMatchingRowKeys(int bucket, K key, N namespace)
            throws IOException {
        byte[] keyNamespacePrefix = mapKeyNamespacePrefix(key, namespace);
        ArrayList<byte[]> rowKeys = new ArrayList<>();

        try (ScanCursor cursor =
                scanRows(
                        bucket, scanStartKey(keyNamespacePrefix), scanEndKey(keyNamespacePrefix))) {
            for (Row row : cursor) {
                byte[] rowKey = row.getKey();
                if (!startsWithMapKeyNamespacePrefix(rowKey, keyNamespacePrefix)) {
                    break;
                }
                rowKeys.add(Arrays.copyOf(rowKey, rowKey.length));
            }
        }

        return rowKeys;
    }

    private byte[] entryRowKey(K key, N namespace, UK userKey) throws IOException {
        return mapEntryRowKey(key, namespace, userKeySerializer, userKey);
    }

    private UV getEntryValueDirect(K key, N namespace, UK userKey) throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey =
                directMapEntryRowKey(key, namespace, userKeySerializer, userKey);
        try (io.cobble.structured.DirectRow directRow =
                db.getDirectWithOptions(
                        bucketForKey(key), directKey.buffer(), directKey.length(), readOptions)) {
            if (directRow == null) {
                return null;
            }
            ByteBuffer directValue = directRow.getBytes(STATE_COLUMN_INDEX);
            if (directValue == null) {
                return null;
            }
            return deserializeValue(userValueSerializer, directValue);
        }
    }

    private <DUK> DUK deserializeUserKey(
            TypeSerializer<DUK> deserializedUserKeySerializer,
            ByteBuffer rowKey,
            int keyNamespacePrefixLength)
            throws IOException {
        int userKeyLength = rowKey.limit() - keyNamespacePrefixLength - 1 - (Integer.BYTES * 2);
        ByteBuffer userKeyView = rowKey.duplicate();
        userKeyView.position(keyNamespacePrefixLength + 1);
        userKeyView.limit(keyNamespacePrefixLength + 1 + userKeyLength);
        return deserializeValue(deserializedUserKeySerializer, userKeyView.slice());
    }

    /** Checks whether a scanned row belongs to the current key + namespace map range. */
    private static boolean startsWithMapKeyNamespacePrefix(
            byte[] rowKey, byte[] keyNamespacePrefix) {
        if (rowKey.length <= keyNamespacePrefix.length || rowKey[keyNamespacePrefix.length] != 0) {
            return false;
        }
        int keyLength = readTrailingInt(rowKey, Integer.BYTES * 2);
        int namespaceLength = readTrailingInt(rowKey, Integer.BYTES);
        if (keyLength < 0
                || namespaceLength < 0
                || keyLength + namespaceLength != keyNamespacePrefix.length) {
            return false;
        }
        for (int index = 0; index < keyNamespacePrefix.length; index++) {
            if (rowKey[index] != keyNamespacePrefix[index]) {
                return false;
            }
        }
        return true;
    }

    private static boolean startsWithMapKeyNamespacePrefix(
            ByteBuffer rowKey, byte[] keyNamespacePrefix) {
        if (rowKey.limit() <= keyNamespacePrefix.length
                || rowKey.get(keyNamespacePrefix.length) != 0) {
            return false;
        }
        int keyLength = readTrailingInt(rowKey, Integer.BYTES * 2);
        int namespaceLength = readTrailingInt(rowKey, Integer.BYTES);
        if (keyLength < 0
                || namespaceLength < 0
                || keyLength + namespaceLength != keyNamespacePrefix.length) {
            return false;
        }
        for (int index = 0; index < keyNamespacePrefix.length; index++) {
            if (rowKey.get(index) != keyNamespacePrefix[index]) {
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

    private static int readTrailingInt(ByteBuffer bytes, int fromEnd) {
        int start = bytes.limit() - fromEnd;
        return ((bytes.get(start) & 0xFF) << 24)
                | ((bytes.get(start + 1) & 0xFF) << 16)
                | ((bytes.get(start + 2) & 0xFF) << 8)
                | (bytes.get(start + 3) & 0xFF);
    }

    /** The map-entry scan starts at key + namespace + 0x00. */
    private static byte[] scanStartKey(byte[] keyNamespacePrefix) {
        return appendByte(keyNamespacePrefix, (byte) 0x00);
    }

    /** The map-entry scan stops before key + namespace + 0xFF. */
    private static byte[] scanEndKey(byte[] keyNamespacePrefix) {
        return appendByte(keyNamespacePrefix, (byte) 0xFF);
    }

    private static byte[] appendByte(byte[] bytes, byte suffix) {
        byte[] result = Arrays.copyOf(bytes, bytes.length + 1);
        result[bytes.length] = suffix;
        return result;
    }

    private DirectScanBounds prepareDirectScanBounds(byte[] keyNamespacePrefix) {
        int keyLength = keyNamespacePrefix.length + 1;
        directScanStartBuffer = ensureCapacity(directScanStartBuffer, keyLength);
        directScanEndBuffer = ensureCapacity(directScanEndBuffer, keyLength);
        directScanStartBuffer.clear();
        directScanStartBuffer.put(keyNamespacePrefix);
        directScanStartBuffer.put((byte) 0x00);
        directScanEndBuffer.clear();
        directScanEndBuffer.put(keyNamespacePrefix);
        directScanEndBuffer.put((byte) 0xFF);
        return new DirectScanBounds(
                directScanStartBuffer, keyLength, directScanEndBuffer, keyLength);
    }

    private static ByteBuffer ensureCapacity(ByteBuffer buffer, int requiredCapacity) {
        if (buffer.capacity() >= requiredCapacity) {
            return buffer;
        }
        int newCapacity = buffer.capacity();
        while (newCapacity < requiredCapacity) {
            newCapacity = Math.max(requiredCapacity, newCapacity << 1);
        }
        return ByteBuffer.allocateDirect(newCapacity);
    }

    private DirectScanCursor scanDirectRowsForEmptyCheck(
            int bucket,
            ByteBuffer startKeyInclusive,
            int startKeyLength,
            ByteBuffer endKeyExclusive,
            int endKeyLength) {
        return db.scanDirectWithOptions(
                bucket,
                startKeyInclusive,
                startKeyLength,
                endKeyExclusive,
                endKeyLength,
                emptyCheckScanOptions);
    }

    private static final class DirectScanBounds {
        private final ByteBuffer startKeyBuffer;
        private final int startKeyLength;
        private final ByteBuffer endKeyBuffer;
        private final int endKeyLength;

        private DirectScanBounds(
                ByteBuffer startKeyBuffer,
                int startKeyLength,
                ByteBuffer endKeyBuffer,
                int endKeyLength) {
            this.startKeyBuffer = startKeyBuffer;
            this.startKeyLength = startKeyLength;
            this.endKeyBuffer = endKeyBuffer;
            this.endKeyLength = endKeyLength;
        }
    }
}
