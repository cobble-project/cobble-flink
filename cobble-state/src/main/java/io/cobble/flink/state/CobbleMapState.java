package io.cobble.flink.state;

import io.cobble.Db;
import io.cobble.DirectEncodedRow;
import io.cobble.DirectEncodedScanBatch;
import io.cobble.DirectEncodedScanEntry;
import io.cobble.DirectScanCursor;
import io.cobble.ScanCursor;
import io.cobble.ScanOptions;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/** Cobble-backed {@link org.apache.flink.api.common.state.MapState}. */
final class CobbleMapState<K, N, UK, UV> extends AbstractCobbleState<K, N, Map<UK, UV>>
        implements InternalMapState<K, N, UK, UV> {

    private final TypeSerializer<UK> userKeySerializer;
    private final TypeSerializer<UV> userValueSerializer;
    private final ScanOptions emptyCheckScanOptions;
    private final ScanOptions emptyCheckFastScanOptions;
    // Fixed user-key length from serializer.getLength(); -1 means variable.
    private final int userKeyFixedLength;
    // Whether map row-key trailer persists key length / namespace length.
    private final boolean mapKeyLengthStored;
    private final boolean namespaceLengthStored;
    // Total trailer bytes at row-key tail (0/4/8).
    private final int mapRowTrailerBytes;
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
        this.emptyCheckScanOptions = ScanOptions.defaults().columnFamily(columnFamily).batchSize(1);
        this.emptyCheckFastScanOptions =
                ScanOptions.defaults().columnFamily(columnFamily).batchSize(1).maxRows(1);
        this.userKeyFixedLength = CobbleStateKeySerializer.maybeFixedLength(userKeySerializer);
        int keyFixedLength = CobbleStateKeySerializer.maybeFixedLength(keySerializer);
        int namespaceFixedLength = CobbleStateKeySerializer.maybeFixedLength(namespaceSerializer);
        this.mapKeyLengthStored =
                CobbleStateKeySerializer.shouldStoreMapKeyLength(
                        keyFixedLength, namespaceFixedLength, userKeyFixedLength);
        this.namespaceLengthStored =
                CobbleStateKeySerializer.shouldStoreMapNamespaceLength(
                        keyFixedLength, namespaceFixedLength, userKeyFixedLength);
        this.mapRowTrailerBytes =
                (mapKeyLengthStored ? Integer.BYTES : 0)
                        + (namespaceLengthStored ? Integer.BYTES : 0);
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
    public Iterable<Map.Entry<UK, UV>> entries() {
        return streamingEntriesIterable(currentKey(), currentBucket(), currentNamespace());
    }

    @Override
    public Iterable<UK> keys() {
        return streamingKeysIterable(currentKey(), currentBucket(), currentNamespace());
    }

    @Override
    public Iterable<UV> values() {
        return streamingValuesIterable(currentKey(), currentBucket(), currentNamespace());
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws IOException {
        return streamEntries(
                currentKey(),
                currentBucket(),
                currentNamespace(),
                userKeySerializer,
                userValueSerializer);
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

    private <DUK, DUV> Iterator<Map.Entry<DUK, DUV>> streamEntries(
            K key,
            int bucket,
            N namespace,
            TypeSerializer<DUK> deserializedUserKeySerializer,
            TypeSerializer<DUV> deserializedUserValueSerializer)
            throws IOException {
        byte[] keyNamespacePrefix = mapKeyNamespacePrefix(key, namespace);
        DirectScanBounds directScanBounds = prepareDirectScanBounds(keyNamespacePrefix);
        DirectScanCursor cursor =
                scanDirectRows(
                        bucket,
                        directScanBounds.startKeyBuffer,
                        directScanBounds.startKeyLength,
                        directScanBounds.endKeyBuffer,
                        directScanBounds.endKeyLength);
        return new StreamingMapEntryIterator<>(
                cursor,
                keyNamespacePrefix,
                deserializedUserKeySerializer,
                deserializedUserValueSerializer);
    }

    private Iterable<Map.Entry<UK, UV>> streamingEntriesIterable(K key, int bucket, N namespace) {
        return () -> {
            try {
                return streamEntries(
                        key, bucket, namespace, userKeySerializer, userValueSerializer);
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to open Cobble map entry iterator for state '"
                                + columnFamily
                                + "'.",
                        e);
            }
        };
    }

    private Iterable<UK> streamingKeysIterable(K key, int bucket, N namespace) {
        return () -> {
            try {
                return keyIterator(
                        streamEntries(
                                key, bucket, namespace, userKeySerializer, userValueSerializer));
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to open Cobble map key iterator for state '" + columnFamily + "'.",
                        e);
            }
        };
    }

    private Iterable<UV> streamingValuesIterable(K key, int bucket, N namespace) {
        return () -> {
            try {
                return valueIterator(
                        streamEntries(
                                key, bucket, namespace, userKeySerializer, userValueSerializer));
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to open Cobble map value iterator for state '"
                                + columnFamily
                                + "'.",
                        e);
            }
        };
    }

    private static <DUK, DUV> Iterator<DUK> keyIterator(Iterator<Map.Entry<DUK, DUV>> entries) {
        return new Iterator<DUK>() {
            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public DUK next() {
                return entries.next().getKey();
            }
        };
    }

    private static <DUK, DUV> Iterator<DUV> valueIterator(Iterator<Map.Entry<DUK, DUV>> entries) {
        return new Iterator<DUV>() {
            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public DUV next() {
                return entries.next().getValue();
            }
        };
    }

    private boolean hasAnyEntry(K key, int bucket, N namespace) throws IOException {
        byte[] keyNamespacePrefix = mapKeyNamespacePrefix(key, namespace);
        DirectScanBounds directScanBounds = prepareDirectScanBounds(keyNamespacePrefix);
        EmptyCheckProbeResult fastResult =
                probeAnyEntryFast(
                        bucket,
                        keyNamespacePrefix,
                        directScanBounds.startKeyBuffer,
                        directScanBounds.startKeyLength,
                        directScanBounds.endKeyBuffer,
                        directScanBounds.endKeyLength);
        if (fastResult != EmptyCheckProbeResult.UNKNOWN) {
            return fastResult == EmptyCheckProbeResult.PRESENT;
        }
        return hasAnyEntrySlowPath(
                bucket,
                keyNamespacePrefix,
                directScanBounds.startKeyBuffer,
                directScanBounds.startKeyLength,
                directScanBounds.endKeyBuffer,
                directScanBounds.endKeyLength);
    }

    private EmptyCheckProbeResult probeAnyEntryFast(
            int bucket,
            byte[] keyNamespacePrefix,
            ByteBuffer startKeyInclusive,
            int startKeyLength,
            ByteBuffer endKeyExclusive,
            int endKeyLength) {
        try (DirectScanCursor cursor =
                scanDirectRowsWithOptions(
                        bucket,
                        startKeyInclusive,
                        startKeyLength,
                        endKeyExclusive,
                        endKeyLength,
                        emptyCheckFastScanOptions)) {
            DirectEncodedScanBatch batch = cursor.nextEncodedBatch();
            try {
                if (batch.size() == 0) {
                    return EmptyCheckProbeResult.ABSENT;
                }
                DirectEncodedScanEntry row = batch.nextEntry();
                if (row == null) {
                    return EmptyCheckProbeResult.ABSENT;
                }
                ByteBuffer rowKey = row.getKey();
                if (startsWithMapKeyNamespacePrefix(rowKey, keyNamespacePrefix)) {
                    return EmptyCheckProbeResult.PRESENT;
                }
                if (isDefinitelyOutsideMapKeyNamespacePrefix(rowKey, keyNamespacePrefix)) {
                    return EmptyCheckProbeResult.ABSENT;
                }
                return EmptyCheckProbeResult.UNKNOWN;
            } finally {
                batch.close();
            }
        }
    }

    private boolean hasAnyEntrySlowPath(
            int bucket,
            byte[] keyNamespacePrefix,
            ByteBuffer startKeyInclusive,
            int startKeyLength,
            ByteBuffer endKeyExclusive,
            int endKeyLength) {
        try (DirectScanCursor cursor =
                scanDirectRowsWithOptions(
                        bucket,
                        startKeyInclusive,
                        startKeyLength,
                        endKeyExclusive,
                        endKeyLength,
                        emptyCheckScanOptions)) {
            while (true) {
                DirectEncodedScanBatch batch = cursor.nextEncodedBatch();
                try {
                    if (batch.size() == 0) {
                        return false;
                    }
                    while (true) {
                        DirectEncodedScanEntry row = batch.nextEntry();
                        if (row == null) {
                            break;
                        }
                        if (!startsWithMapKeyNamespacePrefix(row.getKey(), keyNamespacePrefix)) {
                            return false;
                        }
                        return true;
                    }
                } finally {
                    batch.close();
                }
            }
        }
    }

    @Override
    public void close() {
        try {
            emptyCheckFastScanOptions.close();
        } finally {
            try {
                emptyCheckScanOptions.close();
            } finally {
                super.close();
            }
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
            boolean done = false;
            while (!done) {
                DirectEncodedScanBatch batch = cursor.nextEncodedBatch();
                try {
                    if (batch.size() == 0) {
                        break;
                    }
                    while (true) {
                        DirectEncodedScanEntry row = batch.nextEntry();
                        if (row == null) {
                            break;
                        }
                        ByteBuffer rowKey = row.getKey();
                        if (!startsWithMapKeyNamespacePrefix(rowKey, keyNamespacePrefix)) {
                            done = true;
                            break;
                        }
                        DUK userKey =
                                deserializeUserKey(
                                        deserializedUserKeySerializer,
                                        rowKey,
                                        keyNamespacePrefix.length);
                        DUV userValue =
                                row.decodeColumn(
                                        STATE_COLUMN_INDEX,
                                        input ->
                                                deserializeValue(
                                                        deserializedUserValueSerializer, input));
                        entries.put(userKey, userValue);
                    }
                } finally {
                    batch.close();
                }
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
            for (ScanCursor.Entry row : cursor) {
                byte[] rowKey = row.key;
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
        try (DirectEncodedRow encodedRow =
                db.getDirectEncodedRowWithOptions(
                        bucketForKey(key), directKey.buffer(), directKey.length(), readOptions)) {
            if (encodedRow == null) {
                return null;
            }
            return encodedRow.decodeColumn(
                    STATE_COLUMN_INDEX, input -> deserializeValue(userValueSerializer, input));
        }
    }

    private <DUK> DUK deserializeUserKey(
            TypeSerializer<DUK> deserializedUserKeySerializer,
            ByteBuffer rowKey,
            int keyNamespacePrefixLength)
            throws IOException {
        int userKeyEnd = rowKey.limit() - mapRowTrailerBytes;
        int userKeyLength = userKeyEnd - keyNamespacePrefixLength - 1;
        if (userKeyLength < 0) {
            throw new IOException("Corrupted map entry row key: negative user key length.");
        }
        if (userKeyFixedLength >= 0 && userKeyLength != userKeyFixedLength) {
            throw new IOException(
                    "Corrupted map entry row key: unexpected user key length "
                            + userKeyLength
                            + " (expected "
                            + userKeyFixedLength
                            + ").");
        }
        ByteBuffer userKeyView = rowKey.duplicate();
        userKeyView.position(keyNamespacePrefixLength + 1);
        userKeyView.limit(userKeyEnd);
        return deserializeValue(deserializedUserKeySerializer, userKeyView.slice());
    }

    /** Checks map-entry row shape by separator + encoded key/namespace lengths. */
    private boolean startsWithMapKeyNamespacePrefix(byte[] rowKey, byte[] keyNamespacePrefix) {
        if (rowKey.length <= keyNamespacePrefix.length || rowKey[keyNamespacePrefix.length] != 0) {
            return false;
        }
        return keyNamespaceShapeMatches(rowKey.length, keyNamespacePrefix.length, rowKey);
    }

    private boolean startsWithMapKeyNamespacePrefix(ByteBuffer rowKey, byte[] keyNamespacePrefix) {
        if (rowKey.limit() <= keyNamespacePrefix.length
                || rowKey.get(keyNamespacePrefix.length) != 0) {
            return false;
        }
        return keyNamespaceShapeMatches(rowKey.limit(), keyNamespacePrefix.length, rowKey);
    }

    private static boolean isDefinitelyOutsideMapKeyNamespacePrefix(
            ByteBuffer rowKey, byte[] keyNamespacePrefix) {
        int prefixLength = keyNamespacePrefix.length;
        if (rowKey.limit() <= prefixLength) {
            return true;
        }
        int compareLength = Math.min(prefixLength, rowKey.limit());
        for (int i = 0; i < compareLength; i++) {
            if (rowKey.get(i) != keyNamespacePrefix[i]) {
                return true;
            }
        }
        return rowKey.get(prefixLength) != 0;
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

    private boolean keyNamespaceShapeMatches(
            int rowKeyLength, int keyNamespacePrefixLength, byte[] rowKey) {
        if (rowKeyLength < keyNamespacePrefixLength + 1 + mapRowTrailerBytes) {
            return false;
        }
        if (!mapKeyLengthStored && !namespaceLengthStored) {
            return true;
        }
        if (mapKeyLengthStored && namespaceLengthStored) {
            int keyLength = readTrailingInt(rowKey, Integer.BYTES * 2);
            int namespaceLength = readTrailingInt(rowKey, Integer.BYTES);
            return keyLength >= 0
                    && namespaceLength >= 0
                    && keyLength + namespaceLength == keyNamespacePrefixLength;
        }
        if (mapKeyLengthStored) {
            int keyLength = readTrailingInt(rowKey, Integer.BYTES);
            return keyLength >= 0 && keyLength <= keyNamespacePrefixLength;
        }
        int namespaceLength = readTrailingInt(rowKey, Integer.BYTES);
        return namespaceLength >= 0 && namespaceLength <= keyNamespacePrefixLength;
    }

    private boolean keyNamespaceShapeMatches(
            int rowKeyLength, int keyNamespacePrefixLength, ByteBuffer rowKey) {
        if (rowKeyLength < keyNamespacePrefixLength + 1 + mapRowTrailerBytes) {
            return false;
        }
        if (!mapKeyLengthStored && !namespaceLengthStored) {
            return true;
        }
        if (mapKeyLengthStored && namespaceLengthStored) {
            int keyLength = readTrailingInt(rowKey, Integer.BYTES * 2);
            int namespaceLength = readTrailingInt(rowKey, Integer.BYTES);
            return keyLength >= 0
                    && namespaceLength >= 0
                    && keyLength + namespaceLength == keyNamespacePrefixLength;
        }
        if (mapKeyLengthStored) {
            int keyLength = readTrailingInt(rowKey, Integer.BYTES);
            return keyLength >= 0 && keyLength <= keyNamespacePrefixLength;
        }
        int namespaceLength = readTrailingInt(rowKey, Integer.BYTES);
        return namespaceLength >= 0 && namespaceLength <= keyNamespacePrefixLength;
    }

    /** The map-entry scan starts at key + namespace + 0x00. */
    private static byte[] scanStartKey(byte[] keyNamespacePrefix) {
        return appendByte(keyNamespacePrefix, (byte) 0x00);
    }

    /** The map-entry scan stops before key + namespace + 0x01. */
    private static byte[] scanEndKey(byte[] keyNamespacePrefix) {
        return appendByte(keyNamespacePrefix, (byte) 0x01);
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
        directScanEndBuffer.put((byte) 0x01);
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

    private DirectScanCursor scanDirectRowsWithOptions(
            int bucket,
            ByteBuffer startKeyInclusive,
            int startKeyLength,
            ByteBuffer endKeyExclusive,
            int endKeyLength,
            ScanOptions scanOptions) {
        return db.scanDirectWithOptions(
                bucket,
                startKeyInclusive,
                startKeyLength,
                endKeyExclusive,
                endKeyLength,
                scanOptions);
    }

    private enum EmptyCheckProbeResult {
        PRESENT,
        ABSENT,
        UNKNOWN
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

    private final class StreamingMapEntryIterator<DUK, DUV>
            implements Iterator<Map.Entry<DUK, DUV>> {
        private final DirectScanCursor cursor;
        private final byte[] keyNamespacePrefix;
        private final TypeSerializer<DUK> deserializedUserKeySerializer;
        private final TypeSerializer<DUV> deserializedUserValueSerializer;

        private DirectEncodedScanBatch currentBatch;
        private Map.Entry<DUK, DUV> nextEntry;
        private boolean finished;
        private boolean cursorClosed;

        private StreamingMapEntryIterator(
                DirectScanCursor cursor,
                byte[] keyNamespacePrefix,
                TypeSerializer<DUK> deserializedUserKeySerializer,
                TypeSerializer<DUV> deserializedUserValueSerializer) {
            this.cursor = cursor;
            this.keyNamespacePrefix = keyNamespacePrefix;
            this.deserializedUserKeySerializer = deserializedUserKeySerializer;
            this.deserializedUserValueSerializer = deserializedUserValueSerializer;
        }

        @Override
        public boolean hasNext() {
            if (nextEntry != null) {
                return true;
            }
            if (finished) {
                return false;
            }
            fetchNextEntry();
            return nextEntry != null;
        }

        @Override
        public Map.Entry<DUK, DUV> next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more map entries.");
            }
            Map.Entry<DUK, DUV> result = nextEntry;
            nextEntry = null;
            return result;
        }

        private void fetchNextEntry() {
            while (!finished) {
                try {
                    if (currentBatch == null) {
                        loadNextBatch();
                        if (finished) {
                            return;
                        }
                    }

                    DirectEncodedScanEntry row = currentBatch.nextEntry();
                    if (row == null) {
                        loadNextBatch();
                        continue;
                    }
                    ByteBuffer rowKey = row.getKey();
                    if (!startsWithMapKeyNamespacePrefix(rowKey, keyNamespacePrefix)) {
                        finish();
                        return;
                    }
                    DUK userKey =
                            deserializeUserKey(
                                    deserializedUserKeySerializer,
                                    rowKey,
                                    keyNamespacePrefix.length);
                    DUV userValue =
                            row.decodeColumn(
                                    STATE_COLUMN_INDEX,
                                    input ->
                                            deserializeValue(
                                                    deserializedUserValueSerializer, input));
                    nextEntry = new AbstractMap.SimpleImmutableEntry<>(userKey, userValue);
                    return;
                } catch (IOException e) {
                    finish();
                    throw new IllegalStateException(
                            "Failed to stream Cobble map entries for state '" + columnFamily + "'.",
                            e);
                } catch (RuntimeException e) {
                    finish();
                    throw e;
                }
            }
        }

        private void loadNextBatch() {
            closeCurrentBatch();
            DirectEncodedScanBatch nextBatch = cursor.nextEncodedBatch();
            if (nextBatch.size() == 0) {
                finish();
                return;
            }
            currentBatch = nextBatch;
        }

        private void closeCurrentBatch() {
            if (currentBatch != null) {
                currentBatch.close();
                currentBatch = null;
            }
        }

        private void finish() {
            if (finished) {
                return;
            }
            finished = true;
            nextEntry = null;
            closeCurrentBatch();
            if (!cursorClosed) {
                cursorClosed = true;
                cursor.close();
            }
        }
    }
}
