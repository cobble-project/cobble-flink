package io.cobble.flink.state;

import io.cobble.structured.ColumnValue;
import io.cobble.structured.Db;
import io.cobble.structured.DirectRow;
import io.cobble.structured.ReadOptions;
import io.cobble.structured.Row;
import io.cobble.structured.ScanCursor;
import io.cobble.structured.ScanOptions;
import io.cobble.structured.WriteOptions;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Shared plumbing for Cobble-backed Flink keyed state implementations. */
abstract class AbstractCobbleState<K, N, V> implements InternalKvState<K, N, V>, AutoCloseable {

    protected static final int STATE_COLUMN_INDEX = 0;

    protected final CobbleKeyedStateBackend<K> backend;
    protected final Db db;
    protected final String columnFamily;
    protected final TypeSerializer<K> keySerializer;
    protected final CobbleStateKeySerializer.ReusableSerializedKeyBuilder<K> rowKeyBuilder;
    protected final CobbleStateKeySerializer.ReusableSerializedDirectKeyBuilder<K>
            directRowKeyBuilder;
    protected final ReadOptions readOptions;
    protected final ScanOptions scanOptions;
    protected final WriteOptions writeOptions;
    protected final CobbleStateKeySerializer.ReusableDirectValueSerializer directValueSerializer;
    protected final CobbleStateKeySerializer.ReusableDirectValueDeserializer
            directValueDeserializer;
    private final AtomicBoolean optionsDisposed;
    protected TypeSerializer<N> namespaceSerializer;
    protected TypeSerializer<V> valueSerializer;

    protected N currentNamespace;

    AbstractCobbleState(
            CobbleKeyedStateBackend<K> backend,
            Db db,
            String columnFamily,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            StateTtlConfig ttlConfig) {
        this.backend = backend;
        this.db = db;
        this.columnFamily = columnFamily;
        this.keySerializer = keySerializer;
        this.rowKeyBuilder =
                new CobbleStateKeySerializer.ReusableSerializedKeyBuilder<>(keySerializer, 128);
        this.directRowKeyBuilder =
                new CobbleStateKeySerializer.ReusableSerializedDirectKeyBuilder<>(
                        keySerializer, 128);
        this.readOptions = ReadOptions.defaultsInFamily(columnFamily);
        this.scanOptions = ScanOptions.defaults().columnFamily(columnFamily);
        this.writeOptions = createWriteOptions(columnFamily, ttlConfig);
        this.directValueSerializer =
                new CobbleStateKeySerializer.ReusableDirectValueSerializer(256);
        this.directValueDeserializer =
                new CobbleStateKeySerializer.ReusableDirectValueDeserializer();
        this.optionsDisposed = new AtomicBoolean(false);
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public final TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    public final TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public final TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public final void clear() {
        K key = backend.getCurrentKey();
        if (key == null || currentNamespace == null) {
            return;
        }

        try {
            clearState(key, currentNamespace);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to clear Cobble state '" + columnFamily + "'.", e);
        }
    }

    @Override
    public final void setCurrentNamespace(N namespace) {
        this.currentNamespace =
                Preconditions.checkNotNull(namespace, "Namespace must not be null.");
    }

    @Override
    public final StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return null;
    }

    protected final int currentBucket() {
        return backend.getCurrentKeyGroupIndex();
    }

    protected final int bucketForKey(K key) {
        return KeyGroupRangeAssignment.assignToKeyGroup(
                key, backend.getTotalNumberOfKeyGroupsForState());
    }

    protected final byte[] currentRowKey() throws IOException {
        return rowKey(currentKey(), currentNamespace);
    }

    protected final byte[] rowKey(K key, N namespace) throws IOException {
        return rowKeyBuilder.buildKeyAndNamespace(key, namespaceSerializer, namespace);
    }

    protected final byte[] keyPrefix(K key) throws IOException {
        return rowKeyBuilder.buildKeyPrefix(key);
    }

    protected final <UK> byte[] mapEntryRowKey(
            K key, N namespace, TypeSerializer<UK> userKeySerializer, UK userKey)
            throws IOException {
        return rowKeyBuilder.buildMapKeyUserKeyAndNamespace(
                key, userKeySerializer, userKey, namespaceSerializer, namespace);
    }

    protected final <UK> CobbleStateKeySerializer.DirectBufferSlice directMapEntryRowKey(
            K key, N namespace, TypeSerializer<UK> userKeySerializer, UK userKey)
            throws IOException {
        return directRowKeyBuilder.buildMapKeyUserKeyAndNamespace(
                key, userKeySerializer, userKey, namespaceSerializer, namespace);
    }

    protected final K currentKey() {
        return Preconditions.checkNotNull(
                backend.getCurrentKey(),
                "Current key is not set while accessing Cobble state '%s'.",
                columnFamily);
    }

    protected final byte[] getBytes(K key, N namespace) throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey =
                directRowKeyBuilder.buildKeyAndNamespace(key, namespaceSerializer, namespace);
        return getDirectBytes(bucketForKey(key), directKey);
    }

    protected final byte[] getBytes(int bucket, byte[] rowKey) throws IOException {
        return getDirectBytes(bucket, rowKey);
    }

    protected final byte[] getCurrentBytes() throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey =
                directRowKeyBuilder.buildKeyAndNamespace(
                        currentKey(),
                        namespaceSerializer,
                        Preconditions.checkNotNull(
                                currentNamespace,
                                "Current namespace is not set for Cobble state '%s'.",
                                columnFamily));
        return getDirectBytes(currentBucket(), directKey);
    }

    protected final byte[][] getList(K key, N namespace) throws IOException {
        Row row = db.getWithOptions(bucketForKey(key), rowKey(key, namespace), readOptions);
        return row == null ? null : row.getList(STATE_COLUMN_INDEX);
    }

    protected final byte[][] getCurrentList() throws IOException {
        Row row = db.getWithOptions(currentBucket(), currentRowKey(), readOptions);
        return row == null ? null : row.getList(STATE_COLUMN_INDEX);
    }

    protected final void putCurrentBytes(byte[] value) throws IOException {
        db.putWithOptions(
                currentBucket(),
                currentRowKey(),
                STATE_COLUMN_INDEX,
                ColumnValue.ofBytes(value),
                writeOptions);
    }

    protected final <T> void putCurrentDirectValue(TypeSerializer<T> serializer, T value)
            throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey =
                directRowKeyBuilder.buildKeyAndNamespace(
                        currentKey(),
                        namespaceSerializer,
                        Preconditions.checkNotNull(
                                currentNamespace,
                                "Current namespace is not set for Cobble state '%s'.",
                                columnFamily));
        CobbleStateKeySerializer.DirectBufferSlice directValue =
                directValueSerializer.serialize(serializer, value);
        db.putDirectWithOptions(
                currentBucket(),
                directKey.buffer(),
                directKey.length(),
                STATE_COLUMN_INDEX,
                directValue.buffer(),
                directValue.length(),
                writeOptions);
    }

    protected final <T> T getCurrentDirectValue(TypeSerializer<T> serializer) throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey =
                directRowKeyBuilder.buildKeyAndNamespace(
                        currentKey(),
                        namespaceSerializer,
                        Preconditions.checkNotNull(
                                currentNamespace,
                                "Current namespace is not set for Cobble state '%s'.",
                                columnFamily));
        try (DirectRow directRow =
                db.getDirectWithOptions(
                        currentBucket(), directKey.buffer(), directKey.length(), readOptions)) {
            if (directRow == null) {
                return null;
            }
            ByteBuffer buffer = directRow.getBytes(STATE_COLUMN_INDEX);
            return buffer == null ? null : directValueDeserializer.deserialize(serializer, buffer);
        }
    }

    protected final <T> T getDirectValue(K key, N namespace, TypeSerializer<T> serializer)
            throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey =
                directRowKeyBuilder.buildKeyAndNamespace(key, namespaceSerializer, namespace);
        try (DirectRow directRow =
                db.getDirectWithOptions(
                        bucketForKey(key), directKey.buffer(), directKey.length(), readOptions)) {
            if (directRow == null) {
                return null;
            }
            ByteBuffer buffer = directRow.getBytes(STATE_COLUMN_INDEX);
            return buffer == null ? null : directValueDeserializer.deserialize(serializer, buffer);
        }
    }

    protected final <T> T deserializeValue(TypeSerializer<T> serializer, ByteBuffer directValue)
            throws IOException {
        return directValueDeserializer.deserialize(serializer, directValue);
    }

    protected final void putBytes(int bucket, byte[] rowKey, byte[] value) throws IOException {
        db.putWithOptions(
                bucket, rowKey, STATE_COLUMN_INDEX, ColumnValue.ofBytes(value), writeOptions);
    }

    protected final void putCurrentList(byte[][] values) throws IOException {
        db.putWithOptions(
                currentBucket(),
                currentRowKey(),
                STATE_COLUMN_INDEX,
                ColumnValue.ofList(values),
                writeOptions);
    }

    protected final void mergeCurrentList(byte[][] values) throws IOException {
        db.mergeWithOptions(
                currentBucket(),
                currentRowKey(),
                STATE_COLUMN_INDEX,
                ColumnValue.ofList(values),
                writeOptions);
    }

    protected final void putList(K key, N namespace, byte[][] values) throws IOException {
        db.putWithOptions(
                bucketForKey(key),
                rowKey(key, namespace),
                STATE_COLUMN_INDEX,
                ColumnValue.ofList(values),
                writeOptions);
    }

    protected final void delete(K key, N namespace) throws IOException {
        db.deleteWithOptions(
                bucketForKey(key), rowKey(key, namespace), STATE_COLUMN_INDEX, writeOptions);
    }

    protected final void delete(int bucket, byte[] rowKey) throws IOException {
        db.deleteWithOptions(bucket, rowKey, STATE_COLUMN_INDEX, writeOptions);
    }

    protected final ScanCursor scanRows(
            int bucket, byte[] startKeyInclusive, byte[] endKeyExclusive) {
        return db.scanWithOptions(bucket, startKeyInclusive, endKeyExclusive, scanOptions);
    }

    protected void clearState(K key, N namespace) throws IOException {
        delete(key, namespace);
    }

    protected final <T> byte[] serializeValue(TypeSerializer<T> serializer, T value)
            throws IOException {
        return CobbleStateKeySerializer.serialize(serializer, value);
    }

    protected final <T> T deserializeValue(TypeSerializer<T> serializer, byte[] value)
            throws IOException {
        return CobbleStateKeySerializer.deserialize(serializer, value);
    }

    private byte[] getDirectBytes(
            int bucket, CobbleStateKeySerializer.DirectBufferSlice directKey) {
        try (DirectRow directRow =
                db.getDirectWithOptions(
                        bucket, directKey.buffer(), directKey.length(), readOptions)) {
            if (directRow == null) {
                return null;
            }
            return toHeapBytes(directRow.getBytes(STATE_COLUMN_INDEX));
        }
    }

    private byte[] getDirectBytes(int bucket, byte[] rowKey) {
        try (DirectRow directRow = db.getDirectWithOptions(bucket, rowKey, readOptions)) {
            if (directRow == null) {
                return null;
            }
            return toHeapBytes(directRow.getBytes(STATE_COLUMN_INDEX));
        }
    }

    private static byte[] toHeapBytes(ByteBuffer directBuffer) {
        if (directBuffer == null) {
            return null;
        }
        ByteBuffer view = directBuffer.duplicate();
        byte[] copied = new byte[view.remaining()];
        view.get(copied);
        return copied;
    }

    protected final <T> byte[][] serializeListValues(
            TypeSerializer<T> serializer, Iterable<T> values) throws IOException {
        List<byte[]> encoded = new ArrayList<>();
        for (T value : values) {
            encoded.add(serializeValue(serializer, value));
        }
        return encoded.toArray(new byte[0][]);
    }

    protected final <T> List<T> deserializeListValues(TypeSerializer<T> serializer, byte[][] values)
            throws IOException {
        List<T> decoded = new ArrayList<>(values.length);
        for (byte[] value : values) {
            decoded.add(deserializeValue(serializer, value));
        }
        return decoded;
    }

    @Override
    public void close() {
        if (!optionsDisposed.compareAndSet(false, true)) {
            return;
        }
        writeOptions.close();
        scanOptions.close();
        readOptions.close();
    }

    /** Uses Cobble native per-write TTL when Flink enabled TTL for this state descriptor. */
    private static WriteOptions createWriteOptions(String columnFamily, StateTtlConfig ttlConfig) {
        WriteOptions options = WriteOptions.withColumnFamily(columnFamily);
        if (ttlConfig != null && ttlConfig.isEnabled()) {
            long ttlMillis = ttlConfig.getTtl().toMilliseconds();
            long ttlSeconds = Math.max(1L, (ttlMillis + 999L) / 1000L);
            options.ttlSeconds((int) Math.min(Integer.MAX_VALUE, ttlSeconds));
        }
        return options;
    }
}
