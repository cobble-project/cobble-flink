package io.cobble.flink.state;

import io.cobble.structured.Db;
import io.cobble.structured.DirectScanCursor;
import io.cobble.structured.ReadOptions;
import io.cobble.structured.ScanCursor;
import io.cobble.structured.ScanOptions;
import io.cobble.structured.WriteOptions;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/** Shared plumbing for Cobble-backed Flink keyed state implementations. */
abstract class AbstractCobbleState<K, N, V> implements InternalKvState<K, N, V>, AutoCloseable {

    protected static final int STATE_COLUMN_INDEX = 0;

    protected final CobbleKeyedStateBackend<K> backend;
    protected final Db db;
    protected final String columnFamily;
    protected final TypeSerializer<K> keySerializer;
    protected final CobbleStateKeySerializer.ReusableSerializedKeyBuilder<K, N> rowKeyBuilder;
    protected final CobbleStateKeySerializer.ReusableSerializedDirectKeyBuilder<K, N>
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
    // Flink table/window operators can deliberately use a null namespace for accumulator state.
    // Keep a separate "was set" bit so explicit null is different from an uninitialized state
    // access.
    protected boolean currentNamespaceSet;

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
                new CobbleStateKeySerializer.ReusableSerializedKeyBuilder<>(
                        keySerializer, namespaceSerializer, 128);
        this.directRowKeyBuilder =
                new CobbleStateKeySerializer.ReusableSerializedDirectKeyBuilder<>(
                        keySerializer, namespaceSerializer, 128);
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
        if (key == null || !currentNamespaceSet) {
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
        this.currentNamespace = namespace;
        this.currentNamespaceSet = true;
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

    protected final byte[] rowKey(K key, N namespace) throws IOException {
        return rowKeyBuilder.buildKeyAndNamespace(key, namespace);
    }

    protected final <UK> byte[] mapEntryRowKey(
            K key, N namespace, TypeSerializer<UK> userKeySerializer, UK userKey)
            throws IOException {
        return rowKeyBuilder.buildMapKeyNamespaceAndUserKey(
                key, userKeySerializer, userKey, namespace);
    }

    protected final <UK> CobbleStateKeySerializer.DirectBufferSlice directMapEntryRowKey(
            K key, N namespace, TypeSerializer<UK> userKeySerializer, UK userKey)
            throws IOException {
        return directRowKeyBuilder.buildMapKeyNamespaceAndUserKey(
                key, userKeySerializer, userKey, namespace);
    }

    protected final byte[] mapKeyNamespacePrefix(K key, N namespace) throws IOException {
        return rowKeyBuilder.buildMapKeyNamespacePrefix(key, namespace);
    }

    protected final K currentKey() {
        return Preconditions.checkNotNull(
                backend.getCurrentKey(),
                "Current key is not set while accessing Cobble state '%s'.",
                columnFamily);
    }

    protected final <T> T deserializeValue(TypeSerializer<T> serializer, ByteBuffer directValue)
            throws IOException {
        return directValueDeserializer.deserialize(serializer, directValue);
    }

    protected final <T> T deserializeValue(TypeSerializer<T> serializer, InputStream input)
            throws IOException {
        return serializer.deserialize(new DataInputViewStreamWrapper(input));
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

    protected final DirectScanCursor scanDirectRows(
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
                scanOptions);
    }

    protected void clearState(K key, N namespace) throws IOException {
        delete(key, namespace);
    }

    protected final CobbleStateKeySerializer.DirectBufferSlice currentDirectKey()
            throws IOException {
        return directRowKeyBuilder.buildKeyAndNamespace(currentKey(), checkedCurrentNamespace());
    }

    protected final N checkedCurrentNamespace() {
        Preconditions.checkState(
                currentNamespaceSet,
                "Current namespace is not set for Cobble state '%s'.",
                columnFamily);
        return currentNamespace;
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
