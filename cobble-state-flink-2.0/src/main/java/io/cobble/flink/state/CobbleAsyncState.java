package io.cobble.flink.state;

import io.cobble.flink.common.CobbleStateDescriptor;
import io.cobble.structured.ColumnValue;
import io.cobble.structured.Db;
import io.cobble.structured.DirectEncodedRow;
import io.cobble.structured.ReadOptions;
import io.cobble.structured.ScanOptions;
import io.cobble.structured.StreamingMultiGet;
import io.cobble.structured.StreamingMultiGetResult;
import io.cobble.structured.StreamingWriteBatch;
import io.cobble.structured.WriteOptions;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.asyncprocessing.StateRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** Thread-safe physical access shared by Cobble's Flink 2.0 asynchronous states. */
abstract class CobbleAsyncState<K, N, V> implements AutoCloseable {

    static final int STATE_COLUMN_INDEX = 0;
    private static final int STREAMING_BATCH_SIZE = 1024 * 1024;

    final Db db;
    final String columnFamily;
    final CobbleStateDescriptor stateDescriptor;
    final N defaultNamespace;
    final ReadOptions readOptions;
    final ScanOptions scanOptions;
    final WriteOptions writeOptions;

    private final ThreadLocal<CobbleStateKeySerializer.ReusableSerializedKeyBuilder<K, N>>
            rowKeyBuilders;
    private final ThreadLocal<TypeSerializer<V>> valueSerializers;
    private final ThreadLocal<ReusableDirectBuffers> directMergeBuffers;
    private final ThreadLocal<Map<Integer, StreamingWriteContext>> streamingWriteContexts;
    private final ThreadLocal<StreamingMultiGet> streamingMultiGets;
    private final AtomicBoolean closed;

    CobbleAsyncState(
            Db db,
            CobbleStateDescriptor stateDescriptor,
            TypeSerializer<K> keySerializer,
            N defaultNamespace,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            StateTtlConfig ttlConfig) {
        this.db = db;
        this.columnFamily = stateDescriptor.columnFamily();
        this.stateDescriptor = stateDescriptor;
        this.defaultNamespace = defaultNamespace;
        this.readOptions = ReadOptions.defaultsInFamily(columnFamily);
        this.scanOptions = ScanOptions.defaults().columnFamily(columnFamily);
        this.writeOptions = createWriteOptions(columnFamily, ttlConfig);
        this.rowKeyBuilders =
                ThreadLocal.withInitial(
                        () ->
                                new CobbleStateKeySerializer.ReusableSerializedKeyBuilder<>(
                                        keySerializer.duplicate(),
                                        namespaceSerializer.duplicate(),
                                        128));
        this.valueSerializers = ThreadLocal.withInitial(valueSerializer::duplicate);
        this.directMergeBuffers = ThreadLocal.withInitial(ReusableDirectBuffers::new);
        this.streamingWriteContexts = ThreadLocal.withInitial(HashMap::new);
        this.streamingMultiGets =
                ThreadLocal.withInitial(
                        () -> db.streamingMultiGet(readOptions, STREAMING_BATCH_SIZE));
        this.closed = new AtomicBoolean(false);
    }

    abstract boolean isReadRequest(StateRequest<?, ?, ?, ?> request);

    abstract Object execute(StateRequest<?, ?, ?, ?> request) throws Exception;

    Object[] executeBatch(List<StateRequest<?, ?, ?, ?>> requests) throws Exception {
        Object[] results = new Object[requests.size()];
        for (int i = 0; i < requests.size(); i++) {
            results[i] = execute(requests.get(i));
        }
        return results;
    }

    final int bucket(StateRequest<?, ?, ?, ?> request) {
        return request.getRecordContext().getKeyGroup();
    }

    @SuppressWarnings("unchecked")
    final K key(StateRequest<?, ?, ?, ?> request) {
        return (K) request.getRecordContext().getKey();
    }

    @SuppressWarnings("unchecked")
    final N namespace(StateRequest<?, ?, ?, ?> request) {
        N namespace = (N) request.getNamespace();
        return namespace == null ? defaultNamespace : namespace;
    }

    final byte[] rowKey(StateRequest<?, ?, ?, ?> request) throws IOException {
        return rowKey(key(request), namespace(request));
    }

    final byte[] rowKey(K key, N namespace) throws IOException {
        return rowKeyBuilders.get().buildKeyAndNamespace(key, namespace);
    }

    final <UK> byte[] mapEntryRowKey(
            StateRequest<?, ?, ?, ?> request, TypeSerializer<UK> userKeySerializer, UK userKey)
            throws IOException {
        return rowKeyBuilders
                .get()
                .buildMapKeyNamespaceAndUserKey(
                        key(request), userKeySerializer, userKey, namespace(request));
    }

    final byte[] mapPrefix(StateRequest<?, ?, ?, ?> request) throws IOException {
        return rowKeyBuilders.get().buildMapKeyNamespacePrefix(key(request), namespace(request));
    }

    final TypeSerializer<V> valueSerializer() {
        return valueSerializers.get();
    }

    final byte[] readBytes(int bucket, byte[] key) throws IOException {
        try (DirectEncodedRow row = db.getDirectEncodedRowWithOptions(bucket, key, readOptions)) {
            if (row == null) {
                return null;
            }
            return row.decodeBytesColumn(STATE_COLUMN_INDEX, CobbleAsyncState::readAllBytes);
        }
    }

    final <T> Object[] readValuesBatch(int[] buckets, byte[][] keys, TypeSerializer<T> serializer)
            throws IOException {
        if (buckets.length != keys.length) {
            throw new IllegalArgumentException("Cobble multi-get buckets and keys differ in size.");
        }
        StreamingMultiGet batch = streamingMultiGets.get();
        batch.clear();
        DataOutputStream keyOutput = batch.keyOutput();
        for (int i = 0; i < keys.length; i++) {
            keyOutput.write(keys[i]);
            batch.finishKey(buckets[i]);
        }

        Object[] values = new Object[keys.length];
        try (StreamingMultiGetResult result = batch.execute()) {
            for (int i = 0; i < values.length; i++) {
                DataInputStream input = result.nextValue();
                if (input == null) {
                    continue;
                }
                values[i] = serializer.deserialize(new DataInputViewStreamWrapper(input));
                if (input.available() != 0) {
                    throw new IOException(
                            "Flink serializer left unread bytes in Cobble streaming value " + i);
                }
            }
        }
        return values;
    }

    final boolean containsRow(int bucket, byte[] key) {
        try (DirectEncodedRow row = db.getDirectEncodedRowWithOptions(bucket, key, readOptions)) {
            return row != null;
        }
    }

    final void putBytes(int bucket, byte[] key, byte[] value) {
        db.putWithOptions(
                bucket, key, STATE_COLUMN_INDEX, ColumnValue.ofBytes(value), writeOptions);
    }

    final void mergeBytes(int bucket, byte[] key, byte[] value) {
        ReusableDirectBuffers buffers = directMergeBuffers.get();
        ByteBuffer directKey = buffers.copyKey(key);
        ByteBuffer directValue = buffers.copyValue(value);
        // This is the same pre-encoded merge path used by the synchronous ListState. The generic
        // structured merge API treats byte columns as a different merge flavor.
        db.mergeDirectWithOptions(
                bucket,
                directKey,
                key.length,
                STATE_COLUMN_INDEX,
                directValue,
                value.length,
                writeOptions);
    }

    final <T> void putValuesBatch(
            int bucket, List<byte[]> keys, List<T> values, TypeSerializer<T> serializer)
            throws IOException {
        if (keys.size() != values.size()) {
            throw new IllegalArgumentException(
                    "Cobble write batch keys and values differ in size.");
        }
        StreamingWriteContext context = streamingWriteContext(bucket);
        context.batch.clear();
        try {
            for (int i = 0; i < keys.size(); i++) {
                context.output.write(keys.get(i));
                context.batch.finishKey();
                serializer.serialize(values.get(i), context.outputView);
                context.batch.finishElement();
            }
            context.batch.flush();
        } catch (IOException | RuntimeException error) {
            context.batch.clear();
            throw error;
        }
    }

    final void delete(int bucket, byte[] key) {
        db.deleteWithOptions(bucket, key, STATE_COLUMN_INDEX, writeOptions);
    }

    final byte[] serializeValue(TypeSerializer<?> serializer, Object value) throws IOException {
        @SuppressWarnings("unchecked")
        TypeSerializer<Object> typed = (TypeSerializer<Object>) serializer;
        return CobbleStateKeySerializer.serialize(typed, value);
    }

    final <T> T deserializeValue(TypeSerializer<T> serializer, byte[] value) throws IOException {
        return serializer.deserialize(
                new DataInputViewStreamWrapper(new ByteArrayInputStream(value)));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        closeStreamingWriteContexts();
        streamingWriteContexts.remove();
        streamingMultiGets.get().close();
        streamingMultiGets.remove();
        writeOptions.close();
        scanOptions.close();
        readOptions.close();
        rowKeyBuilders.remove();
        valueSerializers.remove();
        directMergeBuffers.remove();
    }

    private static WriteOptions createWriteOptions(String columnFamily, StateTtlConfig ttlConfig) {
        WriteOptions options = WriteOptions.withColumnFamily(columnFamily);
        if (ttlConfig != null && ttlConfig.isEnabled()) {
            long ttlMillis = ttlConfig.getTimeToLive().toMillis();
            long ttlSeconds = Math.max(1L, (ttlMillis + 999L) / 1000L);
            options.ttlSeconds((int) Math.min(Integer.MAX_VALUE, ttlSeconds));
        }
        return options;
    }

    private static byte[] readAllBytes(InputStream input) throws IOException {
        byte[] buffer = new byte[256];
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int read;
        while ((read = input.read(buffer)) != -1) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private StreamingWriteContext streamingWriteContext(int bucket) {
        return streamingWriteContexts
                .get()
                .computeIfAbsent(
                        bucket,
                        ignored ->
                                new StreamingWriteContext(
                                        db.streamingWriteBatch(
                                                bucket,
                                                STATE_COLUMN_INDEX,
                                                writeOptions,
                                                STREAMING_BATCH_SIZE)));
    }

    private void closeStreamingWriteContexts() {
        IOException firstError = null;
        for (StreamingWriteContext context : streamingWriteContexts.get().values()) {
            try {
                context.batch.close();
            } catch (IOException error) {
                if (firstError == null) {
                    firstError = error;
                } else {
                    firstError.addSuppressed(error);
                }
            }
        }
        if (firstError != null) {
            throw new IllegalStateException(
                    "Failed to close Cobble streaming write batches", firstError);
        }
    }

    private static final class StreamingWriteContext {
        private final StreamingWriteBatch batch;
        private final DataOutputStream output;
        private final DataOutputViewStreamWrapper outputView;

        private StreamingWriteContext(StreamingWriteBatch batch) {
            this.batch = batch;
            this.output = batch.output();
            this.outputView = new DataOutputViewStreamWrapper(output);
        }
    }

    private static final class ReusableDirectBuffers {
        private ByteBuffer key = ByteBuffer.allocateDirect(128);
        private ByteBuffer value = ByteBuffer.allocateDirect(256);

        private ByteBuffer copyKey(byte[] bytes) {
            key = copy(key, bytes);
            return key;
        }

        private ByteBuffer copyValue(byte[] bytes) {
            value = copy(value, bytes);
            return value;
        }

        private static ByteBuffer copy(ByteBuffer target, byte[] bytes) {
            if (target.capacity() < bytes.length) {
                target = ByteBuffer.allocateDirect(growCapacity(target.capacity(), bytes.length));
            }
            target.clear();
            target.put(bytes);
            target.flip();
            return target;
        }

        private static int growCapacity(int current, int required) {
            int capacity = current;
            while (capacity < required) {
                capacity = Math.multiplyExact(capacity, 2);
            }
            return capacity;
        }
    }
}
