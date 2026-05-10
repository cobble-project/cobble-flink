package io.cobble.flink.state;

import io.cobble.Db;
import io.cobble.DirectEncodedRow;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Cobble-backed {@link org.apache.flink.api.common.state.ListState}. */
final class CobbleListState<K, N, V> extends AbstractCobbleState<K, N, List<V>>
        implements InternalListState<K, N, V> {
    private final TypeSerializer<V> elementSerializer;
    private final DirectDelimitedListSerializer directListSerializer;

    CobbleListState(
            CobbleKeyedStateBackend<K> backend,
            Db db,
            String columnFamily,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            ListSerializer<V> valueSerializer,
            TypeSerializer<V> elementSerializer,
            StateTtlConfig ttlConfig) {
        super(
                backend,
                db,
                columnFamily,
                keySerializer,
                namespaceSerializer,
                valueSerializer,
                ttlConfig);
        this.elementSerializer = elementSerializer;
        this.directListSerializer = new DirectDelimitedListSerializer(256);
    }

    @Override
    public Iterable<V> get() throws Exception {
        return getInternal();
    }

    @Override
    public void add(V value) throws IOException {
        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
        mergeCurrentEncodedListPayload(directListSerializer.encodeSingle(elementSerializer, value));
    }

    @Override
    public List<V> getInternal() throws IOException {
        return getCurrentDelimitedList(elementSerializer);
    }

    @Override
    public void updateInternal(List<V> valueToStore) throws Exception {
        if (valueToStore == null || valueToStore.isEmpty()) {
            clear();
            return;
        }
        putCurrentEncodedListPayload(
                directListSerializer.encodeAll(elementSerializer, valueToStore));
    }

    @Override
    public void update(List<V> values) throws Exception {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");
        if (values.isEmpty()) {
            clear();
            return;
        }

        List<V> copy = new ArrayList<>(values.size());
        for (V value : values) {
            Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
            copy.add(value);
        }
        updateInternal(copy);
    }

    @Override
    public void addAll(List<V> values) throws Exception {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");
        if (values.isEmpty()) {
            return;
        }
        for (V value : values) {
            Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
        }
        mergeCurrentEncodedListPayload(directListSerializer.encodeAll(elementSerializer, values));
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (sources == null || sources.isEmpty()) {
            return;
        }

        K key = currentKey();
        CobbleStateKeySerializer.ReusableSerializedDirectKeyBuilder<K, N> targetKeyBuilder =
                new CobbleStateKeySerializer.ReusableSerializedDirectKeyBuilder<>(
                        keySerializer, namespaceSerializer, 128);
        int bucket = bucketForKey(key);
        CobbleStateKeySerializer.DirectBufferSlice targetKey =
                targetKeyBuilder.buildKeyAndNamespace(key, target);

        for (N source : sources) {
            if (source == null) {
                continue;
            }
            CobbleStateKeySerializer.DirectBufferSlice sourceKey =
                    directRowKeyBuilder.buildKeyAndNamespace(key, source);
            CobbleStateKeySerializer.DirectBufferSlice sourceValue =
                    getStoredListPayload(bucket, sourceKey);
            if (sourceValue == null) {
                continue;
            }
            delete(key, source);
            db.mergeDirectWithOptions(
                    bucket,
                    targetKey.buffer(),
                    targetKey.length(),
                    STATE_COLUMN_INDEX,
                    sourceValue.buffer(),
                    sourceValue.length(),
                    writeOptions);
        }
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<List<V>> safeValueSerializer)
            throws Exception {
        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);
        CobbleStateKeySerializer.DirectBufferSlice directKey =
                directRowKeyBuilder.buildKeyAndNamespace(keyAndNamespace.f0, keyAndNamespace.f1);
        try (DirectEncodedRow encodedRow =
                db.getDirectEncodedRowWithOptions(
                        bucketForKey(keyAndNamespace.f0),
                        directKey.buffer(),
                        directKey.length(),
                        readOptions)) {
            if (encodedRow == null) {
                return null;
            }
            return encodedRow.decodeColumn(
                    STATE_COLUMN_INDEX, directListSerializer::copyRawWithoutTrailingDelimiter);
        }
    }

    private <T> List<T> getCurrentDelimitedList(TypeSerializer<T> serializer) throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey = currentDirectKey();
        try (DirectEncodedRow encodedRow =
                db.getDirectEncodedRowWithOptions(
                        currentBucket(), directKey.buffer(), directKey.length(), readOptions)) {
            if (encodedRow == null) {
                return null;
            }
            return encodedRow.decodeColumn(
                    STATE_COLUMN_INDEX, input -> directListSerializer.decode(serializer, input));
        }
    }

    private CobbleStateKeySerializer.DirectBufferSlice getStoredListPayload(
            int bucket, CobbleStateKeySerializer.DirectBufferSlice directKey) throws IOException {
        try (DirectEncodedRow encodedRow =
                db.getDirectEncodedRowWithOptions(
                        bucket, directKey.buffer(), directKey.length(), readOptions)) {
            if (encodedRow == null) {
                return null;
            }
            return encodedRow.decodeColumn(STATE_COLUMN_INDEX, directListSerializer::copyRaw);
        }
    }

    private void putCurrentEncodedListPayload(CobbleStateKeySerializer.DirectBufferSlice payload)
            throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey = currentDirectKey();
        db.putDirectWithOptions(
                currentBucket(),
                directKey.buffer(),
                directKey.length(),
                STATE_COLUMN_INDEX,
                payload.buffer(),
                payload.length(),
                writeOptions);
    }

    private void mergeCurrentEncodedListPayload(CobbleStateKeySerializer.DirectBufferSlice payload)
            throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey = currentDirectKey();
        db.mergeDirectWithOptions(
                currentBucket(),
                directKey.buffer(),
                directKey.length(),
                STATE_COLUMN_INDEX,
                payload.buffer(),
                payload.length(),
                writeOptions);
    }
}
