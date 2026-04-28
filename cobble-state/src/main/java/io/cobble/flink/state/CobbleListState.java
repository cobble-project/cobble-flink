package io.cobble.flink.state;

import io.cobble.structured.Db;
import io.cobble.structured.DirectEncodedRow;
import io.cobble.structured.DirectListValueBuilder;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Cobble-backed {@link org.apache.flink.api.common.state.ListState}. */
final class CobbleListState<K, N, V> extends AbstractCobbleState<K, N, List<V>>
        implements InternalListState<K, N, V> {

    private final TypeSerializer<V> elementSerializer;
    private final DirectListValueBuilder directListValueBuilder;
    private final DataOutputViewStreamWrapper directListValueOutputView;

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
        this.directListValueBuilder = new DirectListValueBuilder(256);
        this.directListValueOutputView =
                new DataOutputViewStreamWrapper(directListValueBuilder.outputStream());
    }

    @Override
    public Iterable<V> get() throws Exception {
        return getInternal();
    }

    @Override
    public void add(V value) throws IOException {
        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
        directListValueBuilder.clear();
        appendListValue(value);
        mergeCurrentEncodedListPayload(
                directListValueBuilder.buffer(), directListValueBuilder.length());
    }

    @Override
    public List<V> getInternal() throws IOException {
        return getCurrentDirectList(elementSerializer);
    }

    @Override
    public void updateInternal(List<V> valueToStore) throws Exception {
        if (valueToStore == null || valueToStore.isEmpty()) {
            clear();
            return;
        }
        encodeListValues(valueToStore);
        putCurrentEncodedListPayload(
                directListValueBuilder.buffer(), directListValueBuilder.length());
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
        encodeListValues(values);
        mergeCurrentEncodedListPayload(
                directListValueBuilder.buffer(), directListValueBuilder.length());
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (sources == null || sources.isEmpty()) {
            return;
        }

        K key = currentKey();
        List<V> mergedValues = new ArrayList<>();
        List<V> targetValues = getDirectList(key, target, elementSerializer);
        if (targetValues != null) {
            mergedValues.addAll(targetValues);
        }

        for (N source : sources) {
            List<V> sourceValues = getDirectList(key, source, elementSerializer);
            if (sourceValues != null) {
                mergedValues.addAll(sourceValues);
                delete(key, source);
            }
        }

        if (mergedValues.isEmpty()) {
            delete(key, target);
        } else {
            encodeListValues(mergedValues);
            putEncodedListPayload(
                    key, target, directListValueBuilder.buffer(), directListValueBuilder.length());
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
        TypeSerializer<V> safeElementSerializer =
                ((ListSerializer<V>) safeValueSerializer).getElementSerializer();
        List<V> values =
                getDirectList(keyAndNamespace.f0, keyAndNamespace.f1, safeElementSerializer);
        if (values == null) {
            return null;
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
        for (int i = 0; i < values.size(); i++) {
            safeElementSerializer.serialize(values.get(i), outputView);
            if (i < values.size() - 1) {
                outputView.writeByte(',');
            }
        }
        outputView.flush();
        return outputStream.toByteArray();
    }

    private void encodeListValues(List<V> values) throws IOException {
        directListValueBuilder.clear();
        for (V value : values) {
            appendListValue(value);
        }
    }

    private void appendListValue(V value) throws IOException {
        directListValueBuilder.beginElement();
        elementSerializer.serialize(value, directListValueOutputView);
        directListValueBuilder.finishElement();
    }

    private <T> List<T> getCurrentDirectList(TypeSerializer<T> serializer) throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey = currentDirectKey();
        try (DirectEncodedRow encodedRow =
                db.getDirectEncodedRowWithOptions(
                        currentBucket(), directKey.buffer(), directKey.length(), readOptions)) {
            if (encodedRow == null) {
                return null;
            }
            return encodedRow.decodeListColumn(
                    STATE_COLUMN_INDEX, input -> deserializeValue(serializer, input));
        }
    }

    private <T> List<T> getDirectList(K key, N namespace, TypeSerializer<T> serializer)
            throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey =
                directRowKeyBuilder.buildKeyAndNamespace(key, namespaceSerializer, namespace);
        try (DirectEncodedRow encodedRow =
                db.getDirectEncodedRowWithOptions(
                        bucketForKey(key), directKey.buffer(), directKey.length(), readOptions)) {
            if (encodedRow == null) {
                return null;
            }
            return encodedRow.decodeListColumn(
                    STATE_COLUMN_INDEX, input -> deserializeValue(serializer, input));
        }
    }

    private void putCurrentEncodedListPayload(ByteBuffer payload, int payloadLength)
            throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey = currentDirectKey();
        db.putEncodedListDirectWithOptions(
                currentBucket(),
                directKey.buffer(),
                directKey.length(),
                STATE_COLUMN_INDEX,
                payload,
                payloadLength,
                writeOptions);
    }

    private void mergeCurrentEncodedListPayload(ByteBuffer payload, int payloadLength)
            throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey = currentDirectKey();
        db.mergeEncodedListDirectWithOptions(
                currentBucket(),
                directKey.buffer(),
                directKey.length(),
                STATE_COLUMN_INDEX,
                payload,
                payloadLength,
                writeOptions);
    }

    private void putEncodedListPayload(K key, N namespace, ByteBuffer payload, int payloadLength)
            throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey =
                directRowKeyBuilder.buildKeyAndNamespace(key, namespaceSerializer, namespace);
        db.putEncodedListDirectWithOptions(
                bucketForKey(key),
                directKey.buffer(),
                directKey.length(),
                STATE_COLUMN_INDEX,
                payload,
                payloadLength,
                writeOptions);
    }
}
