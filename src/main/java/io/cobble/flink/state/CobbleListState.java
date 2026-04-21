package io.cobble.flink.state;

import io.cobble.structured.Db;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Cobble-backed {@link org.apache.flink.api.common.state.ListState}. */
final class CobbleListState<K, N, V> extends AbstractCobbleState<K, N, List<V>>
        implements InternalListState<K, N, V> {

    private final TypeSerializer<V> elementSerializer;

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
    }

    @Override
    public Iterable<V> get() throws Exception {
        return getInternal();
    }

    @Override
    public void add(V value) throws IOException {
        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
        mergeCurrentList(new byte[][] {serializeValue(elementSerializer, value)});
    }

    @Override
    public List<V> getInternal() throws IOException {
        byte[][] stored = getCurrentList();
        return stored == null ? null : deserializeListValues(elementSerializer, stored);
    }

    @Override
    public void updateInternal(List<V> valueToStore) throws Exception {
        if (valueToStore == null || valueToStore.isEmpty()) {
            clear();
            return;
        }
        putCurrentList(serializeListValues(elementSerializer, valueToStore));
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
        mergeCurrentList(serializeListValues(elementSerializer, values));
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (sources == null || sources.isEmpty()) {
            return;
        }

        K key = currentKey();
        List<byte[]> merged = new ArrayList<>();
        byte[][] targetValues = getList(key, target);
        if (targetValues != null) {
            for (byte[] targetValue : targetValues) {
                merged.add(targetValue);
            }
        }

        for (N source : sources) {
            byte[][] sourceValues = getList(key, source);
            if (sourceValues != null) {
                for (byte[] sourceValue : sourceValues) {
                    merged.add(sourceValue);
                }
                delete(key, source);
            }
        }

        if (merged.isEmpty()) {
            delete(key, target);
        } else {
            putList(key, target, merged.toArray(new byte[0][]));
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
        byte[][] stored = getList(keyAndNamespace.f0, keyAndNamespace.f1);
        if (stored == null) {
            return null;
        }

        TypeSerializer<V> safeElementSerializer =
                ((ListSerializer<V>) safeValueSerializer).getElementSerializer();
        List<V> values = deserializeListValues(safeElementSerializer, stored);
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
}
