package io.cobble.flink.state;

import io.cobble.structured.Db;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;

import java.io.IOException;

/** Cobble-backed {@link org.apache.flink.api.common.state.ValueState}. */
final class CobbleValueState<K, N, V> extends AbstractCobbleState<K, N, V>
        implements InternalValueState<K, N, V> {

    private final V defaultValue;

    CobbleValueState(
            CobbleKeyedStateBackend<K> backend,
            Db db,
            String columnFamily,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue,
            StateTtlConfig ttlConfig) {
        super(
                backend,
                db,
                columnFamily,
                keySerializer,
                namespaceSerializer,
                valueSerializer,
                ttlConfig);
        this.defaultValue = defaultValue;
    }

    @Override
    public V value() throws IOException {
        byte[] stored = getCurrentBytes();
        return stored == null ? defaultValue() : deserializeValue(valueSerializer, stored);
    }

    @Override
    public void update(V value) throws IOException {
        if (value == null) {
            clear();
            return;
        }
        putCurrentBytes(serializeValue(valueSerializer, value));
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer)
            throws Exception {
        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);
        return getBytes(keyAndNamespace.f0, keyAndNamespace.f1);
    }

    private V defaultValue() {
        return defaultValue == null ? null : valueSerializer.copy(defaultValue);
    }
}
