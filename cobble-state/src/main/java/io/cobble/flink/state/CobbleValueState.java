package io.cobble.flink.state;

import io.cobble.Db;
import io.cobble.DirectEncodedRow;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

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
        V stored = getCurrentDirectValue();
        return stored == null ? defaultValue() : stored;
    }

    @Override
    public void update(V value) throws IOException {
        if (value == null) {
            clear();
            return;
        }
        putCurrentDirectValue(value);
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

    private V getCurrentDirectValue() throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey = currentDirectKey();
        try (DirectEncodedRow encodedRow =
                db.getDirectEncodedRowWithOptions(
                        currentBucket(), directKey.buffer(), directKey.length(), readOptions)) {
            if (encodedRow == null) {
                return null;
            }
            return encodedRow.decodeColumn(
                    STATE_COLUMN_INDEX, input -> deserializeValue(valueSerializer, input));
        }
    }

    private void putCurrentDirectValue(V value) throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey = currentDirectKey();
        CobbleStateKeySerializer.DirectBufferSlice directValue =
                directValueSerializer.serialize(valueSerializer, value);
        db.putDirectWithOptions(
                currentBucket(),
                directKey.buffer(),
                directKey.length(),
                STATE_COLUMN_INDEX,
                directValue.buffer(),
                directValue.length(),
                writeOptions);
    }

    private byte[] getBytes(K key, N namespace) throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey =
                directRowKeyBuilder.buildKeyAndNamespace(key, namespace);
        try (DirectEncodedRow encodedRow =
                db.getDirectEncodedRowWithOptions(
                        bucketForKey(key), directKey.buffer(), directKey.length(), readOptions)) {
            if (encodedRow == null) {
                return null;
            }
            return encodedRow.decodeColumn(STATE_COLUMN_INDEX, CobbleValueState::readAllBytes);
        }
    }

    private static byte[] readAllBytes(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[256];
        int read;
        while ((read = input.read(buffer)) != -1) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }
}
