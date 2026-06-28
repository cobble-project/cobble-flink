package io.cobble.flink.state;

import io.cobble.structured.Db;
import io.cobble.structured.DirectEncodedRow;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Cobble-backed {@link org.apache.flink.api.common.state.ReducingState}.
 *
 * <p>Storage shape is identical to {@link CobbleValueState}: one serialized {@code V} bytes column
 * at {@code STATE_COLUMN_INDEX = 0}, keyed by row {@code [key][namespace]}. This makes restored
 * Flink RocksDB reducing-state bytes load directly with no transformation.
 *
 * <p>{@link #mergeNamespaces} precisely emulates Flink heap's {@code mergeNamespaces} semantics:
 *
 * <ul>
 *   <li>For each source namespace, the source row is treated as removed-then-folded; duplicate
 *       source namespaces (by serialized bytes) contribute only on first appearance, matching
 *       Heap's {@code removeAndGetOld} returning {@code null} on the second visit.
 *   <li>If the target namespace appears in the sources, the target row is consumed as one of the
 *       captured source values during the fold and never additionally re-read as the "target value"
 *       passed to the final {@code reduce(target, foldedSources)} call. The final write is just the
 *       folded result. This avoids the {@code reduce(Vt, Vt)} self-reduce that a naive
 *       read-target-then-fold would produce.
 *   <li>If the target is absent from sources but exists on disk, the final value is {@code
 *       reduce(targetValue, foldedSources)}.
 *   <li>The target row is written unconditionally when any source contributed; we deliberately
 *       avoid a reference-equality guard so reducers that mutate-and-return their first argument
 *       still persist.
 * </ul>
 */
final class CobbleReducingState<K, N, V> extends AbstractCobbleState<K, N, V>
        implements InternalReducingState<K, N, V> {

    private final ReduceFunction<V> reduceFunction;

    CobbleReducingState(
            CobbleKeyedStateBackend<K> backend,
            Db db,
            String columnFamily,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            ReduceFunction<V> reduceFunction,
            StateTtlConfig ttlConfig) {
        super(
                backend,
                db,
                columnFamily,
                keySerializer,
                namespaceSerializer,
                valueSerializer,
                ttlConfig);
        this.reduceFunction = Preconditions.checkNotNull(reduceFunction, "reduceFunction");
    }

    @Override
    public V get() throws IOException {
        return getCurrentDirectValue();
    }

    @Override
    public void add(V value) throws Exception {
        if (value == null) {
            // Mirror HeapReducingState.add(null): clear current entry, then return.
            clear();
            return;
        }
        V current = getCurrentDirectValue();
        V toStore = current == null ? value : reduceFunction.reduce(current, value);
        putCurrentDirectValue(toStore);
    }

    @Override
    public V getInternal() throws IOException {
        return getCurrentDirectValue();
    }

    @Override
    public void updateInternal(V valueToStore) throws IOException {
        if (valueToStore == null) {
            clear();
            return;
        }
        putCurrentDirectValue(valueToStore);
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (sources == null || sources.isEmpty()) {
            return;
        }

        K key = currentKey();
        int bucket = bucketForKey(key);

        // Step 1: serialize target into a stable byte[]. rowKey(...) returns a fresh copy per call
        // (ReusableSerializedKeyBuilder.buildKeyAndNamespace uses getCopyOfBuffer()), so the
        // returned array is safe to retain across later builder reuses below.
        byte[] targetRowKey = rowKey(key, target);

        // Step 2: walk sources once, serialize each, dedup by Arrays.equals on the full row-key
        // bytes (row key contains the namespace; key part is identical for all entries, so
        // row-key equality is equivalent to namespace equality here). Drop nulls and
        // second-and-later duplicates. Determine whether target itself is among the kept sources.
        List<byte[]> keptSourceRowKeys = new ArrayList<>(sources.size());
        boolean targetWasRemoved = false;
        for (N source : sources) {
            if (source == null) {
                continue;
            }
            byte[] srcRowKey = rowKey(key, source);
            if (containsRowKey(keptSourceRowKeys, srcRowKey)) {
                // Duplicate source namespace; on Heap's second pass removeAndGetOld returns null.
                continue;
            }
            keptSourceRowKeys.add(srcRowKey);
            if (Arrays.equals(srcRowKey, targetRowKey)) {
                targetWasRemoved = true;
            }
        }

        if (keptSourceRowKeys.isEmpty()) {
            return;
        }

        // Step 3: read each kept source row; collect non-null values in order.
        List<V> capturedSources = new ArrayList<>(keptSourceRowKeys.size());
        for (byte[] srcRowKey : keptSourceRowKeys) {
            V sourceValue = readValueAt(bucket, srcRowKey);
            if (sourceValue != null) {
                capturedSources.add(sourceValue);
            }
        }

        // Step 4: left-fold non-null captured source values via the reduce function.
        V foldedSources = null;
        if (!capturedSources.isEmpty()) {
            foldedSources = capturedSources.get(0);
            for (int i = 1; i < capturedSources.size(); i++) {
                foldedSources = reduceFunction.reduce(foldedSources, capturedSources.get(i));
            }
        }

        if (foldedSources == null) {
            // No source contributed anything; mirror Heap's "no transform call" branch. We still
            // delete any kept source rows that exist on disk only as sentinels (none here, since
            // they'd have produced a value); nothing to write or delete.
            return;
        }

        // Step 5: targetValue. If target appeared in sources, its row was conceptually
        // remove-and-folded already, so targetValue is null.
        V targetValue;
        if (targetWasRemoved) {
            targetValue = null;
        } else {
            targetValue = readValueAt(bucket, targetRowKey);
        }

        // Step 6: combine.
        V newTarget;
        if (targetValue == null) {
            newTarget = foldedSources;
        } else {
            newTarget = reduceFunction.reduce(targetValue, foldedSources);
        }

        // Step 7: write target row unconditionally (no reference-equality guard).
        putValueAt(bucket, targetRowKey, newTarget);

        // Step 8: delete every kept source row whose bytes differ from the target's. The target
        // row itself, if it was in sources, has just been re-written and must not be deleted.
        for (byte[] srcRowKey : keptSourceRowKeys) {
            if (!Arrays.equals(srcRowKey, targetRowKey)) {
                delete(bucket, srcRowKey);
            }
        }
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

    private V getCurrentDirectValue() throws IOException {
        CobbleStateKeySerializer.DirectBufferSlice directKey = currentDirectKey();
        try (DirectEncodedRow encodedRow =
                db.getDirectEncodedRowWithOptions(
                        currentBucket(), directKey.buffer(), directKey.length(), readOptions)) {
            if (encodedRow == null) {
                return null;
            }
            return encodedRow.decodeBytesColumn(
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

    private V readValueAt(int bucket, byte[] rowKey) throws IOException {
        try (DirectEncodedRow encodedRow =
                db.getDirectEncodedRowWithOptions(bucket, rowKey, readOptions)) {
            if (encodedRow == null) {
                return null;
            }
            return encodedRow.decodeBytesColumn(
                    STATE_COLUMN_INDEX, input -> deserializeValue(valueSerializer, input));
        }
    }

    private void putValueAt(int bucket, byte[] rowKey, V value) throws IOException {
        // The merge path needs to write to a row keyed by an arbitrary namespace (not necessarily
        // the current one), so we serialize the value into a fresh byte[] and use the byte[] put.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        org.apache.flink.core.memory.DataOutputViewStreamWrapper view =
                new org.apache.flink.core.memory.DataOutputViewStreamWrapper(out);
        valueSerializer.serialize(value, view);
        db.putWithOptions(
                bucket,
                rowKey,
                STATE_COLUMN_INDEX,
                io.cobble.structured.ColumnValue.ofBytes(out.toByteArray()),
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
            return encodedRow.decodeBytesColumn(
                    STATE_COLUMN_INDEX, CobbleReducingState::readAllBytes);
        }
    }

    private static boolean containsRowKey(List<byte[]> kept, byte[] candidate) {
        for (byte[] entry : kept) {
            if (Arrays.equals(entry, candidate)) {
                return true;
            }
        }
        return false;
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
