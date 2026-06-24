package io.cobble.flink.state;

import io.cobble.structured.Db;
import io.cobble.structured.DirectEncodedRow;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Cobble-backed {@link org.apache.flink.api.common.state.AggregatingState}.
 *
 * <p>Storage shape is identical to {@link CobbleValueState} and {@link CobbleReducingState}: one
 * serialized accumulator {@code ACC} bytes column at {@code STATE_COLUMN_INDEX = 0}, keyed by row
 * {@code [key][namespace]}. The persisted type is the accumulator, not the output. This makes
 * restored Flink RocksDB aggregating-state bytes load directly with no transformation.
 *
 * <p>All accumulator writes route through {@link #updateInternal(Object)} so a null accumulator
 * returned from {@link AggregateFunction#add} or {@link AggregateFunction#merge} clears the row,
 * mirroring the {@link InternalAggregatingState} contract.
 *
 * <p>{@link #mergeNamespaces} mirrors Heap's {@code mergeNamespaces} for AggregatingState using
 * {@link AggregateFunction#merge}, not {@code reduce}:
 *
 * <ul>
 *   <li>For each source namespace, the source row is treated as removed-then-folded; duplicate
 *       source namespaces (by serialized row-key bytes) contribute only on first appearance,
 *       matching Heap's {@code removeAndGetOld} returning {@code null} on the second visit.
 *   <li>If {@link AggregateFunction#merge} returns null while folding sources, folding continues: a
 *       later non-null source becomes the new merged value exactly like Heap's {@code merged ==
 *       null} branch.
 *   <li>If the target namespace appears in the sources, the target row is consumed as one of the
 *       captured source values. When the final folded source value is null, no transform is applied
 *       and the target row is deleted as a removed source.
 *   <li>If the target is absent from sources and the final folded source value is null, the target
 *       row is left untouched; this avoids refreshing TTL when Heap would skip {@code transform}.
 *   <li>If the target is absent from sources but exists on disk and the folded source value is
 *       non-null, the final value is {@code merge(targetValue, foldedSources)}.
 *   <li>Steps 1-6 are pure computation; the only writes happen in steps 7-8 (compute-then-write
 *       boundary). If the final target transform returns null, the target row is deleted instead of
 *       written.
 * </ul>
 */
final class CobbleAggregatingState<K, N, IN, ACC, OUT> extends AbstractCobbleState<K, N, ACC>
        implements InternalAggregatingState<K, N, IN, ACC, OUT> {

    private final AggregateFunction<IN, ACC, OUT> aggFunction;

    CobbleAggregatingState(
            CobbleKeyedStateBackend<K> backend,
            Db db,
            String columnFamily,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<ACC> accumulatorSerializer,
            AggregateFunction<IN, ACC, OUT> aggFunction,
            StateTtlConfig ttlConfig) {
        super(
                backend,
                db,
                columnFamily,
                keySerializer,
                namespaceSerializer,
                accumulatorSerializer,
                ttlConfig);
        this.aggFunction = Preconditions.checkNotNull(aggFunction, "aggFunction");
    }

    @Override
    public OUT get() throws IOException {
        ACC acc = getCurrentDirectValue();
        if (acc == null) {
            return null;
        }
        return aggFunction.getResult(acc);
    }

    @Override
    public void add(IN value) throws Exception {
        if (value == null) {
            // Mirror HeapAggregatingState.add(null): clear current entry, then return.
            clear();
            return;
        }
        ACC current = getCurrentDirectValue();
        if (current == null) {
            current = aggFunction.createAccumulator();
        }
        // Route through updateInternal so null result clears the row.
        updateInternal(aggFunction.add(value, current));
    }

    @Override
    public ACC getInternal() throws IOException {
        return getCurrentDirectValue();
    }

    @Override
    public void updateInternal(ACC valueToStore) throws IOException {
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

        // Step 1: serialize target into a stable byte[]. rowKey(...) returns a fresh copy per
        // call (ReusableSerializedKeyBuilder.buildKeyAndNamespace uses getCopyOfBuffer()), so
        // the returned array is safe to retain across later builder reuses below.
        byte[] targetRowKey = rowKey(key, target);

        // Step 2: walk sources once, serialize each, dedup by Arrays.equals on the full row-key
        // bytes. Drop nulls and second-and-later duplicates. Determine whether target itself is
        // among the kept sources.
        List<byte[]> keptSourceRowKeys = new ArrayList<>(sources.size());
        boolean targetWasRemoved = false;
        for (N source : sources) {
            if (source == null) {
                continue;
            }
            byte[] srcRowKey = rowKey(key, source);
            if (containsRowKey(keptSourceRowKeys, srcRowKey)) {
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

        // Step 3: read each kept source row; collect non-null accumulators in order.
        List<ACC> capturedSources = new ArrayList<>(keptSourceRowKeys.size());
        for (byte[] srcRowKey : keptSourceRowKeys) {
            ACC sourceValue = readValueAt(bucket, srcRowKey);
            if (sourceValue != null) {
                capturedSources.add(sourceValue);
            }
        }

        // Step 4: left-fold non-null captured source accumulators via AggregateFunction.merge.
        // Match Heap exactly: if merge(a,b) returns null, do NOT stop. A later non-null source
        // re-seeds the merged value through the `merged == null` branch.
        ACC foldedSources = null;
        for (ACC sourceValue : capturedSources) {
            if (foldedSources != null) {
                foldedSources = aggFunction.merge(foldedSources, sourceValue);
            } else {
                foldedSources = sourceValue;
            }
        }

        // Step 5: targetValue. If target appeared in sources, its row was conceptually
        // remove-and-folded already, so targetValue is null.
        ACC targetValue;
        if (targetWasRemoved) {
            targetValue = null;
        } else {
            targetValue = readValueAt(bucket, targetRowKey);
        }

        // Step 6: combine target with the folded sources only if Heap would transform target. When
        // foldedSources is null, Heap skips map.transform(target, ...), so an independent target
        // must be left untouched (and must not get its TTL refreshed by a no-op rewrite).
        boolean shouldWriteTarget = foldedSources != null || targetWasRemoved;
        ACC newTarget = null;
        if (foldedSources != null) {
            if (targetValue == null) {
                newTarget = foldedSources;
            } else {
                newTarget = aggFunction.merge(targetValue, foldedSources);
            }
        }

        // Step 7: write target row only when Heap would transform target, or when target was among
        // sources and therefore has been conceptually removed. Compute-then-write boundary: this is
        // the first mutation performed by this method.
        if (shouldWriteTarget) {
            if (newTarget == null) {
                delete(bucket, targetRowKey);
            } else {
                putValueAt(bucket, targetRowKey, newTarget);
            }
        }

        // Step 8: delete every kept source row whose bytes differ from the target's. The target
        // row itself, if it was in sources, has just been re-written/deleted and must not be
        // deleted again here.
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
            TypeSerializer<ACC> safeValueSerializer)
            throws Exception {
        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);
        return getBytes(keyAndNamespace.f0, keyAndNamespace.f1);
    }

    private ACC getCurrentDirectValue() throws IOException {
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

    private void putCurrentDirectValue(ACC value) throws IOException {
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

    private ACC readValueAt(int bucket, byte[] rowKey) throws IOException {
        try (DirectEncodedRow encodedRow =
                db.getDirectEncodedRowWithOptions(bucket, rowKey, readOptions)) {
            if (encodedRow == null) {
                return null;
            }
            return encodedRow.decodeBytesColumn(
                    STATE_COLUMN_INDEX, input -> deserializeValue(valueSerializer, input));
        }
    }

    private void putValueAt(int bucket, byte[] rowKey, ACC value) throws IOException {
        // The merge path writes to a row keyed by an arbitrary namespace (not necessarily the
        // current one), so we serialize into a fresh byte[] and use the byte[] put.
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
                    STATE_COLUMN_INDEX, CobbleAggregatingState::readAllBytes);
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
