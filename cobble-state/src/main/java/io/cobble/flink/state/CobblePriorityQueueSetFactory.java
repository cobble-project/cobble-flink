package io.cobble.flink.state;

import io.cobble.structured.Db;
import io.cobble.structured.PriorityQueue;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StateMigrationException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Creates Cobble timer queues using the same partitioning boundary as Flink's RocksDB backend.
 *
 * <p>Each timer state owns one native priority-queue column family. Each returned logical queue is
 * then partitioned into one {@link CobbleCachingPriorityQueueSet} per local Flink key group. Since
 * a child queue binds {@code bucket = keyGroup} at construction, rescale preserves Cobble's bucket
 * identity while Flink changes only the local key-group range.
 */
final class CobblePriorityQueueSetFactory implements PriorityQueueSetFactory {

    private static final String TIMER_QUEUE_COLUMN_FAMILY_PREFIX = "__cobble_timer__";

    private final Db cobbleDb;
    private final KeyGroupRange keyGroupRange;
    private final int totalKeyGroups;
    private final boolean restoredNativeQueuesMayContainEntries;
    private final BiConsumer<String, TypeSerializer<?>> schemaRegistration;
    private final Map<String, RegisteredPriorityQueueStateBackendMetaInfo<?>> metaInfos;
    private final Map<String, CobbleTimerPriorityQueue<?>> queues;

    CobblePriorityQueueSetFactory(
            Db cobbleDb,
            KeyGroupRange keyGroupRange,
            int totalKeyGroups,
            boolean restoredNativeQueuesMayContainEntries,
            BiConsumer<String, TypeSerializer<?>> schemaRegistration) {
        this.cobbleDb = cobbleDb;
        this.keyGroupRange = keyGroupRange;
        this.totalKeyGroups = totalKeyGroups;
        this.restoredNativeQueuesMayContainEntries = restoredNativeQueuesMayContainEntries;
        this.schemaRegistration = schemaRegistration;
        this.metaInfos = new HashMap<>();
        this.queues = new HashMap<>();
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    String stateName, TypeSerializer<T> byteOrderedElementSerializer) {
        return create(stateName, byteOrderedElementSerializer, false);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    String stateName,
                    TypeSerializer<T> byteOrderedElementSerializer,
                    boolean allowFutureMetadataUpdates) {
        RegisteredPriorityQueueStateBackendMetaInfo<T> existingMetaInfo =
                (RegisteredPriorityQueueStateBackendMetaInfo<T>) metaInfos.get(stateName);
        CobbleTimerPriorityQueue<T> existingQueue =
                (CobbleTimerPriorityQueue<T>) queues.get(stateName);
        if (existingMetaInfo != null && existingQueue != null) {
            TypeSerializerSchemaCompatibility<T> compatibility =
                    existingMetaInfo.updateElementSerializer(byteOrderedElementSerializer);
            if (compatibility.isIncompatible() || compatibility.isCompatibleAfterMigration()) {
                throw new FlinkRuntimeException(
                        new StateMigrationException(
                                "Cobble timer queues require byte-ordered serializers that stay compatible as-is; migration is not supported."));
            }
            TypeSerializer<T> effectiveSerializer =
                    compatibility.isCompatibleWithReconfiguredSerializer()
                            ? compatibility.getReconfiguredSerializer()
                            : existingMetaInfo.getElementSerializer();
            existingQueue.updateSerializer(effectiveSerializer);
            registerSchema(stateName, effectiveSerializer);
            return existingQueue;
        }

        RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo =
                new RegisteredPriorityQueueStateBackendMetaInfo<>(
                        stateName, byteOrderedElementSerializer);
        // Native checkpoints retain timer bytes but not Flink serializer snapshots. Restored jobs
        // must therefore provide a byte-compatible timer serializer before opening the queue.
        if (allowFutureMetadataUpdates) {
            metaInfo = metaInfo.withSerializerUpgradesAllowed();
        }
        PriorityQueue nativeQueue =
                cobbleDb.getOrNewPriorityQueue(timerQueueColumnFamilyName(stateName));
        CobbleTimerPriorityQueue<T> queue =
                new CobbleTimerPriorityQueue<>(
                        cobbleDb,
                        nativeQueue,
                        metaInfo.getElementSerializer(),
                        keyGroupRange,
                        totalKeyGroups,
                        restoredNativeQueuesMayContainEntries);
        metaInfos.put(stateName, metaInfo);
        queues.put(stateName, queue);
        registerSchema(stateName, metaInfo.getElementSerializer());
        return queue;
    }

    private void registerSchema(String stateName, TypeSerializer<?> timerSerializer) {
        if (schemaRegistration != null) {
            schemaRegistration.accept(stateName, timerSerializer);
        }
    }

    static String timerQueueColumnFamilyName(String stateName) {
        return TIMER_QUEUE_COLUMN_FAMILY_PREFIX + stateName;
    }

    static boolean isTimerQueueColumnFamily(String columnFamily) {
        return columnFamily.startsWith(TIMER_QUEUE_COLUMN_FAMILY_PREFIX);
    }

    boolean hasQueues() {
        return !queues.isEmpty();
    }

    void close() throws IOException {
        IOException error = null;
        for (Map.Entry<String, CobbleTimerPriorityQueue<?>> entry : queues.entrySet()) {
            try {
                entry.getValue().close();
            } catch (RuntimeException e) {
                IOException closeError =
                        new IOException(
                                "Failed to close Cobble timer queue resources for '"
                                        + entry.getKey()
                                        + "'.",
                                e);
                if (error == null) {
                    error = closeError;
                } else {
                    error.addSuppressed(closeError);
                }
            }
        }
        if (error != null) {
            throw error;
        }
    }
}
