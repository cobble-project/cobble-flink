package io.cobble.flink.state;

import io.cobble.structured.Db;
import io.cobble.structured.PriorityQueue;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.KeyGroupPartitionedPriorityQueue;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Flink-facing timer queue that partitions one logical timer state into one queue per key group.
 *
 * <p>This follows Flink's RocksDB timer-queue shape: {@link KeyGroupPartitionedPriorityQueue}
 * routes elements and keeps a heap of child-queue heads, while each {@link
 * CobbleCachingPriorityQueueSet} owns exactly one key group. Cobble buckets use the same numeric
 * identity as Flink key groups, so rescale can move key-group ranges without translating timer
 * storage.
 *
 * <p>The inherited implementation would export each child queue's complete iterator from {@link
 * #getSubsetForKeyGroup(int)}. Cobble instead exports only each child queue's overlay. The native
 * shard snapshot already retains timers that have not yet been prefetched, while prefetched timers
 * have been removed from the native queue by {@link PriorityQueue#pollBatchDirect(int)} and must
 * therefore be checkpointed through Flink's legacy timer snapshot path.
 */
final class CobbleTimerPriorityQueue<
                T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
        extends KeyGroupPartitionedPriorityQueue<T, CobbleCachingPriorityQueueSet<T>> {

    private final KeyGroupRange keyGroupRange;
    private final Map<Integer, CobbleCachingPriorityQueueSet<T>> queuesByKeyGroup;
    private final CobbleTimerSerializationContext<T> serializationContext;
    private final PriorityQueue priorityQueue;

    CobbleTimerPriorityQueue(
            Db db,
            PriorityQueue priorityQueue,
            TypeSerializer<T> serializer,
            KeyGroupRange keyGroupRange,
            int totalKeyGroups,
            boolean restoredNativeQueuesMayContainEntries) {
        this(
                db,
                priorityQueue,
                keyGroupRange,
                totalKeyGroups,
                restoredNativeQueuesMayContainEntries,
                new HashMap<>(),
                new CobbleTimerSerializationContext<>(serializer));
    }

    private CobbleTimerPriorityQueue(
            Db db,
            PriorityQueue priorityQueue,
            KeyGroupRange keyGroupRange,
            int totalKeyGroups,
            boolean restoredNativeQueuesMayContainEntries,
            Map<Integer, CobbleCachingPriorityQueueSet<T>> queuesByKeyGroup,
            CobbleTimerSerializationContext<T> serializationContext) {
        super(
                KeyExtractorFunction.forKeyedObjects(),
                PriorityComparator.forPriorityComparableObjects(),
                (keyGroup, ignoredTotalKeyGroups, ignoredKeyExtractor, ignoredComparator) -> {
                    CobbleCachingPriorityQueueSet<T> queue =
                            new CobbleCachingPriorityQueueSet<>(
                                    db,
                                    priorityQueue,
                                    serializationContext,
                                    keyGroup,
                                    restoredNativeQueuesMayContainEntries);
                    queuesByKeyGroup.put(keyGroup, queue);
                    return queue;
                },
                keyGroupRange,
                totalKeyGroups);
        this.keyGroupRange =
                Preconditions.checkNotNull(keyGroupRange, "keyGroupRange must not be null");
        this.queuesByKeyGroup = queuesByKeyGroup;
        this.serializationContext = serializationContext;
        this.priorityQueue =
                Preconditions.checkNotNull(priorityQueue, "priorityQueue must not be null");
    }

    void updateSerializer(TypeSerializer<T> serializer) {
        serializationContext.updateSerializer(serializer);
    }

    /** Releases child queues and closes the native priority-queue handle. */
    void close() {
        RuntimeException error = null;
        for (CobbleCachingPriorityQueueSet<T> queue : queuesByKeyGroup.values()) {
            try {
                queue.close();
            } catch (RuntimeException e) {
                if (error == null) {
                    error = e;
                } else {
                    error.addSuppressed(e);
                }
            }
        }
        try {
            priorityQueue.close();
        } catch (RuntimeException e) {
            if (error == null) {
                error = e;
            } else {
                error.addSuppressed(e);
            }
        }
        if (error != null) {
            throw error;
        }
    }

    @Override
    public Set<T> getSubsetForKeyGroup(int keyGroup) {
        if (!keyGroupRange.contains(keyGroup)) {
            return Collections.emptySet();
        }
        return new HashSet<>(queuesByKeyGroup.get(keyGroup).overlaySnapshotElements());
    }

    /** Exposes the fixed Cobble bucket selected for a Flink key group in focused tests. */
    int bucketForKeyGroup(int keyGroup) {
        Preconditions.checkArgument(
                keyGroupRange.contains(keyGroup),
                "Timer key-group %s is outside the local range %s.",
                keyGroup,
                keyGroupRange);
        return queuesByKeyGroup.get(keyGroup).bucket();
    }
}
