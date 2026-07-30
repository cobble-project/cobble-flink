package io.cobble.flink.state;

import io.cobble.structured.PriorityQueue;

import org.apache.flink.util.Preconditions;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Timer-state-wide pending native writes, partitioned by key-group bucket.
 *
 * <p>Serialized keys are detached by {@link CobbleTimerSerializationContext} before ownership is
 * transferred here. The global bound therefore covers one logical timer state rather than being
 * multiplied by its number of local key groups.
 */
final class CobbleTimerWriteBuffer<T> {

    static final int MAX_PENDING_WRITES = 1_024;

    private static final ByteBuffer EMPTY_DIRECT_VALUE = ByteBuffer.allocateDirect(0);

    private final int maxPendingWrites;
    private final PendingWriteSink sink;
    private final Map<Integer, NavigableSet<PendingTimer<T>>> pendingByBucket;

    private ByteBuffer directKeyBuffer;
    private int pendingWriteCount;
    private long nativeMutationVersion;

    CobbleTimerWriteBuffer(PriorityQueue priorityQueue) {
        this(MAX_PENDING_WRITES, null, priorityQueue);
    }

    CobbleTimerWriteBuffer(int maxPendingWrites, PendingWriteSink sink) {
        this(maxPendingWrites, Preconditions.checkNotNull(sink), null);
    }

    private CobbleTimerWriteBuffer(
            int maxPendingWrites, PendingWriteSink sink, PriorityQueue priorityQueue) {
        Preconditions.checkArgument(maxPendingWrites > 0, "maxPendingWrites must be positive");
        this.maxPendingWrites = maxPendingWrites;
        this.sink =
                sink != null
                        ? sink
                        : (bucket, serializedKey) ->
                                offerDirect(priorityQueue, bucket, serializedKey);
        this.pendingByBucket = new TreeMap<>();
        this.directKeyBuffer = ByteBuffer.allocateDirect(128);
    }

    boolean add(int bucket, byte[] serializedKey) {
        NavigableSet<PendingTimer<T>> pending =
                pendingByBucket.computeIfAbsent(bucket, ignored -> newPendingSet());
        boolean added = pending.add(new PendingTimer<>(serializedKey));
        if (!added) {
            return false;
        }
        pendingWriteCount++;
        if (pendingWriteCount >= maxPendingWrites) {
            flushPendingWrites();
        }
        return true;
    }

    PendingTimer<T> get(int bucket, byte[] serializedKey) {
        NavigableSet<PendingTimer<T>> pending = pendingByBucket.get(bucket);
        if (pending == null) {
            return null;
        }
        PendingTimer<T> candidate = pending.ceiling(new PendingTimer<>(serializedKey));
        return candidate != null
                        && CobbleTimerSerializationContext.compareSerializedKeys(
                                        candidate.serializedKey, serializedKey)
                                == 0
                ? candidate
                : null;
    }

    PendingTimer<T> first(int bucket) {
        NavigableSet<PendingTimer<T>> pending = pendingByBucket.get(bucket);
        return pending == null || pending.isEmpty() ? null : pending.first();
    }

    PendingTimer<T> remove(int bucket, byte[] serializedKey) {
        NavigableSet<PendingTimer<T>> pending = pendingByBucket.get(bucket);
        if (pending == null) {
            return null;
        }
        PendingTimer<T> candidate = pending.ceiling(new PendingTimer<>(serializedKey));
        if (candidate == null
                || CobbleTimerSerializationContext.compareSerializedKeys(
                                candidate.serializedKey, serializedKey)
                        != 0
                || !pending.remove(candidate)) {
            return null;
        }
        pendingWriteCount--;
        if (pending.isEmpty()) {
            pendingByBucket.remove(bucket);
        }
        return candidate;
    }

    List<PendingTimer<T>> entries(int bucket) {
        NavigableSet<PendingTimer<T>> pending = pendingByBucket.get(bucket);
        return pending == null
                ? Collections.emptyList()
                : new ArrayList<>(pending);
    }

    int size() {
        return pendingWriteCount;
    }

    long nativeMutationVersion() {
        return nativeMutationVersion;
    }

    void flushPendingWrites() {
        if (pendingWriteCount == 0) {
            return;
        }
        boolean attempted = false;
        try {
            for (Map.Entry<Integer, NavigableSet<PendingTimer<T>>> bucket :
                    pendingByBucket.entrySet()) {
                for (PendingTimer<T> timer : bucket.getValue()) {
                    attempted = true;
                    sink.offer(bucket.getKey(), timer.serializedKey);
                }
            }
        } finally {
            if (attempted) {
                nativeMutationVersion++;
            }
        }
        pendingByBucket.clear();
        pendingWriteCount = 0;
    }

    private void offerDirect(PriorityQueue priorityQueue, int bucket, byte[] serializedKey) {
        ensureDirectKeyCapacity(serializedKey.length);
        ((Buffer) directKeyBuffer).clear();
        directKeyBuffer.put(serializedKey);
        ((Buffer) directKeyBuffer).flip();
        priorityQueue.offerDirect(
                bucket,
                directKeyBuffer,
                serializedKey.length,
                EMPTY_DIRECT_VALUE.duplicate(),
                0);
    }

    private void ensureDirectKeyCapacity(int requiredCapacity) {
        if (directKeyBuffer.capacity() >= requiredCapacity) {
            return;
        }
        int newCapacity = directKeyBuffer.capacity();
        while (newCapacity < requiredCapacity) {
            newCapacity = Math.multiplyExact(newCapacity, 2);
        }
        directKeyBuffer = ByteBuffer.allocateDirect(newCapacity);
    }

    private static <T> NavigableSet<PendingTimer<T>> newPendingSet() {
        return new TreeSet<>(
                (left, right) ->
                        CobbleTimerSerializationContext.compareSerializedKeys(
                                left.serializedKey, right.serializedKey));
    }

    @FunctionalInterface
    interface PendingWriteSink {
        void offer(int bucket, byte[] serializedKey);
    }

    static final class PendingTimer<T> {
        final byte[] serializedKey;
        T cachedElement;

        private PendingTimer(byte[] serializedKey) {
            this.serializedKey = serializedKey;
        }
    }
}
