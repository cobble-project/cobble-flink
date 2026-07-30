package io.cobble.flink.state;

import io.cobble.structured.Db;
import io.cobble.structured.DirectPriorityQueueBatch;
import io.cobble.structured.DirectPriorityQueueEntry;
import io.cobble.structured.PriorityQueue;
import io.cobble.structured.Row;
import io.cobble.structured.ScanCursor;

import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * One Cobble-backed timer queue for one Flink key group.
 *
 * <p>The constructor binds {@code keyGroup} to {@code bucket} once. Every native operation uses
 * that immutable bucket, preserving the one-to-one key-group/bucket contract required by rescale.
 *
 * <p>The queue follows a three-tier model:
 *
 * <p>1. Native Cobble storage keeps the future tail for this key group.
 *
 * <p>2. A timer-state-wide {@link CobbleTimerWriteBuffer} batches new tail writes across key
 * groups.
 *
 * <p>3. {@code overlay} keeps one ordered hot prefix read with {@link
 * PriorityQueue#peekBatchDirect(int, int)}. Native storage remains the durable owner until the
 * overlay is drained or exported for a snapshot.
 *
 * <p>The fixed-size batch is deliberately used instead of the physical-boundary overload. A
 * physical batch may not be a contiguous key prefix across merged sources, so advancing to its
 * last key could hide timers that were not prefetched.
 */
final class CobbleCachingPriorityQueueSet<T>
        implements InternalPriorityQueue<T>, HeapPriorityQueueElement {

    private static final int PREFETCH_BATCH_SIZE = 1_024;

    private final Db db;
    private final PriorityQueue priorityQueue;
    private final CobbleTimerSerializationContext<T> serializationContext;
    private final CobbleTimerWriteBuffer<T> writeBuffer;
    private final int bucket;
    private final NavigableSet<OverlayTimer<T>> overlay;

    private int internalIndex;
    // Fresh queues can maintain an exact size incrementally. A restored native queue may already
    // contain rows, so its size remains unknown until the first explicit size() call scans it.
    private int cachedSize;
    private boolean nativeExhausted;
    private boolean cursorInitialized;
    private byte[] cursor;
    private byte[] pendingAdvanceKey;
    private long observedNativeMutationVersion;

    CobbleCachingPriorityQueueSet(
            Db db,
            PriorityQueue priorityQueue,
            CobbleTimerSerializationContext<T> serializationContext,
            int keyGroup,
            boolean restoredNativeQueueMayContainEntries) {
        this(
                db,
                priorityQueue,
                serializationContext,
                new CobbleTimerWriteBuffer<>(priorityQueue),
                keyGroup,
                restoredNativeQueueMayContainEntries);
    }

    CobbleCachingPriorityQueueSet(
            Db db,
            PriorityQueue priorityQueue,
            CobbleTimerSerializationContext<T> serializationContext,
            CobbleTimerWriteBuffer<T> writeBuffer,
            int keyGroup,
            boolean restoredNativeQueueMayContainEntries) {
        this.db = Preconditions.checkNotNull(db, "db must not be null");
        this.priorityQueue =
                Preconditions.checkNotNull(priorityQueue, "priorityQueue must not be null");
        this.serializationContext =
                Preconditions.checkNotNull(
                        serializationContext, "serializationContext must not be null");
        this.writeBuffer = Preconditions.checkNotNull(writeBuffer, "writeBuffer must not be null");
        this.bucket = keyGroup;
        this.overlay =
                new TreeSet<>(
                        Comparator.comparing(
                                timer -> timer.serializedKey,
                                CobbleTimerSerializationContext::compareSerializedKeys));
        this.internalIndex = NOT_CONTAINED;
        this.cachedSize = restoredNativeQueueMayContainEntries ? -1 : 0;
        this.nativeExhausted = false;
        this.observedNativeMutationVersion = writeBuffer.nativeMutationVersion();
    }

    int bucket() {
        return bucket;
    }

    List<T> overlaySnapshotElements() {
        Map<byte[], T> elements = newSerializedKeyMap();
        for (OverlayTimer<T> timer : overlay) {
            elements.put(timer.serializedKey, materializeElement(timer));
        }
        for (CobbleTimerWriteBuffer.PendingTimer<T> timer : writeBuffer.entries(bucket)) {
            elements.putIfAbsent(timer.serializedKey, materializeElement(timer));
        }
        return new ArrayList<>(elements.values());
    }

    void close() {
        overlay.clear();
    }

    @Override
    public T poll() {
        ensureLoaded();
        OverlayTimer<T> overlayHead = overlay.isEmpty() ? null : overlay.first();
        CobbleTimerWriteBuffer.PendingTimer<T> pendingHead = writeBuffer.first(bucket);
        if (overlayHead == null && pendingHead == null) {
            return null;
        }
        if (pendingHead != null
                && (overlayHead == null
                        || compareSerializedKeys(
                                        pendingHead.serializedKey, overlayHead.serializedKey)
                                <= 0)) {
            byte[] serializedKey = pendingHead.serializedKey;
            T element = materializeElement(pendingHead);
            priorityQueue.delete(bucket, serializedKey);
            writeBuffer.remove(bucket, serializedKey);
            if (overlayHead != null
                    && compareSerializedKeys(serializedKey, overlayHead.serializedKey) == 0) {
                overlay.pollFirst();
                if (overlay.isEmpty()) {
                    advancePrefetchedBatch();
                }
            }
            if (cachedSize >= 0) {
                cachedSize--;
            }
            return element;
        }

        OverlayTimer<T> head = overlay.pollFirst();
        T element = materializeElement(head);
        if (cachedSize >= 0) {
            cachedSize--;
        }
        if (overlay.isEmpty()) {
            advancePrefetchedBatch();
        }
        return element;
    }

    @Override
    public T peek() {
        ensureLoaded();
        OverlayTimer<T> overlayHead = overlay.isEmpty() ? null : overlay.first();
        CobbleTimerWriteBuffer.PendingTimer<T> pendingHead = writeBuffer.first(bucket);
        if (pendingHead == null) {
            return overlayHead == null ? null : materializeElement(overlayHead);
        }
        if (overlayHead == null
                || compareSerializedKeys(pendingHead.serializedKey, overlayHead.serializedKey) <= 0) {
            return materializeElement(pendingHead);
        }
        return materializeElement(overlayHead);
    }

    @Override
    public boolean add(T element) {
        Preconditions.checkNotNull(element, "Timer element must not be null.");
        observeNativeMutations();
        initializeCursor();

        CobbleTimerSerializationContext.SerializedKey serializedKey =
                serializationContext.serializeElementKey(element);

        if (isOwnedByOverlay(serializedKey.heapBytes)) {
            if (writeBuffer.get(bucket, serializedKey.heapBytes) != null) {
                return false;
            }
            // The native queue is monotonic behind its truncation cursor. A prefetched batch is
            // likewise owned by the overlay until its pending cursor advance, so re-registering a
            // timer at or behind either boundary must remain in memory.
            // Serialized bytes are already detached from caller-owned timer objects.
            OverlayTimer<T> candidate = new OverlayTimer<>(serializedKey.heapBytes);
            boolean added = overlay.add(candidate);
            if (added && cachedSize >= 0) {
                cachedSize++;
            }
            return added && overlay.first() == candidate;
        }

        byte[] knownHead = knownHeadKey();
        // InternalPriorityQueue reports whether the head changed or remains unknown.
        boolean headMayHaveChanged =
                knownHead == null
                        || compareSerializedKeys(serializedKey.heapBytes, knownHead) < 0;
        boolean added = writeBuffer.add(bucket, serializedKey.heapBytes);
        if (added) {
            cachedSize = -1;
        }
        observeNativeMutations();
        return added && headMayHaveChanged;
    }

    @Override
    public boolean remove(T element) {
        Preconditions.checkNotNull(element, "Timer element must not be null.");
        // A timer may already be owned by a prefetched overlay even if this child queue has not
        // exposed it as the global head yet. Refill the hot prefix before deciding that a covered
        // key is gone; otherwise Flink session-window merges can fail to delete old cleanup timers.
        ensureLoaded();
        initializeCursor();

        byte[] headBeforeRemove = knownHeadKey();
        CobbleTimerSerializationContext.SerializedKey serializedKey =
                serializationContext.serializeElementKey(element);
        CobbleTimerWriteBuffer.PendingTimer<T> pending =
                writeBuffer.get(bucket, serializedKey.heapBytes);
        if (pending != null) {
            // Delete conservatively before removing the buffered owner so an older native copy
            // cannot reappear after this remove.
            priorityQueue.delete(bucket, serializedKey.heapBytes);
            writeBuffer.remove(bucket, serializedKey.heapBytes);
            overlay.remove(new OverlayTimer<>(serializedKey.heapBytes));
            if (cachedSize >= 0) {
                cachedSize--;
            }
            if (overlay.isEmpty()) {
                advancePrefetchedBatch();
            }
            return headBeforeRemove != null
                    && compareSerializedKeys(serializedKey.heapBytes, headBeforeRemove) == 0;
        }

        if (overlay.remove(new OverlayTimer<>(serializedKey.heapBytes))) {
            if (cachedSize >= 0) {
                cachedSize--;
            }
            if (overlay.isEmpty()) {
                advancePrefetchedBatch();
            }
            return headBeforeRemove != null
                    && compareSerializedKeys(serializedKey.heapBytes, headBeforeRemove) == 0;
        }

        if (isOwnedByOverlay(serializedKey.heapBytes) || !existsInDb(serializedKey.heapBytes)) {
            return false;
        }
        priorityQueue.delete(bucket, serializedKey.heapBytes);
        if (cachedSize >= 0) {
            cachedSize--;
        }
        // A native tail deletion cannot change the known overlay head.
        return false;
    }

    @Override
    public boolean isEmpty() {
        return peek() == null;
    }

    @Override
    public int size() {
        if (cachedSize < 0) {
            advancePrefetchedBatch();
            NavigableSet<byte[]> serializedKeys = newSerializedKeySet();
            for (OverlayTimer<T> timer : overlay) {
                serializedKeys.add(timer.serializedKey);
            }
            for (CobbleTimerWriteBuffer.PendingTimer<T> timer : writeBuffer.entries(bucket)) {
                serializedKeys.add(timer.serializedKey);
            }
            try (ScanCursor scan = db.scan(bucket, null, null, priorityQueue.columnFamily())) {
                for (Row row : scan) {
                    serializedKeys.add(row.getKey());
                }
            }
            cachedSize = serializedKeys.size();
        }
        return cachedSize;
    }

    @Override
    public void addAll(Collection<? extends T> elements) {
        if (elements == null) {
            return;
        }
        for (T element : elements) {
            add(element);
        }
    }

    @Override
    public CloseableIterator<T> iterator() {
        advancePrefetchedBatch();
        Map<byte[], T> elements = newSerializedKeyMap();
        for (OverlayTimer<T> timer : overlay) {
            elements.put(timer.serializedKey, materializeElement(timer));
        }
        for (CobbleTimerWriteBuffer.PendingTimer<T> timer : writeBuffer.entries(bucket)) {
            elements.putIfAbsent(timer.serializedKey, materializeElement(timer));
        }
        try (ScanCursor scan = db.scan(bucket, null, null, priorityQueue.columnFamily())) {
            for (Row row : scan) {
                byte[] serializedKey = row.getKey();
                elements.putIfAbsent(
                        serializedKey, serializationContext.deserializeElement(serializedKey));
            }
        }
        return CloseableIterator.adapterForIterator(elements.values().iterator());
    }

    @Override
    public int getInternalIndex() {
        return internalIndex;
    }

    @Override
    public void setInternalIndex(int newIndex) {
        this.internalIndex = newIndex;
    }

    private void ensureLoaded() {
        observeNativeMutations();
        if (overlay.isEmpty() && !nativeExhausted) {
            reload();
        }
    }

    /**
     * Prefetches one ordered fixed-size batch and hands logical ownership to the overlay.
     *
     * <p>The direct entry key views are valid only while the batch is open, so the serialized key
     * bytes are copied before closing the batch. Timer elements are decoded lazily from those
     * immutable bytes.
     */
    private void reload() {
        try (DirectPriorityQueueBatch prefetched =
                priorityQueue.peekBatchDirect(bucket, PREFETCH_BATCH_SIZE)) {
            if (prefetched.isEmpty()) {
                nativeExhausted = true;
                initializeCursor();
                return;
            }

            byte[] lastKey = null;
            for (DirectPriorityQueueEntry entry : prefetched) {
                ByteBuffer serializedKey = entry.getKey();
                byte[] heapKey =
                        CobbleTimerSerializationContext.copyBytes(
                                serializedKey, ((Buffer) serializedKey).limit());
                overlay.add(new OverlayTimer<>(heapKey));
                lastKey = heapKey;
            }

            pendingAdvanceKey = lastKey;
            nativeExhausted = false;
        }
    }

    /** Moves the native cursor after the overlay has consumed, or snapshot has captured, its batch. */
    void advancePrefetchedBatch() {
        if (pendingAdvanceKey == null) {
            return;
        }
        priorityQueue.advance(bucket, pendingAdvanceKey);
        cursor = pendingAdvanceKey;
        cursorInitialized = true;
        pendingAdvanceKey = null;
    }

    private void initializeCursor() {
        if (!cursorInitialized) {
            cursor = priorityQueue.cursor(bucket);
            cursorInitialized = true;
        }
    }

    private boolean isTruncated(byte[] serializedKey) {
        return cursor != null
                && CobbleTimerSerializationContext.compareSerializedKeys(serializedKey, cursor)
                        <= 0;
    }

    private boolean isOwnedByOverlay(byte[] serializedKey) {
        return isTruncated(serializedKey)
                || (pendingAdvanceKey != null
                        && CobbleTimerSerializationContext.compareSerializedKeys(
                                        serializedKey, pendingAdvanceKey)
                                <= 0);
    }

    private boolean existsInDb(byte[] serializedKey) {
        return db.get(bucket, serializedKey, priorityQueue.columnFamily()) != null;
    }

    private void observeNativeMutations() {
        long currentVersion = writeBuffer.nativeMutationVersion();
        if (currentVersion != observedNativeMutationVersion) {
            observedNativeMutationVersion = currentVersion;
            nativeExhausted = false;
            cachedSize = -1;
        }
    }

    private byte[] knownHeadKey() {
        OverlayTimer<T> overlayHead = overlay.isEmpty() ? null : overlay.first();
        CobbleTimerWriteBuffer.PendingTimer<T> pendingHead = writeBuffer.first(bucket);
        if (pendingHead == null) {
            return overlayHead == null ? null : overlayHead.serializedKey;
        }
        if (overlayHead == null
                || compareSerializedKeys(pendingHead.serializedKey, overlayHead.serializedKey) <= 0) {
            return pendingHead.serializedKey;
        }
        return overlayHead.serializedKey;
    }

    private Map<byte[], T> newSerializedKeyMap() {
        return new TreeMap<>(CobbleCachingPriorityQueueSet::compareSerializedKeys);
    }

    private NavigableSet<byte[]> newSerializedKeySet() {
        return new TreeSet<>(CobbleCachingPriorityQueueSet::compareSerializedKeys);
    }

    private static int compareSerializedKeys(byte[] left, byte[] right) {
        return CobbleTimerSerializationContext.compareSerializedKeys(left, right);
    }

    private T materializeElement(OverlayTimer<T> timer) {
        if (timer.cachedElement == null) {
            timer.cachedElement = serializationContext.deserializeElement(timer.serializedKey);
        }
        return timer.cachedElement;
    }

    private T materializeElement(CobbleTimerWriteBuffer.PendingTimer<T> timer) {
        if (timer.cachedElement == null) {
            timer.cachedElement = serializationContext.deserializeElement(timer.serializedKey);
        }
        return timer.cachedElement;
    }

    /** One logical timer currently owned by the overlay. */
    private static final class OverlayTimer<T> {
        // serializedKey is the ownership and ordering truth; cachedElement is only a lazy cache.
        private final byte[] serializedKey;
        private T cachedElement;

        private OverlayTimer(byte[] serializedKey) {
            this.serializedKey = serializedKey;
        }
    }
}
