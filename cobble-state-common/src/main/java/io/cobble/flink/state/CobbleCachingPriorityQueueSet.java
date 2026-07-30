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
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * One Cobble-backed timer queue for one Flink key group.
 *
 * <p>The constructor binds {@code keyGroup} to {@code bucket} once. Every native operation uses
 * that immutable bucket, preserving the one-to-one key-group/bucket contract required by rescale.
 *
 * <p>The queue follows a two-tier model:
 *
 * <p>1. Native Cobble storage keeps the future tail for this key group.
 *
 * <p>2. {@code overlay} keeps one ordered hot prefix read with {@link
 * PriorityQueue#peekBatchDirect(int, int)}. Native storage remains the durable owner until the
 * overlay is drained or exported for a snapshot.
 *
 * <p>The fixed-size batch is deliberately used instead of the physical-boundary overload. A
 * physical batch may not be a contiguous key prefix across merged sources, so advancing to its
 * last key could hide timers that were not prefetched.
 */
final class CobbleCachingPriorityQueueSet<T>
        implements InternalPriorityQueue<T>, HeapPriorityQueueElement {

    private static final ByteBuffer EMPTY_DIRECT_VALUE = ByteBuffer.allocateDirect(0);
    private static final int PREFETCH_BATCH_SIZE = 1_024;

    private final Db db;
    private final PriorityQueue priorityQueue;
    private final CobbleTimerSerializationContext<T> serializationContext;
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

    CobbleCachingPriorityQueueSet(
            Db db,
            PriorityQueue priorityQueue,
            CobbleTimerSerializationContext<T> serializationContext,
            int keyGroup,
            boolean restoredNativeQueueMayContainEntries) {
        this.db = Preconditions.checkNotNull(db, "db must not be null");
        this.priorityQueue =
                Preconditions.checkNotNull(priorityQueue, "priorityQueue must not be null");
        this.serializationContext =
                Preconditions.checkNotNull(
                        serializationContext, "serializationContext must not be null");
        this.bucket = keyGroup;
        this.overlay =
                new TreeSet<>(
                        Comparator.comparing(
                                timer -> timer.serializedKey,
                                CobbleTimerSerializationContext::compareSerializedKeys));
        this.internalIndex = NOT_CONTAINED;
        this.cachedSize = restoredNativeQueueMayContainEntries ? -1 : 0;
        this.nativeExhausted = false;
    }

    int bucket() {
        return bucket;
    }

    List<T> overlayElements() {
        List<T> elements = new ArrayList<>(overlay.size());
        for (OverlayTimer<T> timer : overlay) {
            elements.add(materializeElement(timer));
        }
        return elements;
    }

    List<T> overlaySnapshotElements() {
        return overlayElements();
    }

    void close() {
        overlay.clear();
    }

    @Override
    public T poll() {
        ensureLoaded();
        OverlayTimer<T> head = overlay.pollFirst();
        if (head == null) {
            return null;
        }
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
        return overlay.isEmpty() ? null : materializeElement(overlay.first());
    }

    @Override
    public boolean add(T element) {
        Preconditions.checkNotNull(element, "Timer element must not be null.");
        initializeCursor();

        CobbleTimerSerializationContext.SerializedKey serializedKey =
                serializationContext.serializeElementKey(element);

        if (isOwnedByOverlay(serializedKey.heapBytes)) {
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

        // InternalPriorityQueue reports whether the head changed or remains unknown.
        boolean headMayHaveChanged = overlay.isEmpty();
        priorityQueue.offerDirect(
                bucket,
                serializedKey.directBuffer,
                serializedKey.directLength,
                EMPTY_DIRECT_VALUE.duplicate(),
                0);
        nativeExhausted = false;
        // offerDirect is an upsert. Avoid a read-before-write existence check on the hot add path;
        // size() can cold-scan later if an exact answer is needed.
        cachedSize = -1;
        return headMayHaveChanged;
    }

    @Override
    public boolean remove(T element) {
        Preconditions.checkNotNull(element, "Timer element must not be null.");
        // A timer may already be owned by a prefetched overlay even if this child queue has not
        // exposed it as the global head yet. Refill the hot prefix before deciding that a covered
        // key is gone; otherwise Flink session-window merges can fail to delete old cleanup timers.
        ensureLoaded();
        initializeCursor();

        OverlayTimer<T> headBeforeRemove = overlay.isEmpty() ? null : overlay.first();
        CobbleTimerSerializationContext.SerializedKey serializedKey =
                serializationContext.serializeElementKey(element);
        if (overlay.remove(new OverlayTimer<>(serializedKey.heapBytes))) {
            if (cachedSize >= 0) {
                cachedSize--;
            }
            if (overlay.isEmpty()) {
                advancePrefetchedBatch();
            }
            return headBeforeRemove != null
                    && CobbleTimerSerializationContext.compareSerializedKeys(
                                    serializedKey.heapBytes, headBeforeRemove.serializedKey)
                            == 0;
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
            cachedSize = overlay.size();
            try (ScanCursor scan = db.scan(bucket, null, null, priorityQueue.columnFamily())) {
                for (Row ignored : scan) {
                    cachedSize++;
                }
            }
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
        List<T> elements = overlayElements();
        try (ScanCursor scan = db.scan(bucket, null, null, priorityQueue.columnFamily())) {
            for (Row row : scan) {
                elements.add(serializationContext.deserializeElement(row.getKey()));
            }
        }
        return CloseableIterator.adapterForIterator(elements.iterator());
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

    private T materializeElement(OverlayTimer<T> timer) {
        if (timer.cachedElement == null) {
            timer.cachedElement =
                    serializationContext.deserializeElement(timer.serializedKey);
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
