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
 * <p>2. {@code overlay} keeps the current hot prefix that has already been removed from the native
 * queue with one {@link PriorityQueue#pollBatchDirect(int)} call.
 *
 * <p>Polling a native batch advances the Cobble truncation cursor immediately and then places the
 * deserialized timers into {@code overlay}. This matches the desired timer semantics for Flink:
 * prefetched timers disappear from native snapshots right away, while the overlay is exported
 * through Flink's legacy timer snapshot and therefore survives checkpoint/restore.
 */
final class CobbleCachingPriorityQueueSet<T>
        implements InternalPriorityQueue<T>, HeapPriorityQueueElement {

    private static final ByteBuffer EMPTY_DIRECT_VALUE = ByteBuffer.allocateDirect(0);

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
            elements.add(timer.element);
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
        if (cachedSize >= 0) {
            cachedSize--;
        }
        return head.element;
    }

    @Override
    public T peek() {
        ensureLoaded();
        return overlay.isEmpty() ? null : overlay.first().element;
    }

    @Override
    public boolean add(T element) {
        Preconditions.checkNotNull(element, "Timer element must not be null.");
        initializeCursor();

        CobbleTimerSerializationContext.SerializedKey serializedKey =
                serializationContext.serializeElementKey(element);

        if (isTruncated(serializedKey.heapBytes)) {
            // The native queue is monotonic behind its truncation cursor, so a key at or behind the
            // cursor would be hidden if written back to Cobble. The cursor is only the ownership
            // boundary: native storage owns keys after it, and the checkpointed overlay owns keys
            // at or before it. Native residual files may still contain rows already handed to the
            // overlay by pollBatchDirect(), and a later add for the same timer is a valid
            // re-registration.
            boolean added = overlay.add(new OverlayTimer<>(serializedKey.heapBytes, element));
            if (added && cachedSize >= 0) {
                cachedSize++;
            }
            return added;
        }

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
        return true;
    }

    @Override
    public boolean remove(T element) {
        Preconditions.checkNotNull(element, "Timer element must not be null.");
        // A timer may already have been prefetched out of Cobble by pollBatchDirect() even if this
        // child queue has not exposed it as the global head yet. Refill the hot prefix before
        // deciding that a cursor-covered key is gone; otherwise Flink session-window merges can
        // fail to delete old cleanup timers and later fire stale windows.
        ensureLoaded();
        initializeCursor();

        CobbleTimerSerializationContext.SerializedKey serializedKey =
                serializationContext.serializeElementKey(element);
        if (overlay.remove(new OverlayTimer<>(serializedKey.heapBytes, null))) {
            if (cachedSize >= 0) {
                cachedSize--;
            }
            return true;
        }

        if (isTruncated(serializedKey.heapBytes) || !existsInDb(serializedKey.heapBytes)) {
            return false;
        }
        priorityQueue.delete(bucket, serializedKey.heapBytes);
        if (cachedSize >= 0) {
            cachedSize--;
        }
        return true;
    }

    @Override
    public boolean isEmpty() {
        return peek() == null;
    }

    @Override
    public int size() {
        if (cachedSize < 0) {
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
     * Pulls one physical-boundary-sized batch out of Cobble and immediately hands ownership to the
     * overlay.
     *
     * <p>The direct entry key views are valid only while the batch is open, so the serialized key
     * bytes are copied before closing the batch. The timer element itself is kept exactly as the
     * Flink serializer returns it.
     */
    private void reload() {
        try (DirectPriorityQueueBatch prefetched = priorityQueue.pollBatchDirect(bucket)) {
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
                overlay.add(
                        new OverlayTimer<>(
                                heapKey, serializationContext.deserializeElement(serializedKey)));
                lastKey = heapKey;
            }

            cursor = lastKey;
            cursorInitialized = true;
            nativeExhausted = false;
        }
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

    private boolean existsInDb(byte[] serializedKey) {
        return db.get(bucket, serializedKey, priorityQueue.columnFamily()) != null;
    }

    /** One logical timer currently owned by the overlay. */
    private static final class OverlayTimer<T> {
        private final byte[] serializedKey;
        private final T element;

        private OverlayTimer(byte[] serializedKey, T element) {
            this.serializedKey = serializedKey;
            this.element = element;
        }
    }
}
