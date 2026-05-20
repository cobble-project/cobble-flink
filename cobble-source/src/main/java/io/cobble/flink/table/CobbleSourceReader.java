package io.cobble.flink.table;

import io.cobble.structured.Db;
import io.cobble.structured.Row;
import io.cobble.structured.ScanCursor;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Source reader that consumes stable bucket splits and applies snapshot replacements in place. */
final class CobbleSourceReader implements SourceReader<RowData, CobbleSourceSplit> {

    private final CobbleDynamicTableSource.SerializableConfig config;
    private final SourceReaderContext context;
    private final Map<Integer, BucketSplitState> ownedStatesBySplit = new HashMap<>();
    private final ArrayDeque<BucketSplitState> runnableStates = new ArrayDeque<>();
    private final CobbleRowDataDecoders.RuntimeRowDecoder rowDecoder;
    private CompletableFuture<Void> availability = new CompletableFuture<>();
    private Path workingDirectory;
    private BucketSplitState currentState;
    private boolean noMoreSplits;
    private boolean closed;

    CobbleSourceReader(
            CobbleDynamicTableSource.SerializableConfig config, SourceReaderContext context) {
        this.config = config;
        this.context = context;
        this.rowDecoder = new CobbleRowDataDecoders.RuntimeRowDecoder(config);
    }

    @Override
    public void start() {
        try {
            workingDirectory =
                    Files.createTempDirectory(
                            "cobble-source-reader-"
                                    + Integer.toString(context.getIndexOfSubtask())
                                    + "-");
        } catch (IOException e) {
            throw new RuntimeException("Failed to create Cobble source restore workspace.", e);
        }
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        BucketSplitState state = moveToRunnableState();
        if (state == null) {
            resetAvailabilityIfIdle();
            return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
        }

        Row row = state.nextRow();
        if (row != null) {
            output.collect(rowDecoder.decode(row));
            signalAvailableIfNeeded();
            return hasMoreWork() ? InputStatus.MORE_AVAILABLE : InputStatus.NOTHING_AVAILABLE;
        }

        if (state.isStreamingOwned()) {
            currentState = null;
            signalOwnedSplits();
            signalAvailableIfNeeded();
            return hasMoreWork() ? InputStatus.MORE_AVAILABLE : InputStatus.NOTHING_AVAILABLE;
        }

        removeOwnedState(state.splitId);
        currentState = null;
        signalOwnedSplits();
        if (hasMoreWork()) {
            return InputStatus.MORE_AVAILABLE;
        }
        return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<CobbleSourceSplit> snapshotState(long checkpointId) {
        List<CobbleSourceSplit> splits = new ArrayList<>(ownedStatesBySplit.size());
        for (BucketSplitState state : ownedStatesBySplit.values()) {
            splits.add(state.toSplit());
        }
        return splits;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        if (hasMoreWork()) {
            return CompletableFuture.completedFuture(null);
        }
        return availability;
    }

    @Override
    public void addSplits(List<CobbleSourceSplit> splits) {
        for (CobbleSourceSplit split : splits) {
            BucketSplitState existing = ownedStatesBySplit.get(Integer.valueOf(split.splitId));
            if (existing == null) {
                BucketSplitState state = new BucketSplitState(split, config.isStreamingLatest());
                ownedStatesBySplit.put(Integer.valueOf(split.splitId), state);
                enqueueIfRunnable(state);
            } else {
                existing.restoreFromSplit(split);
                enqueueIfRunnable(existing);
            }
        }
        signalOwnedSplits();
        signalAvailable();
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
        signalAvailable();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (!(sourceEvent instanceof CobbleSourceEvents.ReplaceSplitEvent)) {
            return;
        }
        CobbleSourceEvents.ReplaceSplitEvent event =
                (CobbleSourceEvents.ReplaceSplitEvent) sourceEvent;
        BucketSplitState state = ownedStatesBySplit.get(Integer.valueOf(event.split.splitId));
        if (state == null) {
            return;
        }
        state.applyReplacement(event.split);
        enqueueIfRunnable(state);
        signalOwnedSplits();
        signalAvailable();
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (currentState != null) {
            currentState.closeRuntime();
        }
        for (BucketSplitState state : ownedStatesBySplit.values()) {
            state.closeRuntime();
        }
        CobbleSourceRuntime.deleteRecursively(workingDirectory);
    }

    private BucketSplitState moveToRunnableState() {
        if (currentState != null && currentState.hasWork()) {
            return currentState;
        }
        currentState = null;
        while (!runnableStates.isEmpty()) {
            BucketSplitState next = runnableStates.pollFirst();
            next.enqueued = false;
            if (next.hasWork()) {
                currentState = next;
                return next;
            }
        }
        return null;
    }

    private boolean hasMoreWork() {
        if (currentState != null && currentState.hasWork()) {
            return true;
        }
        for (BucketSplitState state : runnableStates) {
            if (state.hasWork()) {
                return true;
            }
        }
        return false;
    }

    private void enqueueIfRunnable(BucketSplitState state) {
        if (!state.hasWork() || state.enqueued || state == currentState) {
            return;
        }
        runnableStates.addLast(state);
        state.enqueued = true;
    }

    private void removeOwnedState(int splitId) {
        BucketSplitState removed = ownedStatesBySplit.remove(Integer.valueOf(splitId));
        if (removed != null) {
            runnableStates.remove(removed);
            removed.enqueued = false;
            removed.closeRuntime();
        }
    }

    private void signalOwnedSplits() {
        List<Integer> splitIds = new ArrayList<>(ownedStatesBySplit.keySet());
        Collections.sort(splitIds);
        int[] ids = new int[splitIds.size()];
        for (int i = 0; i < splitIds.size(); i++) {
            ids[i] = splitIds.get(i).intValue();
        }
        context.sendSourceEventToCoordinator(new CobbleSourceEvents.OwnedSplitsEvent(ids));
    }

    private void signalAvailable() {
        if (!availability.isDone()) {
            availability.complete(null);
        }
    }

    private void signalAvailableIfNeeded() {
        if (hasMoreWork()) {
            signalAvailable();
        } else {
            resetAvailabilityIfIdle();
        }
    }

    private void resetAvailabilityIfIdle() {
        if (!closed && !noMoreSplits && !hasMoreWork() && availability.isDone()) {
            availability = new CompletableFuture<>();
        }
    }

    private final class BucketSplitState {
        private final int splitId;
        private final int bucketId;
        private final boolean streamingOwned;
        private long snapshotId;
        private String manifestPath;
        private byte[] lastConsumedKey;
        private byte[] anchorKey;
        private byte[] resumeKey;
        private CobbleSourceSplit.ScanPhase phase;
        private boolean enqueued;
        private int resolvedTotalBuckets;
        private Db db;
        private ScanCursor cursor;
        private Iterator<Row> iterator;
        private Path restoreDirectory;

        private BucketSplitState(CobbleSourceSplit split, boolean streamingOwned) {
            this.splitId = split.splitId;
            this.bucketId = split.bucketId;
            this.streamingOwned = streamingOwned;
            this.resolvedTotalBuckets = config.bucketCount;
            restoreFromSplit(split);
        }

        private boolean isStreamingOwned() {
            return streamingOwned;
        }

        private void restoreFromSplit(CobbleSourceSplit split) {
            this.snapshotId = split.snapshotId;
            this.manifestPath = split.manifestPath;
            this.lastConsumedKey = copy(split.lastConsumedKey);
            this.anchorKey = copy(split.anchorKey);
            this.resumeKey = copy(split.resumeKey);
            this.phase = split.phase;
            if (config.hasConfiguredBucketCount()) {
                this.resolvedTotalBuckets = config.bucketCount;
            }
            closeRuntime();
        }

        private void applyReplacement(CobbleSourceSplit replacement) {
            this.snapshotId = replacement.snapshotId;
            this.manifestPath = replacement.manifestPath;
            this.anchorKey = copy(lastConsumedKey);
            this.resumeKey = null;
            this.phase = CobbleSourceSplit.ScanPhase.FORWARD;
            closeRuntime();
        }

        private boolean hasWork() {
            return manifestPath != null && phase != CobbleSourceSplit.ScanPhase.IDLE;
        }

        private Row nextRow() throws Exception {
            while (hasWork()) {
                ensureCursor();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    if (shouldSkip(row.getKey())) {
                        continue;
                    }
                    byte[] key = copy(row.getKey());
                    this.lastConsumedKey = key;
                    this.resumeKey = key;
                    return row;
                }
                advancePhase();
            }
            closeRuntime();
            return null;
        }

        private void ensureCursor() throws Exception {
            if (iterator != null) {
                return;
            }
            if (restoreDirectory == null) {
                restoreDirectory =
                        workingDirectory.resolve(
                                "split-"
                                        + Integer.toString(splitId)
                                        + "-bucket-"
                                        + Integer.toString(bucketId));
            }
            db =
                    CobbleSourceRuntime.openSnapshotDb(
                            config, resolveTotalBuckets(), restoreDirectory, manifestPath);
            cursor = db.scan(bucketId, scanStartKey(), scanEndKey());
            iterator = cursor.iterator();
        }

        private int resolveTotalBuckets() throws IOException {
            if (resolvedTotalBuckets > 0) {
                return resolvedTotalBuckets;
            }
            resolvedTotalBuckets =
                    CobbleSourceRuntime.resolveSnapshotBucketCount(config, snapshotId);
            return resolvedTotalBuckets;
        }

        private byte[] scanStartKey() {
            if (phase == CobbleSourceSplit.ScanPhase.WRAPPED) {
                return CobbleSourceRuntime.MIN_KEY;
            }
            if (resumeKey != null) {
                return resumeKey;
            }
            if (anchorKey != null) {
                return anchorKey;
            }
            return CobbleSourceRuntime.MIN_KEY;
        }

        private byte[] scanEndKey() {
            if (phase == CobbleSourceSplit.ScanPhase.WRAPPED && anchorKey != null) {
                return anchorKey;
            }
            return CobbleSourceRuntime.MAX_KEY;
        }

        private boolean shouldSkip(byte[] key) {
            if (phase == CobbleSourceSplit.ScanPhase.FORWARD) {
                byte[] lowerBound = resumeKey != null ? resumeKey : anchorKey;
                if (lowerBound != null && compareKeys(key, lowerBound) <= 0) {
                    return true;
                }
                return false;
            }
            if (anchorKey != null && compareKeys(key, anchorKey) >= 0) {
                return true;
            }
            if (resumeKey != null && compareKeys(key, resumeKey) <= 0) {
                return true;
            }
            return false;
        }

        private void advancePhase() {
            closeCursorOnly();
            if (phase == CobbleSourceSplit.ScanPhase.FORWARD && anchorKey != null) {
                phase = CobbleSourceSplit.ScanPhase.WRAPPED;
                resumeKey = null;
                return;
            }
            phase = CobbleSourceSplit.ScanPhase.IDLE;
            anchorKey = null;
            resumeKey = null;
        }

        private CobbleSourceSplit toSplit() {
            return new CobbleSourceSplit(
                    splitId,
                    bucketId,
                    snapshotId,
                    manifestPath,
                    lastConsumedKey,
                    anchorKey,
                    resumeKey,
                    phase);
        }

        private void closeRuntime() {
            closeCursorOnly();
            if (db != null) {
                db.close();
                db = null;
            }
        }

        private void closeCursorOnly() {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
            iterator = null;
        }

        private byte[] copy(byte[] bytes) {
            return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
        }
    }

    private static int compareKeys(byte[] left, byte[] right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        int min = Math.min(left.length, right.length);
        for (int i = 0; i < min; i++) {
            int cmp = Integer.compare(left[i] & 0xFF, right[i] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(left.length, right.length);
    }
}
