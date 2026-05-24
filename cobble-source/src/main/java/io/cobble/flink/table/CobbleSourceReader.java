package io.cobble.flink.table;

import io.cobble.ScanCursor;
import io.cobble.ScanOptions;
import io.cobble.ScanSplit;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Source reader that reopens each planned split from snapshot metadata and resumes from the last
 * emitted logical key when checkpoint state or snapshot replacement provides resume progress.
 */
final class CobbleSourceReader implements SourceReader<RowData, CobbleSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(CobbleSourceReader.class);

    private final CobbleDynamicTableSource.SerializableConfig config;
    private final SourceReaderContext context;
    private final Map<String, SourceSplitState> ownedStatesBySplit = new HashMap<>();
    private final ArrayDeque<SourceSplitState> runnableStates = new ArrayDeque<>();
    private final CobbleRowDataDecoders.RuntimeRowDecoder rowDecoder;
    private final int[] projectedColumnIndexes;
    private CompletableFuture<Void> availability = new CompletableFuture<>();
    private SourceSplitState currentState;
    private ScanOptions scanOptions;
    private boolean noMoreSplits;
    private boolean closed;

    CobbleSourceReader(
            CobbleDynamicTableSource.SerializableConfig config, SourceReaderContext context) {
        this.config = config;
        this.context = context;
        this.rowDecoder = new CobbleRowDataDecoders.RuntimeRowDecoder(config);
        this.projectedColumnIndexes = projectedColumnIndexes(config);
    }

    @Override
    public void start() {
        this.scanOptions = ScanOptions.forColumns(projectedColumnIndexes);
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        SourceSplitState state = moveToRunnableState();
        if (state == null) {
            resetAvailabilityIfIdle();
            return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
        }

        ScanCursor.Entry entry = state.nextEntry();
        if (entry != null) {
            output.collect(rowDecoder.decode(entry.key, entry.columns));
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
        for (SourceSplitState state : ownedStatesBySplit.values()) {
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
            SourceSplitState existing = ownedStatesBySplit.get(split.splitId());
            if (existing == null) {
                SourceSplitState state = new SourceSplitState(split, config.isStreamingLatest());
                ownedStatesBySplit.put(split.splitId(), state);
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
        LOG.info(
                "Reader {} received split replacement for split {} at snapshot {}.",
                context.getIndexOfSubtask(),
                event.split.splitId(),
                event.split.snapshotId);
        SourceSplitState state = ownedStatesBySplit.get(event.split.splitId());
        if (state == null) {
            LOG.warn(
                    "Reader {} has no owned state for replacement split {}.",
                    context.getIndexOfSubtask(),
                    event.split.splitId());
            return;
        }
        state.applyReplacement(event.split);
        enqueueIfRunnable(state);
        signalAvailable();
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (currentState != null) {
            currentState.closeRuntime();
        }
        for (SourceSplitState state : ownedStatesBySplit.values()) {
            state.closeRuntime();
        }
        if (scanOptions != null) {
            scanOptions.close();
            scanOptions = null;
        }
    }

    private SourceSplitState moveToRunnableState() {
        if (currentState != null && currentState.hasWork()) {
            return currentState;
        }
        currentState = null;
        while (!runnableStates.isEmpty()) {
            SourceSplitState next = runnableStates.pollFirst();
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
        for (SourceSplitState state : runnableStates) {
            if (state.hasWork()) {
                return true;
            }
        }
        return false;
    }

    private void enqueueIfRunnable(SourceSplitState state) {
        if (!state.hasWork() || state.enqueued || state == currentState) {
            return;
        }
        runnableStates.addLast(state);
        state.enqueued = true;
    }

    private void removeOwnedState(String splitId) {
        SourceSplitState removed = ownedStatesBySplit.remove(splitId);
        if (removed != null) {
            runnableStates.remove(removed);
            removed.enqueued = false;
            removed.closeRuntime();
        }
    }

    private void signalOwnedSplits() {
        if (!config.isStreamingLatest()) {
            return;
        }
        List<String> splitIds = new ArrayList<>(ownedStatesBySplit.keySet());
        Collections.sort(splitIds);
        String[] ids = new String[splitIds.size()];
        for (int i = 0; i < splitIds.size(); i++) {
            ids[i] = splitIds.get(i);
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

    /** Runtime holder for one assigned split plus its persisted resume position. */
    private final class SourceSplitState {
        private final String splitId;
        private final int rangeStartBucket;
        private final int rangeEndBucket;
        private final int totalBuckets;
        private final boolean streamingOwned;
        private long snapshotId;
        private ScanSplit scanSplit;
        private ScanSplit wrapSplit;
        private int startBucket;
        private byte[] startKeyExclusive;
        private CobbleSourceSplit.ScanState scanState;
        private boolean enqueued;
        private int resolvedTotalBuckets;
        private ScanCursor cursor;
        // ACTIVE checkpoint progress reuses the cursor-owned key bytes; Flink copies only when the
        // split snapshot is serialized.
        private int checkpointBucket;
        private byte[] checkpointKeyExclusive;
        private boolean restoredFromCheckpoint;

        private SourceSplitState(CobbleSourceSplit split, boolean streamingOwned) {
            this.splitId = split.splitId();
            this.rangeStartBucket = split.rangeStartBucket;
            this.rangeEndBucket = split.rangeEndBucket;
            this.totalBuckets = split.totalBuckets;
            this.streamingOwned = streamingOwned;
            this.resolvedTotalBuckets = split.totalBuckets;
            restoreFromSplit(split);
        }

        private boolean isStreamingOwned() {
            return streamingOwned;
        }

        private void restoreFromSplit(CobbleSourceSplit split) {
            this.snapshotId = split.snapshotId;
            this.scanSplit = null;
            this.wrapSplit = null;
            this.startBucket = split.startBucket;
            this.startKeyExclusive = copy(split.startKeyExclusive);
            this.scanState = split.scanState;
            this.checkpointBucket = -1;
            this.checkpointKeyExclusive = null;
            this.restoredFromCheckpoint = split.startBucket >= 0 && split.startKeyExclusive != null;
            if (config.hasConfiguredBucketCount()) {
                this.resolvedTotalBuckets = config.bucketCount;
            }
            if (this.scanState == CobbleSourceSplit.ScanState.IDLE) {
                clearStartBoundary();
            }
            closeRuntime();
        }

        private void applyReplacement(CobbleSourceSplit replacement) {
            this.snapshotId = replacement.snapshotId;
            this.scanSplit = null;
            this.wrapSplit = null;
            if (!hasStartBoundary()) {
                this.scanState = CobbleSourceSplit.ScanState.ACTIVE;
                clearStartBoundary();
            } else if (restoredFromCheckpoint) {
                this.scanState = CobbleSourceSplit.ScanState.WRAP;
            } else {
                this.scanState = CobbleSourceSplit.ScanState.ACTIVE;
            }
            closeRuntime();
        }

        private boolean hasWork() {
            return scanState != CobbleSourceSplit.ScanState.IDLE;
        }

        private ScanCursor.Entry nextEntry() throws Exception {
            while (hasWork()) {
                ensureCursor();
                ScanCursor.Entry entry = cursor.nextEntry();
                if (entry != null) {
                    if (scanState == CobbleSourceSplit.ScanState.ACTIVE) {
                        advanceStartBoundary(entry);
                    }
                    return entry;
                }
                if (scanState == CobbleSourceSplit.ScanState.WRAP && wrapSplit != null) {
                    closeRuntime();
                    scanSplit = wrapSplit;
                    wrapSplit = null;
                    continue;
                }
                clearStartBoundary();
                scanState = CobbleSourceSplit.ScanState.IDLE;
            }
            closeRuntime();
            return null;
        }

        private void ensureCursor() throws Exception {
            if (cursor != null) {
                return;
            }
            if (scanSplit == null) {
                ScanSplit resolved = CobbleSourceRuntime.resolveSourceSplit(config, toSplit());
                if (scanState == CobbleSourceSplit.ScanState.WRAP && hasStartBoundary()) {
                    ScanSplit.Partition partition =
                            resolved.splitAfter(
                                    currentBoundaryBucket(), currentBoundaryKeyExclusive());
                    scanSplit = partition.after;
                    wrapSplit = partition.before;
                } else {
                    scanSplit = resolved;
                    wrapSplit = null;
                    if (hasStartBoundary()) {
                        scanSplit =
                                scanSplit.splitAfter(
                                                currentBoundaryBucket(),
                                                currentBoundaryKeyExclusive())
                                        .after;
                    }
                }
            }
            cursor =
                    scanSplit.openScannerWithOptions(
                            CobbleSourceRuntime.createSourceScanConfig(
                                    config, resolveTotalBuckets()),
                            scanOptions);
        }

        private int resolveTotalBuckets() throws IOException {
            if (resolvedTotalBuckets > 0) {
                return resolvedTotalBuckets;
            }
            resolvedTotalBuckets = totalBuckets;
            return resolvedTotalBuckets;
        }

        private CobbleSourceSplit toSplit() {
            int boundaryBucket = currentBoundaryBucket();
            byte[] boundaryKeyExclusive = currentBoundaryKeyExclusive();
            return new CobbleSourceSplit(
                    rangeStartBucket,
                    rangeEndBucket,
                    totalBuckets,
                    snapshotId,
                    boundaryBucket,
                    boundaryKeyExclusive,
                    scanState);
        }

        private void closeRuntime() {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
        }

        private byte[] copy(byte[] bytes) {
            return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
        }

        private boolean hasStartBoundary() {
            return currentBoundaryBucket() >= 0 && currentBoundaryKeyExclusive() != null;
        }

        private void clearStartBoundary() {
            startBucket = -1;
            startKeyExclusive = null;
            checkpointBucket = -1;
            checkpointKeyExclusive = null;
            restoredFromCheckpoint = false;
        }

        private void advanceStartBoundary(ScanCursor.Entry entry) {
            checkpointBucket = entry.bucket;
            checkpointKeyExclusive = entry.key;
        }

        private int currentBoundaryBucket() {
            return checkpointKeyExclusive != null ? checkpointBucket : startBucket;
        }

        private byte[] currentBoundaryKeyExclusive() {
            return checkpointKeyExclusive != null ? checkpointKeyExclusive : startKeyExclusive;
        }
    }

    private static int[] projectedColumnIndexes(
            CobbleDynamicTableSource.SerializableConfig config) {
        int[] indexes = new int[config.valueFields.size()];
        for (int i = 0; i < config.valueFields.size(); i++) {
            indexes[i] = config.valueFields.get(i).structuredColumnIndex;
        }
        return indexes;
    }
}
