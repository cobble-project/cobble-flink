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
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Source reader that reopens each planned split from its snapshot metadata and scans it to
 * completion.
 *
 * <p>Checkpoint recovery does not resume from a logical key inside the split. Active splits are
 * reopened from the start of their planned range.
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

    /** Runtime holder for one assigned split and its reopened scan cursor. */
    private final class SourceSplitState {
        private final String splitId;
        private final int rangeStartBucket;
        private final int rangeEndBucket;
        private final int totalBuckets;
        private final boolean streamingOwned;
        private long snapshotId;
        private ScanSplit scanSplit;
        private CobbleSourceSplit.ScanState scanState;
        private boolean enqueued;
        private int resolvedTotalBuckets;
        private ScanCursor cursor;
        private Set<ByteBuffer> seenKeys;

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
            this.scanState = split.scanState;
            if (config.hasConfiguredBucketCount()) {
                this.resolvedTotalBuckets = config.bucketCount;
            }
            closeRuntime();
        }

        private void applyReplacement(CobbleSourceSplit replacement) {
            this.snapshotId = replacement.snapshotId;
            this.scanSplit = null;
            this.scanState = CobbleSourceSplit.ScanState.ACTIVE;
            closeRuntime();
        }

        private boolean hasWork() {
            return scanState == CobbleSourceSplit.ScanState.ACTIVE;
        }

        private ScanCursor.Entry nextEntry() throws Exception {
            while (hasWork()) {
                ensureCursor();
                ScanCursor.Entry entry = cursor.nextEntry();
                if (entry != null) {
                    // Raw split scans can surface duplicate physical entries for the same logical
                    // key.
                    if (!markSeen(entry.key)) {
                        continue;
                    }
                    return entry;
                }
                scanState = CobbleSourceSplit.ScanState.IDLE;
            }
            closeRuntime();
            return null;
        }

        private void ensureCursor() throws Exception {
            if (cursor != null) {
                return;
            }
            seenKeys = new HashSet<>();
            if (scanSplit == null) {
                scanSplit = CobbleSourceRuntime.resolveSourceSplit(config, toSplit());
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

        private boolean markSeen(byte[] key) {
            return seenKeys.add(ByteBuffer.wrap(copy(key)));
        }

        private CobbleSourceSplit toSplit() {
            return new CobbleSourceSplit(
                    rangeStartBucket, rangeEndBucket, totalBuckets, snapshotId, scanState);
        }

        private void closeRuntime() {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
            seenKeys = null;
        }

        private byte[] copy(byte[] bytes) {
            return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
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
