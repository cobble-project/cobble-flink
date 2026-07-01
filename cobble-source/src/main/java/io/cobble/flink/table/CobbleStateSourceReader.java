package io.cobble.flink.table;

import io.cobble.ScanCursor;
import io.cobble.ScanOptions;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Bounded reader for Cobble state source key-group splits. */
final class CobbleStateSourceReader implements SourceReader<RowData, CobbleStateSourceSplit> {

    private final StateSourceConfig config;
    private final SourceReaderContext context;
    private final Map<String, SourceSplitState> ownedStatesBySplit = new HashMap<>();
    private final ArrayDeque<SourceSplitState> runnableStates = new ArrayDeque<>();
    private final ArrayDeque<RowData> pendingRows = new ArrayDeque<>();
    private CompletableFuture<Void> availability = new CompletableFuture<>();
    private CobbleStateSourceRuntime.ReaderHandle readerHandle;
    private CobbleStateSourceRuntime.RuntimeSchema runtimeSchema;
    private CobbleStateRowDecoder rowDecoder;
    private String stateColumnFamily;
    private SourceSplitState currentState;
    private boolean noMoreSplits;
    private boolean closed;

    CobbleStateSourceReader(StateSourceConfig config, SourceReaderContext context) {
        this.config = config;
        this.context = context;
    }

    @Override
    public void start() {
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (!pendingRows.isEmpty()) {
            output.collect(pendingRows.removeFirst());
            // If this row came from a partially-consumed entry, advance the intra-entry offset so a
            // mid-entry checkpoint can resume without re-emitting this row.
            if (currentState != null && currentState.partialEntryKey != null) {
                currentState.partialEmittedCount++;
                if (pendingRows.isEmpty()) {
                    // The entry is now fully emitted; advance the scan boundary past it.
                    currentState.advanceStartBoundary(
                            currentState.partialKeyGroup, currentState.partialEntryKey);
                    currentState.clearPartialEntry();
                }
            }
            signalAvailableIfNeeded();
            return hasMoreWork() ? InputStatus.MORE_AVAILABLE : InputStatus.NOTHING_AVAILABLE;
        }

        SourceSplitState state = moveToRunnableState();
        if (state == null) {
            resetAvailabilityIfIdle();
            return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
        }

        // pendingRows is empty here — fetch and decode the next native entry.
        ScanCursor.Entry entry = state.nextEntry();
        if (entry == null) {
            removeOwnedState(state.splitId);
            currentState = null;
            if (hasMoreWork()) {
                return InputStatus.MORE_AVAILABLE;
            }
            return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
        }

        // On resume from a mid-entry checkpoint, the first entry re-read from the cursor should
        // match partialEntryKey; decode it with a skip for already-emitted rows.
        int skipRows = 0;
        if (state.partialEntryKey != null && Arrays.equals(entry.key, state.partialEntryKey)) {
            skipRows = state.partialEmittedCount;
            state.clearPartialEntry();
        }
        List<RowData> decoded =
                ensureRowDecoder()
                        .decode(entry.key, entry.columns, state.splitId, entry.bucket, skipRows);
        if (decoded.isEmpty()) {
            // No rows produced (e.g. an empty list entry or all rows skipped); advance and continue
            // on next poll.
            state.advanceStartBoundary(entry.bucket, entry.key);
            signalAvailableIfNeeded();
            return hasMoreWork() ? InputStatus.MORE_AVAILABLE : InputStatus.NOTHING_AVAILABLE;
        }

        pendingRows.addAll(decoded);
        state.beginPartialEntry(entry);
        // Emit the first row now; remaining rows stay in pendingRows for subsequent polls.
        output.collect(pendingRows.removeFirst());
        state.partialEmittedCount++;
        if (pendingRows.isEmpty()) {
            state.advanceStartBoundary(entry.bucket, entry.key);
            state.clearPartialEntry();
        }
        signalAvailableIfNeeded();
        return hasMoreWork() ? InputStatus.MORE_AVAILABLE : InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<CobbleStateSourceSplit> snapshotState(long checkpointId) {
        List<CobbleStateSourceSplit> splits = new ArrayList<>(ownedStatesBySplit.size());
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
    public void addSplits(List<CobbleStateSourceSplit> splits) {
        for (CobbleStateSourceSplit split : splits) {
            SourceSplitState existing = ownedStatesBySplit.get(split.splitId());
            if (existing == null) {
                SourceSplitState state = new SourceSplitState(split);
                ownedStatesBySplit.put(split.splitId(), state);
                enqueueIfRunnable(state);
            } else {
                existing.restoreFromSplit(split);
                enqueueIfRunnable(existing);
            }
        }
        signalAvailable();
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
        signalAvailable();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {}

    @Override
    public void close() throws Exception {
        closed = true;
        if (currentState != null) {
            currentState.closeRuntime();
        }
        for (SourceSplitState state : ownedStatesBySplit.values()) {
            state.closeRuntime();
        }
        if (readerHandle != null) {
            readerHandle.close();
            readerHandle = null;
        }
    }

    private CobbleStateRowDecoder ensureRowDecoder() throws Exception {
        if (rowDecoder == null) {
            rowDecoder = new CobbleStateRowDecoder(config, ensureRuntimeSchema());
        }
        return rowDecoder;
    }

    private CobbleStateSourceRuntime.RuntimeSchema ensureRuntimeSchema() throws Exception {
        if (runtimeSchema == null) {
            runtimeSchema = CobbleStateSourceRuntime.loadRuntimeSchema(config);
            stateColumnFamily = runtimeSchema.schema.columnFamily();
        }
        return runtimeSchema;
    }

    private CobbleStateSourceRuntime.ReaderHandle ensureReader(long checkpointId) throws Exception {
        if (readerHandle == null) {
            readerHandle = CobbleStateSourceRuntime.openReader(config, checkpointId);
        }
        return readerHandle;
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
        if (!pendingRows.isEmpty()) {
            return true;
        }
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

    /**
     * Returns {@code true} when the native reader rejects a column family that the checkpoint's
     * global snapshot advertises but a particular shard never registered (because that subtask
     * wrote no data for the state). Such key-groups are genuinely empty and must be skipped rather
     * than failing the scan.
     */
    static boolean isUnknownColumnFamily(RuntimeException e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }
        if (message.startsWith("IO error: ")) {
            message = message.substring("IO error: ".length());
        }
        return message.equals("Unknown column family")
                || message.startsWith("Unknown column family:")
                || message.startsWith("Unknown column family ")
                || message.startsWith("Unknown column family '");
    }

    /** Runtime holder for one assigned key-group split. */
    private final class SourceSplitState {
        private final String splitId;
        private final long checkpointId;
        private final int totalKeyGroups;
        private final int keyGroupStart;
        private final int keyGroupEnd;
        private final String operatorId;
        private final String stateName;
        private final String stateKind;
        private int currentKeyGroup;
        private int startKeyGroup;
        private byte[] startKeyExclusive;
        private int checkpointKeyGroup;
        private byte[] checkpointKeyExclusive;
        private ScanCursor cursor;
        private ScanCursor.Entry bufferedEntry;
        private ScanOptions scanOptions;
        private boolean enqueued;
        private boolean finished;
        // Intra-entry resume state: when a native entry decodes to multiple rows (LIST), we track
        // the entry being consumed so a mid-entry checkpoint can resume correctly. The scan
        // boundary (checkpointKeyExclusive) only advances once the entry's rows are fully emitted.
        private int partialKeyGroup;
        private byte[] partialEntryKey;
        private byte[][] partialEntryColumns;
        private int partialEmittedCount;

        private SourceSplitState(CobbleStateSourceSplit split) {
            this.splitId = split.splitId();
            this.checkpointId = split.checkpointId;
            this.totalKeyGroups = split.totalKeyGroups;
            this.keyGroupStart = split.keyGroupStart;
            this.keyGroupEnd = split.keyGroupEnd;
            this.operatorId = split.operatorId;
            this.stateName = split.stateName;
            this.stateKind = split.stateKind;
            restoreFromSplit(split);
        }

        private void restoreFromSplit(CobbleStateSourceSplit split) {
            this.startKeyGroup = split.startKeyGroup;
            this.startKeyExclusive = copy(split.startKeyExclusive);
            this.checkpointKeyGroup = -1;
            this.checkpointKeyExclusive = null;
            this.currentKeyGroup =
                    split.startKeyGroup >= 0 ? split.startKeyGroup : split.keyGroupStart;
            this.finished = false;
            this.partialEntryKey = copy(split.partialEntryKey);
            this.partialEmittedCount = split.partialEmittedCount;
            this.partialEntryColumns = null;
            this.partialKeyGroup = -1;
            closeRuntime();
        }

        private boolean hasWork() {
            return !finished;
        }

        private ScanCursor.Entry nextEntry() throws Exception {
            while (!finished) {
                ensureCursor();
                ScanCursor.Entry entry;
                if (cursor == null) {
                    // ensureCursor left the cursor unset because the key-group's shard does not
                    // carry this column family (no data for the state). Treat as empty and advance.
                    entry = null;
                } else if (bufferedEntry != null) {
                    entry = bufferedEntry;
                    bufferedEntry = null;
                } else {
                    entry = cursor.nextEntry();
                }
                if (entry != null) {
                    return entry;
                }
                closeRuntime();
                if (currentKeyGroup >= keyGroupEnd) {
                    finished = true;
                    clearBoundary();
                    return null;
                }
                currentKeyGroup++;
                if (checkpointKeyGroup < currentKeyGroup) {
                    checkpointKeyGroup = -1;
                    checkpointKeyExclusive = null;
                }
                if (startKeyGroup < currentKeyGroup) {
                    startKeyGroup = -1;
                    startKeyExclusive = null;
                }
            }
            return null;
        }

        private void ensureCursor() throws Exception {
            if (cursor != null) {
                return;
            }
            byte[] start = CobbleStateSourceRuntime.emptyScanKey();
            if (currentBoundaryKeyGroup() == currentKeyGroup
                    && currentBoundaryKeyExclusive() != null) {
                start = currentBoundaryKeyExclusive();
            }
            scanOptions =
                    CobbleStateSourceRuntime.scanOptions(
                            ensureStateColumnFamily(), Integer.MAX_VALUE);
            try {
                cursor =
                        ensureReader(checkpointId)
                                .reader
                                .scanWithOptions(
                                        currentKeyGroup,
                                        start,
                                        CobbleStateSourceRuntime.maxScanKey(),
                                        scanOptions);
            } catch (RuntimeException e) {
                // A key-group's shard may not have registered the requested column family when it
                // never wrote any data for that state. The checkpoint's global snapshot still lists
                // the column family (it was registered by at least one subtask), so this is not an
                // error: the shard simply has no rows for this state in that key-group. Treat the
                // scan as empty and let nextEntry advance to the next key-group.
                if (isUnknownColumnFamily(e)) {
                    return;
                }
                throw e;
            }
            // Reader.scanWithOptions starts inclusively. Skip only when the first row is exactly
            // the
            // checkpointed/resumed boundary; otherwise preserve it for the next poll.
            if (currentBoundaryKeyGroup() == currentKeyGroup
                    && currentBoundaryKeyExclusive() != null) {
                ScanCursor.Entry first = cursor.nextEntry();
                if (first != null && !Arrays.equals(first.key, currentBoundaryKeyExclusive())) {
                    bufferedEntry = first;
                }
            }
        }

        private String ensureStateColumnFamily() throws Exception {
            ensureRuntimeSchema();
            return stateColumnFamily;
        }

        private void advanceStartBoundary(int keyGroup, byte[] key) {
            checkpointKeyGroup = keyGroup;
            checkpointKeyExclusive = key;
        }

        /** Records that {@code entry} is being consumed and its rows are in pendingRows. */
        private void beginPartialEntry(ScanCursor.Entry entry) {
            partialKeyGroup = entry.bucket;
            partialEntryKey = entry.key;
            partialEntryColumns = entry.columns;
            partialEmittedCount = 0;
        }

        /** Clears intra-entry state once an entry's rows are fully emitted. */
        private void clearPartialEntry() {
            partialKeyGroup = -1;
            partialEntryKey = null;
            partialEntryColumns = null;
            partialEmittedCount = 0;
        }

        private CobbleStateSourceSplit toSplit() {
            return new CobbleStateSourceSplit(
                    splitId,
                    checkpointId,
                    totalKeyGroups,
                    keyGroupStart,
                    keyGroupEnd,
                    operatorId,
                    stateName,
                    stateKind,
                    currentBoundaryKeyGroup(),
                    currentBoundaryKeyExclusive(),
                    partialEntryKey,
                    partialEmittedCount);
        }

        private int currentBoundaryKeyGroup() {
            return checkpointKeyExclusive != null ? checkpointKeyGroup : startKeyGroup;
        }

        private byte[] currentBoundaryKeyExclusive() {
            return checkpointKeyExclusive != null ? checkpointKeyExclusive : startKeyExclusive;
        }

        private void clearBoundary() {
            startKeyGroup = -1;
            startKeyExclusive = null;
            checkpointKeyGroup = -1;
            checkpointKeyExclusive = null;
        }

        private void closeRuntime() {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
            bufferedEntry = null;
            if (scanOptions != null) {
                scanOptions.close();
                scanOptions = null;
            }
        }

        private byte[] copy(byte[] bytes) {
            return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
        }
    }
}
