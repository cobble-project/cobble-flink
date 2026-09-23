package io.cobble.flink.table;

import io.cobble.flink.common.CobbleConnectorMetrics;
import io.cobble.table.TableReadCursor;
import io.cobble.table.TableReadEntry;
import io.cobble.table.TableReadPosition;
import io.cobble.table.TableReadRange;
import io.cobble.table.TableReadSession;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Bounded reader for checkpoint-pinned Cobble state source key-group splits. */
final class CobbleStateSourceReader implements SourceReader<RowData, CobbleStateSourceSplit> {
    private final StateSourceConfig config;
    private final SourceReaderContext context;
    private final Map<String, SourceSplitState> ownedStatesBySplit = new HashMap<>();
    private final ArrayDeque<SourceSplitState> runnableStates = new ArrayDeque<>();
    private final CobbleConnectorMetrics.SourceMetrics metrics;
    private CompletableFuture<Void> availability = new CompletableFuture<>();
    private CobbleStateTableReadProvider provider;
    private TableReadSession<RowData, RowData> session;
    private long sessionCheckpointId = -1L;
    private SourceSplitState currentState;
    private boolean noMoreSplits;
    private boolean closed;

    CobbleStateSourceReader(StateSourceConfig config, SourceReaderContext context) {
        this.config = config;
        this.context = context;
        this.metrics = CobbleConnectorMetrics.source(context.metricGroup());
    }

    @Override
    public void start() {
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        SourceSplitState state = moveToRunnableState();
        if (state == null) {
            resetAvailabilityIfIdle();
            return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
        }
        TableReadEntry<RowData> entry;
        try {
            entry = state.nextRow();
        } catch (Exception error) {
            metrics.error();
            throw error;
        }
        if (entry != null) {
            if (entry.countsPhysicalEntry()) metrics.nativeEntry(entry.physicalBytes());
            try {
                output.collect(entry.value());
                metrics.emittedRow();
                state.advance(entry.position());
            } catch (Exception error) {
                metrics.error();
                throw error;
            }
            signalAvailableIfNeeded();
            return hasMoreWork() ? InputStatus.MORE_AVAILABLE : InputStatus.NOTHING_AVAILABLE;
        }

        removeOwnedState(state.splitId);
        currentState = null;
        if (hasMoreWork()) return InputStatus.MORE_AVAILABLE;
        return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<CobbleStateSourceSplit> snapshotState(long checkpointId) {
        List<CobbleStateSourceSplit> splits =
                new ArrayList<CobbleStateSourceSplit>(ownedStatesBySplit.size());
        for (SourceSplitState state : ownedStatesBySplit.values()) splits.add(state.toSplit());
        return splits;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return hasMoreWork() ? CompletableFuture.completedFuture(null) : availability;
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
    public void close() {
        closed = true;
        for (SourceSplitState state : ownedStatesBySplit.values()) state.closeRuntime();
        closeSession();
    }

    private TableReadSession<RowData, RowData> sessionFor(long checkpointId) throws Exception {
        if (session != null && sessionCheckpointId == checkpointId) return session;
        if (session != null) {
            throw new IllegalStateException(
                    "state source reader received splits from multiple checkpoints");
        }
        provider = CobbleStateTableReadProvider.forScan(config, checkpointId);
        session = provider.open();
        sessionCheckpointId = checkpointId;
        return session;
    }

    private void closeSession() {
        if (session != null) {
            session.close();
            session = null;
        }
        if (provider != null) {
            provider.close();
            provider = null;
        }
    }

    private SourceSplitState moveToRunnableState() {
        if (currentState != null && currentState.hasWork()) return currentState;
        currentState = null;
        while (!runnableStates.isEmpty()) {
            SourceSplitState next = runnableStates.removeFirst();
            next.enqueued = false;
            if (next.hasWork()) {
                currentState = next;
                return next;
            }
        }
        return null;
    }

    private boolean hasMoreWork() {
        if (currentState != null && currentState.hasWork()) return true;
        for (SourceSplitState state : runnableStates) if (state.hasWork()) return true;
        return false;
    }

    private void enqueueIfRunnable(SourceSplitState state) {
        if (!state.hasWork() || state.enqueued || state == currentState) return;
        runnableStates.addLast(state);
        state.enqueued = true;
    }

    private void removeOwnedState(String splitId) {
        SourceSplitState removed = ownedStatesBySplit.remove(splitId);
        if (removed != null) {
            runnableStates.remove(removed);
            removed.closeRuntime();
        }
    }

    private void signalAvailable() {
        if (!availability.isDone()) availability.complete(null);
    }

    private void signalAvailableIfNeeded() {
        if (hasMoreWork()) signalAvailable();
        else resetAvailabilityIfIdle();
    }

    private void resetAvailabilityIfIdle() {
        if (!closed && !noMoreSplits && !hasMoreWork() && availability.isDone()) {
            availability = new CompletableFuture<Void>();
        }
    }

    private final class SourceSplitState {
        private final String splitId;
        private final long checkpointId;
        private final int totalKeyGroups;
        private final int keyGroupStart;
        private final int keyGroupEnd;
        private TableReadPosition position;
        private TableReadCursor<RowData> cursor;
        private boolean enqueued;
        private boolean finished;

        private SourceSplitState(CobbleStateSourceSplit split) {
            splitId = split.splitId();
            checkpointId = split.checkpointId;
            totalKeyGroups = split.totalKeyGroups;
            keyGroupStart = split.keyGroupStart;
            keyGroupEnd = split.keyGroupEnd;
            restoreFromSplit(split);
        }

        private void restoreFromSplit(CobbleStateSourceSplit split) {
            closeRuntime();
            if (split.resumePhysicalKey != null) {
                position =
                        new TableReadPosition(
                                split.resumeBucket,
                                split.resumePhysicalKey,
                                split.resumeIntraEntryOffset);
            } else {
                position = null;
            }
            finished = false;
        }

        private boolean hasWork() {
            return !finished;
        }

        private TableReadEntry<RowData> nextRow() throws Exception {
            if (cursor == null) {
                cursor =
                        sessionFor(checkpointId)
                                .scan(new TableReadRange(keyGroupStart, keyGroupEnd), position);
            }
            TableReadEntry<RowData> entry = cursor.next();
            if (entry == null) {
                closeRuntime();
                finished = true;
            }
            return entry;
        }

        private void advance(TableReadPosition next) {
            position = next;
        }

        private CobbleStateSourceSplit toSplit() {
            if (position == null) {
                return new CobbleStateSourceSplit(
                        splitId,
                        checkpointId,
                        totalKeyGroups,
                        keyGroupStart,
                        keyGroupEnd,
                        config.operatorId(),
                        config.stateName(),
                        config.stateKind(),
                        -1,
                        null,
                        0);
            }
            return new CobbleStateSourceSplit(
                    splitId,
                    checkpointId,
                    totalKeyGroups,
                    keyGroupStart,
                    keyGroupEnd,
                    config.operatorId(),
                    config.stateName(),
                    config.stateKind(),
                    position.bucket(),
                    position.physicalKey(),
                    position.intraEntryOffset());
        }

        private void closeRuntime() {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
        }
    }
}
