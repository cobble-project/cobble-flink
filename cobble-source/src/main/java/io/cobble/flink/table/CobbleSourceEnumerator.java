package io.cobble.flink.table;

import io.cobble.GlobalSnapshot;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Enumerator that assigns stable raw scan splits and refreshes them for newer snapshots. */
final class CobbleSourceEnumerator
        implements SplitEnumerator<CobbleSourceSplit, CobbleSourceEnumeratorState> {
    private final CobbleDynamicTableSource.SerializableConfig config;
    private final SplitEnumeratorContext<CobbleSourceSplit> context;
    /**
     * Splits that still need coordinator assignment or need to be re-pushed after a replacement.
     */
    private final Map<String, CobbleSourceSplit> pendingSplitsById = new LinkedHashMap<>();
    /**
     * Latest planned split payload for the current snapshot, keyed by the stable range triple id.
     */
    private final Map<String, CobbleSourceSplit> latestSplitsById = new HashMap<>();
    /** Current runtime owner of each split, rebuilt from reader events and not checkpointed. */
    private final Map<String, Integer> activeReaderBySplit = new HashMap<>();
    /**
     * Sticky assignment hint for keeping the same logical split on the same reader when possible.
     *
     * <p>This is runtime-only coordinator state. It is intentionally not checkpointed because the
     * split activity state lives in reader state; after recovery the readers re-advertise ownership
     * and rebuild the hint.
     */
    private final Map<String, Integer> preferredReaderBySplit = new LinkedHashMap<>();

    private volatile long currentSnapshotId;
    private volatile long nextSnapshotId;
    private boolean noMoreSplitsSignaled;

    CobbleSourceEnumerator(
            CobbleDynamicTableSource.SerializableConfig config,
            SplitEnumeratorContext<CobbleSourceSplit> context,
            CobbleSourceEnumeratorState checkpoint)
            throws Exception {
        this.config = config;
        this.context = context;
        if (checkpoint != null) {
            this.currentSnapshotId = checkpoint.currentSnapshotId;
            this.nextSnapshotId = checkpoint.nextSnapshotId;
            for (CobbleSourceSplit split : checkpoint.pendingSplits) {
                this.pendingSplitsById.put(split.splitId(), split);
            }
            if (checkpoint.currentSnapshotId > 0L) {
                GlobalSnapshot snapshot =
                        CobbleSourceRuntime.loadSnapshotById(config, checkpoint.currentSnapshotId);
                for (CobbleSourceSplit split :
                        CobbleSourceRuntime.createSourceSplits(config, snapshot)) {
                    this.latestSplitsById.put(split.splitId(), split);
                }
            }
            if (this.nextSnapshotId == 0L && this.currentSnapshotId > 0L) {
                this.nextSnapshotId = this.currentSnapshotId + 1L;
            }
        }
    }

    @Override
    public void start() {
        try {
            if (!config.isStreamingLatest()) {
                if (currentSnapshotId == 0L) {
                    initializeInitialSnapshot();
                }
                assignAvailableSplits();
                signalNoMoreSplitsIfBounded();
                return;
            }

            if (currentSnapshotId == 0L) {
                GlobalSnapshot initial = CobbleSourceRuntime.loadConfiguredSnapshot(config);
                if (initial != null) {
                    installLatestSnapshot(initial);
                }
            }
            assignAvailableSplits();
            startLatestSnapshotPolling();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Cobble source enumerator.", e);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        assignAvailableSplits();
        signalNoMoreSplitsIfBounded();
    }

    @Override
    public void addSplitsBack(List<CobbleSourceSplit> splits, int subtaskId) {
        for (CobbleSourceSplit split : splits) {
            String splitId = split.splitId();
            activeReaderBySplit.remove(splitId);
            preferredReaderBySplit.put(splitId, subtaskId);
            CobbleSourceSplit latest = latestSplitsById.get(splitId);
            CobbleSourceSplit restored =
                    latest != null && latest.snapshotId > split.snapshotId ? latest : split;
            pendingSplitsById.put(restored.splitId(), restored);
        }
        assignAvailableSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        assignAvailableSplits();
        signalNoMoreSplitsIfBounded();
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (!(sourceEvent instanceof CobbleSourceEvents.OwnedSplitsEvent)) {
            return;
        }
        List<String> currentlyOwnedByReader = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : activeReaderBySplit.entrySet()) {
            if (entry.getValue().intValue() == subtaskId) {
                currentlyOwnedByReader.add(entry.getKey());
            }
        }
        for (String splitId : currentlyOwnedByReader) {
            activeReaderBySplit.remove(splitId);
        }
        CobbleSourceEvents.OwnedSplitsEvent event =
                (CobbleSourceEvents.OwnedSplitsEvent) sourceEvent;
        for (String splitId : event.splitIds) {
            activeReaderBySplit.put(splitId, subtaskId);
            preferredReaderBySplit.put(splitId, subtaskId);
            CobbleSourceSplit pending = pendingSplitsById.remove(splitId);
            if (pending != null) {
                context.sendEventToSourceReader(
                        subtaskId, new CobbleSourceEvents.ReplaceSplitEvent(pending));
            }
        }
        assignAvailableSplits();
    }

    @Override
    public CobbleSourceEnumeratorState snapshotState(long checkpointId) {
        return new CobbleSourceEnumeratorState(
                currentSnapshotId, nextSnapshotId, pendingSplitsById.values());
    }

    @Override
    public void close() throws IOException {}

    private void initializeInitialSnapshot() throws Exception {
        GlobalSnapshot initial = CobbleSourceRuntime.loadConfiguredSnapshot(config);
        if (initial == null) {
            throw new IOException(
                    "Cobble source could not resolve checkpoint "
                            + config.scanCheckpointId
                            + " for path "
                            + config.pathUri
                            + ".");
        }
        currentSnapshotId = initial.id;
        for (CobbleSourceSplit split : CobbleSourceRuntime.createSourceSplits(config, initial)) {
            pendingSplitsById.put(split.splitId(), split);
            latestSplitsById.put(split.splitId(), split);
        }
        nextSnapshotId = initial.id + 1L;
    }

    private void installLatestSnapshot(GlobalSnapshot snapshot) throws IOException {
        currentSnapshotId = snapshot.id;
        nextSnapshotId = snapshot.id + 1L;
        List<CobbleSourceSplit> latestSplits =
                CobbleSourceRuntime.createSourceSplits(config, snapshot);
        latestSplitsById.clear();
        for (CobbleSourceSplit split : latestSplits) {
            String splitId = split.splitId();
            latestSplitsById.put(splitId, split);
            Integer activeReader = activeReaderBySplit.get(splitId);
            if (activeReader != null && context.registeredReaders().containsKey(activeReader)) {
                context.sendEventToSourceReader(
                        activeReader.intValue(), new CobbleSourceEvents.ReplaceSplitEvent(split));
                continue;
            }
            pendingSplitsById.put(splitId, split);
        }
    }

    private void assignAvailableSplits() {
        if (pendingSplitsById.isEmpty() || context.registeredReaders().isEmpty()) {
            return;
        }

        Map<Integer, List<CobbleSourceSplit>> assignment = new HashMap<>();
        Map<Integer, Integer> loadByReader = new HashMap<>();
        List<Integer> readers = new ArrayList<>(context.registeredReaders().keySet());
        Collections.sort(readers);

        for (Integer readerId : readers) {
            loadByReader.put(readerId, countActiveSplits(readerId));
        }

        List<CobbleSourceSplit> pending = new ArrayList<>(pendingSplitsById.values());
        Collections.sort(
                pending,
                Comparator.comparingInt((CobbleSourceSplit split) -> split.rangeStartBucket)
                        .thenComparingInt(split -> split.rangeEndBucket)
                        .thenComparingInt(split -> split.totalBuckets));

        for (CobbleSourceSplit split : pending) {
            String splitId = split.splitId();
            Integer readerId = selectReaderForSplit(splitId, loadByReader);
            assignment.computeIfAbsent(readerId, ignored -> new ArrayList<>()).add(split);
            pendingSplitsById.remove(splitId);
            activeReaderBySplit.put(splitId, readerId);
            preferredReaderBySplit.put(splitId, readerId);
            loadByReader.put(readerId, loadByReader.get(readerId) + 1);
        }

        if (!assignment.isEmpty()) {
            context.assignSplits(new SplitsAssignment<>(assignment));
        }
    }

    private void signalNoMoreSplitsIfBounded() {
        if (config.isStreamingLatest() || noMoreSplitsSignaled || !pendingSplitsById.isEmpty()) {
            return;
        }
        for (Integer readerId : context.registeredReaders().keySet()) {
            context.signalNoMoreSplits(readerId.intValue());
        }
        noMoreSplitsSignaled = true;
    }

    private int countActiveSplits(int readerId) {
        int count = 0;
        for (Integer owner : activeReaderBySplit.values()) {
            if (owner.intValue() == readerId) {
                count++;
            }
        }
        return count;
    }

    private Integer selectReaderForSplit(String splitId, Map<Integer, Integer> loadByReader) {
        Integer preferredReader = preferredReaderBySplit.get(splitId);
        if (preferredReader != null && loadByReader.containsKey(preferredReader)) {
            return preferredReader;
        }
        return selectLeastLoadedReader(loadByReader);
    }

    private Integer selectLeastLoadedReader(Map<Integer, Integer> loadByReader) {
        Integer selected = null;
        int bestLoad = Integer.MAX_VALUE;
        for (Map.Entry<Integer, Integer> entry : loadByReader.entrySet()) {
            int load = entry.getValue().intValue();
            if (selected == null || load < bestLoad) {
                selected = entry.getKey();
                bestLoad = load;
            }
        }
        return selected;
    }

    private void startLatestSnapshotPolling() {
        context.callAsync(
                () -> CobbleSourceRuntime.loadLatestSnapshotIfNewer(config, currentSnapshotId),
                (snapshot, throwable) -> {
                    if (throwable != null) {
                        throw new RuntimeException(
                                "Failed to poll Cobble latest snapshot.", throwable);
                    }
                    if (snapshot == null) {
                        return;
                    }
                    try {
                        installLatestSnapshot(snapshot);
                        assignAvailableSplits();
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to refresh Cobble source splits.", e);
                    }
                },
                config.pollIntervalMillis,
                config.pollIntervalMillis);
    }
}
