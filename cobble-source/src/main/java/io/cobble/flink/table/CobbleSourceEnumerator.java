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

/** Enumerator that assigns stable per-bucket splits and refreshes them for newer snapshots. */
final class CobbleSourceEnumerator
        implements SplitEnumerator<CobbleSourceSplit, CobbleSourceEnumeratorState> {

    private final CobbleDynamicTableSource.SerializableConfig config;
    private final SplitEnumeratorContext<CobbleSourceSplit> context;
    private final Map<Integer, CobbleSourceSplit> pendingSplitsById = new LinkedHashMap<>();
    private final Map<Integer, Integer> splitOwnerByReader = new HashMap<>();
    private final Map<Integer, CobbleSourceSplit> latestSplitsById = new HashMap<>();
    private long currentSnapshotId;
    private long nextSnapshotId;
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
                this.pendingSplitsById.put(Integer.valueOf(split.splitId), split);
            }
            if (checkpoint.currentSnapshotId > 0L) {
                GlobalSnapshot snapshot =
                        CobbleSourceRuntime.loadSnapshotById(config, checkpoint.currentSnapshotId);
                for (CobbleSourceSplit split :
                        CobbleSourceRuntime.createBucketSplits(config, snapshot)) {
                    this.latestSplitsById.put(Integer.valueOf(split.splitId), split);
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
            context.callAsync(
                    new java.util.concurrent.Callable<GlobalSnapshot>() {
                        @Override
                        public GlobalSnapshot call() throws Exception {
                            return CobbleSourceRuntime.loadLatestSnapshotIfNewer(
                                    config, nextSnapshotId > 0L ? nextSnapshotId - 1L : 0L);
                        }
                    },
                    new java.util.function.BiConsumer<GlobalSnapshot, Throwable>() {
                        @Override
                        public void accept(GlobalSnapshot snapshot, Throwable throwable) {
                            if (throwable != null) {
                                throw new RuntimeException(
                                        "Failed to poll Cobble latest snapshot.", throwable);
                            }
                            if (snapshot != null) {
                                try {
                                    installLatestSnapshot(snapshot);
                                    assignAvailableSplits();
                                } catch (IOException e) {
                                    throw new RuntimeException(
                                            "Failed to refresh Cobble source splits.", e);
                                }
                            }
                        }
                    },
                    config.pollIntervalMillis,
                    config.pollIntervalMillis);
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
            splitOwnerByReader.remove(Integer.valueOf(split.splitId));
            CobbleSourceSplit latest = latestSplitsById.get(Integer.valueOf(split.splitId));
            CobbleSourceSplit restored =
                    latest != null && latest.snapshotId > split.snapshotId ? latest : split;
            pendingSplitsById.put(Integer.valueOf(restored.splitId), restored);
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
        CobbleSourceEvents.OwnedSplitsEvent event =
                (CobbleSourceEvents.OwnedSplitsEvent) sourceEvent;
        List<Integer> currentlyOwnedByReader = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : splitOwnerByReader.entrySet()) {
            if (entry.getValue().intValue() == subtaskId) {
                currentlyOwnedByReader.add(entry.getKey());
            }
        }
        for (Integer splitId : currentlyOwnedByReader) {
            splitOwnerByReader.remove(splitId);
        }
        for (int splitId : event.splitIds) {
            splitOwnerByReader.put(Integer.valueOf(splitId), Integer.valueOf(subtaskId));
            CobbleSourceSplit pending = pendingSplitsById.remove(Integer.valueOf(splitId));
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
        for (CobbleSourceSplit split : CobbleSourceRuntime.createBucketSplits(config, initial)) {
            pendingSplitsById.put(Integer.valueOf(split.splitId), split);
            latestSplitsById.put(Integer.valueOf(split.splitId), split);
        }
        nextSnapshotId = initial.id + 1L;
    }

    private void installLatestSnapshot(GlobalSnapshot snapshot) throws IOException {
        currentSnapshotId = snapshot.id;
        nextSnapshotId = snapshot.id + 1L;
        List<CobbleSourceSplit> latestSplits =
                CobbleSourceRuntime.createBucketSplits(config, snapshot);
        latestSplitsById.clear();
        for (CobbleSourceSplit split : latestSplits) {
            Integer splitId = Integer.valueOf(split.splitId);
            latestSplitsById.put(splitId, split);
            Integer owner = splitOwnerByReader.get(splitId);
            if (owner == null) {
                pendingSplitsById.put(splitId, split);
            } else {
                pendingSplitsById.remove(splitId);
                context.sendEventToSourceReader(
                        owner.intValue(), new CobbleSourceEvents.ReplaceSplitEvent(split));
            }
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
            loadByReader.put(readerId, Integer.valueOf(countOwnedSplits(readerId.intValue())));
        }

        List<CobbleSourceSplit> pending = new ArrayList<>(pendingSplitsById.values());
        Collections.sort(
                pending,
                new Comparator<CobbleSourceSplit>() {
                    @Override
                    public int compare(CobbleSourceSplit left, CobbleSourceSplit right) {
                        return Integer.compare(left.splitId, right.splitId);
                    }
                });

        for (CobbleSourceSplit split : pending) {
            Integer readerId = selectLeastLoadedReader(loadByReader);
            assignment.computeIfAbsent(readerId, ignored -> new ArrayList<>()).add(split);
            pendingSplitsById.remove(Integer.valueOf(split.splitId));
            splitOwnerByReader.put(Integer.valueOf(split.splitId), readerId);
            loadByReader.put(readerId, Integer.valueOf(loadByReader.get(readerId).intValue() + 1));
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

    private int countOwnedSplits(int readerId) {
        int count = 0;
        for (Integer owner : splitOwnerByReader.values()) {
            if (owner.intValue() == readerId) {
                count++;
            }
        }
        return count;
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
}
