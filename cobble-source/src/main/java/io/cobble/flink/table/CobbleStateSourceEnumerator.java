package io.cobble.flink.table;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Bounded enumerator that plans and assigns checkpoint key-group ranges. */
final class CobbleStateSourceEnumerator
        implements SplitEnumerator<CobbleStateSourceSplit, CobbleStateSourceEnumeratorState> {

    private final StateSourceConfig config;
    private final SplitEnumeratorContext<CobbleStateSourceSplit> context;
    private final Map<String, CobbleStateSourceSplit> pendingSplitsById = new LinkedHashMap<>();
    private final Map<String, Integer> preferredReaderBySplit = new LinkedHashMap<>();
    private final Set<Integer> noMoreSplitsSignaledReaders = new HashSet<>();
    private long checkpointId;
    private boolean started;

    CobbleStateSourceEnumerator(
            StateSourceConfig config,
            SplitEnumeratorContext<CobbleStateSourceSplit> context,
            CobbleStateSourceEnumeratorState checkpoint) {
        this.config = config;
        this.context = context;
        if (checkpoint != null) {
            this.checkpointId = checkpoint.checkpointId;
            for (CobbleStateSourceSplit split : checkpoint.pendingSplits) {
                this.pendingSplitsById.put(split.splitId(), split);
            }
        }
    }

    @Override
    public void start() {
        try {
            if (!started) {
                started = true;
                if (pendingSplitsById.isEmpty()) {
                    List<CobbleStateSourceSplit> splits =
                            CobbleStateSourceRuntime.createStateSourceSplits(config);
                    for (CobbleStateSourceSplit split : splits) {
                        pendingSplitsById.put(split.splitId(), split);
                    }
                    if (!splits.isEmpty()) {
                        checkpointId = splits.get(0).checkpointId;
                    }
                }
            }
            assignAvailableSplits();
            signalNoMoreSplitsIfDone();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Cobble state source enumerator.", e);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        assignAvailableSplits();
        signalNoMoreSplitsIfDone();
    }

    @Override
    public void addSplitsBack(List<CobbleStateSourceSplit> splits, int subtaskId) {
        for (CobbleStateSourceSplit split : splits) {
            pendingSplitsById.put(split.splitId(), split);
            preferredReaderBySplit.put(split.splitId(), subtaskId);
        }
        assignAvailableSplits();
        signalNoMoreSplitsIfDone();
    }

    @Override
    public void addReader(int subtaskId) {
        assignAvailableSplits();
        signalNoMoreSplitsIfDone();
    }

    @Override
    public CobbleStateSourceEnumeratorState snapshotState(long checkpointId) {
        return new CobbleStateSourceEnumeratorState(this.checkpointId, pendingSplitsById.values());
    }

    @Override
    public void close() throws IOException {}

    private void assignAvailableSplits() {
        if (pendingSplitsById.isEmpty() || context.registeredReaders().isEmpty()) {
            return;
        }

        Map<Integer, List<CobbleStateSourceSplit>> assignment = new HashMap<>();
        Map<Integer, Integer> loadByReader = new HashMap<>();
        List<Integer> readers = new ArrayList<>(context.registeredReaders().keySet());
        Collections.sort(readers);
        for (Integer readerId : readers) {
            loadByReader.put(readerId, 0);
        }

        List<CobbleStateSourceSplit> pending = new ArrayList<>(pendingSplitsById.values());
        Collections.sort(
                pending,
                Comparator.comparingInt((CobbleStateSourceSplit split) -> split.keyGroupStart)
                        .thenComparingInt(split -> split.keyGroupEnd)
                        .thenComparingInt(split -> split.totalKeyGroups));

        for (CobbleStateSourceSplit split : pending) {
            Integer readerId = selectReaderForSplit(split.splitId(), loadByReader);
            assignment.computeIfAbsent(readerId, ignored -> new ArrayList<>()).add(split);
            pendingSplitsById.remove(split.splitId());
            preferredReaderBySplit.put(split.splitId(), readerId);
            loadByReader.put(readerId, loadByReader.get(readerId) + 1);
        }

        if (!assignment.isEmpty()) {
            context.assignSplits(new SplitsAssignment<>(assignment));
        }
    }

    private void signalNoMoreSplitsIfDone() {
        if (!pendingSplitsById.isEmpty()) {
            return;
        }
        Collection<Integer> readers = context.registeredReaders().keySet();
        for (Integer readerId : readers) {
            if (noMoreSplitsSignaledReaders.add(readerId)) {
                context.signalNoMoreSplits(readerId.intValue());
            }
        }
    }

    private Integer selectReaderForSplit(String splitId, Map<Integer, Integer> loadByReader) {
        Integer preferredReader = preferredReaderBySplit.get(splitId);
        if (preferredReader != null && loadByReader.containsKey(preferredReader)) {
            return preferredReader;
        }
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
