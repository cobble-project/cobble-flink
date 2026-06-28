package io.cobble.flink.table;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/** Commits checkpointed committables and keeps only the latest shard per subtask. */
final class GlobalCommitOperator extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<CommittableMessage<CobbleShardCommittable>, Void>,
                BoundedOneInput,
                CheckpointListener {

    private static final long END_INPUT_CHECKPOINT_ID = Long.MAX_VALUE;

    private final CobbleDynamicTableSink.SerializableConfig config;
    private final NavigableMap<Long, List<CobbleShardCommittable>> pendingByCheckpoint =
            new TreeMap<>();
    private transient CobbleSqlSink.Global global;

    GlobalCommitOperator(
            StreamOperatorParameters<Void> parameters,
            CobbleDynamicTableSink.SerializableConfig config) {
        this.config = config;
        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.global = new CobbleSqlSink.Global(config);
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<CobbleShardCommittable>> element) {
        CommittableMessage<CobbleShardCommittable> message = element.getValue();
        if (!(message instanceof CommittableWithLineage)) {
            return;
        }
        CommittableWithLineage<CobbleShardCommittable> withLineage =
                (CommittableWithLineage<CobbleShardCommittable>) message;
        long checkpointId = withLineage.getCheckpointId();
        pendingByCheckpoint
                .computeIfAbsent(checkpointId, ignored -> new ArrayList<>())
                .add(withLineage.getCommittable());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        commitUpTo(checkpointId);
    }

    @Override
    public void endInput() throws Exception {
        commitUpTo(END_INPUT_CHECKPOINT_ID);
    }

    private void commitUpTo(long checkpointId) throws Exception {
        NavigableMap<Long, List<CobbleShardCommittable>> head =
                pendingByCheckpoint.headMap(checkpointId, true);
        if (head.isEmpty()) {
            return;
        }
        List<CobbleShardCommittable> merged = new ArrayList<>();
        for (List<CobbleShardCommittable> committables : head.values()) {
            merged.addAll(committables);
        }
        head.clear();
        if (merged.isEmpty()) {
            return;
        }

        Map<Integer, CobbleShardCommittable> latestBySubtask = new LinkedHashMap<>();
        List<CobbleShardCommittable> abandoned = new ArrayList<>();
        for (CobbleShardCommittable committable : merged) {
            CobbleShardCommittable replaced =
                    latestBySubtask.put(committable.bucketId, committable);
            if (replaced != null) {
                abandoned.add(replaced);
            }
        }

        global.commitCommittables(new ArrayList<>(latestBySubtask.values()), abandoned);
    }

    @Override
    public void close() throws Exception {
        try {
            if (global != null) {
                global.close();
            }
        } finally {
            pendingByCheckpoint.clear();
            super.close();
        }
    }
}
