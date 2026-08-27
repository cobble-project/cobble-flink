package io.cobble.flink.table;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
    private transient ListState<byte[]> pendingState;

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
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        pendingState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<byte[]>(
                                        "cobble-table-pending-committables", byte[].class));
        if (context.isRestored()) {
            for (byte[] entry : pendingState.get()) {
                PendingCommittable pending = PendingCommittable.deserialize(entry);
                pendingByCheckpoint
                        .computeIfAbsent(pending.checkpointId, ignored -> new ArrayList<>())
                        .add(pending.committable);
            }
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        pendingState.clear();
        for (Map.Entry<Long, List<CobbleShardCommittable>> entry : pendingByCheckpoint.entrySet()) {
            for (CobbleShardCommittable committable : entry.getValue()) {
                pendingState.add(
                        new PendingCommittable(entry.getKey().longValue(), committable)
                                .serialize());
            }
        }
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
        if (merged.isEmpty()) {
            head.clear();
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

        global.commitCommittables(
                checkpointId, new ArrayList<>(latestBySubtask.values()), abandoned);
        head.clear();
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

    private static final class PendingCommittable {
        private static final int VERSION = 1;
        private static final CobbleShardCommittable.Serializer SERIALIZER =
                new CobbleShardCommittable.Serializer();

        private final long checkpointId;
        private final CobbleShardCommittable committable;

        private PendingCommittable(long checkpointId, CobbleShardCommittable committable) {
            this.checkpointId = checkpointId;
            this.committable = committable;
        }

        private byte[] serialize() throws IOException {
            byte[] committableBytes = SERIALIZER.serialize(committable);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.writeInt(VERSION);
            output.writeLong(checkpointId);
            output.writeInt(SERIALIZER.getVersion());
            output.writeInt(committableBytes.length);
            output.write(committableBytes);
            output.flush();
            return bytes.toByteArray();
        }

        private static PendingCommittable deserialize(byte[] bytes) throws IOException {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
            int version = input.readInt();
            if (version != VERSION) {
                throw new IOException("Unsupported pending committable version: " + version);
            }
            long checkpointId = input.readLong();
            int committableVersion = input.readInt();
            int length = input.readInt();
            if (length < 0 || length > bytes.length) {
                throw new IOException("Invalid pending committable length: " + length);
            }
            byte[] committableBytes = new byte[length];
            input.readFully(committableBytes);
            if (input.available() != 0) {
                throw new IOException("Trailing bytes in pending Cobble committable state.");
            }
            return new PendingCommittable(
                    checkpointId, SERIALIZER.deserialize(committableVersion, committableBytes));
        }
    }
}
