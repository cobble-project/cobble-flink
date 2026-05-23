package io.cobble.flink.table;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Checkpointed enumerator state for the Cobble FLIP-27 source. */
final class CobbleSourceEnumeratorState {

    final long currentSnapshotId;
    final long nextSnapshotId;
    final List<CobbleSourceSplit> pendingSplits;

    CobbleSourceEnumeratorState(
            long currentSnapshotId,
            long nextSnapshotId,
            Collection<CobbleSourceSplit> pendingSplits) {
        this.currentSnapshotId = currentSnapshotId;
        this.nextSnapshotId = nextSnapshotId;
        this.pendingSplits = new ArrayList<>(pendingSplits);
    }

    static final class Serializer
            implements SimpleVersionedSerializer<CobbleSourceEnumeratorState> {
        private static final int VERSION = 1;
        private final CobbleSourceSplit.Serializer splitSerializer =
                new CobbleSourceSplit.Serializer();

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(CobbleSourceEnumeratorState state) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(out);
            dataOut.writeLong(state.currentSnapshotId);
            dataOut.writeLong(state.nextSnapshotId);
            dataOut.writeInt(state.pendingSplits.size());
            for (CobbleSourceSplit split : state.pendingSplits) {
                byte[] serializedSplit = splitSerializer.serialize(split);
                dataOut.writeInt(serializedSplit.length);
                dataOut.write(serializedSplit);
            }
            dataOut.flush();
            return out.toByteArray();
        }

        @Override
        public CobbleSourceEnumeratorState deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != VERSION) {
                throw new IOException(
                        "Unsupported Cobble source enumerator state version: " + version);
            }
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized));
            long currentSnapshotId = input.readLong();
            long nextSnapshotId = input.readLong();
            int splitCount = input.readInt();
            List<CobbleSourceSplit> splits = new ArrayList<>(splitCount);
            for (int i = 0; i < splitCount; i++) {
                int length = input.readInt();
                byte[] bytes = new byte[length];
                input.readFully(bytes);
                splits.add(splitSerializer.deserialize(splitSerializer.getVersion(), bytes));
            }
            return new CobbleSourceEnumeratorState(currentSnapshotId, nextSnapshotId, splits);
        }
    }
}
