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

/** Checkpointed enumerator state for the bounded Cobble state source. */
final class CobbleStateSourceEnumeratorState {

    final long checkpointId;
    final List<CobbleStateSourceSplit> pendingSplits;

    CobbleStateSourceEnumeratorState(
            long checkpointId, Collection<CobbleStateSourceSplit> pendingSplits) {
        this.checkpointId = checkpointId;
        this.pendingSplits = new ArrayList<>(pendingSplits);
    }

    static final class Serializer
            implements SimpleVersionedSerializer<CobbleStateSourceEnumeratorState> {
        private static final int VERSION = 1;
        private final CobbleStateSourceSplit.Serializer splitSerializer =
                new CobbleStateSourceSplit.Serializer();

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(CobbleStateSourceEnumeratorState state) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(out);
            dataOut.writeLong(state.checkpointId);
            dataOut.writeInt(state.pendingSplits.size());
            for (CobbleStateSourceSplit split : state.pendingSplits) {
                byte[] serializedSplit = splitSerializer.serialize(split);
                dataOut.writeInt(serializedSplit.length);
                dataOut.write(serializedSplit);
            }
            dataOut.flush();
            return out.toByteArray();
        }

        @Override
        public CobbleStateSourceEnumeratorState deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != VERSION) {
                throw new IOException(
                        "Unsupported Cobble state source enumerator state version: " + version);
            }
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized));
            long checkpointId = input.readLong();
            int splitCount = input.readInt();
            List<CobbleStateSourceSplit> splits = new ArrayList<>(splitCount);
            for (int i = 0; i < splitCount; i++) {
                int length = input.readInt();
                byte[] bytes = new byte[length];
                input.readFully(bytes);
                splits.add(splitSerializer.deserialize(splitSerializer.getVersion(), bytes));
            }
            return new CobbleStateSourceEnumeratorState(checkpointId, splits);
        }
    }
}
