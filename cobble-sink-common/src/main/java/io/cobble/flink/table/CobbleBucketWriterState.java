package io.cobble.flink.table;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/** Durable identity of one single-bucket table writer. */
final class CobbleBucketWriterState implements Serializable {
    private static final long serialVersionUID = 1L;

    final String dbId;
    final long snapshotId;
    final int rangeStart;
    final int rangeEnd;

    CobbleBucketWriterState(String dbId, long snapshotId, int rangeStart, int rangeEnd) {
        this.dbId = dbId;
        this.snapshotId = snapshotId;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
    }

    static CobbleBucketWriterState bucket(String dbId, long snapshotId, int bucketId) {
        return new CobbleBucketWriterState(dbId, snapshotId, bucketId, bucketId);
    }

    int bucketId() {
        if (rangeStart != rangeEnd) {
            throw new IllegalStateException("Cobble table writer state must cover one bucket.");
        }
        return rangeStart;
    }

    static final class Serializer implements SimpleVersionedSerializer<CobbleBucketWriterState> {
        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(CobbleBucketWriterState state) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.writeUTF(state.dbId);
            output.writeLong(state.snapshotId);
            output.writeInt(state.rangeStart);
            output.writeInt(state.rangeEnd);
            output.flush();
            return bytes.toByteArray();
        }

        @Override
        public CobbleBucketWriterState deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != VERSION) {
                throw new IOException(
                        "Unsupported Cobble catalog writer state version: " + version);
            }
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized));
            CobbleBucketWriterState state =
                    new CobbleBucketWriterState(
                            input.readUTF(), input.readLong(), input.readInt(), input.readInt());
            if (state.dbId.isEmpty()
                    || state.snapshotId < 0L
                    || state.rangeStart < 0
                    || state.rangeEnd != state.rangeStart) {
                throw new IOException("Invalid Cobble catalog writer state.");
            }
            return state;
        }
    }
}
