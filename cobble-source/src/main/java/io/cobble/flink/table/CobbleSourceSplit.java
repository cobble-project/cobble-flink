package io.cobble.flink.table;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/** Logical per-bucket split for the Cobble FLIP-27 source. */
final class CobbleSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    enum ScanPhase {
        FORWARD,
        WRAPPED,
        IDLE
    }

    private static final String SPLIT_ID_PREFIX = "split-";

    final int splitId;
    final int bucketId;
    final long snapshotId;
    final String manifestPath;
    final byte[] lastConsumedKey;
    final byte[] anchorKey;
    final byte[] resumeKey;
    final ScanPhase phase;

    CobbleSourceSplit(
            int splitId,
            int bucketId,
            long snapshotId,
            String manifestPath,
            byte[] lastConsumedKey,
            byte[] anchorKey,
            byte[] resumeKey,
            ScanPhase phase) {
        this.splitId = splitId;
        this.bucketId = bucketId;
        this.snapshotId = snapshotId;
        this.manifestPath = manifestPath;
        this.lastConsumedKey = copy(lastConsumedKey);
        this.anchorKey = copy(anchorKey);
        this.resumeKey = copy(resumeKey);
        this.phase = phase;
    }

    static CobbleSourceSplit forSnapshot(
            int splitId, int bucketId, long snapshotId, String manifestPath) {
        return new CobbleSourceSplit(
                splitId, bucketId, snapshotId, manifestPath, null, null, null, ScanPhase.FORWARD);
    }

    @Override
    public String splitId() {
        return SPLIT_ID_PREFIX + splitId;
    }

    private static byte[] copy(byte[] bytes) {
        return bytes == null ? null : java.util.Arrays.copyOf(bytes, bytes.length);
    }

    static final class Serializer implements SimpleVersionedSerializer<CobbleSourceSplit> {
        private static final int VERSION = 2;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(CobbleSourceSplit split) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(out);
            dataOut.writeInt(split.splitId);
            dataOut.writeInt(split.bucketId);
            dataOut.writeLong(split.snapshotId);
            writeString(dataOut, split.manifestPath);
            writeBytes(dataOut, split.lastConsumedKey);
            writeBytes(dataOut, split.anchorKey);
            writeBytes(dataOut, split.resumeKey);
            dataOut.writeInt(split.phase.ordinal());
            dataOut.flush();
            return out.toByteArray();
        }

        @Override
        public CobbleSourceSplit deserialize(int version, byte[] serialized) throws IOException {
            if (version == 1) {
                return deserializeV1(serialized);
            }
            if (version != VERSION) {
                throw new IOException("Unsupported CobbleSourceSplit version: " + version);
            }
            return deserializeV2(serialized);
        }

        private static CobbleSourceSplit deserializeV1(byte[] serialized) throws IOException {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized));
            int bucketId = input.readInt();
            return new CobbleSourceSplit(
                    bucketId,
                    bucketId,
                    input.readLong(),
                    readString(input),
                    readBytes(input),
                    readBytes(input),
                    readBytes(input),
                    ScanPhase.values()[input.readInt()]);
        }

        private static CobbleSourceSplit deserializeV2(byte[] serialized) throws IOException {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized));
            return new CobbleSourceSplit(
                    input.readInt(),
                    input.readInt(),
                    input.readLong(),
                    readString(input),
                    readBytes(input),
                    readBytes(input),
                    readBytes(input),
                    ScanPhase.values()[input.readInt()]);
        }

        private static void writeString(DataOutputStream out, String value) throws IOException {
            if (value == null) {
                out.writeBoolean(false);
                return;
            }
            out.writeBoolean(true);
            byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        private static String readString(DataInputStream input) throws IOException {
            if (!input.readBoolean()) {
                return null;
            }
            int length = input.readInt();
            byte[] bytes = new byte[length];
            input.readFully(bytes);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }

        private static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
            if (bytes == null) {
                out.writeInt(-1);
                return;
            }
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        private static byte[] readBytes(DataInputStream input) throws IOException {
            int length = input.readInt();
            if (length < 0) {
                return null;
            }
            byte[] bytes = new byte[length];
            input.readFully(bytes);
            return bytes;
        }
    }
}
