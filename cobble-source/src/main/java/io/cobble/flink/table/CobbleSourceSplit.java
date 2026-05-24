package io.cobble.flink.table;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Checkpointed source split metadata for reopening one planned Cobble scan range, including the
 * last emitted bucket/key position when a split resumes in place.
 */
final class CobbleSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    /** Minimal reader lifecycle persisted in checkpoints. */
    enum ScanState {
        ACTIVE,
        WRAP,
        IDLE
    }

    final int rangeStartBucket;
    final int rangeEndBucket;
    final int totalBuckets;
    final long snapshotId;
    final int startBucket;
    final byte[] startKeyExclusive;
    final ScanState scanState;

    CobbleSourceSplit(
            int rangeStartBucket,
            int rangeEndBucket,
            int totalBuckets,
            long snapshotId,
            int startBucket,
            byte[] startKeyExclusive,
            ScanState scanState) {
        this.rangeStartBucket = rangeStartBucket;
        this.rangeEndBucket = rangeEndBucket;
        this.totalBuckets = totalBuckets;
        this.snapshotId = snapshotId;
        this.startBucket = startBucket;
        this.startKeyExclusive = copyOrNull(startKeyExclusive);
        this.scanState = scanState;
    }

    static CobbleSourceSplit forSnapshot(
            int rangeStartBucket, int rangeEndBucket, int totalBuckets, long snapshotId) {
        return new CobbleSourceSplit(
                rangeStartBucket,
                rangeEndBucket,
                totalBuckets,
                snapshotId,
                -1,
                null,
                ScanState.ACTIVE);
    }

    @Override
    public String splitId() {
        return splitIdForRange(rangeStartBucket, rangeEndBucket, totalBuckets);
    }

    static String splitIdForRange(int rangeStartBucket, int rangeEndBucket, int totalBuckets) {
        return rangeStartBucket + ":" + rangeEndBucket + ":" + totalBuckets;
    }

    /** Serializer for checkpointing split metadata without embedding raw ScanSplit payloads. */
    static final class Serializer implements SimpleVersionedSerializer<CobbleSourceSplit> {
        private static final int VERSION = 6;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(CobbleSourceSplit split) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(out);
            dataOut.writeInt(split.rangeStartBucket);
            dataOut.writeInt(split.rangeEndBucket);
            dataOut.writeInt(split.totalBuckets);
            dataOut.writeLong(split.snapshotId);
            dataOut.writeInt(split.startBucket);
            writeBytes(dataOut, split.startKeyExclusive);
            dataOut.writeInt(split.scanState.ordinal());
            dataOut.flush();
            return out.toByteArray();
        }

        @Override
        public CobbleSourceSplit deserialize(int version, byte[] serialized) throws IOException {
            if (version != VERSION) {
                throw new IOException("Unsupported CobbleSourceSplit version: " + version);
            }
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized));
            return new CobbleSourceSplit(
                    input.readInt(),
                    input.readInt(),
                    input.readInt(),
                    input.readLong(),
                    input.readInt(),
                    readBytes(input),
                    ScanState.values()[input.readInt()]);
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

    private static byte[] copyOrNull(byte[] bytes) {
        return bytes == null ? null : java.util.Arrays.copyOf(bytes, bytes.length);
    }
}
