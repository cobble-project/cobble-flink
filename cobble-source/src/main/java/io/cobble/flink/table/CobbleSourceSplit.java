package io.cobble.flink.table;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/** Checkpointed source split metadata for reopening one planned Cobble scan range. */
final class CobbleSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    /** Minimal reader lifecycle persisted in checkpoints. */
    enum ScanState {
        ACTIVE,
        IDLE
    }

    final int rangeStartBucket;
    final int rangeEndBucket;
    final int totalBuckets;
    final long snapshotId;
    final ScanState scanState;

    CobbleSourceSplit(
            int rangeStartBucket,
            int rangeEndBucket,
            int totalBuckets,
            long snapshotId,
            ScanState scanState) {
        this.rangeStartBucket = rangeStartBucket;
        this.rangeEndBucket = rangeEndBucket;
        this.totalBuckets = totalBuckets;
        this.snapshotId = snapshotId;
        this.scanState = scanState;
    }

    static CobbleSourceSplit forSnapshot(
            int rangeStartBucket, int rangeEndBucket, int totalBuckets, long snapshotId) {
        return new CobbleSourceSplit(
                rangeStartBucket, rangeEndBucket, totalBuckets, snapshotId, ScanState.ACTIVE);
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
        private static final int VERSION = 5;

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
                    ScanState.values()[input.readInt()]);
        }
    }
}
