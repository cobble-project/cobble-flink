package io.cobble.flink.table;

import io.cobble.ShardSnapshot;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** One precommitted Cobble shard snapshot emitted by a sink writer. */
final class CobbleShardCommittable implements Serializable {
    private static final long serialVersionUID = 1L;

    final int totalBuckets;
    final int bucketId;
    final String writerPath;
    final ShardSnapshot shardSnapshot;

    CobbleShardCommittable(
            int totalBuckets, int bucketId, String writerPath, ShardSnapshot shardSnapshot) {
        this.totalBuckets = totalBuckets;
        this.bucketId = bucketId;
        this.writerPath = writerPath;
        this.shardSnapshot = shardSnapshot;
    }

    /** Explicit binary serializer for Cobble shard committables. */
    static final class Serializer implements SimpleVersionedSerializer<CobbleShardCommittable> {

        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(CobbleShardCommittable obj) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(out);
            dataOut.writeInt(obj.totalBuckets);
            dataOut.writeInt(obj.bucketId);
            writeString(dataOut, obj.writerPath);
            writeShardSnapshot(dataOut, obj.shardSnapshot);
            dataOut.flush();
            return out.toByteArray();
        }

        @Override
        public CobbleShardCommittable deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != VERSION) {
                throw new IOException("Unsupported CobbleShardCommittable version: " + version);
            }
            DataInputStream input =
                    new DataInputStream(new java.io.ByteArrayInputStream(serialized));
            int totalBuckets = input.readInt();
            int bucketId = input.readInt();
            String writerPath = readString(input);
            ShardSnapshot shardSnapshot = readShardSnapshot(input);
            return new CobbleShardCommittable(totalBuckets, bucketId, writerPath, shardSnapshot);
        }

        private static void writeShardSnapshot(DataOutputStream out, ShardSnapshot shardSnapshot)
                throws IOException {
            writeString(out, shardSnapshot.dbId);
            out.writeLong(shardSnapshot.snapshotId);
            writeString(out, shardSnapshot.manifestPath);
            out.writeLong(shardSnapshot.timestampSeconds);
            out.writeLong(shardSnapshot.dataSizeBytes);
            out.writeLong(shardSnapshot.incrementalDataSizeBytes);

            out.writeInt(shardSnapshot.columnFamilyIds.size());
            for (Map.Entry<String, Integer> entry : shardSnapshot.columnFamilyIds.entrySet()) {
                writeString(out, entry.getKey());
                out.writeInt(entry.getValue().intValue());
            }

            out.writeInt(shardSnapshot.ranges.size());
            for (ShardSnapshot.Range range : shardSnapshot.ranges) {
                out.writeInt(range.start);
                out.writeInt(range.end);
            }
        }

        private static ShardSnapshot readShardSnapshot(DataInputStream input) throws IOException {
            ShardSnapshot shardSnapshot = new ShardSnapshot();
            shardSnapshot.dbId = readString(input);
            shardSnapshot.snapshotId = input.readLong();
            shardSnapshot.manifestPath = readString(input);
            shardSnapshot.timestampSeconds = input.readLong();
            shardSnapshot.dataSizeBytes = input.readLong();
            shardSnapshot.incrementalDataSizeBytes = input.readLong();

            int columnFamilyCount = input.readInt();
            shardSnapshot.columnFamilyIds = new LinkedHashMap<>(columnFamilyCount);
            for (int i = 0; i < columnFamilyCount; i++) {
                shardSnapshot.columnFamilyIds.put(
                        readString(input), Integer.valueOf(input.readInt()));
            }

            int rangeCount = input.readInt();
            for (int i = 0; i < rangeCount; i++) {
                ShardSnapshot.Range range = new ShardSnapshot.Range();
                range.start = input.readInt();
                range.end = input.readInt();
                shardSnapshot.ranges.add(range);
            }
            return shardSnapshot;
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
    }
}
