package io.cobble.flink.state;

import io.cobble.ShardSnapshot;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/** Serialized shard-snapshot payload uploaded through Flink checkpoint state handles. */
final class CobbleSnapshotMetadata {

    /** Identifies Cobble shard metadata inside Flink's generic checkpoint meta streams. */
    private static final int MAGIC = 0x43425348;

    private static final int VERSION = 1;

    private final ShardSnapshot shardSnapshot;

    private CobbleSnapshotMetadata(ShardSnapshot shardSnapshot) {
        this.shardSnapshot = shardSnapshot;
    }

    static CobbleSnapshotMetadata fromShardSnapshot(ShardSnapshot shardSnapshot) {
        return new CobbleSnapshotMetadata(shardSnapshot);
    }

    static CobbleSnapshotMetadata read(DataInputView input) throws IOException {
        int magic = input.readInt();
        if (magic != MAGIC) {
            throw new IOException("Unsupported Cobble snapshot metadata magic: " + magic);
        }
        return readPayload(input);
    }

    static CobbleSnapshotMetadata readIfPresent(DataInputView input) throws IOException {
        final int magic;
        try {
            magic = input.readInt();
        } catch (EOFException ignored) {
            return null;
        }
        if (magic != MAGIC) {
            return null;
        }
        return readPayload(input);
    }

    private static CobbleSnapshotMetadata readPayload(DataInputView input) throws IOException {
        int version = input.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported Cobble snapshot metadata version: " + version);
        }

        ShardSnapshot shardSnapshot = new ShardSnapshot();
        shardSnapshot.dbId = input.readUTF();
        shardSnapshot.snapshotId = input.readLong();
        shardSnapshot.manifestPath = input.readUTF();
        shardSnapshot.timestampSeconds = input.readLong();
        shardSnapshot.dataSizeBytes = input.readLong();
        shardSnapshot.incrementalDataSizeBytes = input.readLong();

        int rangeCount = input.readInt();
        for (int index = 0; index < rangeCount; index++) {
            ShardSnapshot.Range range = new ShardSnapshot.Range();
            range.start = input.readInt();
            range.end = input.readInt();
            shardSnapshot.ranges.add(range);
        }

        int columnFamilyCount = input.readInt();
        Map<String, Integer> columnFamilyIds = new LinkedHashMap<>(columnFamilyCount);
        for (int index = 0; index < columnFamilyCount; index++) {
            columnFamilyIds.put(input.readUTF(), input.readInt());
        }
        shardSnapshot.columnFamilyIds = columnFamilyIds;

        return new CobbleSnapshotMetadata(shardSnapshot);
    }

    void write(DataOutputView output) throws IOException {
        output.writeInt(MAGIC);
        output.writeInt(VERSION);
        output.writeUTF(nullToEmpty(shardSnapshot.dbId));
        output.writeLong(shardSnapshot.snapshotId);
        output.writeUTF(nullToEmpty(shardSnapshot.manifestPath));
        output.writeLong(shardSnapshot.timestampSeconds);
        output.writeLong(shardSnapshot.dataSizeBytes);
        output.writeLong(shardSnapshot.incrementalDataSizeBytes);

        output.writeInt(shardSnapshot.ranges.size());
        for (ShardSnapshot.Range range : shardSnapshot.ranges) {
            output.writeInt(range.start);
            output.writeInt(range.end);
        }

        output.writeInt(shardSnapshot.columnFamilyIds.size());
        for (Map.Entry<String, Integer> entry : shardSnapshot.columnFamilyIds.entrySet()) {
            output.writeUTF(entry.getKey());
            output.writeInt(entry.getValue());
        }
    }

    ShardSnapshot shardSnapshot() {
        return shardSnapshot;
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
