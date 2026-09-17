package io.cobble.flink.common;

import io.cobble.ShardSnapshot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/** Shared wire codec for Cobble shard snapshot metadata persisted by Flink. */
public final class CobbleShardSnapshotCodec {

    private static final int MAX_COLLECTION_ENTRIES = 100_000;
    private static final int MAX_STRING_BYTES = 16 * 1024 * 1024;

    private CobbleShardSnapshotCodec() {}

    public static void write(ShardSnapshot snapshot, DataOutput output) throws IOException {
        writeString(output, snapshot.dbId);
        output.writeLong(snapshot.snapshotId);
        writeString(output, snapshot.manifestPath);
        output.writeLong(snapshot.timestampSeconds);
        output.writeLong(snapshot.dataSizeBytes);
        output.writeLong(snapshot.incrementalDataSizeBytes);

        requireCollectionSize("bucket ranges", snapshot.ranges.size());
        output.writeInt(snapshot.ranges.size());
        for (ShardSnapshot.Range range : snapshot.ranges) {
            output.writeInt(range.start);
            output.writeInt(range.end);
        }

        writeColumnFamilyIds(snapshot.columnFamilyIds, output);
        output.writeLong(snapshot.schemaId);
        writeColumnFamilies(snapshot.columnFamilies, output);
    }

    public static ShardSnapshot read(DataInput input) throws IOException {
        ShardSnapshot snapshot = new ShardSnapshot();
        snapshot.dbId = readString(input);
        snapshot.snapshotId = input.readLong();
        snapshot.manifestPath = readString(input);
        snapshot.timestampSeconds = input.readLong();
        snapshot.dataSizeBytes = input.readLong();
        snapshot.incrementalDataSizeBytes = input.readLong();

        int rangeCount = readCollectionSize(input, "bucket ranges");
        for (int index = 0; index < rangeCount; index++) {
            ShardSnapshot.Range range = new ShardSnapshot.Range();
            range.start = input.readInt();
            range.end = input.readInt();
            snapshot.ranges.add(range);
        }

        snapshot.columnFamilyIds = readColumnFamilyIds(input);
        snapshot.schemaId = input.readLong();
        snapshot.columnFamilies = readColumnFamilies(input);
        return snapshot;
    }

    private static void writeColumnFamilyIds(
            Map<String, Integer> columnFamilyIds, DataOutput output) throws IOException {
        output.writeBoolean(columnFamilyIds != null);
        if (columnFamilyIds == null) {
            return;
        }
        requireCollectionSize("column-family IDs", columnFamilyIds.size());
        output.writeInt(columnFamilyIds.size());
        for (Map.Entry<String, Integer> entry : columnFamilyIds.entrySet()) {
            writeString(output, entry.getKey());
            output.writeInt(entry.getValue());
        }
    }

    private static Map<String, Integer> readColumnFamilyIds(DataInput input) throws IOException {
        if (!input.readBoolean()) {
            return null;
        }
        int count = readCollectionSize(input, "column-family IDs");
        Map<String, Integer> result = new LinkedHashMap<>(count);
        for (int index = 0; index < count; index++) {
            result.put(readString(input), input.readInt());
        }
        return result;
    }

    private static void writeColumnFamilies(
            Map<String, ShardSnapshot.SnapshotColumnFamily> columnFamilies, DataOutput output)
            throws IOException {
        int count = columnFamilies == null ? 0 : columnFamilies.size();
        requireCollectionSize("column families", count);
        output.writeInt(count);
        if (columnFamilies == null) {
            return;
        }
        for (Map.Entry<String, ShardSnapshot.SnapshotColumnFamily> entry :
                columnFamilies.entrySet()) {
            ShardSnapshot.SnapshotColumnFamily family = entry.getValue();
            if (family == null || family.options == null) {
                throw new IOException(
                        "Cobble shard snapshot column family '"
                                + entry.getKey()
                                + "' has no definition or options.");
            }
            writeString(output, entry.getKey());
            output.writeInt(family.id);
            output.writeInt(family.numColumns);
            output.writeBoolean(family.options.valueHasTtl);
            writeString(output, family.options.metadata);
        }
    }

    private static Map<String, ShardSnapshot.SnapshotColumnFamily> readColumnFamilies(
            DataInput input) throws IOException {
        int count = readCollectionSize(input, "column families");
        Map<String, ShardSnapshot.SnapshotColumnFamily> result = new LinkedHashMap<>(count);
        for (int index = 0; index < count; index++) {
            String name = readString(input);
            ShardSnapshot.SnapshotColumnFamily family = new ShardSnapshot.SnapshotColumnFamily();
            family.id = input.readInt();
            family.numColumns = input.readInt();
            family.options.valueHasTtl = input.readBoolean();
            family.options.metadata = readString(input);
            result.put(name, family);
        }
        return result;
    }

    private static void writeString(DataOutput output, String value) throws IOException {
        output.writeBoolean(value != null);
        if (value == null) {
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_BYTES) {
            throw new IOException(
                    "Cobble shard snapshot string length "
                            + bytes.length
                            + " exceeds "
                            + MAX_STRING_BYTES
                            + '.');
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(DataInput input) throws IOException {
        if (!input.readBoolean()) {
            return null;
        }
        int length = input.readInt();
        if (length < 0 || length > MAX_STRING_BYTES) {
            throw new IOException(
                    "Cobble shard snapshot string length "
                            + length
                            + " is out of range [0, "
                            + MAX_STRING_BYTES
                            + "].");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static int readCollectionSize(DataInput input, String name) throws IOException {
        int count = input.readInt();
        if (count < 0 || count > MAX_COLLECTION_ENTRIES) {
            throw new IOException(
                    "Cobble shard snapshot "
                            + name
                            + " count "
                            + count
                            + " is out of range [0, "
                            + MAX_COLLECTION_ENTRIES
                            + "].");
        }
        return count;
    }

    private static void requireCollectionSize(String name, int count) throws IOException {
        if (count > MAX_COLLECTION_ENTRIES) {
            throw new IOException(
                    "Cobble shard snapshot "
                            + name
                            + " count "
                            + count
                            + " exceeds "
                            + MAX_COLLECTION_ENTRIES
                            + '.');
        }
    }
}
