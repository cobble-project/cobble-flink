package io.cobble.flink.common;

import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/** Wire codec for the Cobble payload stored in Flink's generic checkpoint metadata stream. */
public final class CobbleSnapshotMetadataCodec {

    private static final Logger LOG = LoggerFactory.getLogger(CobbleSnapshotMetadataCodec.class);

    /** Identifies a Cobble keyed-state payload inside a Flink metadata stream. */
    public static final int MAGIC = 0x43425348;

    public static final int VERSION = 1;
    public static final int MAX_SCHEMA_BYTES = 16 * 1024 * 1024;

    private CobbleSnapshotMetadataCodec() {}

    public static CobbleSnapshotMetadataPayload read(DataInputView input) throws IOException {
        int magic = input.readInt();
        if (magic != MAGIC) {
            throw new IOException("Unsupported Cobble snapshot metadata magic: " + magic);
        }
        return readPayload(input);
    }

    /** Returns {@code null} when the handle is empty or belongs to another state backend. */
    public static CobbleSnapshotMetadataPayload readIfPresent(DataInputView input)
            throws IOException {
        final int magic;
        try {
            magic = input.readInt();
        } catch (EOFException ignored) {
            return null;
        }
        return magic == MAGIC ? readPayload(input) : null;
    }

    public static void write(CobbleSnapshotMetadataPayload payload, DataOutputView output)
            throws IOException {
        ShardSnapshot shardSnapshot = payload.shardSnapshot();
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
        output.writeBoolean(payload.containsCobbleTimers());

        byte[] schemaBytes = payload.schemaStore().toBytes();
        if (schemaBytes.length > MAX_SCHEMA_BYTES) {
            schemaBytes = StateInspectSchemaStore.empty().toBytes();
        }
        output.writeInt(schemaBytes.length);
        output.write(schemaBytes);
    }

    private static CobbleSnapshotMetadataPayload readPayload(DataInputView input)
            throws IOException {
        int version = input.readInt();
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported Cobble snapshot metadata version: "
                            + version
                            + " (expected "
                            + VERSION
                            + ")");
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
        boolean containsCobbleTimers = input.readBoolean();
        return new CobbleSnapshotMetadataPayload(
                shardSnapshot, containsCobbleTimers, readSchemaPayload(input));
    }

    private static StateInspectSchemaStore readSchemaPayload(DataInputView input)
            throws IOException {
        int schemaBytesLength = input.readInt();
        if (schemaBytesLength < 0 || schemaBytesLength > MAX_SCHEMA_BYTES) {
            throw new IOException(
                    "Cobble inspect schema bytes length "
                            + schemaBytesLength
                            + " is out of range [0, "
                            + MAX_SCHEMA_BYTES
                            + "].");
        }
        if (schemaBytesLength == 0) {
            return StateInspectSchemaStore.empty();
        }
        byte[] schemaBytes = new byte[schemaBytesLength];
        input.readFully(schemaBytes);
        try {
            return StateInspectSchemaStore.fromBytes(schemaBytes);
        } catch (RuntimeException | IOException e) {
            LOG.warn(
                    "Failed to parse Cobble inspect schema from checkpoint metadata: {}. "
                            + "Falling back to empty schema store.",
                    e.getMessage());
            return StateInspectSchemaStore.empty();
        }
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
