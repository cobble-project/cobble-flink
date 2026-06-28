package io.cobble.flink.state;

import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/** Serialized shard-snapshot payload uploaded through Flink checkpoint state handles. */
final class CobbleSnapshotMetadata {

    private static final Logger LOG = LoggerFactory.getLogger(CobbleSnapshotMetadata.class);

    /** Identifies Cobble shard metadata inside Flink's generic checkpoint meta streams. */
    private static final int MAGIC = 0x43425348;

    @VisibleForTesting static final int VERSION = 1;

    @VisibleForTesting static final int MAX_SCHEMA_BYTES = 16 * 1024 * 1024;

    private final ShardSnapshot shardSnapshot;
    private final boolean containsCobbleTimers;
    private final StateInspectSchemaStore schemaStore;

    private CobbleSnapshotMetadata(
            ShardSnapshot shardSnapshot,
            boolean containsCobbleTimers,
            StateInspectSchemaStore schemaStore) {
        this.shardSnapshot = shardSnapshot;
        this.containsCobbleTimers = containsCobbleTimers;
        this.schemaStore = schemaStore;
    }

    static CobbleSnapshotMetadata fromShardSnapshot(
            ShardSnapshot shardSnapshot,
            boolean containsCobbleTimers,
            StateInspectSchemaStore schemaStore) {
        return new CobbleSnapshotMetadata(shardSnapshot, containsCobbleTimers, schemaStore);
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

        StateInspectSchemaStore schemaStore = readSchemaPayload(input);
        return new CobbleSnapshotMetadata(shardSnapshot, containsCobbleTimers, schemaStore);
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
                    "Failed to parse Cobble inspect schema from checkpoint metadata: {}."
                            + " Falling back to empty schema store.",
                    e.getMessage());
            return StateInspectSchemaStore.empty();
        }
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
        output.writeBoolean(containsCobbleTimers);

        byte[] schemaBytes = schemaStore.toBytes();
        if (schemaBytes.length > MAX_SCHEMA_BYTES) {
            // Serialized schema exceeds size cap; write empty store so JM can proceed
            // without rejecting the checkpoint. The large serializer payloads that
            // cause this are already degraded to class-name-only in the monitor path.
            schemaBytes = StateInspectSchemaStore.empty().toBytes();
        }
        output.writeInt(schemaBytes.length);
        output.write(schemaBytes);
    }

    ShardSnapshot shardSnapshot() {
        return shardSnapshot;
    }

    boolean containsCobbleTimers() {
        return containsCobbleTimers;
    }

    StateInspectSchemaStore schemaStore() {
        return schemaStore;
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
