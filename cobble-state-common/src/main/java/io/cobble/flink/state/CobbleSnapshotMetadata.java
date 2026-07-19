package io.cobble.flink.state;

import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleSnapshotMetadataCodec;
import io.cobble.flink.common.CobbleSnapshotMetadataPayload;
import io.cobble.flink.common.CobbleStateDescriptor;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.List;

/** Serialized shard-snapshot payload uploaded through Flink checkpoint state handles. */
final class CobbleSnapshotMetadata {

    @VisibleForTesting static final int VERSION = CobbleSnapshotMetadataCodec.VERSION;

    @VisibleForTesting static final int MAX_SCHEMA_BYTES =
            CobbleSnapshotMetadataCodec.MAX_SCHEMA_BYTES;

    private final CobbleSnapshotMetadataPayload payload;

    private CobbleSnapshotMetadata(CobbleSnapshotMetadataPayload payload) {
        this.payload = payload;
    }

    static CobbleSnapshotMetadata fromShardSnapshot(
            ShardSnapshot shardSnapshot,
            boolean containsCobbleTimers,
            List<CobbleStateDescriptor> stateDescriptors,
            StateInspectSchemaStore schemaStore) {
        return new CobbleSnapshotMetadata(
                new CobbleSnapshotMetadataPayload(
                        shardSnapshot, containsCobbleTimers, stateDescriptors, schemaStore));
    }

    static CobbleSnapshotMetadata read(DataInputView input) throws IOException {
        return new CobbleSnapshotMetadata(CobbleSnapshotMetadataCodec.read(input));
    }

    static CobbleSnapshotMetadata readIfPresent(DataInputView input) throws IOException {
        CobbleSnapshotMetadataPayload payload = CobbleSnapshotMetadataCodec.readIfPresent(input);
        return payload == null ? null : new CobbleSnapshotMetadata(payload);
    }

    void write(DataOutputView output) throws IOException {
        CobbleSnapshotMetadataCodec.write(payload, output);
    }

    ShardSnapshot shardSnapshot() {
        return payload.shardSnapshot();
    }

    boolean containsCobbleTimers() {
        return payload.containsCobbleTimers();
    }

    List<CobbleStateDescriptor> stateDescriptors() {
        return payload.stateDescriptors();
    }

    StateInspectSchemaStore schemaStore() {
        return payload.schemaStore();
    }
}
