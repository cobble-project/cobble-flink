package io.cobble.flink.common;

import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;

/** Immutable Cobble payload embedded in a Flink keyed-state metadata handle. */
public final class CobbleSnapshotMetadataPayload {

    private final ShardSnapshot shardSnapshot;
    private final boolean containsCobbleTimers;
    private final StateInspectSchemaStore schemaStore;

    public CobbleSnapshotMetadataPayload(
            ShardSnapshot shardSnapshot,
            boolean containsCobbleTimers,
            StateInspectSchemaStore schemaStore) {
        this.shardSnapshot = shardSnapshot;
        this.containsCobbleTimers = containsCobbleTimers;
        this.schemaStore = schemaStore == null ? StateInspectSchemaStore.empty() : schemaStore;
    }

    public ShardSnapshot shardSnapshot() {
        return shardSnapshot;
    }

    public boolean containsCobbleTimers() {
        return containsCobbleTimers;
    }

    public StateInspectSchemaStore schemaStore() {
        return schemaStore;
    }
}
