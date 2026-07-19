package io.cobble.flink.common;

import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable Cobble payload embedded in a Flink keyed-state metadata handle. */
public final class CobbleSnapshotMetadataPayload {

    private final ShardSnapshot shardSnapshot;
    private final boolean containsCobbleTimers;
    private final List<CobbleStateDescriptor> stateDescriptors;
    private final StateInspectSchemaStore schemaStore;

    public CobbleSnapshotMetadataPayload(
            ShardSnapshot shardSnapshot,
            boolean containsCobbleTimers,
            List<CobbleStateDescriptor> stateDescriptors,
            StateInspectSchemaStore schemaStore) {
        this.shardSnapshot = Objects.requireNonNull(shardSnapshot, "shardSnapshot");
        this.containsCobbleTimers = containsCobbleTimers;
        this.stateDescriptors =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Objects.requireNonNull(stateDescriptors, "stateDescriptors")));
        this.schemaStore = schemaStore == null ? StateInspectSchemaStore.empty() : schemaStore;
    }

    public ShardSnapshot shardSnapshot() {
        return shardSnapshot;
    }

    public boolean containsCobbleTimers() {
        return containsCobbleTimers;
    }

    public List<CobbleStateDescriptor> stateDescriptors() {
        return stateDescriptors;
    }

    public StateInspectSchemaStore schemaStore() {
        return schemaStore;
    }
}
