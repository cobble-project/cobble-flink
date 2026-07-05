package io.cobble.flink.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.util.Objects;

/**
 * Immutable per-state metadata entry captured at registration time for canonical savepoint
 * creation.
 *
 * <p>Each entry corresponds to exactly one state (KV or priority-queue) and carries the {@code
 * kvStateId} (the index in the canonical savepoint's metadata list), the state name, the {@link
 * StateDescriptor.Type} (for KV states), the Cobble column family to scan, and the Flink {@link
 * StateMetaInfoSnapshot} that goes into the canonical savepoint header.
 *
 * <p>This class is immutable. Instances are created by {@link CobbleCanonicalSavepointMetadata}
 * during state registration and frozen into a {@link CobbleCanonicalSavepointMetadataSnapshot} when
 * a savepoint is requested.
 */
public final class CobbleCanonicalStateMeta {

    private final int kvStateId;
    private final String stateName;
    private final StateDescriptor.Type stateType;
    private final boolean priorityQueue;
    private final String columnFamily;
    private final StateMetaInfoSnapshot metaInfoSnapshot;

    CobbleCanonicalStateMeta(
            int kvStateId,
            String stateName,
            StateDescriptor.Type stateType,
            boolean priorityQueue,
            String columnFamily,
            StateMetaInfoSnapshot metaInfoSnapshot) {
        this.kvStateId = kvStateId;
        this.stateName = Objects.requireNonNull(stateName, "stateName");
        this.stateType = stateType;
        this.priorityQueue = priorityQueue;
        this.columnFamily = Objects.requireNonNull(columnFamily, "columnFamily");
        this.metaInfoSnapshot = Objects.requireNonNull(metaInfoSnapshot, "metaInfoSnapshot");
    }

    /** The zero-based index of this state in the canonical savepoint's metadata list. */
    public int kvStateId() {
        return kvStateId;
    }

    /** The state name as declared by the Flink state descriptor. */
    public String stateName() {
        return stateName;
    }

    /**
     * The {@link StateDescriptor.Type} for KV states (VALUE / LIST / MAP / REDUCING / AGGREGATING),
     * or {@code null} for priority-queue (timer) states.
     */
    public StateDescriptor.Type stateType() {
        return stateType;
    }

    /** Whether this state is a priority-queue (timer) state rather than a KV state. */
    public boolean priorityQueue() {
        return priorityQueue;
    }

    /** The Cobble column family that stores this state's rows. */
    public String columnFamily() {
        return columnFamily;
    }

    /** The Flink metadata snapshot for the canonical savepoint header. */
    public StateMetaInfoSnapshot metaInfoSnapshot() {
        return metaInfoSnapshot;
    }

    @Override
    public String toString() {
        return "CobbleCanonicalStateMeta{"
                + "kvStateId="
                + kvStateId
                + ", stateName='"
                + stateName
                + '\''
                + ", stateType="
                + stateType
                + ", priorityQueue="
                + priorityQueue
                + ", columnFamily='"
                + columnFamily
                + '\''
                + '}';
    }
}
