package io.cobble.flink.state;

import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Immutable snapshot of all registered canonical savepoint metadata, produced by {@link
 * CobbleCanonicalSavepointMetadata#snapshot()}.
 *
 * <p>The {@link #entries()} list is the single source of truth for Phase 2: both the metadata
 * header ({@link #metaInfoSnapshots()}) and the {@code KeyValueStateIterator} state IDs derive from
 * it. The list index of each entry equals its {@code kvStateId}.
 *
 * <p>This class is immutable and holds no references to live mutable maps or lists.
 */
public final class CobbleCanonicalSavepointMetadataSnapshot {

    private final List<CobbleCanonicalStateMeta> entries;
    private final List<StateMetaInfoSnapshot> metaInfoSnapshots;

    CobbleCanonicalSavepointMetadataSnapshot(List<CobbleCanonicalStateMeta> entries) {
        this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
        List<StateMetaInfoSnapshot> snapshots = new ArrayList<>(entries.size());
        for (CobbleCanonicalStateMeta entry : entries) {
            snapshots.add(entry.metaInfoSnapshot());
        }
        this.metaInfoSnapshots = Collections.unmodifiableList(snapshots);
    }

    /**
     * The immutable list of state metadata entries. The list index equals the {@code kvStateId}.
     */
    public List<CobbleCanonicalStateMeta> entries() {
        return entries;
    }

    /** The immutable list of Flink {@link StateMetaInfoSnapshot}s for the savepoint header. */
    public List<StateMetaInfoSnapshot> metaInfoSnapshots() {
        return metaInfoSnapshots;
    }

    /** The number of registered states. */
    public int stateCount() {
        return entries.size();
    }

    /** Returns the entry for the given {@code kvStateId}, or {@code null} if out of range. */
    public CobbleCanonicalStateMeta entry(int kvStateId) {
        if (kvStateId < 0 || kvStateId >= entries.size()) {
            return null;
        }
        return entries.get(kvStateId);
    }
}
