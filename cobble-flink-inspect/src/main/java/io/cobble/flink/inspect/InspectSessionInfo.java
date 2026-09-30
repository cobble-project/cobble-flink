package io.cobble.flink.inspect;

import java.util.Objects;

/** Stable metadata about a live, read-only inspection session. */
public final class InspectSessionInfo {
    private final InspectSelection selection;
    private final boolean sourceOpen;
    private final int totalBuckets;

    public InspectSessionInfo(InspectSelection selection, boolean sourceOpen, int totalBuckets) {
        this.selection = Objects.requireNonNull(selection, "selection");
        this.sourceOpen = sourceOpen;
        if (totalBuckets <= 0) {
            throw new IllegalArgumentException("totalBuckets must be positive");
        }
        this.totalBuckets = totalBuckets;
    }

    public InspectSelection selection() {
        return selection;
    }

    public boolean sourceOpen() {
        return sourceOpen;
    }

    /** Pinned snapshot bucket (state key-group) count, or the configured fallback if absent. */
    public int totalBuckets() {
        return totalBuckets;
    }
}
