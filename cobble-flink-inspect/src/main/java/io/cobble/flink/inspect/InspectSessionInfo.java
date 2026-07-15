package io.cobble.flink.inspect;

import java.util.Objects;

/** Stable metadata about a live, read-only inspection session. */
public final class InspectSessionInfo {
    private final InspectSelection selection;
    private final boolean sourceOpen;

    public InspectSessionInfo(InspectSelection selection, boolean sourceOpen) {
        this.selection = Objects.requireNonNull(selection, "selection");
        this.sourceOpen = sourceOpen;
    }

    public InspectSelection selection() {
        return selection;
    }

    public boolean sourceOpen() {
        return sourceOpen;
    }
}
