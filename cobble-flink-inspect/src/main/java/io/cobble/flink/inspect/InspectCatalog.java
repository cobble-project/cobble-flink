package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** A pinned, immutable view of inspectable Cobble checkpoints. */
public final class InspectCatalog {
    private final String sourceKind;
    private final String rootDirectory;
    private final List<CheckpointInfo> checkpoints;

    public InspectCatalog(
            String sourceKind, String rootDirectory, List<CheckpointInfo> checkpoints) {
        this.sourceKind = Objects.requireNonNull(sourceKind, "sourceKind");
        this.rootDirectory = Objects.requireNonNull(rootDirectory, "rootDirectory");
        this.checkpoints =
                Collections.unmodifiableList(
                        new ArrayList<>(Objects.requireNonNull(checkpoints, "checkpoints")));
    }

    public String sourceKind() {
        return sourceKind;
    }

    public String rootDirectory() {
        return rootDirectory;
    }

    public List<CheckpointInfo> checkpoints() {
        return checkpoints;
    }
}
