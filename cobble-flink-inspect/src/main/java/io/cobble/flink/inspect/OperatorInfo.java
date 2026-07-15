package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** An operator that can be inspected in a checkpoint. */
public final class OperatorInfo {
    private final String operatorId;
    private final boolean globalSnapshotLayout;
    private final boolean embeddedCheckpoint;
    private final List<String> readerVolumeDirectories;

    public OperatorInfo(
            String operatorId,
            boolean globalSnapshotLayout,
            boolean embeddedCheckpoint,
            List<String> readerVolumeDirectories) {
        this.operatorId = Objects.requireNonNull(operatorId, "operatorId");
        this.globalSnapshotLayout = globalSnapshotLayout;
        this.embeddedCheckpoint = embeddedCheckpoint;
        this.readerVolumeDirectories =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Objects.requireNonNull(
                                        readerVolumeDirectories, "readerVolumeDirectories")));
    }

    public String operatorId() {
        return operatorId;
    }

    public boolean globalSnapshotLayout() {
        return globalSnapshotLayout;
    }

    public boolean embeddedCheckpoint() {
        return embeddedCheckpoint;
    }

    public List<String> readerVolumeDirectories() {
        return readerVolumeDirectories;
    }
}
