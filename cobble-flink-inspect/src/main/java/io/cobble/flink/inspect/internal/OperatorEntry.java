package io.cobble.flink.inspect.internal;

import io.cobble.flink.common.CobbleEmbeddedCheckpoint;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OperatorEntry {
    public final String operatorId;
    public final String manifestCopyPath;
    public final String operatorSnapshotDirectory;
    public final List<String> readerVolumeDirectories;
    public final boolean globalSnapshotLayout;
    public final CobbleEmbeddedCheckpoint.OperatorSnapshot embeddedCheckpoint;

    public OperatorEntry(
            String operatorId, String manifestCopyPath, String operatorSnapshotDirectory) {
        this(
                operatorId,
                manifestCopyPath,
                operatorSnapshotDirectory,
                Collections.singletonList(operatorSnapshotDirectory),
                false,
                null);
    }

    public OperatorEntry(
            String operatorId,
            String manifestCopyPath,
            String operatorSnapshotDirectory,
            List<String> readerVolumeDirectories) {
        this(
                operatorId,
                manifestCopyPath,
                operatorSnapshotDirectory,
                readerVolumeDirectories,
                false,
                null);
    }

    public OperatorEntry(
            String operatorId,
            String manifestCopyPath,
            String operatorSnapshotDirectory,
            List<String> readerVolumeDirectories,
            boolean globalSnapshotLayout) {
        this(
                operatorId,
                manifestCopyPath,
                operatorSnapshotDirectory,
                readerVolumeDirectories,
                globalSnapshotLayout,
                null);
    }

    private OperatorEntry(
            String operatorId,
            String manifestCopyPath,
            String operatorSnapshotDirectory,
            List<String> readerVolumeDirectories,
            boolean globalSnapshotLayout,
            CobbleEmbeddedCheckpoint.OperatorSnapshot embeddedCheckpoint) {
        this.operatorId = operatorId;
        this.manifestCopyPath = manifestCopyPath;
        this.operatorSnapshotDirectory = operatorSnapshotDirectory;
        this.readerVolumeDirectories = readerVolumeDirectories;
        this.globalSnapshotLayout = globalSnapshotLayout;
        this.embeddedCheckpoint = embeddedCheckpoint;
    }

    public static OperatorEntry embeddedCheckpoint(
            CobbleEmbeddedCheckpoint.OperatorSnapshot snapshot) {
        return new OperatorEntry(
                snapshot.operatorId(), null, null, Collections.emptyList(), false, snapshot);
    }

    public OperatorEntry withEmbeddedCheckpoint(
            CobbleEmbeddedCheckpoint.OperatorSnapshot snapshot) {
        if (!operatorId.equals(snapshot.operatorId())) {
            throw new IllegalArgumentException(
                    "Cannot attach embedded checkpoint metadata for "
                            + snapshot.operatorId()
                            + " to operator "
                            + operatorId);
        }
        return new OperatorEntry(
                operatorId,
                manifestCopyPath,
                operatorSnapshotDirectory,
                readerVolumeDirectories,
                globalSnapshotLayout,
                snapshot);
    }

    public Map<String, Object> toJson() {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("operator_id", operatorId);
        output.put("manifest_copy_path", manifestCopyPath);
        output.put("operator_snapshot_directory", operatorSnapshotDirectory);
        output.put("reader_volume_directories", readerVolumeDirectories);
        output.put("global_snapshot_layout", globalSnapshotLayout);
        output.put("embedded_checkpoint", embeddedCheckpoint != null);
        return output;
    }
}
