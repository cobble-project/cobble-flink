package io.cobble.flink.monitor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class OperatorEntry {
    final String operatorId;
    final String manifestCopyPath;
    final String operatorSnapshotDirectory;
    final List<String> readerVolumeDirectories;
    final boolean globalSnapshotLayout;

    OperatorEntry(String operatorId, String manifestCopyPath, String operatorSnapshotDirectory) {
        this(
                operatorId,
                manifestCopyPath,
                operatorSnapshotDirectory,
                Collections.singletonList(operatorSnapshotDirectory),
                false);
    }

    OperatorEntry(
            String operatorId,
            String manifestCopyPath,
            String operatorSnapshotDirectory,
            List<String> readerVolumeDirectories) {
        this(
                operatorId,
                manifestCopyPath,
                operatorSnapshotDirectory,
                readerVolumeDirectories,
                false);
    }

    OperatorEntry(
            String operatorId,
            String manifestCopyPath,
            String operatorSnapshotDirectory,
            List<String> readerVolumeDirectories,
            boolean globalSnapshotLayout) {
        this.operatorId = operatorId;
        this.manifestCopyPath = manifestCopyPath;
        this.operatorSnapshotDirectory = operatorSnapshotDirectory;
        this.readerVolumeDirectories = readerVolumeDirectories;
        this.globalSnapshotLayout = globalSnapshotLayout;
    }

    Map<String, Object> toJson() {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("operator_id", operatorId);
        output.put("manifest_copy_path", manifestCopyPath);
        output.put("operator_snapshot_directory", operatorSnapshotDirectory);
        output.put("reader_volume_directories", readerVolumeDirectories);
        output.put("global_snapshot_layout", globalSnapshotLayout);
        return output;
    }
}
