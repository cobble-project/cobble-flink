package io.cobble.flink.monitor;

import io.cobble.flink.common.CobbleNativeSavepoint;

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
    final CobbleNativeSavepoint.OperatorSnapshot nativeSavepoint;

    OperatorEntry(String operatorId, String manifestCopyPath, String operatorSnapshotDirectory) {
        this(
                operatorId,
                manifestCopyPath,
                operatorSnapshotDirectory,
                Collections.singletonList(operatorSnapshotDirectory),
                false,
                null);
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
                false,
                null);
    }

    OperatorEntry(
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
            CobbleNativeSavepoint.OperatorSnapshot nativeSavepoint) {
        this.operatorId = operatorId;
        this.manifestCopyPath = manifestCopyPath;
        this.operatorSnapshotDirectory = operatorSnapshotDirectory;
        this.readerVolumeDirectories = readerVolumeDirectories;
        this.globalSnapshotLayout = globalSnapshotLayout;
        this.nativeSavepoint = nativeSavepoint;
    }

    static OperatorEntry nativeSavepoint(CobbleNativeSavepoint.OperatorSnapshot snapshot) {
        return new OperatorEntry(
                snapshot.operatorId(), null, null, Collections.emptyList(), false, snapshot);
    }

    Map<String, Object> toJson() {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("operator_id", operatorId);
        output.put("manifest_copy_path", manifestCopyPath);
        output.put("operator_snapshot_directory", operatorSnapshotDirectory);
        output.put("reader_volume_directories", readerVolumeDirectories);
        output.put("global_snapshot_layout", globalSnapshotLayout);
        output.put("native_savepoint", nativeSavepoint != null);
        return output;
    }
}
