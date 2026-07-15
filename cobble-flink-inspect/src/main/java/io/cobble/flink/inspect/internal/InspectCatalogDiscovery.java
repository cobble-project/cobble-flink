package io.cobble.flink.inspect.internal;

import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleEmbeddedCheckpoint;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Checkpoint/savepoint and datasource discovery owned by the inspect SDK. */
public final class InspectCatalogDiscovery {
    private InspectCatalogDiscovery() {}

    public static Result discover(String source, CobbleConnectorStorageOptions storageOptions) {
        Path entry = new Path(InspectPathUtils.normalizeStorageDirectory(source));
        boolean checkpointSignal = hasCheckpointSignal(entry);
        try {
            List<CobbleEmbeddedCheckpoint.Location> locations =
                    CobbleEmbeddedCheckpoint.locate(entry);
            List<CheckpointEntry> checkpoints = new ArrayList<>();
            for (CobbleEmbeddedCheckpoint.Location location : locations) {
                List<OperatorEntry> operators = new ArrayList<>();
                for (CobbleEmbeddedCheckpoint.OperatorSnapshot operator :
                        location.checkpoint().operators().values()) {
                    operators.add(OperatorEntry.embeddedCheckpoint(operator));
                }
                operators.sort(Comparator.comparing(value -> value.operatorId));
                checkpoints.add(
                        new CheckpointEntry(
                                location.checkpoint().checkpointId(),
                                InspectPathUtils.pathToStorageString(
                                        location.checkpointDirectory()),
                                operators));
            }
            checkpoints.sort(
                    Comparator.comparingLong((CheckpointEntry value) -> value.id).reversed());
            return new Result("checkpoint", source, checkpoints);
        } catch (IOException | RuntimeException checkpointFailure) {
            if (checkpointSignal) {
                throw new InspectInputException(
                        "Failed to read Cobble checkpoint metadata from "
                                + source
                                + ": "
                                + checkpointFailure.getMessage());
            }
            try {
                List<CheckpointEntry> checkpoints =
                        CobbleDataSourceDiscovery.discover(source, storageOptions);
                return new Result("data_source", source, checkpoints);
            } catch (RuntimeException dataSourceFailure) {
                throw new InspectInputException(
                        checkpointFailure.getMessage()
                                + " Also failed to open as Cobble datasource: "
                                + dataSourceFailure.getMessage());
            }
        }
    }

    private static boolean hasCheckpointSignal(Path entry) {
        try {
            FileSystem fileSystem = entry.getFileSystem();
            if (!fileSystem.exists(entry)) {
                return false;
            }
            FileStatus status = fileSystem.getFileStatus(entry);
            if (!status.isDir()) {
                return "_metadata".equals(entry.getName());
            }
            String name = entry.getName();
            if (name.startsWith("chk-") || fileSystem.exists(new Path(entry, "_metadata"))) {
                return true;
            }
            FileStatus[] children = fileSystem.listStatus(entry);
            if (children != null) {
                for (FileStatus child : children) {
                    if (child.isDir() && child.getPath().getName().startsWith("chk-")) {
                        return true;
                    }
                }
            }
        } catch (IOException ignored) {
            // Let the normal reader surface the IO error with its path context.
        }
        return false;
    }

    public static final class Result {
        public final String sourceKind;
        public final String rootDirectory;
        public final List<CheckpointEntry> checkpoints;

        private Result(String sourceKind, String rootDirectory, List<CheckpointEntry> checkpoints) {
            this.sourceKind = sourceKind;
            this.rootDirectory = rootDirectory;
            this.checkpoints = checkpoints;
        }
    }
}
