package io.cobble.flink.inspect.internal;

import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleEmbeddedCheckpoint;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Checkpoint/savepoint and datasource discovery owned by the inspect SDK. */
public final class InspectCatalogDiscovery {
    private static final String CHECKPOINT_PREFIX = "chk-";
    private static final String COBBLE_MANIFEST_PREFIX = "COBBLE-SNAPSHOT-";
    private static final String COBBLE_MANIFEST_SUFFIX = "-MANIFEST";

    private InspectCatalogDiscovery() {}

    public static Result discover(String source, CobbleConnectorStorageOptions storageOptions) {
        try {
            return discoverCheckpoint(source);
        } catch (InspectInputException checkpointFailure) {
            if (hasCheckpointSignal(new Path(InspectPathUtils.normalizeStorageDirectory(source)))) {
                throw checkpointFailure;
            }
            try {
                String normalized = InspectPathUtils.normalizeStorageDirectory(source);
                return new Result(
                        "data_source",
                        normalized,
                        CobbleDataSourceDiscovery.discover(normalized, storageOptions));
            } catch (RuntimeException dataSourceFailure) {
                throw new InspectInputException(
                        checkpointFailure.getMessage()
                                + " Also failed to open as Cobble datasource: "
                                + dataSourceFailure.getMessage());
            }
        }
    }

    private static Result discoverCheckpoint(String source) {
        Path requested = new Path(InspectPathUtils.normalizeStorageDirectory(source));
        FileSystem fileSystem;
        FileStatus requestedStatus;
        try {
            fileSystem = requested.getFileSystem();
            requestedStatus = fileSystem.getFileStatus(requested);
        } catch (IOException e) {
            throw new InspectInputException(
                    "Failed to open checkpoint path " + source + ": " + e.getMessage());
        }

        if (!requestedStatus.isDir()) {
            return embeddedResult(locate(requested));
        }
        if (hasMetadata(fileSystem, requested)) {
            try {
                return embeddedResult(locate(requested));
            } catch (InspectInputException ignored) {
                // Non-Cobble metadata can still accompany a valid sidecar checkpoint.
            }
        }

        Path root = checkpointDirectoryId(requested) == null ? requested : requested.getParent();
        if (root == null) {
            throw new InspectInputException("Checkpoint path has no parent: " + source);
        }
        List<Path> checkpointDirectories = discoverCheckpointDirectories(fileSystem, requested);
        Map<Long, Path> directoriesById = new HashMap<>();
        Map<Long, CheckpointEntry> checkpointsById = new LinkedHashMap<>();
        for (Path checkpointDirectory : checkpointDirectories) {
            Long checkpointId = checkpointDirectoryId(checkpointDirectory);
            if (checkpointId == null || !hasMetadata(fileSystem, checkpointDirectory)) {
                continue;
            }
            directoriesById.put(checkpointId, checkpointDirectory);
            List<OperatorEntry> operators =
                    discoverOperators(fileSystem, root, checkpointDirectory, checkpointId);
            if (!operators.isEmpty()) {
                checkpointsById.put(
                        checkpointId,
                        new CheckpointEntry(
                                checkpointId,
                                InspectPathUtils.pathToStorageString(checkpointDirectory),
                                operators));
            }
        }
        addDetachedOperators(
                directoriesById,
                checkpointsById,
                discoverCobbleGlobalSnapshotOperators(fileSystem, root));
        addDetachedOperators(
                directoriesById,
                checkpointsById,
                discoverSharedSnapshotOperators(fileSystem, root));
        addEmbeddedCheckpointEntries(requested, checkpointsById);

        List<CheckpointEntry> checkpoints = new ArrayList<>(checkpointsById.values());
        checkpoints.sort(Comparator.comparingLong((CheckpointEntry value) -> value.id).reversed());
        if (checkpoints.isEmpty()) {
            throw new InspectInputException(
                    "No Cobble Flink checkpoints found. Accepted paths include a checkpoint root,"
                            + " chk-N directory, _metadata file, or a related SNAPSHOT-N path."
                            + " Expected embedded Cobble metadata, checkpoint manifest copies,"
                            + " or Cobble global/shared snapshot manifests.");
        }
        return new Result("checkpoint", InspectPathUtils.pathToStorageString(root), checkpoints);
    }

    private static Result embeddedResult(List<CobbleEmbeddedCheckpoint.Location> locations) {
        if (locations.isEmpty()) {
            throw new InspectInputException(
                    "No readable Cobble embedded checkpoint metadata found");
        }
        List<CheckpointEntry> checkpoints = new ArrayList<>();
        for (CobbleEmbeddedCheckpoint.Location location : locations) {
            checkpoints.add(embeddedCheckpointEntry(location));
        }
        checkpoints.sort(Comparator.comparingLong((CheckpointEntry value) -> value.id).reversed());
        Path root = locations.get(0).checkpointDirectory().getParent();
        if (root == null) {
            root = locations.get(0).checkpointDirectory();
        }
        return new Result("checkpoint", InspectPathUtils.pathToStorageString(root), checkpoints);
    }

    private static List<CobbleEmbeddedCheckpoint.Location> locate(Path path) {
        try {
            return CobbleEmbeddedCheckpoint.locate(path);
        } catch (IOException | RuntimeException e) {
            throw new InspectInputException(
                    "Failed to read Cobble embedded checkpoint metadata from "
                            + path
                            + ": "
                            + e.getMessage());
        }
    }

    private static CheckpointEntry embeddedCheckpointEntry(
            CobbleEmbeddedCheckpoint.Location location) {
        Path checkpointDirectory = location.checkpointDirectory();
        try {
            FileSystem fileSystem = checkpointDirectory.getFileSystem();
            Path root = checkpointDirectory.getParent();
            List<OperatorEntry> sidecars =
                    discoverOperators(
                            fileSystem,
                            root == null ? checkpointDirectory : root,
                            checkpointDirectory,
                            location.checkpoint().checkpointId());
            return mergeEmbeddedCheckpoint(
                    new CheckpointEntry(
                            location.checkpoint().checkpointId(),
                            InspectPathUtils.pathToStorageString(checkpointDirectory),
                            sidecars),
                    location);
        } catch (RuntimeException | IOException ignored) {
            List<OperatorEntry> operators = new ArrayList<>();
            for (CobbleEmbeddedCheckpoint.OperatorSnapshot operator :
                    location.checkpoint().operators().values()) {
                operators.add(OperatorEntry.embeddedCheckpoint(operator));
            }
            operators.sort(Comparator.comparing(value -> value.operatorId));
            return new CheckpointEntry(
                    location.checkpoint().checkpointId(),
                    InspectPathUtils.pathToStorageString(checkpointDirectory),
                    operators);
        }
    }

    private static void addEmbeddedCheckpointEntries(
            Path requested, Map<Long, CheckpointEntry> checkpointsById) {
        try {
            for (CobbleEmbeddedCheckpoint.Location location :
                    CobbleEmbeddedCheckpoint.locate(requested)) {
                long checkpointId = location.checkpoint().checkpointId();
                CheckpointEntry existing = checkpointsById.get(checkpointId);
                checkpointsById.put(
                        checkpointId,
                        existing == null
                                ? embeddedCheckpointEntry(location)
                                : mergeEmbeddedCheckpoint(existing, location));
            }
        } catch (IOException | RuntimeException ignored) {
            // A sidecar-only checkpoint remains readable without embedded metadata.
        }
    }

    static CheckpointEntry mergeEmbeddedCheckpoint(
            CheckpointEntry existing, CobbleEmbeddedCheckpoint.Location location) {
        Map<String, OperatorEntry> operators = new LinkedHashMap<>();
        for (OperatorEntry operator : existing.operators) {
            operators.put(operator.operatorId, operator);
        }
        for (CobbleEmbeddedCheckpoint.OperatorSnapshot embedded :
                location.checkpoint().operators().values()) {
            OperatorEntry current = operators.get(embedded.operatorId());
            operators.put(
                    embedded.operatorId(),
                    current == null
                            ? OperatorEntry.embeddedCheckpoint(embedded)
                            : current.withEmbeddedCheckpoint(embedded));
        }
        List<OperatorEntry> merged = new ArrayList<>(operators.values());
        merged.sort(Comparator.comparing(value -> value.operatorId));
        return new CheckpointEntry(existing.id, existing.directory, merged);
    }

    private static void addDetachedOperators(
            Map<Long, Path> directoriesById,
            Map<Long, CheckpointEntry> checkpointsById,
            Map<Long, List<OperatorEntry>> operatorsById) {
        for (Map.Entry<Long, List<OperatorEntry>> entry : operatorsById.entrySet()) {
            Path checkpointDirectory = directoriesById.get(entry.getKey());
            if (checkpointDirectory != null && !checkpointsById.containsKey(entry.getKey())) {
                checkpointsById.put(
                        entry.getKey(),
                        new CheckpointEntry(
                                entry.getKey(),
                                InspectPathUtils.pathToStorageString(checkpointDirectory),
                                entry.getValue()));
            }
        }
    }

    private static List<Path> discoverCheckpointDirectories(FileSystem fs, Path root) {
        List<Path> output = new ArrayList<>();
        collectCheckpointDirectories(fs, root, 0, output);
        return output;
    }

    private static void collectCheckpointDirectories(
            FileSystem fs, Path directory, int depth, List<Path> output) {
        if (checkpointDirectoryId(directory) != null) {
            output.add(directory);
            return;
        }
        if (depth >= 4) {
            return;
        }
        for (FileStatus child : list(fs, directory, "checkpoint directory")) {
            if (!child.isDir()) {
                continue;
            }
            String name = child.getPath().getName();
            if ("shared".equals(name)
                    || "taskowned".equals(name)
                    || "cobble".equals(name)
                    || "snapshot".equals(name)) {
                continue;
            }
            collectCheckpointDirectories(fs, child.getPath(), depth + 1, output);
        }
    }

    private static List<OperatorEntry> discoverOperators(
            FileSystem fs, Path root, Path checkpointDirectory, long checkpointId) {
        Map<String, OperatorEntry> operators = new LinkedHashMap<>();
        for (FileStatus status : list(fs, checkpointDirectory, "checkpoint directory")) {
            String fileName = status.getPath().getName();
            if (status.isDir() || !isCobbleManifestCopy(fileName)) {
                continue;
            }
            String operatorId = operatorIdFromManifestCopy(fileName);
            String operatorDirectory =
                    operatorSnapshotDirectory(root, checkpointDirectory, operatorId);
            String manifest =
                    preferStableManifestPath(
                            fs,
                            checkpointId,
                            InspectPathUtils.pathToStorageString(status.getPath()),
                            operatorDirectory);
            operators.put(
                    operatorId,
                    new OperatorEntry(
                            operatorId,
                            manifest,
                            operatorDirectory,
                            readerVolumeDirectories(
                                    fs, root, checkpointDirectory, operatorId, operatorDirectory),
                            true));
        }
        for (Path cobbleRoot : cobbleRootCandidates(root, checkpointDirectory)) {
            if (!directoryExists(fs, cobbleRoot)) {
                continue;
            }
            for (FileStatus operatorStatus : list(fs, cobbleRoot, "Cobble operator directory")) {
                if (!operatorStatus.isDir()) {
                    continue;
                }
                Path operatorDirectory = operatorStatus.getPath();
                Path manifest =
                        new Path(
                                new Path(operatorDirectory, "snapshot"),
                                "SNAPSHOT-" + checkpointId);
                if (!fileExists(fs, manifest)) {
                    continue;
                }
                String operatorId = operatorDirectory.getName();
                String directory = InspectPathUtils.pathToStorageString(operatorDirectory);
                operators.putIfAbsent(
                        operatorId,
                        new OperatorEntry(
                                operatorId,
                                InspectPathUtils.pathToStorageString(manifest),
                                directory,
                                readerVolumeDirectories(
                                        fs, root, checkpointDirectory, operatorId, directory),
                                true));
            }
        }
        discoverSharedOperators(fs, root, checkpointDirectory, checkpointId, operators);
        List<OperatorEntry> output = new ArrayList<>(operators.values());
        output.sort(Comparator.comparing(value -> value.operatorId));
        return output;
    }

    private static Map<Long, List<OperatorEntry>> discoverCobbleGlobalSnapshotOperators(
            FileSystem fs, Path root) {
        List<Path> cobbleRoots = new ArrayList<>();
        collectCobbleRoots(fs, root, 0, cobbleRoots);
        Map<Long, Map<String, OperatorEntry>> byCheckpoint = new LinkedHashMap<>();
        for (Path cobbleRoot : cobbleRoots) {
            for (FileStatus operatorStatus : list(fs, cobbleRoot, "Cobble operator directory")) {
                if (!operatorStatus.isDir()) {
                    continue;
                }
                Path operatorDirectory = operatorStatus.getPath();
                Path snapshotDirectory = new Path(operatorDirectory, "snapshot");
                if (!directoryExists(fs, snapshotDirectory)) {
                    continue;
                }
                for (FileStatus snapshot :
                        list(fs, snapshotDirectory, "Cobble snapshot directory")) {
                    Long checkpointId =
                            snapshot.isDir()
                                    ? null
                                    : snapshotManifestId(snapshot.getPath().getName());
                    if (checkpointId == null
                            || !snapshotManifestLooksGlobal(fs, snapshot.getPath())) {
                        continue;
                    }
                    String operatorId = operatorDirectory.getName();
                    String directory = InspectPathUtils.pathToStorageString(operatorDirectory);
                    byCheckpoint
                            .computeIfAbsent(checkpointId, ignored -> new LinkedHashMap<>())
                            .putIfAbsent(
                                    operatorId,
                                    new OperatorEntry(
                                            operatorId,
                                            InspectPathUtils.pathToStorageString(
                                                    snapshot.getPath()),
                                            directory,
                                            readerVolumeDirectories(
                                                    fs,
                                                    cobbleRoot.getParent(),
                                                    null,
                                                    operatorId,
                                                    directory),
                                            true));
                }
            }
        }
        return flatten(byCheckpoint);
    }

    private static void collectCobbleRoots(
            FileSystem fs, Path directory, int depth, List<Path> output) {
        if ("cobble".equals(directory.getName())) {
            output.add(directory);
            return;
        }
        if (depth >= 3 || checkpointDirectoryId(directory) != null) {
            return;
        }
        for (FileStatus child : list(fs, directory, "checkpoint directory")) {
            if (!child.isDir()) {
                continue;
            }
            String name = child.getPath().getName();
            if ("shared".equals(name)
                    || "taskowned".equals(name)
                    || name.startsWith(CHECKPOINT_PREFIX)) {
                continue;
            }
            collectCobbleRoots(fs, child.getPath(), depth + 1, output);
        }
    }

    private static Map<Long, List<OperatorEntry>> discoverSharedSnapshotOperators(
            FileSystem fs, Path root) {
        Map<Long, Map<String, OperatorEntry>> byCheckpoint = new LinkedHashMap<>();
        for (Path candidate : sharedRootCandidates(root, root)) {
            Path shared = new Path(candidate, "shared");
            if (directoryExists(fs, shared)) {
                discoverAllSharedSnapshots(fs, shared, byCheckpoint);
            }
        }
        return flatten(byCheckpoint);
    }

    private static void discoverAllSharedSnapshots(
            FileSystem fs, Path shared, Map<Long, Map<String, OperatorEntry>> byCheckpoint) {
        for (FileStatus operatorStatus : list(fs, shared, "Flink shared directory")) {
            if (!operatorStatus.isDir()) {
                continue;
            }
            for (FileStatus volumeStatus :
                    list(fs, operatorStatus.getPath(), "shared operator directory")) {
                if (!volumeStatus.isDir()) {
                    continue;
                }
                Path snapshotDirectory = new Path(volumeStatus.getPath(), "snapshot");
                if (!directoryExists(fs, snapshotDirectory)) {
                    continue;
                }
                for (FileStatus snapshot :
                        list(fs, snapshotDirectory, "shared snapshot directory")) {
                    Long checkpointId =
                            snapshot.isDir()
                                    ? null
                                    : snapshotManifestId(snapshot.getPath().getName());
                    if (checkpointId == null
                            || !snapshotManifestLooksGlobal(fs, snapshot.getPath())) {
                        continue;
                    }
                    Map<String, OperatorEntry> operators =
                            byCheckpoint.computeIfAbsent(
                                    checkpointId, ignored -> new LinkedHashMap<>());
                    String operatorId =
                            uniqueOperatorId(
                                    operators,
                                    operatorStatus.getPath().getName(),
                                    volumeStatus.getPath().getName(),
                                    volumeStatus.getPath());
                    operators.putIfAbsent(
                            operatorId,
                            new OperatorEntry(
                                    operatorId,
                                    InspectPathUtils.pathToStorageString(snapshot.getPath()),
                                    InspectPathUtils.pathToStorageString(volumeStatus.getPath()),
                                    Collections.singletonList(
                                            InspectPathUtils.pathToStorageString(
                                                    volumeStatus.getPath())),
                                    true));
                }
            }
        }
    }

    private static void discoverSharedOperators(
            FileSystem fs,
            Path root,
            Path checkpointDirectory,
            long checkpointId,
            Map<String, OperatorEntry> operators) {
        for (Path candidate : sharedRootCandidates(root, checkpointDirectory)) {
            Path shared = new Path(candidate, "shared");
            if (!directoryExists(fs, shared)) {
                continue;
            }
            for (FileStatus operatorStatus : list(fs, shared, "Flink shared directory")) {
                if (!operatorStatus.isDir()) {
                    continue;
                }
                for (FileStatus volumeStatus :
                        list(fs, operatorStatus.getPath(), "shared operator directory")) {
                    if (!volumeStatus.isDir()) {
                        continue;
                    }
                    Path manifest =
                            new Path(
                                    new Path(volumeStatus.getPath(), "snapshot"),
                                    "SNAPSHOT-" + checkpointId);
                    if (!fileExists(fs, manifest) || !snapshotManifestLooksGlobal(fs, manifest)) {
                        continue;
                    }
                    String operatorId =
                            uniqueOperatorId(
                                    operators,
                                    operatorStatus.getPath().getName(),
                                    volumeStatus.getPath().getName(),
                                    volumeStatus.getPath());
                    operators.putIfAbsent(
                            operatorId,
                            new OperatorEntry(
                                    operatorId,
                                    InspectPathUtils.pathToStorageString(manifest),
                                    InspectPathUtils.pathToStorageString(volumeStatus.getPath())));
                }
            }
        }
    }

    private static Map<Long, List<OperatorEntry>> flatten(
            Map<Long, Map<String, OperatorEntry>> input) {
        Map<Long, List<OperatorEntry>> output = new LinkedHashMap<>();
        for (Map.Entry<Long, Map<String, OperatorEntry>> entry : input.entrySet()) {
            List<OperatorEntry> operators = new ArrayList<>(entry.getValue().values());
            operators.sort(Comparator.comparing(value -> value.operatorId));
            output.put(entry.getKey(), operators);
        }
        return output;
    }

    private static List<String> readerVolumeDirectories(
            FileSystem fs,
            Path root,
            Path checkpointDirectory,
            String operatorId,
            String operatorDirectory) {
        Map<String, String> volumes = new LinkedHashMap<>();
        volumes.put(operatorDirectory, operatorDirectory);
        Path base = checkpointDirectory == null ? root : checkpointDirectory;
        for (Path candidate : sharedRootCandidates(root, base)) {
            Path sharedOperator = new Path(new Path(candidate, "shared"), "op_" + operatorId);
            if (directoryExists(fs, sharedOperator)) {
                String value = InspectPathUtils.pathToStorageString(sharedOperator);
                volumes.putIfAbsent(value, value);
            }
        }
        return new ArrayList<>(volumes.values());
    }

    private static String preferStableManifestPath(
            FileSystem fs, long checkpointId, String copy, String operatorDirectory) {
        Path stable = new Path(new Path(operatorDirectory, "snapshot"), "SNAPSHOT-" + checkpointId);
        return fileExists(fs, stable) ? InspectPathUtils.pathToStorageString(stable) : copy;
    }

    private static List<Path> sharedRootCandidates(Path root, Path checkpointDirectory) {
        Map<String, Path> candidates = new LinkedHashMap<>();
        addCandidate(candidates, root);
        addCandidate(
                candidates, checkpointDirectory == null ? null : checkpointDirectory.getParent());
        Path parent = checkpointDirectory == null ? null : checkpointDirectory.getParent();
        addCandidate(candidates, parent == null ? null : parent.getParent());
        return new ArrayList<>(candidates.values());
    }

    private static List<Path> cobbleRootCandidates(Path root, Path checkpointDirectory) {
        Map<String, Path> candidates = new LinkedHashMap<>();
        Path parent = checkpointDirectory.getParent();
        addCandidate(candidates, parent == null ? null : new Path(parent, "cobble"));
        addCandidate(candidates, new Path(root, "cobble"));
        return new ArrayList<>(candidates.values());
    }

    private static String operatorSnapshotDirectory(
            Path root, Path checkpointDirectory, String operatorId) {
        Path parent = checkpointDirectory.getParent();
        Path base = parent == null ? root : parent;
        return InspectPathUtils.pathToStorageString(new Path(new Path(base, "cobble"), operatorId));
    }

    private static String uniqueOperatorId(
            Map<String, OperatorEntry> operators,
            String operatorId,
            String volumeId,
            Path volumeDirectory) {
        OperatorEntry existing = operators.get(operatorId);
        if (existing == null
                || existing.operatorSnapshotDirectory.equals(
                        InspectPathUtils.pathToStorageString(volumeDirectory))) {
            return operatorId;
        }
        return operatorId + "/" + volumeId;
    }

    private static boolean snapshotManifestLooksGlobal(FileSystem fs, Path manifest) {
        byte[] buffer = new byte[8192];
        int count;
        try (InputStream input = fs.open(manifest)) {
            count = input.read(buffer);
        } catch (IOException e) {
            throw new InspectInputException(
                    "Failed to inspect Cobble snapshot manifest "
                            + manifest
                            + ": "
                            + e.getMessage());
        }
        if (count <= 0) {
            return false;
        }
        String prefix = new String(buffer, 0, count, StandardCharsets.UTF_8);
        return prefix.contains("\"total_buckets\"") || prefix.contains("\"totalBuckets\"");
    }

    private static FileStatus[] list(FileSystem fs, Path path, String label) {
        try {
            FileStatus[] statuses = fs.listStatus(path);
            return statuses == null ? new FileStatus[0] : statuses;
        } catch (IOException e) {
            throw new InspectInputException(
                    "Failed to list " + label + " " + path + ": " + e.getMessage());
        }
    }

    private static boolean hasMetadata(FileSystem fs, Path directory) {
        return fileExists(fs, new Path(directory, "_metadata"));
    }

    private static boolean fileExists(FileSystem fs, Path path) {
        try {
            return fs.exists(path) && !fs.getFileStatus(path).isDir();
        } catch (IOException e) {
            throw new InspectInputException("Failed to inspect " + path + ": " + e.getMessage());
        }
    }

    private static boolean directoryExists(FileSystem fs, Path path) {
        try {
            return fs.exists(path) && fs.getFileStatus(path).isDir();
        } catch (IOException e) {
            throw new InspectInputException("Failed to inspect " + path + ": " + e.getMessage());
        }
    }

    private static void addCandidate(Map<String, Path> candidates, Path path) {
        if (path != null) {
            candidates.putIfAbsent(path.toString(), path);
        }
    }

    private static boolean isCobbleManifestCopy(String name) {
        return name.startsWith(COBBLE_MANIFEST_PREFIX)
                && name.endsWith(COBBLE_MANIFEST_SUFFIX)
                && name.length()
                        > COBBLE_MANIFEST_PREFIX.length() + COBBLE_MANIFEST_SUFFIX.length();
    }

    private static String operatorIdFromManifestCopy(String name) {
        return name.substring(
                COBBLE_MANIFEST_PREFIX.length(), name.length() - COBBLE_MANIFEST_SUFFIX.length());
    }

    private static Long checkpointDirectoryId(Path directory) {
        return numericSuffix(directory.getName(), CHECKPOINT_PREFIX);
    }

    private static Long snapshotManifestId(String name) {
        return numericSuffix(name, "SNAPSHOT-");
    }

    private static Long numericSuffix(String name, String prefix) {
        if (!name.startsWith(prefix) || name.length() == prefix.length()) {
            return null;
        }
        String value = name.substring(prefix.length());
        for (int index = 0; index < value.length(); index++) {
            if (!Character.isDigit(value.charAt(index))) {
                return null;
            }
        }
        try {
            return Long.valueOf(value);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static boolean hasCheckpointSignal(Path entry) {
        try {
            FileSystem fs = entry.getFileSystem();
            if (!fs.exists(entry)) {
                return false;
            }
            FileStatus status = fs.getFileStatus(entry);
            if (!status.isDir()) {
                return "_metadata".equals(entry.getName())
                        || snapshotManifestId(entry.getName()) != null;
            }
            if (checkpointDirectoryId(entry) != null || hasMetadata(fs, entry)) {
                return true;
            }
            return !discoverCheckpointDirectories(fs, entry).isEmpty();
        } catch (RuntimeException | IOException ignored) {
            return false;
        }
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
