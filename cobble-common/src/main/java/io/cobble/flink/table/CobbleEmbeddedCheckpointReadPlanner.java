package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.SnapshotTools;
import io.cobble.flink.common.CobbleEmbeddedCheckpoint;
import io.cobble.flink.common.CobbleFlinkStorageConfig;
import io.cobble.flink.common.CobbleSnapshotVolumeRoots;
import io.cobble.flink.common.CobbleStateReadFormatMetadata;
import io.cobble.table.TablePathRequest;
import io.cobble.table.TableReadSnapshot;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/** Fixed checkpoint-path planning helper owned by the Flink state table-format plugin. */
final class CobbleEmbeddedCheckpointReadPlanner {
    public static final String CHECKPOINT_ID_OPTION = "flink.checkpoint-id";
    public static final String OPERATOR_ID_OPTION = "flink.operator-id";

    static Optional<TableReadSnapshot> resolve(Config config, TablePathRequest request)
            throws Exception {
        return resolve(config, request, CobbleFlinkStorageConfig.empty());
    }

    static Optional<TableReadSnapshot> resolve(
            Config config, TablePathRequest request, CobbleFlinkStorageConfig storageOptions)
            throws Exception {
        Path entry;
        try {
            entry = new Path(request.path());
        } catch (IllegalArgumentException error) {
            return Optional.empty();
        }
        if (!looksLikeCheckpointEntry(entry)) return Optional.empty();
        if (!hasExplicitFlinkSelection(request) && hasNativeCurrentPointer(entry)) {
            // A native root can also contain a Flink job/checkpoint subtree. Preserve its native
            // CURRENT semantics unless the caller explicitly asks to resolve the Flink layout.
            return Optional.empty();
        }
        if (request.snapshotId() != null) {
            throw new IOException(
                    "snapshot-id selects a native Cobble global snapshot; use "
                            + CHECKPOINT_ID_OPTION
                            + " for a Flink checkpoint path");
        }
        CobbleEmbeddedCheckpoint.Location location =
                selectLocation(entry, request.option(CHECKPOINT_ID_OPTION));
        String operatorId = selectOperator(location, request.option(OPERATOR_ID_OPTION));
        CobbleEmbeddedCheckpoint.OperatorSnapshot operator =
                location.checkpoint().operator(operatorId);
        if (operator == null) {
            throw new IOException(
                    "selected Cobble operator disappeared from fixed checkpoint metadata");
        }
        if (operator.maxParallelism() <= 0) {
            throw new IOException(
                    "Cobble embedded checkpoint operator '"
                            + operatorId
                            + "' has invalid maxParallelism "
                            + operator.maxParallelism());
        }
        // Metadata may be local while the selected operator's immutable files are remote.
        CobbleSnapshotVolumeRoots.addReadonlyVolumes(
                config, operator.volumeDirectories(), storageOptions);
        String manifest = existingManifest(location, operatorId);
        List<ShardSnapshot> shards = operator.shards();
        GlobalSnapshot fixed =
                manifest == null
                        ? checkpointSnapshot(location, operator.maxParallelism(), shards)
                        : reuseManifest(
                                metadataConfig(config, manifest),
                                manifest,
                                location.checkpoint().checkpointId(),
                                operator.maxParallelism(),
                                shards);
        // The caller reuses this config when opening the fixed physical reader.
        TableReadSnapshot snapshot =
                TableReadSnapshot.forGlobal(config, fixed, request.tableName());
        if (!CobbleStateReadFormatMetadata.FORMAT_ID.equals(snapshot.formatId())) {
            throw new IOException(
                    "Flink checkpoint state '"
                            + request.tableName()
                            + "' has unsupported table format '"
                            + snapshot.formatId()
                            + "'");
        }
        return Optional.of(snapshot);
    }

    static CobbleEmbeddedCheckpoint.Location selectLocation(Path entry, String requested)
            throws IOException {
        if (entry.getName().startsWith("SNAPSHOT-")) {
            List<CobbleEmbeddedCheckpoint.Location> locations =
                    CobbleEmbeddedCheckpoint.locate(entry);
            return selectLoaded(entry, locations, requested);
        }
        List<CobbleEmbeddedCheckpoint.MetadataLocation> locations =
                CobbleEmbeddedCheckpoint.locateMetadataPaths(entry);
        CobbleEmbeddedCheckpoint.MetadataLocation selected =
                selectMetadata(entry, locations, requested);
        CobbleEmbeddedCheckpoint.Location loaded =
                CobbleEmbeddedCheckpoint.readLocation(selected.metadataPath());
        Long directoryId = selected.directoryCheckpointId();
        if (directoryId != null && loaded.checkpoint().checkpointId() != directoryId.longValue()) {
            throw new IOException(
                    "checkpoint metadata id "
                            + loaded.checkpoint().checkpointId()
                            + " does not match directory "
                            + selected.checkpointDirectory());
        }
        if (requested != null && !requested.trim().isEmpty() && !"latest".equals(requested)) {
            long requestedId = parseRequestedCheckpointId(requested);
            if (loaded.checkpoint().checkpointId() != requestedId) {
                throw new IOException(
                        "Flink checkpoint "
                                + requestedId
                                + " does not match metadata id "
                                + loaded.checkpoint().checkpointId()
                                + " at "
                                + selected.metadataPath());
            }
        }
        return loaded;
    }

    static CobbleEmbeddedCheckpoint.MetadataLocation selectMetadata(
            Path entry, List<CobbleEmbeddedCheckpoint.MetadataLocation> locations, String requested)
            throws IOException {
        if (locations.isEmpty()) {
            throw new IOException("no completed checkpoint metadata was found at " + entry);
        }
        requireSingleJobTree(entry, locations);
        if (requested != null && !requested.trim().isEmpty() && !"latest".equals(requested)) {
            long id = parseRequestedCheckpointId(requested);
            List<CobbleEmbeddedCheckpoint.MetadataLocation> matching = new ArrayList<>();
            for (CobbleEmbeddedCheckpoint.MetadataLocation location : locations) {
                if (Long.valueOf(id).equals(location.directoryCheckpointId()))
                    matching.add(location);
            }
            if (matching.size() == 1) return matching.get(0);
            if (matching.isEmpty() && locations.size() == 1) return locations.get(0);
            throw new IOException(
                    "Flink checkpoint "
                            + id
                            + " is "
                            + (matching.isEmpty() ? "not present" : "ambiguous")
                            + " under "
                            + entry
                            + ". Available directory ids: "
                            + checkpointDirectoryIds(locations));
        }
        List<CobbleEmbeddedCheckpoint.MetadataLocation> numbered = new ArrayList<>();
        for (CobbleEmbeddedCheckpoint.MetadataLocation location : locations) {
            if (location.directoryCheckpointId() != null) numbered.add(location);
        }
        if (numbered.isEmpty()) {
            if (locations.size() == 1) return locations.get(0);
            throw new IOException(
                    "Flink checkpoint path "
                            + entry
                            + " has multiple unnumbered checkpoint metadata files; set "
                            + CHECKPOINT_ID_OPTION);
        }
        Collections.sort(
                numbered,
                Comparator.comparingLong(value -> value.directoryCheckpointId().longValue()));
        return numbered.get(numbered.size() - 1);
    }

    private static CobbleEmbeddedCheckpoint.Location selectLoaded(
            Path entry, List<CobbleEmbeddedCheckpoint.Location> locations, String requested)
            throws IOException {
        if (locations.isEmpty()) {
            throw new IOException(
                    "no readable Cobble embedded checkpoint metadata was found at " + entry);
        }
        requireSingleLoadedJobTree(entry, locations);
        if (requested != null && !requested.trim().isEmpty() && !"latest".equals(requested)) {
            final long id = parseRequestedCheckpointId(requested);
            List<CobbleEmbeddedCheckpoint.Location> matching = new ArrayList<>();
            for (CobbleEmbeddedCheckpoint.Location location : locations) {
                if (location.checkpoint().checkpointId() == id) matching.add(location);
            }
            if (matching.size() != 1) {
                throw new IOException(
                        "Flink checkpoint "
                                + id
                                + " is "
                                + (matching.isEmpty() ? "not present" : "ambiguous")
                                + " under "
                                + entry
                                + ". Available ids: "
                                + checkpointIds(locations));
            }
            return matching.get(0);
        }
        if (locations.size() == 1) return locations.get(0);
        List<CobbleEmbeddedCheckpoint.Location> ordered = new ArrayList<>(locations);
        Collections.sort(
                ordered, Comparator.comparingLong(value -> value.checkpoint().checkpointId()));
        return ordered.get(ordered.size() - 1);
    }

    private static long parseRequestedCheckpointId(String requested) throws IOException {
        try {
            long id = Long.parseLong(requested);
            if (id <= 0L) throw new NumberFormatException(requested);
            return id;
        } catch (NumberFormatException error) {
            throw new IOException(
                    CHECKPOINT_ID_OPTION + " must be 'latest' or a positive checkpoint id", error);
        }
    }

    private static void requireSingleJobTree(
            Path entry, List<CobbleEmbeddedCheckpoint.MetadataLocation> locations)
            throws IOException {
        String parent = null;
        for (CobbleEmbeddedCheckpoint.MetadataLocation location : locations) {
            Path checkpointParent = location.checkpointDirectory().getParent();
            String candidate = checkpointParent == null ? "" : checkpointParent.toString();
            if (parent == null) parent = candidate;
            else if (!parent.equals(candidate)) {
                throw new IOException(
                        "Flink checkpoint path "
                                + entry
                                + " contains multiple job checkpoint trees; set "
                                + "a path scoped to one job directory");
            }
        }
    }

    private static void requireSingleLoadedJobTree(
            Path entry, List<CobbleEmbeddedCheckpoint.Location> locations) throws IOException {
        String parent = null;
        for (CobbleEmbeddedCheckpoint.Location location : locations) {
            Path checkpointParent = location.checkpointDirectory().getParent();
            String candidate = checkpointParent == null ? "" : checkpointParent.toString();
            if (parent == null) parent = candidate;
            else if (!parent.equals(candidate)) {
                throw new IOException(
                        "Flink checkpoint path "
                                + entry
                                + " contains multiple job checkpoint trees; set "
                                + "a path scoped to one job directory");
            }
        }
    }

    private static String selectOperator(
            CobbleEmbeddedCheckpoint.Location location, String requested) throws IOException {
        List<String> operators = new ArrayList<>(location.checkpoint().operators().keySet());
        Collections.sort(operators);
        if (requested != null && !requested.trim().isEmpty()) {
            if (!operators.contains(requested)) {
                throw new IOException(
                        "Cobble operator '"
                                + requested
                                + "' was not found in checkpoint "
                                + location.checkpoint().checkpointId()
                                + ". Available operators: "
                                + String.join(", ", operators));
            }
            return requested;
        }
        if (operators.size() != 1) {
            throw new IOException(
                    "Flink checkpoint "
                            + location.checkpoint().checkpointId()
                            + " has multiple Cobble operators; set "
                            + OPERATOR_ID_OPTION
                            + ". Available operators: "
                            + String.join(", ", operators));
        }
        return operators.get(0);
    }

    private static String existingManifest(
            CobbleEmbeddedCheckpoint.Location location, String operatorId) throws IOException {
        Path checkpoint = location.checkpointDirectory();
        Path direct = new Path(checkpoint, "COBBLE-SNAPSHOT-" + operatorId + "-MANIFEST");
        if (exists(direct)) return direct.toString();
        Path root = checkpoint.getName().startsWith("chk-") ? checkpoint.getParent() : checkpoint;
        if (root == null) return null;
        Path staged =
                new Path(
                        new Path(new Path(new Path(root, "cobble"), operatorId), "snapshot"),
                        "SNAPSHOT-" + location.checkpoint().checkpointId());
        return exists(staged) ? staged.toString() : null;
    }

    private static GlobalSnapshot checkpointSnapshot(
            CobbleEmbeddedCheckpoint.Location location,
            int totalBuckets,
            List<ShardSnapshot> shards)
            throws IOException {
        try {
            return attachCheckpointSchemas(
                    SnapshotTools.buildGlobalSnapshot(
                            totalBuckets, location.checkpoint().checkpointId(), shards),
                    shards);
        } catch (RuntimeException error) {
            throw new IOException(
                    "cannot build fixed Cobble global manifest from checkpoint shards", error);
        }
    }

    private static GlobalSnapshot reuseManifest(
            Config config,
            String manifestPath,
            long checkpointId,
            int totalBuckets,
            List<ShardSnapshot> checkpointShards)
            throws IOException {
        final GlobalSnapshot persisted;
        try {
            persisted = SnapshotTools.loadGlobalSnapshot(config, manifestPath);
        } catch (RuntimeException error) {
            throw new IOException(
                    "cannot read existing Cobble global manifest " + manifestPath, error);
        }
        if (persisted.id != checkpointId) {
            throw new IOException(
                    "existing Cobble global manifest id "
                            + persisted.id
                            + " does not match checkpoint "
                            + checkpointId);
        }
        if (persisted.totalBuckets != totalBuckets) {
            throw new IOException(
                    "existing Cobble global manifest bucket count "
                            + persisted.totalBuckets
                            + " does not match operator max parallelism "
                            + totalBuckets);
        }
        if (persisted.shardSnapshots.size() != checkpointShards.size()) {
            throw new IOException(
                    "existing Cobble global manifest shard references differ from checkpoint");
        }
        return attachCheckpointSchemas(persisted, checkpointShards);
    }

    private static GlobalSnapshot attachCheckpointSchemas(
            GlobalSnapshot fixed, List<ShardSnapshot> checkpointShards) throws IOException {
        List<ShardSnapshot> merged = new ArrayList<ShardSnapshot>(fixed.shardSnapshots.size());
        for (ShardSnapshot persistedShard : fixed.shardSnapshots) {
            ShardSnapshot checkpointShard = matchingShard(checkpointShards, persistedShard);
            if (checkpointShard == null) {
                throw new IOException(
                        "existing Cobble global manifest references a shard not owned by checkpoint");
            }
            ShardSnapshot copy = persistedShard.copy();
            copy.columnFamilies = checkpointShard.copy().columnFamilies;
            merged.add(copy);
        }
        fixed.shardSnapshots = merged;
        return fixed;
    }

    private static ShardSnapshot matchingShard(
            List<ShardSnapshot> candidates, ShardSnapshot expected) {
        for (ShardSnapshot candidate : candidates) {
            if (candidate.dbId.equals(expected.dbId)
                    && candidate.snapshotId == expected.snapshotId
                    && normalizePath(candidate.manifestPath)
                            .equals(normalizePath(expected.manifestPath))
                    && sameRanges(candidate.ranges, expected.ranges)) {
                return candidate;
            }
        }
        return null;
    }

    private static boolean sameRanges(
            List<ShardSnapshot.Range> left, List<ShardSnapshot.Range> right) {
        if (left.size() != right.size()) return false;
        for (int index = 0; index < left.size(); index++) {
            if (left.get(index).start != right.get(index).start
                    || left.get(index).end != right.get(index).end) return false;
        }
        return true;
    }

    private static Config metadataConfig(Config source, String manifestPath) {
        Config config = source.copy();
        if (containsMetadataPath(config, manifestPath)) return config;
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = manifestParent(manifestPath);
        volume.kinds = Collections.singletonList(Config.VolumeUsageKind.META);
        CobbleFlinkStorageConfig.empty().register(config, volume);
        return config;
    }

    private static boolean containsMetadataPath(Config config, String path) {
        if (config.volumes == null) return false;
        for (Config.VolumeDescriptor volume : config.volumes) {
            if (volume != null
                    && volume.kinds != null
                    && volume.kinds.contains(Config.VolumeUsageKind.META)
                    && CobbleFlinkStorageConfig.containsPath(volume.baseDir, path)) return true;
        }
        return false;
    }

    private static String manifestParent(String value) {
        try {
            java.net.URI uri = java.net.URI.create(value);
            if ("file".equalsIgnoreCase(uri.getScheme())) {
                java.nio.file.Path parent = java.nio.file.Paths.get(uri).getParent();
                if (parent == null) throw new IllegalArgumentException("manifest has no parent");
                return parent.toAbsolutePath().normalize().toUri().toString();
            }
        } catch (IllegalArgumentException ignored) {
            // Fall through to generic URI/path handling.
        }
        int separator = value.lastIndexOf('/');
        if (separator <= 0) throw new IllegalArgumentException("manifest has no parent: " + value);
        return value.substring(0, separator + 1);
    }

    private static String trimTrailingSlash(String value) {
        while (value.endsWith("/") && value.length() > 1) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    private static String normalizePath(String value) {
        try {
            java.net.URI uri = java.net.URI.create(value);
            if ("file".equalsIgnoreCase(uri.getScheme())) {
                return java.nio.file.Paths.get(uri).toAbsolutePath().normalize().toUri().toString();
            }
            return uri.normalize().toString();
        } catch (IllegalArgumentException error) {
            return value;
        }
    }

    private static boolean exists(Path path) throws IOException {
        FileSystem fs = path.getFileSystem();
        return fs.exists(path);
    }

    private static boolean hasExplicitFlinkSelection(TablePathRequest request) {
        return request.option(CHECKPOINT_ID_OPTION) != null
                || request.option(OPERATOR_ID_OPTION) != null;
    }

    private static boolean hasNativeCurrentPointer(Path entry) throws IOException {
        FileSystem fs = entry.getFileSystem();
        FileStatus status = fs.getFileStatus(entry);
        return status.isDir() && fs.exists(new Path(new Path(entry, "snapshot"), "CURRENT"));
    }

    private static boolean looksLikeCheckpointEntry(Path entry) throws IOException {
        FileSystem fs = entry.getFileSystem();
        if (!fs.exists(entry)) return false;
        String name = entry.getName();
        if ("_metadata".equals(name) || name.startsWith("chk-") || name.startsWith("SNAPSHOT-")) {
            return true;
        }
        FileStatus status = fs.getFileStatus(entry);
        if (!status.isDir()) return false;
        return containsCheckpointMetadata(fs, entry, 0);
    }

    private static boolean containsCheckpointMetadata(FileSystem fs, Path directory, int depth)
            throws IOException {
        if (fs.exists(new Path(directory, "_metadata"))) return true;
        if (depth >= 2) return false;
        FileStatus[] children = fs.listStatus(directory);
        if (children == null) return false;
        for (FileStatus child : children) {
            if (!child.isDir()) continue;
            String name = child.getPath().getName();
            if (name.startsWith("chk-")
                    || containsCheckpointMetadata(fs, child.getPath(), depth + 1)) {
                return true;
            }
        }
        return false;
    }

    private static String checkpointIds(List<CobbleEmbeddedCheckpoint.Location> locations) {
        List<String> ids = new ArrayList<>();
        for (CobbleEmbeddedCheckpoint.Location location : locations) {
            ids.add(Long.toString(location.checkpoint().checkpointId()));
        }
        return String.join(", ", ids);
    }

    private static String checkpointDirectoryIds(
            List<CobbleEmbeddedCheckpoint.MetadataLocation> locations) {
        List<String> ids = new ArrayList<>();
        for (CobbleEmbeddedCheckpoint.MetadataLocation location : locations) {
            ids.add(
                    location.directoryCheckpointId() == null
                            ? location.checkpointDirectory().toString()
                            : location.directoryCheckpointId().toString());
        }
        return String.join(", ", ids);
    }
}
