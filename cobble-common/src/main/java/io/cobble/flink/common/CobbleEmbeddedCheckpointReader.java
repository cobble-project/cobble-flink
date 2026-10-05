package io.cobble.flink.common;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.Reader;
import io.cobble.ShardSnapshot;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Opens a fixed embedded Flink checkpoint without mutating its original storage.
 *
 * <p>Workers stage only global/shard/schema metadata locally. SST and value files remain on their
 * original configured volumes. When an exact global manifest is supplied, it is validated and
 * reused; otherwise a local metadata-only global manifest is synthesized from fixed shard refs.
 */
public final class CobbleEmbeddedCheckpointReader {
    private CobbleEmbeddedCheckpointReader() {}

    public static Prepared open(
            Config sourceConfig,
            long globalSnapshotId,
            int totalBuckets,
            List<ShardSnapshot> expectedShards,
            String existingGlobalManifestPath)
            throws IOException {
        return open(
                sourceConfig,
                globalSnapshotId,
                totalBuckets,
                expectedShards,
                existingGlobalManifestPath,
                null);
    }

    /**
     * Opens a fixed embedded checkpoint while excluding an input metadata file from native data
     * volumes. The location remains available to Flink staging, but an {@code _metadata} or {@code
     * SNAPSHOT-N} file is never treated as a Cobble volume root.
     */
    public static Prepared open(
            Config sourceConfig,
            long globalSnapshotId,
            int totalBuckets,
            List<ShardSnapshot> expectedShards,
            String existingGlobalManifestPath,
            String inputLocation)
            throws IOException {
        return open(
                sourceConfig,
                globalSnapshotId,
                totalBuckets,
                expectedShards,
                existingGlobalManifestPath,
                inputLocation,
                CobbleConnectorStorageOptions.empty());
    }

    /** Opens with the same scoped provider configuration used to discover the checkpoint. */
    public static Prepared open(
            Config sourceConfig,
            long globalSnapshotId,
            int totalBuckets,
            List<ShardSnapshot> expectedShards,
            String existingGlobalManifestPath,
            String inputLocation,
            CobbleConnectorStorageOptions storageOptions)
            throws IOException {
        if (sourceConfig == null)
            throw new IllegalArgumentException("sourceConfig must not be null");
        if (globalSnapshotId < 0L || totalBuckets <= 0) {
            throw new IllegalArgumentException(
                    "invalid fixed global snapshot identity or bucket count");
        }
        if (expectedShards == null || expectedShards.isEmpty()) {
            throw new IOException("embedded checkpoint has no shard snapshots");
        }
        File workspace =
                Files.createTempDirectory("cobble-embedded-read-" + globalSnapshotId + "-")
                        .toFile();
        Reader bootstrap = null;
        CobbleFlinkStorageConfig storage =
                CobbleFlinkStorageConfig.empty().withRoutes(sourceConfig);
        try {
            if (existingGlobalManifestPath == null) {
                Config coordinatorConfig =
                        readConfig(sourceConfig, workspace, totalBuckets, inputLocation);
                try (DbCoordinator coordinator = DbCoordinator.open(coordinatorConfig)) {
                    coordinator.materializeGlobalSnapshot(
                            totalBuckets, globalSnapshotId, expectedShards);
                }
            } else {
                copyFile(
                        new Path(existingGlobalManifestPath),
                        workspace
                                .toPath()
                                .resolve("snapshot")
                                .resolve("SNAPSHOT-" + globalSnapshotId),
                        storage,
                        storageOptions);
            }

            Config bootstrapConfig =
                    readConfig(sourceConfig, workspace, totalBuckets, inputLocation);
            bootstrap = Reader.open(bootstrapConfig, globalSnapshotId);
            GlobalSnapshot snapshot = bootstrap.currentGlobalSnapshot();
            validate(snapshot, globalSnapshotId, totalBuckets, expectedShards);

            Map<String, String> shardRoots = new LinkedHashMap<String, String>();
            for (ShardSnapshot shard : snapshot.shardSnapshots) {
                String root = copyShardMetadata(shard, workspace, storage, storageOptions);
                if (root != null) shardRoots.put(root, root);
            }
            bootstrap.close();
            bootstrap = null;

            Config readerConfig = readConfig(sourceConfig, workspace, totalBuckets, inputLocation);
            for (String root : shardRoots.values()) addVolume(readerConfig, sourceConfig, root);
            return new Prepared(Reader.open(readerConfig, globalSnapshotId), workspace, snapshot);
        } catch (IOException | RuntimeException error) {
            if (bootstrap != null) bootstrap.close();
            deleteRecursively(workspace);
            throw error;
        }
    }

    private static Config readConfig(
            Config source, File workspace, int totalBuckets, String inputLocation) {
        Config result = source.copy();
        result.totalBuckets(totalBuckets);
        result.snapshotRetention = null;
        List<Config.VolumeDescriptor> originalVolumes = result.volumes;
        result.volumes = new ArrayList<Config.VolumeDescriptor>();
        addVolume(result, source, workspace.getAbsolutePath());
        if (originalVolumes != null) {
            for (Config.VolumeDescriptor volume : originalVolumes) {
                if (volume != null && !sameLocation(volume.baseDir, inputLocation)) {
                    result.addVolume(volume);
                }
            }
        }
        return result;
    }

    private static void addVolume(Config target, Config source, String root) {
        Config.VolumeDescriptor descriptor = new Config.VolumeDescriptor();
        descriptor.baseDir = normalize(root);
        descriptor.kinds =
                java.util.Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT);
        CobbleFlinkStorageConfig.empty().withRoutes(source).register(target, descriptor);
    }

    private static void validate(
            GlobalSnapshot actual,
            long expectedId,
            int expectedBuckets,
            List<ShardSnapshot> expectedShards)
            throws IOException {
        if (actual == null) throw new IOException("embedded global manifest contains no snapshot");
        if (actual.id != expectedId) {
            throw new IOException(
                    "embedded global manifest id "
                            + actual.id
                            + " does not match checkpoint "
                            + expectedId);
        }
        if (actual.totalBuckets != expectedBuckets) {
            throw new IOException(
                    "embedded global manifest bucket count "
                            + actual.totalBuckets
                            + " does not match checkpoint operator "
                            + expectedBuckets);
        }
        Map<String, ShardSnapshot> expected = byIdentity(expectedShards);
        Map<String, ShardSnapshot> found = byIdentity(actual.shardSnapshots);
        if (!expected.keySet().equals(found.keySet())) {
            throw new IOException(
                    "embedded global manifest shard references do not exactly match the selected"
                            + " operator");
        }
        for (String identity : expected.keySet()) {
            if (!sameReference(expected.get(identity), found.get(identity))) {
                throw new IOException(
                        "embedded global manifest shard reference differs for " + identity);
            }
        }
    }

    private static Map<String, ShardSnapshot> byIdentity(List<ShardSnapshot> shards)
            throws IOException {
        Map<String, ShardSnapshot> result = new LinkedHashMap<String, ShardSnapshot>();
        if (shards == null) return result;
        for (ShardSnapshot shard : shards) {
            if (shard == null || shard.dbId == null || shard.dbId.trim().isEmpty()) {
                throw new IOException("embedded global manifest has a shard without dbId");
            }
            String identity = shard.dbId + ':' + shard.snapshotId;
            if (result.put(identity, shard) != null) {
                throw new IOException("embedded global manifest repeats shard " + identity);
            }
        }
        return result;
    }

    private static boolean sameReference(ShardSnapshot first, ShardSnapshot second) {
        if (!normalize(first.manifestPath).equals(normalize(second.manifestPath))) return false;
        if (first.ranges == null || second.ranges == null) return first.ranges == second.ranges;
        if (first.ranges.size() != second.ranges.size()) return false;
        for (int index = 0; index < first.ranges.size(); index++) {
            ShardSnapshot.Range left = first.ranges.get(index);
            ShardSnapshot.Range right = second.ranges.get(index);
            if (left == null
                    || right == null
                    || left.start != right.start
                    || left.end != right.end) {
                return false;
            }
        }
        return true;
    }

    private static String copyShardMetadata(
            ShardSnapshot shard,
            File workspace,
            CobbleFlinkStorageConfig storage,
            CobbleConnectorStorageOptions storageOptions)
            throws IOException {
        if (shard.manifestPath == null || shard.manifestPath.trim().isEmpty()) return null;
        Path manifest = new Path(shard.manifestPath);
        Path snapshotDirectory = manifest.getParent();
        if (snapshotDirectory == null || snapshotDirectory.getParent() == null) return null;
        Path root = snapshotDirectory.getParent();
        java.nio.file.Path localRoot = workspace.toPath().resolve(shard.dbId);
        copyFile(
                manifest,
                localRoot.resolve("snapshot").resolve("SNAPSHOT-" + shard.snapshotId),
                storage,
                storageOptions);
        copyDirectoryIfExists(
                new Path(root, "schema"), localRoot.resolve("schema"), storage, storageOptions);
        return normalize(root.toString());
    }

    private static void copyDirectoryIfExists(
            Path source,
            java.nio.file.Path target,
            CobbleFlinkStorageConfig storage,
            CobbleConnectorStorageOptions storageOptions)
            throws IOException {
        FileSystem fs = fileSystem(source, storage, storageOptions);
        if (!fs.exists(source) || !fs.getFileStatus(source).isDir()) return;
        Files.createDirectories(target);
        FileStatus[] children = fs.listStatus(source);
        if (children == null) return;
        for (FileStatus child : children) {
            java.nio.file.Path childTarget = target.resolve(child.getPath().getName());
            if (child.isDir())
                copyDirectoryIfExists(child.getPath(), childTarget, storage, storageOptions);
            else copyFile(child.getPath(), childTarget, storage, storageOptions);
        }
    }

    private static void copyFile(
            Path source,
            java.nio.file.Path target,
            CobbleFlinkStorageConfig storage,
            CobbleConnectorStorageOptions storageOptions)
            throws IOException {
        Files.createDirectories(target.getParent());
        try (FSDataInputStream input = fileSystem(source, storage, storageOptions).open(source)) {
            Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static FileSystem fileSystem(
            Path source,
            CobbleFlinkStorageConfig storage,
            CobbleConnectorStorageOptions storageOptions)
            throws IOException {
        return CobbleFlinkFileSystemResolver.resolve(
                source.toString(), storage.resolve(source.toString(), storageOptions));
    }

    private static String normalize(String root) {
        if (root == null) return "";
        try {
            URI uri = URI.create(root);
            if ("file".equals(uri.getScheme())) return new File(uri).getAbsolutePath();
        } catch (IllegalArgumentException ignored) {
            // Native config accepts plain paths unchanged.
        }
        return root;
    }

    private static boolean sameLocation(String first, String second) {
        return second != null
                && !second.trim().isEmpty()
                && normalize(first).equals(normalize(second));
    }

    private static void deleteRecursively(File directory) {
        if (directory == null || !directory.exists()) return;
        try {
            try (Stream<java.nio.file.Path> paths = Files.walk(directory.toPath())) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(
                                path -> {
                                    try {
                                        Files.deleteIfExists(path);
                                    } catch (IOException ignored) {
                                        // Best-effort local worker workspace cleanup.
                                    }
                                });
            }
        } catch (IOException ignored) {
            // Best-effort local worker workspace cleanup.
        }
    }

    /** Reader plus its executor-owned metadata workspace. */
    public static final class Prepared implements AutoCloseable {
        private final Reader reader;
        private final File workspace;
        private final GlobalSnapshot snapshot;

        private Prepared(Reader reader, File workspace, GlobalSnapshot snapshot) {
            this.reader = reader;
            this.workspace = workspace;
            this.snapshot = snapshot.copy();
        }

        public Reader reader() {
            return reader;
        }

        public GlobalSnapshot snapshot() {
            return snapshot.copy();
        }

        public File workspace() {
            return workspace;
        }

        @Override
        public void close() {
            try {
                reader.close();
            } finally {
                deleteRecursively(workspace);
            }
        }
    }
}
