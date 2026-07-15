package io.cobble.flink.inspect.internal;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.Reader;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleEmbeddedCheckpoint;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Internal bridge used by the HTTP monitor while its routes are migrated to the typed SDK.
 *
 * <p>The bridge owns a native reader and every temporary metadata volume needed to open it. It is
 * deliberately internal: public SDK callers receive decoded models instead of native handles.
 */
public final class MonitorReaderSession implements AutoCloseable {
    private final Reader reader;
    private final List<File> temporaryDirectories;

    private MonitorReaderSession(Reader reader, List<File> temporaryDirectories) {
        this.reader = reader;
        this.temporaryDirectories = temporaryDirectories;
    }

    public static MonitorReaderSession open(
            int configuredTotalBuckets,
            CobbleConnectorStorageOptions storageOptions,
            String sourceKind,
            CheckpointEntry checkpoint,
            OperatorEntry operator) {
        try {
            if ("data_source".equals(sourceKind)) {
                Config config =
                        CobbleReaderConfigs.dataSource(
                                configuredTotalBuckets, checkpoint.directory, storageOptions);
                config.snapshotRetention = null;
                return new MonitorReaderSession(Reader.open(config, checkpoint.id), empty());
            }
            if (operator.globalSnapshotLayout) {
                try {
                    return openGlobalSnapshot(
                            configuredTotalBuckets, storageOptions, checkpoint, operator);
                } catch (RuntimeException globalFailure) {
                    if (operator.embeddedCheckpoint == null) {
                        throw globalFailure;
                    }
                    try {
                        return openEmbeddedSnapshot(storageOptions, checkpoint, operator);
                    } catch (RuntimeException embeddedFailure) {
                        globalFailure.addSuppressed(embeddedFailure);
                        throw globalFailure;
                    }
                }
            }
            if (operator.embeddedCheckpoint != null) {
                return openEmbeddedSnapshot(storageOptions, checkpoint, operator);
            }
            Config config =
                    CobbleReaderConfigs.checkpoint(
                            configuredTotalBuckets,
                            operator.readerVolumeDirectories,
                            storageOptions);
            config.snapshotRetention = null;
            return new MonitorReaderSession(Reader.open(config, checkpoint.id), empty());
        } catch (InspectInputException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new InspectInputException("Failed to open inspection reader: " + e.getMessage());
        }
    }

    public Reader reader() {
        return reader;
    }

    public List<File> temporaryDirectories() {
        return temporaryDirectories;
    }

    @Override
    public void close() {
        try {
            reader.close();
        } finally {
            deleteTemporaryDirectories(temporaryDirectories);
        }
    }

    private static MonitorReaderSession openGlobalSnapshot(
            int configuredTotalBuckets,
            CobbleConnectorStorageOptions storageOptions,
            CheckpointEntry checkpoint,
            OperatorEntry operator) {
        List<File> directories = new ArrayList<>();
        Reader bootstrapReader = null;
        boolean opened = false;
        try {
            File unifiedVolume =
                    Files.createTempDirectory(
                                    "cobble-flink-monitor-"
                                            + checkpoint.id
                                            + "-"
                                            + safeFileName(operator.operatorId)
                                            + "-")
                            .toFile();
            directories.add(unifiedVolume);
            copyGlobalManifest(operator, checkpoint.id, unifiedVolume);

            Config bootstrapConfig = CobbleReaderConfigs.base(configuredTotalBuckets);
            CobbleReaderConfigs.addVolume(
                    bootstrapConfig, localVolume(unifiedVolume), storageOptions);
            bootstrapReader = Reader.open(bootstrapConfig, checkpoint.id);
            GlobalSnapshot snapshot = bootstrapReader.currentGlobalSnapshot();
            int totalBuckets =
                    snapshot == null || snapshot.totalBuckets <= 0
                            ? configuredTotalBuckets
                            : snapshot.totalBuckets;
            Map<String, String> shardVolumes = new LinkedHashMap<>();
            if (snapshot != null && snapshot.shardSnapshots != null) {
                for (ShardSnapshot shard : snapshot.shardSnapshots) {
                    String root = copyShardMetadata(shard, unifiedVolume);
                    if (root != null) {
                        shardVolumes.putIfAbsent(root, root);
                    }
                }
            }
            bootstrapReader.close();
            bootstrapReader = null;

            Config config = CobbleReaderConfigs.base(totalBuckets);
            CobbleReaderConfigs.addVolume(config, localVolume(unifiedVolume), storageOptions);
            for (String root : shardVolumes.values()) {
                CobbleReaderConfigs.addVolume(config, root, storageOptions);
            }
            for (String root : operator.readerVolumeDirectories) {
                if (!root.equals(operator.operatorSnapshotDirectory)) {
                    CobbleReaderConfigs.addVolume(config, root, storageOptions);
                }
            }
            MonitorReaderSession session =
                    new MonitorReaderSession(Reader.open(config, checkpoint.id), directories);
            opened = true;
            return session;
        } catch (IOException e) {
            throw new InspectInputException(
                    "Failed to prepare Cobble checkpoint metadata for operator "
                            + operator.operatorId
                            + ": "
                            + e.getMessage());
        } finally {
            if (bootstrapReader != null) {
                bootstrapReader.close();
            }
            if (!opened) {
                deleteTemporaryDirectories(directories);
            }
        }
    }

    private static MonitorReaderSession openEmbeddedSnapshot(
            CobbleConnectorStorageOptions storageOptions,
            CheckpointEntry checkpoint,
            OperatorEntry operator) {
        List<File> directories = new ArrayList<>();
        try {
            CobbleEmbeddedCheckpoint.OperatorSnapshot embedded = operator.embeddedCheckpoint;
            if (embedded.maxParallelism() <= 0) {
                throw new InspectInputException(
                        "Embedded checkpoint operator "
                                + operator.operatorId
                                + " has invalid maxParallelism "
                                + embedded.maxParallelism());
            }
            File unifiedVolume =
                    Files.createTempDirectory(
                                    "cobble-flink-embedded-checkpoint-"
                                            + checkpoint.id
                                            + "-"
                                            + safeFileName(operator.operatorId)
                                            + "-")
                            .toFile();
            directories.add(unifiedVolume);
            Config coordinatorConfig = CobbleReaderConfigs.base(embedded.maxParallelism());
            CobbleReaderConfigs.addVolume(
                    coordinatorConfig, localVolume(unifiedVolume), storageOptions);
            try (DbCoordinator coordinator = DbCoordinator.open(coordinatorConfig)) {
                coordinator.materializeGlobalSnapshot(
                        embedded.maxParallelism(), checkpoint.id, embedded.shards());
            }

            Config readerConfig = CobbleReaderConfigs.base(embedded.maxParallelism());
            CobbleReaderConfigs.addVolume(readerConfig, localVolume(unifiedVolume), storageOptions);
            Map<String, String> roots = new LinkedHashMap<>();
            for (ShardSnapshot shard : embedded.shards()) {
                String root = copyShardMetadata(shard, unifiedVolume);
                if (root != null) {
                    roots.putIfAbsent(root, root);
                }
            }
            for (String root : roots.values()) {
                CobbleReaderConfigs.addVolume(readerConfig, root, storageOptions);
            }
            return new MonitorReaderSession(Reader.open(readerConfig, checkpoint.id), directories);
        } catch (IOException | RuntimeException e) {
            deleteTemporaryDirectories(directories);
            if (e instanceof InspectInputException) {
                throw (InspectInputException) e;
            }
            throw new InspectInputException(
                    "Failed to prepare Cobble embedded checkpoint for operator "
                            + operator.operatorId
                            + ": "
                            + e.getMessage());
        }
    }

    private static List<File> empty() {
        return Collections.emptyList();
    }

    private static void copyGlobalManifest(OperatorEntry operator, long checkpointId, File volume)
            throws IOException {
        java.nio.file.Path target =
                volume.toPath().resolve("snapshot").resolve("SNAPSHOT-" + checkpointId);
        Path fallback =
                new Path(
                        new Path(operator.operatorSnapshotDirectory, "snapshot"),
                        "SNAPSHOT-" + checkpointId);
        Path source =
                operator.manifestCopyPath == null ? fallback : new Path(operator.manifestCopyPath);
        try {
            copyFile(source, target);
        } catch (IOException primaryError) {
            if (source.toString().equals(fallback.toString())) {
                throw primaryError;
            }
            try {
                copyFile(fallback, target);
            } catch (IOException fallbackError) {
                throw new IOException(
                        primaryError.getMessage()
                                + "; fallback "
                                + fallback
                                + " also failed: "
                                + fallbackError.getMessage(),
                        primaryError);
            }
        }
    }

    private static String copyShardMetadata(ShardSnapshot shard, File volume) throws IOException {
        if (shard.manifestPath == null || shard.manifestPath.trim().isEmpty()) {
            return null;
        }
        Path manifest = new Path(shard.manifestPath);
        Path snapshotDirectory = manifest.getParent();
        if (snapshotDirectory == null || snapshotDirectory.getParent() == null) {
            return null;
        }
        Path shardRoot = snapshotDirectory.getParent();
        java.nio.file.Path localRoot = volume.toPath().resolve(shard.dbId);
        copyDirectoryIfExists(snapshotDirectory, localRoot.resolve("snapshot"));
        copyDirectoryIfExists(new Path(shardRoot, "schema"), localRoot.resolve("schema"));
        return InspectPathUtils.pathToStorageString(shardRoot);
    }

    private static void copyDirectoryIfExists(Path source, java.nio.file.Path target)
            throws IOException {
        FileSystem fileSystem = source.getFileSystem();
        if (!fileSystem.exists(source)) {
            return;
        }
        FileStatus status = fileSystem.getFileStatus(source);
        if (!status.isDir()) {
            return;
        }
        Files.createDirectories(target);
        FileStatus[] children = fileSystem.listStatus(source);
        if (children == null) {
            return;
        }
        for (FileStatus child : children) {
            java.nio.file.Path childTarget = target.resolve(child.getPath().getName());
            if (child.isDir()) {
                copyDirectoryIfExists(child.getPath(), childTarget);
            } else {
                copyFile(child.getPath(), childTarget);
            }
        }
    }

    private static void copyFile(Path source, java.nio.file.Path target) throws IOException {
        Files.createDirectories(target.getParent());
        try (InputStream input = source.getFileSystem().open(source)) {
            Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static String localVolume(File directory) {
        return directory.getAbsoluteFile().toPath().normalize().toString();
    }

    private static String safeFileName(String value) {
        StringBuilder builder = new StringBuilder(value.length());
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            builder.append(
                    Character.isLetterOrDigit(character) || character == '-' || character == '_'
                            ? character
                            : '-');
        }
        return builder.length() == 0 ? "operator" : builder.toString();
    }

    public static void deleteTemporaryDirectories(List<File> directories) {
        for (File directory : directories) {
            if (directory == null || !directory.exists()) {
                continue;
            }
            try {
                Files.walk(directory.toPath())
                        .sorted(Comparator.reverseOrder())
                        .forEach(MonitorReaderSession::deletePathQuietly);
            } catch (IOException ignored) {
                // Best-effort cleanup for materialized metadata.
            }
        }
    }

    private static void deletePathQuietly(java.nio.file.Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // Best-effort cleanup for materialized metadata.
        }
    }
}
