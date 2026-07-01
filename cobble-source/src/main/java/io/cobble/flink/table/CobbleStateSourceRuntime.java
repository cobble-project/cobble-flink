package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.Reader;
import io.cobble.ScanOptions;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/** Runtime helpers for checkpoint-root Cobble state source scans. */
final class CobbleStateSourceRuntime {

    private static final String CHECKPOINT_PREFIX = "chk-";
    private static final String FLINK_METADATA = "_metadata";
    private static final String COBBLE_DIR = "cobble";
    private static final String SHARED_DIR = "shared";
    private static final String SNAPSHOT_DIR = "snapshot";
    private static final String INSPECT_SCHEMA = "inspect-schema";
    private static final String EVENTS = "events";
    private static final String BLOBS = "blobs";
    private static final String COBBLE_MANIFEST_PREFIX = "COBBLE-SNAPSHOT-";
    private static final String COBBLE_MANIFEST_SUFFIX = "-MANIFEST";
    private static final int SCAN_BLOCK_CACHE_BYTES = 8 * 1024 * 1024;

    private CobbleStateSourceRuntime() {}

    static long resolveCheckpointId(StateSourceConfig config) throws IOException {
        if (!"latest".equals(config.scanCheckpointId())) {
            long checkpointId = Long.parseLong(config.scanCheckpointId());
            Path checkpointDir = checkpointDir(config.pathUri(), checkpointId);
            if (!exists(checkpointDir) || !exists(new Path(checkpointDir, FLINK_METADATA))) {
                throw new IOException(
                        "Cobble state source checkpoint "
                                + checkpointId
                                + " was not found under "
                                + config.pathUri()
                                + ".");
            }
            Path manifest = manifestCopyPath(checkpointDir, config.operatorId());
            if (!exists(manifest)) {
                throw new IOException(
                        "Cobble state source checkpoint "
                                + checkpointId
                                + " is missing manifest "
                                + manifest
                                + ".");
            }
            return checkpointId;
        }

        Path root = new Path(config.pathUri());
        FileStatus[] children = listStatus(root);
        long latest = -1L;
        if (children != null) {
            for (FileStatus child : children) {
                if (!child.isDir()) {
                    continue;
                }
                Long checkpointId = parseCheckpointDirName(child.getPath().getName());
                if (checkpointId == null) {
                    continue;
                }
                if (!exists(new Path(child.getPath(), FLINK_METADATA))) {
                    continue;
                }
                if (!exists(manifestCopyPath(child.getPath(), config.operatorId()))) {
                    continue;
                }
                if (checkpointId.longValue() > latest) {
                    latest = checkpointId.longValue();
                }
            }
        }
        if (latest < 0L) {
            throw new IOException(
                    "Cobble state source could not find a readable chk-* checkpoint for operator '"
                            + config.operatorId()
                            + "' under "
                            + config.pathUri()
                            + ".");
        }
        return latest;
    }

    static List<CobbleStateSourceSplit> createStateSourceSplits(StateSourceConfig config)
            throws IOException {
        long checkpointId = resolveCheckpointId(config);
        try (ReaderHandle handle = openReader(config, checkpointId)) {
            return createStateSourceSplits(
                    config, handle.reader.currentGlobalSnapshot(), checkpointId);
        }
    }

    static List<CobbleStateSourceSplit> createStateSourceSplits(
            StateSourceConfig config, GlobalSnapshot snapshot, long checkpointId)
            throws IOException {
        int totalKeyGroups = validateSnapshot(config, snapshot, checkpointId);
        boolean[] covered = new boolean[totalKeyGroups];
        List<CobbleStateSourceSplit> splits = new ArrayList<>();
        if (snapshot.shardSnapshots != null) {
            for (ShardSnapshot shard : snapshot.shardSnapshots) {
                if (shard == null || shard.ranges == null) {
                    continue;
                }
                for (ShardSnapshot.Range range : shard.ranges) {
                    if (range == null) {
                        continue;
                    }
                    markCovered(range, totalKeyGroups, covered, checkpointId);
                    splits.add(
                            CobbleStateSourceSplit.forRange(
                                    checkpointId,
                                    totalKeyGroups,
                                    range.start,
                                    range.end,
                                    config.operatorId(),
                                    config.stateName(),
                                    config.stateKind()));
                }
            }
        }
        ensureCompleteCoverage(covered, checkpointId);
        return splits;
    }

    static ReaderHandle openReader(StateSourceConfig config, long checkpointId) throws IOException {
        Path checkpointDir = checkpointDir(config.pathUri(), checkpointId);
        Path operatorSnapshotDir = operatorSnapshotDir(config.pathUri(), config.operatorId());
        File unifiedVolume =
                Files.createTempDirectory(
                                "cobble-state-source-"
                                        + checkpointId
                                        + "-"
                                        + safeFileName(config.operatorId())
                                        + "-")
                        .toFile();
        Reader bootstrapReader = null;
        try {
            copyGlobalManifest(
                    checkpointDir,
                    operatorSnapshotDir,
                    config.operatorId(),
                    checkpointId,
                    unifiedVolume);

            Config bootstrapConfig = baseConfig(config, config.bucketCount());
            addVolume(bootstrapConfig, pathToCobbleConfigString(unifiedVolume));
            bootstrapReader = Reader.open(bootstrapConfig, checkpointId);
            GlobalSnapshot snapshot = bootstrapReader.currentGlobalSnapshot();
            int totalKeyGroups = validateSnapshot(config, snapshot, checkpointId);

            Map<String, String> shardVolumes = new LinkedHashMap<>();
            if (snapshot.shardSnapshots != null) {
                for (ShardSnapshot shardSnapshot : snapshot.shardSnapshots) {
                    String shardVolume = copyShardMetadata(shardSnapshot, unifiedVolume);
                    if (shardVolume != null) {
                        shardVolumes.putIfAbsent(shardVolume, shardVolume);
                    }
                }
            }
            bootstrapReader.close();
            bootstrapReader = null;

            Config readerConfig = baseConfig(config, totalKeyGroups);
            addVolume(readerConfig, pathToCobbleConfigString(unifiedVolume));
            for (String shardVolume : shardVolumes.values()) {
                addVolume(readerConfig, shardVolume);
            }
            addSharedVolumes(readerConfig, config.pathUri());
            return new ReaderHandle(Reader.open(readerConfig, checkpointId), unifiedVolume);
        } catch (IOException | RuntimeException e) {
            if (bootstrapReader != null) {
                bootstrapReader.close();
            }
            deleteRecursively(unifiedVolume);
            throw e;
        }
    }

    static RuntimeSchema loadRuntimeSchema(StateSourceConfig config) throws IOException {
        StateInspectSchemaStore store = readSchemaStore(config);
        StateInspectSchema schema = store.byStateName().get(config.stateName());
        if (schema == null) {
            throw new IOException(
                    "Cobble state source runtime could not find state '"
                            + config.stateName()
                            + "' in inspect schema store.");
        }
        StateInspectSemanticSchema semantic = store.semanticSchema(config.stateName());
        if (semantic == null || semantic.isEmpty()) {
            throw new IOException(
                    "Cobble state source runtime found no semantic schema for state '"
                            + config.stateName()
                            + "'.");
        }
        if (!schema.stateKind().wireName().equals(config.stateKind())) {
            throw new IOException(
                    "Cobble state source runtime expected state kind '"
                            + config.stateKind()
                            + "' but schema has '"
                            + schema.stateKind().wireName()
                            + "'.");
        }
        return new RuntimeSchema(schema, semantic);
    }

    static ScanOptions scanOptions(String columnFamily, int maxRows) {
        ScanOptions options = new ScanOptions().maxRows(maxRows).columns(0);
        if (columnFamily != null) {
            options.columnFamily(columnFamily);
        }
        return options;
    }

    static byte[] emptyScanKey() {
        return new byte[0];
    }

    static byte[] maxScanKey() {
        byte[] key = new byte[64];
        Arrays.fill(key, (byte) 0xFF);
        return key;
    }

    private static StateInspectSchemaStore readSchemaStore(StateSourceConfig config)
            throws IOException {
        Path eventsDir =
                new Path(
                        new Path(
                                new Path(
                                        new Path(new Path(config.pathUri()), COBBLE_DIR),
                                        config.operatorId()),
                                INSPECT_SCHEMA),
                        EVENTS);
        FileStatus[] statuses = listStatus(eventsDir);
        InspectSchemaRegistryLayout.SchemaEvent best = null;
        if (statuses != null) {
            for (FileStatus status : statuses) {
                if (status.isDir()) {
                    continue;
                }
                InspectSchemaRegistryLayout.SchemaEvent event =
                        InspectSchemaRegistryLayout.parseEventFileName(status.getPath().getName());
                if (event == null || event.checkpointId() > config.schemaCheckpointId()) {
                    continue;
                }
                if (best == null || event.checkpointId() > best.checkpointId()) {
                    best = event;
                }
            }
        }
        if (best == null) {
            throw new IOException(
                    "No Cobble state inspect schema event with checkpoint id <= "
                            + config.schemaCheckpointId()
                            + " was found for operator '"
                            + config.operatorId()
                            + "'.");
        }
        Path blobPath =
                new Path(
                        new Path(
                                new Path(
                                        new Path(
                                                new Path(new Path(config.pathUri()), COBBLE_DIR),
                                                config.operatorId()),
                                        INSPECT_SCHEMA),
                                BLOBS),
                        InspectSchemaRegistryLayout.blobFileName(best.hash()));
        if (!exists(blobPath)) {
            throw new IOException("Cobble state inspect schema blob is missing: " + blobPath);
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (FSDataInputStream input = blobPath.getFileSystem().open(blobPath)) {
            byte[] chunk = new byte[8 * 1024];
            int read;
            while ((read = input.read(chunk)) >= 0) {
                buffer.write(chunk, 0, read);
            }
        }
        StateInspectSchemaStore store = StateInspectSchemaStore.fromBytes(buffer.toByteArray());
        if (store.isEmpty()) {
            throw new IOException("Cobble state inspect schema blob parsed as an empty store.");
        }
        return store;
    }

    private static int validateSnapshot(
            StateSourceConfig config, GlobalSnapshot snapshot, long checkpointId)
            throws IOException {
        if (snapshot == null) {
            throw new IOException(
                    "Cobble state source checkpoint "
                            + checkpointId
                            + " did not contain a snapshot.");
        }
        if (snapshot.totalBuckets <= 0) {
            throw new IOException(
                    "Cobble state source checkpoint "
                            + checkpointId
                            + " has invalid totalBuckets "
                            + snapshot.totalBuckets
                            + ".");
        }
        if (config.bucketCount() > 0 && config.bucketCount() != snapshot.totalBuckets) {
            throw new IOException(
                    "Cobble state source bucket count mismatch. Source expects "
                            + config.bucketCount()
                            + " key groups, but checkpoint "
                            + checkpointId
                            + " has "
                            + snapshot.totalBuckets
                            + ".");
        }
        return snapshot.totalBuckets;
    }

    private static void markCovered(
            ShardSnapshot.Range range, int totalKeyGroups, boolean[] covered, long checkpointId)
            throws IOException {
        if (range.start < 0 || range.end < range.start || range.end >= totalKeyGroups) {
            throw new IOException(
                    "Invalid key-group range "
                            + range.start
                            + "-"
                            + range.end
                            + " in checkpoint "
                            + checkpointId
                            + ".");
        }
        for (int keyGroup = range.start; keyGroup <= range.end; keyGroup++) {
            if (covered[keyGroup]) {
                throw new IOException(
                        "Duplicate key-group coverage for "
                                + keyGroup
                                + " in checkpoint "
                                + checkpointId
                                + ".");
            }
            covered[keyGroup] = true;
        }
    }

    private static void ensureCompleteCoverage(boolean[] covered, long checkpointId)
            throws IOException {
        for (int keyGroup = 0; keyGroup < covered.length; keyGroup++) {
            if (!covered[keyGroup]) {
                throw new IOException(
                        "Missing key-group coverage for "
                                + keyGroup
                                + " in checkpoint "
                                + checkpointId
                                + ".");
            }
        }
    }

    private static void copyGlobalManifest(
            Path checkpointDir,
            Path operatorSnapshotDir,
            String operatorId,
            long checkpointId,
            File unifiedVolume)
            throws IOException {
        java.nio.file.Path target =
                unifiedVolume.toPath().resolve(SNAPSHOT_DIR).resolve("SNAPSHOT-" + checkpointId);
        Path primary = manifestCopyPath(checkpointDir, operatorId);
        Path fallback =
                new Path(new Path(operatorSnapshotDir, SNAPSHOT_DIR), "SNAPSHOT-" + checkpointId);
        try {
            copyFile(primary, target);
        } catch (IOException primaryError) {
            if (pathToStorageString(primary).equals(pathToStorageString(fallback))) {
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

    private static String copyShardMetadata(ShardSnapshot shardSnapshot, File unifiedVolume)
            throws IOException {
        if (shardSnapshot.manifestPath == null || shardSnapshot.manifestPath.trim().isEmpty()) {
            return null;
        }
        Path manifest = new Path(shardSnapshot.manifestPath);
        Path snapshotDirectory = manifest.getParent();
        if (snapshotDirectory == null) {
            return null;
        }
        Path shardRoot = snapshotDirectory.getParent();
        if (shardRoot == null) {
            return null;
        }
        java.nio.file.Path localShardRoot = unifiedVolume.toPath().resolve(shardSnapshot.dbId);
        copyDirectoryIfExists(snapshotDirectory, localShardRoot.resolve(SNAPSHOT_DIR));
        copyDirectoryIfExists(new Path(shardRoot, "schema"), localShardRoot.resolve("schema"));
        return pathToStorageString(shardRoot);
    }

    private static void addSharedVolumes(Config readerConfig, String checkpointRootUri)
            throws IOException {
        Path checkpointRoot = new Path(checkpointRootUri);
        TreeSet<String> volumes = new TreeSet<>();
        for (Path candidate : sharedRootCandidates(checkpointRoot)) {
            Path shared = new Path(candidate, SHARED_DIR);
            FileStatus[] statuses = listStatus(shared);
            if (statuses == null) {
                continue;
            }
            for (FileStatus status : statuses) {
                if (status.isDir()) {
                    volumes.add(pathToStorageString(status.getPath()));
                }
            }
        }
        for (String volume : volumes) {
            addVolume(readerConfig, volume);
        }
    }

    /**
     * Returns the candidate roots that may contain a {@code shared/} directory.
     *
     * <p>Flink materializes checkpoints under {@code <configured-dir>/<jobHash>/chk-N/}. The Cobble
     * shared-state volume lives at {@code <configured-dir>/shared/}, which is the parent (or
     * grandparent) of the job-hash directory this source treats as its checkpoint root. Searching a
     * few ancestors mirrors the monitor's {@code sharedRootCandidates} logic so the reader volumes
     * always reach the SST files that register non-default column families.
     */
    private static List<Path> sharedRootCandidates(Path checkpointRoot) {
        List<Path> candidates = new ArrayList<>();
        addPathCandidate(candidates, checkpointRoot);
        addPathCandidate(candidates, checkpointRoot.getParent());
        addPathCandidate(
                candidates,
                checkpointRoot.getParent() != null ? checkpointRoot.getParent().getParent() : null);
        return candidates;
    }

    private static void addPathCandidate(List<Path> candidates, Path path) {
        if (path != null) {
            candidates.add(path);
        }
    }

    private static Config baseConfig(StateSourceConfig config, int totalKeyGroups) {
        Config cobbleConfig = new Config().numColumns(1);
        if (totalKeyGroups > 0) {
            cobbleConfig.totalBuckets(totalKeyGroups);
        }
        cobbleConfig.snapshotRetention = null;
        cobbleConfig.governanceMode = Config.GovernanceMode.NOOP;
        cobbleConfig.logConsole = false;
        cobbleConfig.memtableCapacity = 1;
        cobbleConfig.memtableBufferCount = 1;
        cobbleConfig.blockCacheSize = SCAN_BLOCK_CACHE_BYTES;
        cobbleConfig.blockCacheHybridEnabled = false;
        cobbleConfig.blockCacheHybridDiskSize = 0;
        Config.ReaderConfigEntry readerOptions = new Config.ReaderConfigEntry();
        readerOptions.blockCacheSize = nonNegativeInt(config.sourceBlockCacheMemoryBytes());
        readerOptions.reloadToleranceSeconds = 0L;
        cobbleConfig.reader = readerOptions;
        return cobbleConfig;
    }

    private static void addVolume(Config config, String volumeDirectory) {
        Config.VolumeDescriptor volume =
                Config.VolumeDescriptor.singleVolume(normalizeStorageScheme(volumeDirectory));
        config.addVolume(volume);
    }

    private static String normalizeStorageScheme(String value) {
        if (value == null) {
            return null;
        }
        URI uri = URI.create(value);
        if (uri.getScheme() == null) {
            return value;
        }
        if ("file".equals(uri.getScheme())) {
            return new File(uri).getAbsolutePath();
        }
        return value;
    }

    private static Path checkpointDir(String checkpointRootUri, long checkpointId) {
        return new Path(new Path(checkpointRootUri), CHECKPOINT_PREFIX + checkpointId);
    }

    private static Path operatorSnapshotDir(String checkpointRootUri, String operatorId) {
        return new Path(new Path(new Path(checkpointRootUri), COBBLE_DIR), operatorId);
    }

    private static Path manifestCopyPath(Path checkpointDir, String operatorId) {
        return new Path(
                checkpointDir, COBBLE_MANIFEST_PREFIX + operatorId + COBBLE_MANIFEST_SUFFIX);
    }

    private static Long parseCheckpointDirName(String name) {
        if (name == null || !name.startsWith(CHECKPOINT_PREFIX)) {
            return null;
        }
        try {
            long value = Long.parseLong(name.substring(CHECKPOINT_PREFIX.length()));
            return value > 0L ? value : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static boolean exists(Path path) throws IOException {
        return path.getFileSystem().exists(path);
    }

    private static FileStatus[] listStatus(Path path) throws IOException {
        FileSystem fs = path.getFileSystem();
        if (!fs.exists(path)) {
            return null;
        }
        return fs.listStatus(path);
    }

    private static void copyDirectoryIfExists(
            Path sourceDirectory, java.nio.file.Path targetDirectory) throws IOException {
        FileSystem fileSystem = sourceDirectory.getFileSystem();
        if (!fileSystem.exists(sourceDirectory)) {
            return;
        }
        FileStatus status = fileSystem.getFileStatus(sourceDirectory);
        if (!status.isDir()) {
            return;
        }
        Files.createDirectories(targetDirectory);
        FileStatus[] children = fileSystem.listStatus(sourceDirectory);
        if (children == null) {
            return;
        }
        for (FileStatus child : children) {
            java.nio.file.Path childTarget = targetDirectory.resolve(child.getPath().getName());
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

    private static String pathToCobbleConfigString(File directory) {
        return directory.getAbsoluteFile().toPath().normalize().toString();
    }

    private static String pathToStorageString(Path path) {
        URI uri = path.toUri();
        if ("file".equals(uri.getScheme())) {
            return new File(uri).getAbsolutePath();
        }
        return path.toString();
    }

    private static String safeFileName(String value) {
        if (value == null || value.isEmpty()) {
            return "operator";
        }
        StringBuilder builder = new StringBuilder(value.length());
        for (int index = 0; index < value.length(); index++) {
            char c = value.charAt(index);
            builder.append(Character.isLetterOrDigit(c) || c == '-' || c == '_' ? c : '-');
        }
        return builder.length() == 0 ? "operator" : builder.toString();
    }

    private static void deleteRecursively(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }

    private static int nonNegativeInt(long value) {
        if (value < 0L || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    CobbleSourceTableOptions.SOURCE_BLOCK_CACHE_MEMORY.key()
                            + " must be in [0, "
                            + Integer.MAX_VALUE
                            + "].");
        }
        return (int) value;
    }

    static final class ReaderHandle implements Closeable {
        final Reader reader;
        private final File temporaryDirectory;

        ReaderHandle(Reader reader, File temporaryDirectory) {
            this.reader = reader;
            this.temporaryDirectory = temporaryDirectory;
        }

        @Override
        public void close() {
            reader.close();
            deleteRecursively(temporaryDirectory);
        }
    }

    static final class RuntimeSchema {
        final StateInspectSchema schema;
        final StateInspectSemanticSchema semanticSchema;

        RuntimeSchema(StateInspectSchema schema, StateInspectSemanticSchema semanticSchema) {
            this.schema = schema;
            this.semanticSchema = semanticSchema;
        }
    }
}
