package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.flink.common.CobbleFlinkStorageConfig;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleMetadataFileIO;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

/** Path and config helpers for the initial Cobble SQL sink. */
final class CobbleSinkPaths {

    private CobbleSinkPaths() {}

    static File writerLocalDirectory(
            CobbleDynamicTableSink.SerializableConfig config, int subtaskId) {
        File writerRoot =
                isRemoteTable(config)
                        ? stagingRoot(config)
                        : new File(tableRootDirectory(config), ".writers");
        return new File(writerRoot, "writer-" + subtaskId);
    }

    static File bucketWriterLocalDirectory(
            CobbleDynamicTableSink.SerializableConfig config, int subtaskId, int bucketId) {
        return new File(writerLocalDirectory(config, subtaskId), "bucket-" + bucketId);
    }

    static int writerRangeStart(CobbleDynamicTableSink.SerializableConfig config, int subtaskId) {
        return (int)
                (((long) subtaskId * (long) config.bucketCount) / (long) config.sinkParallelism);
    }

    static int writerRangeEnd(CobbleDynamicTableSink.SerializableConfig config, int subtaskId) {
        int nextStart =
                (int)
                        ((((long) subtaskId + 1L) * (long) config.bucketCount)
                                / (long) config.sinkParallelism);
        return nextStart - 1;
    }

    static File coordinatorLocalDirectory(CobbleDynamicTableSink.SerializableConfig config) {
        return isRemoteTable(config)
                ? new File(stagingRoot(config), "coordinator")
                : tableRootDirectory(config);
    }

    static File tableRootPath(CobbleDynamicTableSink.SerializableConfig config) {
        return isRemoteTable(config) ? stagingRoot(config) : tableRootDirectory(config);
    }

    static Config createWriterConfig(
            CobbleDynamicTableSink.SerializableConfig config, int subtaskId) throws IOException {
        File localDir = writerLocalDirectory(config, subtaskId);
        Files.createDirectories(localDir.toPath());
        return createWriterConfigForLocalDir(config, localDir, 1);
    }

    static Config createTableWriterRuntime(
            CobbleDynamicTableSink.SerializableConfig config,
            int subtaskId,
            int bucketId,
            int ownedBucketCount)
            throws IOException {
        File localDir = bucketWriterLocalDirectory(config, subtaskId, bucketId);
        Files.createDirectories(localDir.toPath());
        return createWriterConfigForLocalDir(config, localDir, ownedBucketCount);
    }

    static Config createCatalogWriterRuntime(
            CobbleDynamicTableSink.SerializableConfig config, int ownedBucketCount) {
        Config runtime = config.catalogTable.runtimeConfig().totalBuckets(config.bucketCount);
        runtime.walEnabled = false;
        runtime.snapshotRetention = null;
        runtime.snapshotOnlyTrack = true;
        runtime.snapshotDisableIncrementalBaseLink = true;
        runtime.memtableType = Config.MemtableType.VEC;
        runtime.governanceMode = Config.GovernanceMode.NOOP;
        runtime.blockCacheSize = 0;
        runtime.blockCacheHybridEnabled = false;
        runtime.blockCacheHybridDiskSize = 0;
        runtime.memtableCapacity =
                writerMemtableCapacity(
                        config.sinkWriterBufferMemoryBytes, ownedBucketCount,
                        CobbleTableOptions.SINK_WRITER_BUFFER_MEMORY.key());
        runtime.memtableBufferCount = 1;
        return runtime;
    }

    static Config createCatalogWriterRuntime(CobbleDynamicTableSink.SerializableConfig config) {
        return createCatalogWriterRuntime(config, 1);
    }

    static Config createWriterConfigForWriterPath(
            CobbleDynamicTableSink.SerializableConfig config, String writerPath) {
        return createWriterConfigForLocalDir(config, new File(writerPath), 1);
    }

    private static Config createWriterConfigForLocalDir(
            CobbleDynamicTableSink.SerializableConfig config, File localDir, int ownedBucketCount) {
        Config dbConfig = new Config().totalBuckets(config.bucketCount);
        // Sink commits are snapshot-based. Keep WAL disabled so recovery cannot cross the committed
        // snapshot boundary.
        dbConfig.walEnabled = false;
        dbConfig.snapshotRetention = null;
        dbConfig.snapshotOnlyTrack = true;
        dbConfig.snapshotDisableIncrementalBaseLink = true;
        dbConfig.memtableType = Config.MemtableType.VEC;
        dbConfig.governanceMode = Config.GovernanceMode.NOOP;
        dbConfig.logConsole = false;
        dbConfig.logPath = new File(localDir, "cobble-writer.log").getAbsolutePath();
        // Sink writes are append/update heavy and do not require block cache.
        dbConfig.blockCacheSize = 0;
        dbConfig.blockCacheHybridEnabled = false;
        dbConfig.blockCacheHybridDiskSize = 0;
        dbConfig.memtableCapacity =
                writerMemtableCapacity(
                        config.sinkWriterBufferMemoryBytes, ownedBucketCount,
                        CobbleTableOptions.SINK_WRITER_BUFFER_MEMORY.key());
        dbConfig.memtableBufferCount = 1;

        Config.VolumeDescriptor localVolume = new Config.VolumeDescriptor();
        localVolume.baseDir = localDir.getAbsolutePath();
        localVolume.kinds =
                Collections.singletonList(Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH);
        CobbleFlinkStorageConfig.empty().register(dbConfig, localVolume);

        // Snapshot manifests are metadata. Keep META with SNAPSHOT so a remote table contains the
        // complete shard snapshot, while active writer data can remain in local staging.
        Config.VolumeDescriptor tableVolume = new Config.VolumeDescriptor();
        tableVolume.baseDir = config.pathUri;
        tableVolume.kinds =
                Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        config.storageOptions.applyTo(tableVolume);
        CobbleFlinkStorageConfig.empty().register(dbConfig, tableVolume);
        return dbConfig;
    }

    private static int positiveInt(long value, String optionKey) {
        if (value <= 0L || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    optionKey + " must be in (0, " + Integer.MAX_VALUE + "].");
        }
        return (int) value;
    }

    private static int writerMemtableCapacity(
            long totalBytes, int ownedBucketCount, String optionKey) {
        if (ownedBucketCount <= 0) {
            throw new IllegalArgumentException("ownedBucketCount must be > 0");
        }
        long perBucket = Math.max(1L, totalBytes / ownedBucketCount);
        return positiveInt(perBucket, optionKey);
    }

    static Config createCoordinatorConfig(CobbleDynamicTableSink.SerializableConfig config)
            throws IOException {
        File localDir = coordinatorLocalDirectory(config);
        Files.createDirectories(localDir.toPath());

        Config coordinatorConfig = new Config().totalBuckets(config.bucketCount);
        coordinatorConfig.governanceMode = Config.GovernanceMode.NOOP;
        coordinatorConfig.logConsole = false;
        coordinatorConfig.logPath = new File(localDir, "cobble-coordinator.log").getAbsolutePath();

        Config.VolumeDescriptor coordinatorVolume = new Config.VolumeDescriptor();
        coordinatorVolume.baseDir =
                isRemoteTable(config) ? config.pathUri : localDir.getAbsolutePath();
        coordinatorVolume.kinds =
                Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        config.storageOptions.applyTo(coordinatorVolume);
        CobbleFlinkStorageConfig.empty().register(coordinatorConfig, coordinatorVolume);
        return coordinatorConfig;
    }

    private static File writerPathIndexFile(CobbleDynamicTableSink.SerializableConfig config) {
        return new File(tableRootDirectory(config), "writer-paths.properties");
    }

    static Map<String, String> loadWriterPathIndex(CobbleDynamicTableSink.SerializableConfig config)
            throws IOException {
        if (!isRemoteTable(config)) {
            File pathIndexFile = writerPathIndexFile(config);
            Map<String, String> result = new HashMap<>();
            if (!pathIndexFile.exists()) {
                return result;
            }
            Properties properties = new Properties();
            try (java.io.FileInputStream input = new java.io.FileInputStream(pathIndexFile)) {
                properties.load(input);
            }
            copyProperties(properties, result);
            return result;
        }
        CobbleMetadataFileIO fileIO = openMetadata(config);
        Map<String, String> result = new HashMap<>();
        if (!fileIO.exists("writer-paths.properties")) {
            return result;
        }
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(fileIO.read("writer-paths.properties")));
        copyProperties(properties, result);
        return result;
    }

    static void storeWriterPathIndex(
            CobbleDynamicTableSink.SerializableConfig config, Map<String, String> writerPathByDbId)
            throws IOException {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : writerPathByDbId.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        if (!isRemoteTable(config)) {
            File pathIndexFile = writerPathIndexFile(config);
            File parent = pathIndexFile.getParentFile();
            if (parent != null) {
                parent.mkdirs();
            }
            try (java.io.FileOutputStream output = new java.io.FileOutputStream(pathIndexFile)) {
                properties.store(output, "Cobble writer path index by dbId");
            }
            return;
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        properties.store(output, "Cobble writer path index by dbId");
        openMetadata(config).write("writer-paths.properties", output.toByteArray());
    }

    static String coordinatorWriterPath(
            CobbleDynamicTableSink.SerializableConfig config, CobbleShardCommittable committable) {
        if (!isRemoteTable(config)) {
            return committable.writerPath;
        }
        return new File(stagingRoot(config), "restore-" + committable.shardSnapshot.dbId)
                .getAbsolutePath();
    }

    static void markEndOfInputSnapshot(
            CobbleDynamicTableSink.SerializableConfig config, CobbleShardCommittable committable)
            throws IOException {
        String markerName = markerFileName(committable.bucketId);
        if (isRemoteTable(config)) {
            String path = ".eoi-markers/" + markerName;
            CobbleMetadataFileIO fileIO = openMetadata(config);
            fileIO.mkdirs(".eoi-markers");
            fileIO.write(path, new CobbleShardCommittable.Serializer().serialize(committable));
            return;
        }
        File markerDir = endOfInputMarkerDirectory(config);
        Files.createDirectories(markerDir.toPath());
        File marker = new File(markerDir, markerName);
        Files.write(
                marker.toPath(), new CobbleShardCommittable.Serializer().serialize(committable));
    }

    static int countEndOfInputMarkers(CobbleDynamicTableSink.SerializableConfig config) {
        if (isRemoteTable(config)) {
            CobbleMetadataFileIO fileIO;
            try {
                fileIO = openMetadata(config);
                int count = 0;
                for (String name : fileIO.list(".eoi-markers")) {
                    Integer bucket = markerBucket(name);
                    if (bucket != null && bucket.intValue() < config.bucketCount) {
                        count++;
                    }
                }
                return count;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to list Cobble end-of-input markers.", e);
            }
        }
        File markerDir = endOfInputMarkerDirectory(config);
        File[] files =
                markerDir.listFiles(
                        (dir, name) -> {
                            Integer bucket = markerBucket(name);
                            return bucket != null && bucket.intValue() < config.bucketCount;
                        });
        return files == null ? 0 : files.length;
    }

    static void clearEndOfInputMarkers(CobbleDynamicTableSink.SerializableConfig config)
            throws IOException {
        if (isRemoteTable(config)) {
            clearEndOfInputMarkers(openMetadata(config));
            return;
        }
        File markerDir = endOfInputMarkerDirectory(config);
        File[] files = markerDir.listFiles((dir, name) -> name.endsWith(".marker"));
        if (files == null) {
            return;
        }
        for (File file : files) {
            Files.deleteIfExists(file.toPath());
        }
    }

    static void clearEndOfInputMarkers(CobbleMetadataFileIO fileIO) throws IOException {
        for (String name : fileIO.list(".eoi-markers")) {
            fileIO.delete(".eoi-markers/" + name);
        }
        fileIO.delete(".eoi-markers");
    }

    static List<CobbleShardCommittable> listEndOfInputCommittables(
            CobbleDynamicTableSink.SerializableConfig config) throws IOException {
        if (isRemoteTable(config)) {
            List<CobbleShardCommittable> committables = new ArrayList<>();
            CobbleShardCommittable.Serializer serializer = new CobbleShardCommittable.Serializer();
            CobbleMetadataFileIO fileIO = openMetadata(config);
            for (String name : fileIO.list(".eoi-markers")) {
                Integer bucket = markerBucket(name);
                if (bucket == null) {
                    continue;
                }
                committables.add(
                        deserializeMarker(
                                serializer,
                                bucket.intValue(),
                                fileIO.read(".eoi-markers/" + name)));
            }
            return committables;
        }
        File markerDir = endOfInputMarkerDirectory(config);
        File[] files = markerDir.listFiles((dir, name) -> markerBucket(name) != null);
        List<CobbleShardCommittable> committables = new ArrayList<>();
        if (files == null) {
            return committables;
        }
        CobbleShardCommittable.Serializer serializer = new CobbleShardCommittable.Serializer();
        for (File file : files) {
            Integer slot = markerBucket(file.getName());
            committables.add(
                    deserializeMarker(
                            serializer, slot.intValue(), Files.readAllBytes(file.toPath())));
        }
        return committables;
    }

    static List<ShardSnapshot> resolveEndOfInputSnapshots(
            CobbleDynamicTableSink.SerializableConfig config,
            List<CobbleShardCommittable> committables,
            Map<String, String> writerPathByDbId)
            throws IOException {
        Map<Integer, CobbleShardCommittable> byBucket = new TreeMap<>();
        for (CobbleShardCommittable committable : committables) {
            validateEndOfInputCommittable(config, committable);
            CobbleShardCommittable previous =
                    byBucket.put(Integer.valueOf(committable.bucketId), committable);
            if (previous != null) {
                throw new IOException(
                        "Duplicate end-of-input marker for bucket "
                                + committable.bucketId
                                + ".");
            }
        }

        List<ShardSnapshot> snapshots = new ArrayList<>(config.bucketCount);
        for (int bucket = 0; bucket < config.bucketCount; bucket++) {
            CobbleShardCommittable committable = byBucket.get(Integer.valueOf(bucket));
            if (committable == null) {
                throw new IOException(
                        "Missing end-of-input marker for bucket " + bucket + ".");
            }
            ShardSnapshot snapshot = committable.shardSnapshot;
            snapshots.add(snapshot);
            writerPathByDbId.put(snapshot.dbId, coordinatorWriterPath(config, committable));
        }
        if (byBucket.size() != config.bucketCount) {
            throw new IOException(
                    "Unexpected end-of-input markers outside the configured bucket count.");
        }
        return snapshots;
    }

    private static void validateEndOfInputCommittable(
            CobbleDynamicTableSink.SerializableConfig config, CobbleShardCommittable committable)
            throws IOException {
        if (committable == null
                || committable.bucketId < 0
                || committable.bucketId >= config.bucketCount) {
            throw new IOException("Invalid bucket in end-of-input marker.");
        }
        if (committable.totalBuckets != config.bucketCount) {
            throw new IOException("Mismatched bucket count in end-of-input marker.");
        }
        ShardSnapshot snapshot = committable.shardSnapshot;
        if (snapshot == null
                || snapshot.dbId == null
                || snapshot.dbId.isEmpty()
                || snapshot.ranges == null
                || snapshot.ranges.isEmpty()) {
            throw new IOException("End-of-input marker contains an incomplete shard snapshot.");
        }
        for (ShardSnapshot.Range range : snapshot.ranges) {
            if (range == null
                    || range.start < 0
                    || range.end < range.start
                    || range.end >= config.bucketCount) {
                throw new IOException("End-of-input marker contains an invalid shard range.");
            }
        }
    }

    private static String markerFileName(int bucketId) {
        return bucketId + ".marker";
    }

    private static CobbleShardCommittable deserializeMarker(
            CobbleShardCommittable.Serializer serializer, int markerBucket, byte[] bytes)
            throws IOException {
        CobbleShardCommittable committable = serializer.deserialize(serializer.getVersion(), bytes);
        if (committable.bucketId != markerBucket) {
            throw new IOException(
                    "End-of-input marker bucket does not match its serialized bucket.");
        }
        return committable;
    }

    private static Integer markerBucket(String name) {
        if (name == null || !name.endsWith(".marker")) {
            return null;
        }
        String value = name.substring(0, name.length() - ".marker".length());
        try {
            int bucket = Integer.parseInt(value);
            return bucket >= 0 && markerFileName(bucket).equals(name)
                    ? Integer.valueOf(bucket)
                    : null;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static File endOfInputMarkerDirectory(
            CobbleDynamicTableSink.SerializableConfig config) {
        return new File(tableRootDirectory(config), ".eoi-markers");
    }

    private static File tableRootDirectory(CobbleDynamicTableSink.SerializableConfig config) {
        URI uri = URI.create(config.pathUri);
        if ("file".equalsIgnoreCase(uri.getScheme())) {
            return new File(uri);
        }
        throw new IllegalArgumentException("Remote Cobble table roots are not local directories.");
    }

    private static File stagingRoot(CobbleDynamicTableSink.SerializableConfig config) {
        String identity =
                UUID.nameUUIDFromBytes(config.pathUri.getBytes(StandardCharsets.UTF_8)).toString();
        return new File(
                new File(System.getProperty("java.io.tmpdir"), "cobble-flink-sink"), identity);
    }

    static boolean isRemoteTable(CobbleDynamicTableSink.SerializableConfig config) {
        URI uri = URI.create(config.pathUri);
        return uri.getScheme() != null && !"file".equalsIgnoreCase(uri.getScheme());
    }

    private static CobbleMetadataFileIO openMetadata(
            CobbleDynamicTableSink.SerializableConfig config) throws IOException {
        return CobbleMetadataFileIO.open(config.pathUri, config.storageOptions);
    }

    private static void copyProperties(Properties properties, Map<String, String> destination) {
        for (String name : properties.stringPropertyNames()) {
            destination.put(name, properties.getProperty(name));
        }
    }
}
