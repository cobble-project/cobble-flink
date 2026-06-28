package io.cobble.flink.table;

import io.cobble.Config;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** Path and config helpers for the initial Cobble SQL sink. */
final class CobbleSinkPaths {

    private CobbleSinkPaths() {}

    static File writerLocalDirectory(
            CobbleDynamicTableSink.SerializableConfig config, int subtaskId) {
        return tableRootDirectory(config);
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
        return tableRootDirectory(config);
    }

    static File tableRootPath(CobbleDynamicTableSink.SerializableConfig config) {
        return tableRootDirectory(config);
    }

    static Config createWriterConfig(
            CobbleDynamicTableSink.SerializableConfig config, int subtaskId) throws IOException {
        File localDir = writerLocalDirectory(config, subtaskId);
        Files.createDirectories(localDir.toPath());
        return createWriterConfigForLocalDir(config, localDir);
    }

    static Config createWriterConfigForWriterPath(
            CobbleDynamicTableSink.SerializableConfig config, String writerPath) {
        return createWriterConfigForLocalDir(config, new File(writerPath));
    }

    private static Config createWriterConfigForLocalDir(
            CobbleDynamicTableSink.SerializableConfig config, File localDir) {
        Config dbConfig =
                new Config().numColumns(config.valueFields.size()).totalBuckets(config.bucketCount);
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
                positiveInt(
                        config.sinkWriterBufferMemoryBytes,
                        CobbleTableOptions.SINK_WRITER_BUFFER_MEMORY.key());
        dbConfig.memtableBufferCount = 1;

        Config.VolumeDescriptor localVolume = new Config.VolumeDescriptor();
        localVolume.baseDir = localDir.getAbsolutePath();
        localVolume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META);
        dbConfig.addVolume(localVolume);

        Config.VolumeDescriptor sharedSnapshotVolume = new Config.VolumeDescriptor();
        sharedSnapshotVolume.baseDir = config.pathUri;
        sharedSnapshotVolume.kinds = Arrays.asList(Config.VolumeUsageKind.SNAPSHOT);
        dbConfig.addVolume(sharedSnapshotVolume);
        return dbConfig;
    }

    private static int positiveInt(long value, String optionKey) {
        if (value <= 0L || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    optionKey + " must be in (0, " + Integer.MAX_VALUE + "].");
        }
        return (int) value;
    }

    static Config createCoordinatorConfig(CobbleDynamicTableSink.SerializableConfig config)
            throws IOException {
        File localDir = coordinatorLocalDirectory(config);
        Files.createDirectories(localDir.toPath());

        Config coordinatorConfig = new Config().totalBuckets(config.bucketCount);
        coordinatorConfig.governanceMode = Config.GovernanceMode.NOOP;
        coordinatorConfig.logConsole = false;
        coordinatorConfig.logPath = new File(localDir, "cobble-coordinator.log").getAbsolutePath();

        Config.VolumeDescriptor localVolume = new Config.VolumeDescriptor();
        localVolume.baseDir = localDir.getAbsolutePath();
        localVolume.kinds =
                Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        coordinatorConfig.addVolume(localVolume);
        return coordinatorConfig;
    }

    static File writerPathIndexFile(CobbleDynamicTableSink.SerializableConfig config) {
        return new File(tableRootDirectory(config), "writer-paths.properties");
    }

    static void markEndOfInputSnapshot(
            CobbleDynamicTableSink.SerializableConfig config, CobbleShardCommittable committable)
            throws IOException {
        File markerDir = endOfInputMarkerDirectory(config);
        Files.createDirectories(markerDir.toPath());
        File marker = new File(markerDir, committable.shardSnapshot.dbId + ".marker");
        Files.write(
                marker.toPath(), new CobbleShardCommittable.Serializer().serialize(committable));
    }

    static boolean hasEndOfInputMarkers(CobbleDynamicTableSink.SerializableConfig config) {
        return countEndOfInputMarkers(config) > 0;
    }

    static int countEndOfInputMarkers(CobbleDynamicTableSink.SerializableConfig config) {
        File markerDir = endOfInputMarkerDirectory(config);
        File[] files = markerDir.listFiles((dir, name) -> name.endsWith(".marker"));
        return files == null ? 0 : files.length;
    }

    static void clearEndOfInputMarkers(CobbleDynamicTableSink.SerializableConfig config)
            throws IOException {
        File markerDir = endOfInputMarkerDirectory(config);
        File[] files = markerDir.listFiles((dir, name) -> name.endsWith(".marker"));
        if (files == null) {
            return;
        }
        for (File file : files) {
            Files.deleteIfExists(file.toPath());
        }
    }

    static List<CobbleShardCommittable> listEndOfInputCommittables(
            CobbleDynamicTableSink.SerializableConfig config) throws IOException {
        File markerDir = endOfInputMarkerDirectory(config);
        File[] files = markerDir.listFiles((dir, name) -> name.endsWith(".marker"));
        List<CobbleShardCommittable> committables = new ArrayList<>();
        if (files == null) {
            return committables;
        }
        Arrays.sort(files, Comparator.comparingLong(File::lastModified).reversed());
        CobbleShardCommittable.Serializer serializer = new CobbleShardCommittable.Serializer();
        for (File file : files) {
            committables.add(
                    serializer.deserialize(
                            serializer.getVersion(), Files.readAllBytes(file.toPath())));
        }
        return committables;
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
        String path = uri.getPath();
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Cobble sink path must include a valid URI path, but got " + config.pathUri);
        }
        return new File(path);
    }
}
