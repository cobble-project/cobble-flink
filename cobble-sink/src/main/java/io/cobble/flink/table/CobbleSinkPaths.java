package io.cobble.flink.table;

import io.cobble.Config;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;

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
        dbConfig.snapshotOnlyTrack = Boolean.TRUE;
        dbConfig.snapshotDisableIncrementalBaseLink = Boolean.TRUE;
        dbConfig.memtableType = Config.MemtableType.VEC;
        dbConfig.governanceMode = Config.GovernanceMode.NOOP;
        dbConfig.logConsole = Boolean.FALSE;
        dbConfig.logPath = new File(localDir, "cobble-writer.log").getAbsolutePath();

        Config.VolumeDescriptor localVolume = new Config.VolumeDescriptor();
        localVolume.baseDir = localDir.getAbsolutePath();
        localVolume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META);
        dbConfig.addVolume(localVolume);

        Config.VolumeDescriptor sharedSnapshotVolume = new Config.VolumeDescriptor();
        sharedSnapshotVolume.baseDir = tableRootDirectory(config).getAbsolutePath();
        sharedSnapshotVolume.kinds = Arrays.asList(Config.VolumeUsageKind.SNAPSHOT);
        dbConfig.addVolume(sharedSnapshotVolume);
        return dbConfig;
    }

    static Config createCoordinatorConfig(CobbleDynamicTableSink.SerializableConfig config)
            throws IOException {
        File localDir = coordinatorLocalDirectory(config);
        Files.createDirectories(localDir.toPath());

        Config coordinatorConfig = new Config().totalBuckets(config.bucketCount);
        coordinatorConfig.governanceMode = Config.GovernanceMode.NOOP;
        coordinatorConfig.logConsole = Boolean.FALSE;
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
            CobbleDynamicTableSink.SerializableConfig config, String dbId) throws IOException {
        File markerDir = endOfInputMarkerDirectory(config);
        Files.createDirectories(markerDir.toPath());
        File marker = new File(markerDir, dbId + ".marker");
        if (!marker.exists()) {
            Files.createFile(marker.toPath());
        }
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

    private static File endOfInputMarkerDirectory(
            CobbleDynamicTableSink.SerializableConfig config) {
        return new File(tableRootDirectory(config), ".eoi-markers");
    }

    private static File tableRootDirectory(CobbleDynamicTableSink.SerializableConfig config) {
        URI uri = URI.create(config.pathUri);
        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException(
                    "Cobble sink currently supports only file:// paths, but got " + config.pathUri);
        }
        return new File(uri);
    }
}
