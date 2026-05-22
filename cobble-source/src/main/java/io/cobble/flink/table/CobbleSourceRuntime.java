package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.structured.Db;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Snapshot lookup and restore helpers for the Cobble SQL source. */
final class CobbleSourceRuntime {

    static final byte[] MIN_KEY = new byte[0];
    static final byte[] MAX_KEY;

    static {
        MAX_KEY = new byte[32];
        Arrays.fill(MAX_KEY, (byte) 0xFF);
    }

    private CobbleSourceRuntime() {}

    static GlobalSnapshot loadConfiguredSnapshot(CobbleDynamicTableSource.SerializableConfig config)
            throws IOException {
        if ("latest".equals(config.scanCheckpointId)) {
            return loadLatestSnapshot(config);
        }
        return loadSnapshotById(config, Long.parseLong(config.scanCheckpointId));
    }

    static GlobalSnapshot loadLatestSnapshotIfNewer(
            CobbleDynamicTableSource.SerializableConfig config, long currentSnapshotId)
            throws IOException {
        GlobalSnapshot latest = loadLatestSnapshot(config);
        if (latest == null || latest.id <= currentSnapshotId) {
            return null;
        }
        return latest;
    }

    static GlobalSnapshot loadSnapshotById(
            CobbleDynamicTableSource.SerializableConfig config, long snapshotId)
            throws IOException {
        try (DbCoordinator coordinator = DbCoordinator.open(createCoordinatorConfig(config))) {
            GlobalSnapshot snapshot = coordinator.getGlobalSnapshot(snapshotId);
            if (snapshot == null) {
                throw new IOException(
                        "Cobble source could not find checkpoint " + snapshotId + ".");
            }
            validateSnapshot(config, snapshot);
            return snapshot;
        }
    }

    static List<CobbleSourceSplit> createBucketSplits(
            CobbleDynamicTableSource.SerializableConfig config, GlobalSnapshot snapshot)
            throws IOException {
        int bucketCount = validateSnapshot(config, snapshot);

        CobbleSourceSplit[] splitsByBucket = new CobbleSourceSplit[bucketCount];
        for (ShardSnapshot shardSnapshot : snapshot.shardSnapshots) {
            if (shardSnapshot == null || shardSnapshot.ranges == null) {
                continue;
            }
            List<Integer> coveredBuckets = flattenCoveredBuckets(shardSnapshot.ranges);
            for (Integer coveredBucket : coveredBuckets) {
                int bucket = coveredBucket.intValue();
                int splitId = splitIdForBucket(bucket);
                if (bucket < 0 || bucket >= bucketCount) {
                    throw new IOException("Invalid bucket " + bucket + " in global snapshot.");
                }
                if (splitsByBucket[bucket] != null) {
                    throw new IOException(
                            "Duplicate shard coverage for bucket "
                                    + bucket
                                    + " in snapshot "
                                    + snapshot.id
                                    + ".");
                }
                splitsByBucket[bucket] =
                        CobbleSourceSplit.forSnapshot(
                                splitId, bucket, snapshot.id, shardSnapshot.manifestPath);
            }
        }

        List<CobbleSourceSplit> splits = new ArrayList<>(bucketCount);
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            if (splitsByBucket[bucket] == null) {
                throw new IOException(
                        "Missing shard coverage for bucket "
                                + bucket
                                + " in snapshot "
                                + snapshot.id
                                + ".");
            }
            splits.add(splitsByBucket[bucket]);
        }
        return splits;
    }

    private static List<Integer> flattenCoveredBuckets(List<ShardSnapshot.Range> ranges) {
        List<Integer> buckets = new ArrayList<>();
        if (ranges == null) {
            return buckets;
        }
        for (ShardSnapshot.Range range : ranges) {
            if (range == null) {
                continue;
            }
            for (int bucket = range.start; bucket <= range.end; bucket++) {
                buckets.add(Integer.valueOf(bucket));
            }
        }
        return buckets;
    }

    private static int splitIdForBucket(int bucket) {
        // Split identity belongs to the source split model, not to snapshot metadata.
        // The current source still scans one bucket per split, so bucket id is the stable split id.
        return bucket;
    }

    static Db openSnapshotDb(
            CobbleDynamicTableSource.SerializableConfig config,
            int totalBuckets,
            Path restoreDirectory,
            String manifestPath)
            throws IOException {
        recreateDirectory(restoreDirectory);
        return Db.restoreWithManifest(
                createRestoreConfig(config, totalBuckets, restoreDirectory), manifestPath);
    }

    static void recreateDirectory(Path directory) throws IOException {
        deleteRecursively(directory);
        Files.createDirectories(directory);
    }

    static Config createLookupReaderConfig(
            CobbleDynamicTableSource.SerializableConfig config, int totalBuckets) {
        Config readerConfig = new Config().totalBuckets(totalBuckets);
        readerConfig.governanceMode = Config.GovernanceMode.NOOP;
        readerConfig.logConsole = Boolean.FALSE;

        Config.ReaderConfigEntry readerOptions = new Config.ReaderConfigEntry();
        readerOptions.blockCacheSize = 0;
        readerOptions.reloadToleranceSeconds = 0L;
        readerConfig.reader = readerOptions;

        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = tableRootDirectory(config).getAbsolutePath();
        volume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT);
        readerConfig.addVolume(volume);
        return readerConfig;
    }

    static void deleteRecursively(Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }
        if (Files.isDirectory(path)) {
            File[] children = path.toFile().listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child.toPath());
                }
            }
        }
        Files.deleteIfExists(path);
    }

    private static GlobalSnapshot loadLatestSnapshot(
            CobbleDynamicTableSource.SerializableConfig config) throws IOException {
        try (DbCoordinator coordinator = DbCoordinator.open(createCoordinatorConfig(config))) {
            GlobalSnapshot snapshot = coordinator.loadCurrentGlobalSnapshot();
            if (snapshot != null) {
                validateSnapshot(config, snapshot);
            }
            return snapshot;
        }
    }

    static int resolveSnapshotBucketCount(
            CobbleDynamicTableSource.SerializableConfig config, long snapshotId)
            throws IOException {
        return validateSnapshot(config, loadSnapshotById(config, snapshotId));
    }

    private static int validateSnapshot(
            CobbleDynamicTableSource.SerializableConfig config, GlobalSnapshot snapshot)
            throws IOException {
        if (snapshot == null) {
            throw new IOException("Cobble source snapshot is missing.");
        }
        if (snapshot.totalBuckets <= 0) {
            throw new IOException(
                    "Cobble source snapshot "
                            + snapshot.id
                            + " has invalid totalBuckets "
                            + snapshot.totalBuckets
                            + '.');
        }
        if (config.hasConfiguredBucketCount() && snapshot.totalBuckets != config.bucketCount) {
            throw new IOException(
                    "Cobble source bucket count mismatch. Source expects "
                            + config.bucketCount
                            + " buckets, but snapshot "
                            + snapshot.id
                            + " has "
                            + snapshot.totalBuckets
                            + '.');
        }
        return snapshot.totalBuckets;
    }

    private static Config createCoordinatorConfig(
            CobbleDynamicTableSource.SerializableConfig config) {
        Config coordinatorConfig = new Config();
        if (config.hasConfiguredBucketCount()) {
            coordinatorConfig.totalBuckets(config.bucketCount);
        }
        coordinatorConfig.governanceMode = Config.GovernanceMode.NOOP;
        coordinatorConfig.logConsole = Boolean.FALSE;

        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = tableRootDirectory(config).getAbsolutePath();
        volume.kinds = Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        coordinatorConfig.addVolume(volume);
        return coordinatorConfig;
    }

    private static Config createRestoreConfig(
            CobbleDynamicTableSource.SerializableConfig config,
            int totalBuckets,
            Path restoreDirectory)
            throws IOException {
        Files.createDirectories(restoreDirectory);

        Config dbConfig =
                new Config().numColumns(config.valueFields.size()).totalBuckets(totalBuckets);
        dbConfig.governanceMode = Config.GovernanceMode.NOOP;
        dbConfig.logConsole = Boolean.FALSE;

        Config.VolumeDescriptor localVolume = new Config.VolumeDescriptor();
        localVolume.baseDir = restoreDirectory.toAbsolutePath().toString();
        localVolume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META);
        dbConfig.addVolume(localVolume);

        Config.VolumeDescriptor snapshotVolume = new Config.VolumeDescriptor();
        snapshotVolume.baseDir = tableRootDirectory(config).getAbsolutePath();
        snapshotVolume.kinds = Arrays.asList(Config.VolumeUsageKind.SNAPSHOT);
        dbConfig.addVolume(snapshotVolume);
        return dbConfig;
    }

    private static File tableRootDirectory(CobbleDynamicTableSource.SerializableConfig config) {
        URI uri = URI.create(config.pathUri);
        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException(
                    "Cobble source currently supports only file:// paths, but got "
                            + config.pathUri);
        }
        return new File(uri);
    }
}
