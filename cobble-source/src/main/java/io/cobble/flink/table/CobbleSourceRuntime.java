package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ScanPlan;
import io.cobble.ScanSplit;
import io.cobble.ShardSnapshot;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Snapshot lookup and split-rebuild helpers for the Cobble FLIP-27 source. */
final class CobbleSourceRuntime {

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

    static List<CobbleSourceSplit> createSourceSplits(
            CobbleDynamicTableSource.SerializableConfig config, GlobalSnapshot snapshot)
            throws IOException {
        int bucketCount = validateSnapshot(config, snapshot);
        boolean[] coveredBuckets = new boolean[bucketCount];
        List<CobbleSourceSplit> splits = new ArrayList<>();
        for (ScanSplit plannedSplit : ScanPlan.fromGlobalSnapshot(snapshot).splits()) {
            if (plannedSplit == null
                    || plannedSplit.shard == null
                    || plannedSplit.shard.ranges == null) {
                continue;
            }
            ShardSnapshot.Range splitRange = extractSplitRange(plannedSplit);
            markCoveredBuckets(splitRange, bucketCount, coveredBuckets, snapshot.id);
            splits.add(
                    CobbleSourceSplit.forSnapshot(
                            splitRange.start, splitRange.end, bucketCount, snapshot.id));
        }
        ensureCompleteBucketCoverage(coveredBuckets, snapshot.id);
        return splits;
    }

    static ScanSplit resolveSourceSplit(
            CobbleDynamicTableSource.SerializableConfig config, CobbleSourceSplit split)
            throws IOException {
        GlobalSnapshot snapshot = loadSnapshotById(config, split.snapshotId);
        int bucketCount = validateSnapshot(config, snapshot);
        if (bucketCount != split.totalBuckets) {
            throw new IOException(
                    "Cobble source split "
                            + split.splitId()
                            + " expects "
                            + split.totalBuckets
                            + " buckets, but snapshot "
                            + split.snapshotId
                            + " has "
                            + bucketCount
                            + '.');
        }

        ScanSplit matchedSplit = null;
        for (ScanSplit plannedSplit : ScanPlan.fromGlobalSnapshot(snapshot).splits()) {
            if (plannedSplit == null
                    || plannedSplit.shard == null
                    || plannedSplit.shard.ranges == null) {
                continue;
            }
            ShardSnapshot.Range splitRange = extractSplitRange(plannedSplit);
            if (splitRange.start != split.rangeStartBucket
                    || splitRange.end != split.rangeEndBucket) {
                continue;
            }
            if (matchedSplit != null) {
                throw new IOException(
                        "Cobble source split "
                                + split.splitId()
                                + " resolved to multiple scan splits in snapshot "
                                + split.snapshotId
                                + '.');
            }
            matchedSplit = plannedSplit;
        }
        if (matchedSplit == null) {
            throw new IOException(
                    "Cobble source split "
                            + split.splitId()
                            + " is missing from snapshot "
                            + split.snapshotId
                            + '.');
        }
        return matchedSplit;
    }

    private static void ensureCompleteBucketCoverage(boolean[] coveredBuckets, long snapshotId)
            throws IOException {
        for (int bucket = 0; bucket < coveredBuckets.length; bucket++) {
            if (coveredBuckets[bucket]) {
                continue;
            }
            throw new IOException(
                    "Missing shard coverage for bucket "
                            + bucket
                            + " in snapshot "
                            + snapshotId
                            + ".");
        }
    }

    private static ShardSnapshot.Range extractSplitRange(ScanSplit split) throws IOException {
        if (split.shard.ranges.size() != 1 || split.shard.ranges.get(0) == null) {
            throw new IOException(
                    "Cobble source expects each planned scan split to have one range.");
        }
        return split.shard.ranges.get(0);
    }

    private static void markCoveredBuckets(
            ShardSnapshot.Range range, int bucketCount, boolean[] coveredBuckets, long snapshotId)
            throws IOException {
        for (int bucket = range.start; bucket <= range.end; bucket++) {
            if (bucket < 0 || bucket >= bucketCount) {
                throw new IOException("Invalid bucket " + bucket + " in global snapshot.");
            }
            if (coveredBuckets[bucket]) {
                throw new IOException(
                        "Duplicate shard coverage for bucket "
                                + bucket
                                + " in snapshot "
                                + snapshotId
                                + ".");
            }
            coveredBuckets[bucket] = true;
        }
    }

    static Config createSourceScanConfig(
            CobbleDynamicTableSource.SerializableConfig config, int totalBuckets)
            throws IOException {
        Config scanConfig =
                new Config().numColumns(config.valueFields.size()).totalBuckets(totalBuckets);
        scanConfig.governanceMode = Config.GovernanceMode.NOOP;
        scanConfig.logConsole = Boolean.FALSE;

        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = tableRootDirectory(config).getAbsolutePath();
        volume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT);
        scanConfig.addVolume(volume);
        return scanConfig;
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
