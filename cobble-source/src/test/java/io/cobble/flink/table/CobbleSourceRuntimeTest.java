package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleConnectorStorageOptions;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class CobbleSourceRuntimeTest {

    @Test
    void createsStableSplitIdsFromRangeTriples() throws Exception {
        GlobalSnapshot snapshot = baseSnapshot();

        List<CobbleSourceSplit> splits =
                CobbleSourceRuntime.createSourceSplits(baseConfig(4), snapshot);

        assertEquals(2, splits.size());
        assertEquals("0:1:4", splits.get(0).splitId());
        assertEquals(0, splits.get(0).rangeStartBucket);
        assertEquals(1, splits.get(0).rangeEndBucket);
        assertEquals(4, splits.get(0).totalBuckets);

        assertEquals("2:3:4", splits.get(1).splitId());
        assertEquals(2, splits.get(1).rangeStartBucket);
        assertEquals(3, splits.get(1).rangeEndBucket);
        assertEquals(4, splits.get(1).totalBuckets);
    }

    @Test
    void infersBucketCountFromSnapshotWhenConfigOmitsIt() throws Exception {
        GlobalSnapshot snapshot = baseSnapshot();

        List<CobbleSourceSplit> splits =
                CobbleSourceRuntime.createSourceSplits(baseConfig(-1), snapshot);

        assertEquals(2, splits.size());
        assertEquals("0:1:4", splits.get(0).splitId());
        assertEquals("2:3:4", splits.get(1).splitId());
    }

    @Test
    void rejectsConfiguredBucketCountMismatch() {
        GlobalSnapshot snapshot = baseSnapshot();

        assertThrows(
                java.io.IOException.class,
                () -> CobbleSourceRuntime.createSourceSplits(baseConfig(3), snapshot));
    }

    @Test
    void appliesReadMemoryBudgetToBlockCache() throws Exception {
        CobbleDynamicTableSource.SerializableConfig config = baseConfig(4);
        Config scanConfig = CobbleSourceRuntime.createSourceScanConfig(config, 4);
        Config lookupConfig = CobbleSourceRuntime.createLookupReaderConfig(config, 4);

        assertEquals(1, scanConfig.memtableCapacity.intValue());
        assertEquals(1, scanConfig.memtableBufferCount.intValue());
        assertEquals(8 * 1024 * 1024, scanConfig.blockCacheSize.intValue());
        assertEquals(false, scanConfig.blockCacheHybridEnabled.booleanValue());
        assertEquals(0, scanConfig.blockCacheHybridDiskSize.intValue());

        assertNull(lookupConfig.memtableCapacity);
        assertNull(lookupConfig.memtableBufferCount);
        assertNull(lookupConfig.blockCacheSize);
        assertEquals(256 * 1024 * 1024, lookupConfig.reader.blockCacheSize.intValue());
    }

    @Test
    void propagatesGenericStorageOptionsToScanLookupAndCoordinatorVolumes() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("storage.option.endpoint", "https://oss.example.com");
        values.put("storage.option.access_key_id", "oss-access");
        values.put("storage.option.access_key_secret", "oss-secret");
        CobbleDynamicTableSource.SerializableConfig config =
                new CobbleDynamicTableSource.SerializableConfig(
                        "oss://bucket/table",
                        4,
                        "1",
                        "batch",
                        1000L,
                        0L,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        CobbleConnectorStorageOptions.from(values));

        Config scan = CobbleSourceRuntime.createSourceScanConfig(config, 4);
        Config lookup = CobbleSourceRuntime.createLookupReaderConfig(config, 4);
        Config coordinator = CobbleSourceRuntime.createCoordinatorConfig(config);
        assertRemoteVolume(scan.volumes.get(0));
        assertRemoteVolume(lookup.volumes.get(0));
        assertRemoteVolume(coordinator.volumes.get(0));
        assertEquals(1, scan.volumes.size());
        assertEquals(1, lookup.volumes.size());
        assertEquals(1, coordinator.volumes.size());
    }

    private static CobbleDynamicTableSource.SerializableConfig baseConfig(int bucketCount) {
        return new CobbleDynamicTableSource.SerializableConfig(
                "file:///tmp/cobble-source-runtime",
                bucketCount,
                "1",
                "batch",
                1000L,
                256L * 1024L * 1024L,
                Collections.emptyList(),
                Collections.emptyList());
    }

    private static GlobalSnapshot baseSnapshot() {
        GlobalSnapshot snapshot = new GlobalSnapshot();
        snapshot.id = 7L;
        snapshot.totalBuckets = 4;
        snapshot.shardSnapshots = Arrays.asList(shard(0, 1), shard(2, 3));
        return snapshot;
    }

    private static void assertRemoteVolume(Config.VolumeDescriptor volume) {
        assertEquals("oss://bucket/table", volume.baseDir);
        assertEquals("https://oss.example.com", volume.customOptions.get("endpoint"));
        assertEquals("oss-access", volume.customOptions.get("access_key_id"));
        assertEquals("oss-secret", volume.customOptions.get("access_key_secret"));
    }

    private static ShardSnapshot shard(int startBucket, int endBucket) {
        ShardSnapshot shardSnapshot = new ShardSnapshot();
        ShardSnapshot.Range range = new ShardSnapshot.Range();
        range.start = startBucket;
        range.end = endBucket;
        shardSnapshot.ranges = Collections.singletonList(range);
        shardSnapshot.manifestPath = "file:///tmp/shard-" + startBucket + "-" + endBucket;
        return shardSnapshot;
    }
}
