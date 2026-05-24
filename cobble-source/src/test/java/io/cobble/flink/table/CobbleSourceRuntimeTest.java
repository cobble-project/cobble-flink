package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
