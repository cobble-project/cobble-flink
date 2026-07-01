package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/** Unit coverage for state source runtime split planning. */
class CobbleStateSourceRuntimeTest {

    @Test
    void createsOneSplitPerSnapshotRange() throws Exception {
        GlobalSnapshot snapshot = snapshot(7L, 4, range(0, 1), range(2, 3));

        java.util.List<CobbleStateSourceSplit> splits =
                CobbleStateSourceRuntime.createStateSourceSplits(config(-1), snapshot, 7L);

        assertEquals(2, splits.size());
        assertEquals("0:1:4", splits.get(0).splitId());
        assertEquals("2:3:4", splits.get(1).splitId());
        assertEquals(7L, splits.get(0).checkpointId);
    }

    @Test
    void rejectsBucketMismatch() {
        GlobalSnapshot snapshot = snapshot(7L, 4, range(0, 3));

        IOExceptionRunnable action =
                () -> CobbleStateSourceRuntime.createStateSourceSplits(config(8), snapshot, 7L);
        Exception error = assertThrows(Exception.class, action::run);
        assertTrue(error.getMessage().contains("bucket count mismatch"));
    }

    @Test
    void rejectsDuplicateCoverage() {
        GlobalSnapshot snapshot = snapshot(7L, 4, range(0, 2), range(2, 3));

        IOExceptionRunnable action =
                () -> CobbleStateSourceRuntime.createStateSourceSplits(config(-1), snapshot, 7L);
        Exception error = assertThrows(Exception.class, action::run);
        assertTrue(error.getMessage().contains("Duplicate key-group coverage"));
    }

    @Test
    void rejectsMissingCoverage() {
        GlobalSnapshot snapshot = snapshot(7L, 4, range(0, 1));

        IOExceptionRunnable action =
                () -> CobbleStateSourceRuntime.createStateSourceSplits(config(-1), snapshot, 7L);
        Exception error = assertThrows(Exception.class, action::run);
        assertTrue(error.getMessage().contains("Missing key-group coverage"));
    }

    private static StateSourceConfig config(int bucketCount) {
        return new StateSourceConfig(
                "file:///tmp/checkpoints",
                StateSourceConfig.Layout.CHECKPOINT_ROOT,
                "operator-1",
                "orders",
                "value",
                "7",
                "batch",
                7L,
                bucketCount,
                0L,
                Collections.singletonList(
                        new StateSourceField("key", "INT", StateSourceField.Group.STATE_KEY, 0)));
    }

    private static GlobalSnapshot snapshot(
            long id, int totalBuckets, ShardSnapshot.Range... ranges) {
        GlobalSnapshot snapshot = new GlobalSnapshot();
        snapshot.id = id;
        snapshot.totalBuckets = totalBuckets;
        ShardSnapshot shard = new ShardSnapshot();
        shard.ranges = Arrays.asList(ranges);
        snapshot.shardSnapshots = Collections.singletonList(shard);
        return snapshot;
    }

    private static ShardSnapshot.Range range(int start, int end) {
        ShardSnapshot.Range range = new ShardSnapshot.Range();
        range.start = start;
        range.end = end;
        return range;
    }

    private interface IOExceptionRunnable {
        void run() throws Exception;
    }
}
