package io.cobble.flink.table;

import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Deterministic single-bucket writer assignment and checkpoint-state validation. */
final class CobbleBucketWriterStates {
    private CobbleBucketWriterStates() {}

    static List<Integer> ownedBuckets(
            CobbleDynamicTableSink.SerializableConfig config, int subtaskId) {
        int start = CobbleSinkPaths.writerRangeStart(config, subtaskId);
        int end = CobbleSinkPaths.writerRangeEnd(config, subtaskId);
        if (end < start) {
            return Collections.emptyList();
        }
        List<Integer> buckets = new ArrayList<Integer>(end - start + 1);
        for (int bucket = start; bucket <= end; bucket++) {
            buckets.add(Integer.valueOf(bucket));
        }
        return buckets;
    }

    static Map<Integer, CobbleBucketWriterState> restoredByBucket(
            Collection<CobbleBucketWriterState> recoveredState, List<Integer> ownedBuckets)
            throws IOException {
        if (recoveredState == null || recoveredState.isEmpty()) {
            if (recoveredState == null || ownedBuckets.isEmpty()) {
                return Collections.emptyMap();
            }
            throw new IOException(
                    "Cobble table sink restore state is empty for assigned buckets. "
                            + "Restoring as a fresh writer is not supported.");
        }
        Map<Integer, CobbleBucketWriterState> byBucket = new LinkedHashMap<>();
        for (CobbleBucketWriterState state : recoveredState) {
            if (state == null) {
                throw new IOException("Cobble table sink restore state contains null.");
            }
            int bucket = state.bucketId();
            requireStableBucketIdentity(state.dbId, bucket);
            if (!ownedBuckets.contains(Integer.valueOf(bucket))) {
                throw new IOException(
                        "Cobble table sink cannot restore bucket "
                                + bucket
                                + " into this writer assignment. Rescaling is not supported.");
            }
            if (byBucket.put(Integer.valueOf(bucket), state) != null) {
                throw new IOException("Cobble table sink restore state repeats bucket " + bucket + ".");
            }
        }
        if (byBucket.size() != ownedBuckets.size()) {
            throw new IOException(
                    "Cobble table sink restore state does not cover every assigned bucket. "
                            + "Rescaling is not supported.");
        }
        return byBucket;
    }

    static Map<Integer, CobbleBucketWriterState> currentByBucket(
            GlobalSnapshot snapshot, List<Integer> ownedBuckets) throws IOException {
        if (snapshot == null) {
            return Collections.emptyMap();
        }
        if (snapshot.shardSnapshots == null) {
            throw new IOException("Cobble table sink current snapshot has no shard coverage.");
        }
        Map<Integer, CobbleBucketWriterState> byBucket = new LinkedHashMap<>();
        for (ShardSnapshot shard : snapshot.shardSnapshots) {
            if (shard == null || shard.ranges == null) {
                continue;
            }
            for (ShardSnapshot.Range range : shard.ranges) {
                if (range == null || range.start != range.end) {
                    throw new IOException(
                            "Cobble table sink requires one bucket per native writer snapshot.");
                }
                Integer bucket = Integer.valueOf(range.start);
                if (ownedBuckets.contains(bucket)) {
                    requireStableBucketIdentity(shard.dbId, range.start);
                    if (byBucket.put(
                                    bucket,
                                    CobbleBucketWriterState.bucket(
                                            shard.dbId, shard.snapshotId, range.start))
                            != null) {
                        throw new IOException(
                                "Cobble table sink current snapshot repeats bucket "
                                        + range.start
                                        + ".");
                    }
                }
            }
        }
        if (byBucket.size() != ownedBuckets.size()) {
            throw new IOException(
                    "Cobble table sink current snapshot does not cover every assigned bucket. "
                            + "Rescaling is not supported.");
        }
        return byBucket;
    }

    private static void requireStableBucketIdentity(String dbId, int bucket) throws IOException {
        String expected = "bucket-" + bucket;
        if (!expected.equals(dbId)) {
            throw new IOException(
                    "Cobble table sink cannot resume legacy shard identity "
                            + dbId
                            + " for bucket "
                            + bucket
                            + "; expected "
                            + expected
                            + ".");
        }
    }
}
