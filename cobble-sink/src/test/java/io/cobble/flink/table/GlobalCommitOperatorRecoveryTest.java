package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.Table;
import io.cobble.table.Value;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

class GlobalCommitOperatorRecoveryTest {

    @TempDir private Path tempDir;

    @Test
    void restoredPendingCommittableIsReplayed() throws Exception {
        CobbleDynamicTableSink.SerializableConfig config = config(tempDir.resolve("replay"), 1);
        CobbleShardCommittable committable = createCommittable(config);
        OperatorSubtaskState snapshot = snapshotPending(config, committable, 7L);

        try (OneInputStreamOperatorTestHarness<CommittableMessage<CobbleShardCommittable>, ?>
                restored = harness(config)) {
            restored.initializeState(snapshot);
            restored.open();

            try (DbCoordinator coordinator =
                    DbCoordinator.open(CobbleSinkPaths.createCoordinatorConfig(config))) {
                GlobalSnapshot committed = coordinator.loadCurrentGlobalSnapshot();
                assertNotNull(committed);
                assertEquals(1, committed.shardSnapshots.size());
                assertEquals(
                        committable.shardSnapshot.snapshotId,
                        committed.shardSnapshots.get(0).snapshotId);
            }
            CobbleSinkPaths.markEndOfInputSnapshot(config, committable);
        }
    }

    @Test
    void failedCommitRemainsPendingInOperatorState() throws Exception {
        CobbleDynamicTableSink.SerializableConfig config = config(tempDir.resolve("retry"), 2);
        CobbleShardCommittable incomplete =
                new CobbleShardCommittable(2, 0, "/writer", shardSnapshot("db", 1L, 0, 0));

        OperatorSubtaskState retrySnapshot;
        try (OneInputStreamOperatorTestHarness<CommittableMessage<CobbleShardCommittable>, ?>
                first = harness(config)) {
            first.initializeEmptyState();
            first.open();
            first.processElement(message(incomplete, 9L));
            assertThrows(IOException.class, () -> first.notifyOfCompletedCheckpoint(9L));
            retrySnapshot = first.snapshot(10L, 10L);
        }

        try (OneInputStreamOperatorTestHarness<CommittableMessage<CobbleShardCommittable>, ?>
                restored = harness(config)) {
            restored.initializeState(retrySnapshot);
            assertThrows(IOException.class, restored::open);
        }
    }

    private OperatorSubtaskState snapshotPending(
            CobbleDynamicTableSink.SerializableConfig config,
            CobbleShardCommittable committable,
            long checkpointId)
            throws Exception {
        try (OneInputStreamOperatorTestHarness<CommittableMessage<CobbleShardCommittable>, ?>
                first = harness(config)) {
            first.initializeEmptyState();
            first.processElement(message(committable, checkpointId));
            return first.snapshot(checkpointId, checkpointId);
        }
    }

    private CobbleShardCommittable createCommittable(
            CobbleDynamicTableSink.SerializableConfig config) throws Exception {
        String writerPath =
                CobbleSinkPaths.bucketWriterLocalDirectory(config, 0, 0).getAbsolutePath();
        try (Table table =
                Table.writerBuilder(CobbleSinkPaths.createTableWriterRuntime(config, 0, 0, 1))
                        .tableName(CobbleTableRowConverter.TABLE_NAME)
                        .bucket(0)
                        .create(config.tableSchema())) {
            table.put(Arrays.asList(Value.int64(1L), Value.string("one")));
            ShardSnapshot snapshot = table.snapshot();
            return new CobbleShardCommittable(1, 0, writerPath, snapshot);
        }
    }

    private OneInputStreamOperatorTestHarness<CommittableMessage<CobbleShardCommittable>, ?>
            harness(CobbleDynamicTableSink.SerializableConfig config) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(new GlobalCommitOperatorFactory(config));
    }

    private StreamRecord<CommittableMessage<CobbleShardCommittable>> message(
            CobbleShardCommittable committable, long checkpointId) {
        return new StreamRecord<>(new CommittableWithLineage<>(committable, checkpointId, 0));
    }

    private CobbleDynamicTableSink.SerializableConfig config(Path path, int buckets) {
        return new CobbleDynamicTableSink.SerializableConfig(
                path.toUri().toString(),
                buckets,
                buckets,
                buckets,
                false,
                1024L * 1024L,
                Collections.singletonList(
                        new CobbleDynamicTableSink.SerializableField("id", "BIGINT", 0, -1)),
                Collections.singletonList(
                        new CobbleDynamicTableSink.SerializableField("name", "STRING", 1, 0)));
    }

    private static ShardSnapshot shardSnapshot(
            String dbId, long snapshotId, int startBucket, int endBucket) {
        ShardSnapshot snapshot = new ShardSnapshot();
        snapshot.dbId = dbId;
        snapshot.snapshotId = snapshotId;
        snapshot.manifestPath = "/missing/manifest";
        ShardSnapshot.Range range = new ShardSnapshot.Range();
        range.start = startBucket;
        range.end = endBucket;
        snapshot.ranges = Arrays.asList(range);
        return snapshot;
    }
}
