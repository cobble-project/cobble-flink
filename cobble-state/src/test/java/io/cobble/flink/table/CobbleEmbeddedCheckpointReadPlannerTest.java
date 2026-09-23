package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.Db;
import io.cobble.DbCoordinator;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleSnapshotMetadataCodec;
import io.cobble.flink.common.CobbleSnapshotMetadataPayload;
import io.cobble.table.TablePathRequest;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

class CobbleEmbeddedCheckpointReadPlannerTest {
    @TempDir java.nio.file.Path tempDir;

    @Test
    void preservesNativeRootWhenItAlsoContainsCheckpointChildren() throws Exception {
        java.nio.file.Path root = Files.createDirectory(tempDir.resolve("native-root"));
        Config config = new Config().addVolume(root.toString()).numColumns(1).totalBuckets(1);
        ShardSnapshot shard;
        try (Db db = Db.open(config)) {
            shard = db.snapshot();
        }
        try (DbCoordinator coordinator = DbCoordinator.open(config)) {
            coordinator.materializeGlobalSnapshot(1, 1L, Collections.singletonList(shard));
        }
        Files.createDirectories(root.resolve("job").resolve("chk-1"));
        Files.write(root.resolve("job").resolve("chk-1").resolve("_metadata"), new byte[] {1});

        assertTrue(
                !CobbleEmbeddedCheckpointReadPlanner.resolve(
                                config,
                                new TablePathRequest(
                                        root.toUri().toString(),
                                        "data",
                                        null,
                                        Collections.<String, String>emptyMap()))
                        .isPresent());
    }

    @Test
    void rejectsMultipleJobTreesAndOperatorsBeforeReadingShardData() throws Exception {
        java.nio.file.Path jobs = Files.createDirectory(tempDir.resolve("jobs"));
        writeCheckpoint(jobs.resolve("job-a").resolve("chk-1"), 1L, new OperatorID(1L, 2L));
        writeCheckpoint(jobs.resolve("job-b").resolve("chk-2"), 2L, new OperatorID(3L, 4L));
        Config config = new Config().addVolume(jobs.toString()).numColumns(1).totalBuckets(4);

        IOException jobsError =
                assertThrows(
                        IOException.class,
                        () ->
                                CobbleEmbeddedCheckpointReadPlanner.resolve(
                                        config,
                                        request(jobs, Collections.<String, String>emptyMap())));
        assertTrue(jobsError.getMessage().contains("multiple job checkpoint trees"));

        java.nio.file.Path checkpoint = Files.createDirectory(tempDir.resolve("savepoint"));
        writeCheckpoint(checkpoint, 3L, new OperatorID(5L, 6L), new OperatorID(7L, 8L));
        IOException operatorsError =
                assertThrows(
                        IOException.class,
                        () ->
                                CobbleEmbeddedCheckpointReadPlanner.resolve(
                                        new Config()
                                                .addVolume(checkpoint.toString())
                                                .numColumns(1)
                                                .totalBuckets(4),
                                        request(
                                                checkpoint,
                                                Collections.<String, String>emptyMap())));
        assertTrue(operatorsError.getMessage().contains("multiple Cobble operators"));
    }

    @Test
    void explicitCheckpointSelectionIgnoresBrokenNewerMetadataButLatestRejectsIt()
            throws Exception {
        java.nio.file.Path job = Files.createDirectory(tempDir.resolve("single-job"));
        java.nio.file.Path older = job.resolve("chk-1");
        writeCheckpoint(older, 1L, new OperatorID(9L, 10L));
        java.nio.file.Path newer = Files.createDirectories(job.resolve("chk-2"));
        Files.write(newer.resolve("_metadata"), new byte[] {1, 2, 3});

        Path entry = new Path(job.toUri());
        assertEquals(
                1L,
                CobbleEmbeddedCheckpointReadPlanner.selectLocation(entry, "1")
                        .checkpoint()
                        .checkpointId());

        assertThrows(
                IOException.class,
                () -> CobbleEmbeddedCheckpointReadPlanner.selectLocation(entry, "latest"));
    }

    private static TablePathRequest request(
            java.nio.file.Path path, java.util.Map<String, String> options) {
        return new TablePathRequest(path.toUri().toString(), "state", null, options);
    }

    private static void writeCheckpoint(
            java.nio.file.Path checkpoint, long checkpointId, OperatorID... operators)
            throws Exception {
        Files.createDirectories(checkpoint);
        OperatorState[] states = new OperatorState[operators.length];
        for (int index = 0; index < operators.length; index++) {
            java.nio.file.Path payload = checkpoint.resolve("state-" + index);
            writePayload(payload, "db-" + index, checkpointId);
            states[index] = operatorState(operators[index], payload);
        }
        try (DataOutputStream output =
                new DataOutputStream(Files.newOutputStream(checkpoint.resolve("_metadata")))) {
            Checkpoints.storeCheckpointMetadata(
                    new CheckpointMetadata(
                            checkpointId, Arrays.asList(states), Collections.emptyList()),
                    output);
        }
    }

    private static void writePayload(java.nio.file.Path payload, String dbId, long checkpointId)
            throws Exception {
        ShardSnapshot shard = new ShardSnapshot();
        shard.dbId = dbId;
        // Checkpoint IDs and shard snapshot IDs are intentionally independent.
        shard.snapshotId = 0L;
        shard.manifestPath =
                payload.getParent()
                        .resolve("volume")
                        .resolve("snapshot")
                        .resolve("SNAPSHOT-0")
                        .toUri()
                        .toString();
        ShardSnapshot.Range range = new ShardSnapshot.Range();
        range.start = 0;
        range.end = 3;
        shard.ranges.add(range);
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(payload))) {
            CobbleSnapshotMetadataCodec.write(
                    new CobbleSnapshotMetadataPayload(shard, false, Collections.emptyList(), null),
                    new DataOutputViewStreamWrapper(output));
        }
    }

    private static OperatorState operatorState(OperatorID operatorId, java.nio.file.Path payload)
            throws Exception {
        IncrementalRemoteKeyedStateHandle handle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        new KeyGroupRange(0, 3),
                        0L,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new FileStateHandle(new Path(payload.toUri()), Files.size(payload)));
        OperatorState state = new OperatorState(operatorId, 1, 4);
        state.putState(
                0,
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(
                                StateObjectCollection.<KeyedStateHandle>singleton(handle))
                        .build());
        return state;
    }
}
