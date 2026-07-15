package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.ShardSnapshot;

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
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

/** Regression coverage for read-only parsing of real Flink NATIVE savepoint metadata. */
class CobbleNativeSavepointTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void loadsTwiceWithoutDiscardingSavepointFiles() throws Exception {
        java.nio.file.Path savepoint = Files.createDirectory(tempDir.resolve("savepoint-native"));
        java.nio.file.Path stateFile = savepoint.resolve("task-state-0");
        writePayload(stateFile);
        OperatorID operatorId = new OperatorID(1L, 2L);
        writeMetadata(savepoint, stateFile, operatorId);

        CobbleNativeSavepoint first = CobbleNativeSavepoint.load(new Path(savepoint.toUri()));
        CobbleNativeSavepoint second = CobbleNativeSavepoint.load(new Path(savepoint.toUri()));

        assertEquals(3L, first.checkpointId());
        assertEquals(first.checkpointId(), second.checkpointId());
        assertEquals(1, second.operator(operatorId.toHexString()).shards().size());
        assertTrue(Files.exists(savepoint.resolve("_metadata")));
        assertTrue(Files.exists(stateFile));
    }

    @Test
    void groupsMultipleShardsByOperatorAndIgnoresNonCobbleHandles() throws Exception {
        java.nio.file.Path savepoint = Files.createDirectory(tempDir.resolve("savepoint-many"));
        java.nio.file.Path firstShard = savepoint.resolve("task-state-a-0");
        java.nio.file.Path secondShard = savepoint.resolve("task-state-a-1");
        java.nio.file.Path thirdShard = savepoint.resolve("task-state-b-0");
        java.nio.file.Path foreignState = savepoint.resolve("task-state-foreign");
        writePayload(firstShard, "db-a-0", 0, 1);
        writePayload(secondShard, "db-a-1", 2, 3);
        writePayload(thirdShard, "db-b-0", 0, 3);
        Files.write(foreignState, new byte[] {1, 2, 3, 4});

        OperatorID firstOperator = new OperatorID(3L, 4L);
        OperatorID secondOperator = new OperatorID(5L, 6L);
        OperatorID foreignOperator = new OperatorID(7L, 8L);
        OperatorState first =
                operatorState(
                        firstOperator,
                        keyedHandle(firstShard, 0, 1),
                        keyedHandle(secondShard, 2, 3));
        OperatorState second = operatorState(secondOperator, keyedHandle(thirdShard, 0, 3));
        OperatorState foreign = operatorState(foreignOperator, keyedHandle(foreignState, 0, 3));
        writeMetadata(savepoint, Arrays.asList(first, second, foreign));

        CobbleNativeSavepoint parsed = CobbleNativeSavepoint.load(new Path(savepoint.toUri()));

        assertEquals(2, parsed.operators().size());
        assertEquals(2, parsed.operator(firstOperator.toHexString()).shards().size());
        assertEquals(1, parsed.operator(secondOperator.toHexString()).shards().size());
        assertNull(parsed.operator(foreignOperator.toHexString()));
    }

    private static void writePayload(java.nio.file.Path stateFile) throws Exception {
        writePayload(stateFile, "db-0", 0, 3);
    }

    private static void writePayload(
            java.nio.file.Path stateFile, String dbId, int rangeStart, int rangeEnd)
            throws Exception {
        ShardSnapshot shard = new ShardSnapshot();
        shard.dbId = dbId;
        shard.snapshotId = 3L;
        shard.manifestPath =
                stateFile
                        .getParent()
                        .resolve("shared/op-0/volume/snapshot/SNAPSHOT-3")
                        .toUri()
                        .toString();
        ShardSnapshot.Range range = new ShardSnapshot.Range();
        range.start = rangeStart;
        range.end = rangeEnd;
        shard.ranges.add(range);
        try (DataOutputStream output =
                new DataOutputStream(
                        Files.newOutputStream(
                                stateFile,
                                StandardOpenOption.CREATE_NEW,
                                StandardOpenOption.WRITE))) {
            CobbleSnapshotMetadataCodec.write(
                    new CobbleSnapshotMetadataPayload(shard, false, null),
                    new DataOutputViewStreamWrapper(output));
        }
    }

    private static void writeMetadata(
            java.nio.file.Path savepoint, java.nio.file.Path stateFile, OperatorID operatorId)
            throws Exception {
        writeMetadata(
                savepoint,
                Collections.singletonList(operatorState(operatorId, keyedHandle(stateFile, 0, 3))));
    }

    private static void writeMetadata(
            java.nio.file.Path savepoint, java.util.List<OperatorState> states) throws Exception {
        CheckpointMetadata metadata = new CheckpointMetadata(3L, states, Collections.emptyList());
        try (DataOutputStream output =
                new DataOutputStream(Files.newOutputStream(savepoint.resolve("_metadata")))) {
            Checkpoints.storeCheckpointMetadata(metadata, output);
        }
    }

    private static IncrementalRemoteKeyedStateHandle keyedHandle(
            java.nio.file.Path stateFile, int rangeStart, int rangeEnd) throws Exception {
        return new IncrementalRemoteKeyedStateHandle(
                UUID.randomUUID(),
                new KeyGroupRange(rangeStart, rangeEnd),
                3L,
                Collections.emptyList(),
                Collections.emptyList(),
                new FileStateHandle(new Path(stateFile.toUri()), Files.size(stateFile)));
    }

    private static OperatorState operatorState(
            OperatorID operatorId, IncrementalRemoteKeyedStateHandle... handles) {
        OperatorState state = new OperatorState(operatorId, handles.length, 4);
        for (int index = 0; index < handles.length; index++) {
            state.putState(
                    index,
                    OperatorSubtaskState.builder()
                            .setManagedKeyedState(
                                    StateObjectCollection.<KeyedStateHandle>singleton(
                                            handles[index]))
                            .build());
        }
        return state;
    }
}
