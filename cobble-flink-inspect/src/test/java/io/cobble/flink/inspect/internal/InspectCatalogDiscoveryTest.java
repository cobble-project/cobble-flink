package io.cobble.flink.inspect.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleSnapshotMetadataCodec;
import io.cobble.flink.common.CobbleSnapshotMetadataPayload;

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
import java.util.Collections;
import java.util.UUID;

class InspectCatalogDiscoveryTest {
    @TempDir private java.nio.file.Path tempDir;

    @Test
    void discoversCheckpointRootDirectoryMetadataAndShardManifest() throws Exception {
        java.nio.file.Path root = Files.createDirectory(tempDir.resolve("checkpoints"));
        java.nio.file.Path checkpoint = Files.createDirectory(root.resolve("chk-7"));
        java.nio.file.Path stateFile = checkpoint.resolve("task-state");
        java.nio.file.Path manifest =
                Files.createDirectories(root.resolve("shared/op/volume/snapshot"))
                        .resolve("SNAPSHOT-7");
        Files.write(manifest, new byte[] {0});
        writePayload(stateFile, manifest);
        writeMetadata(checkpoint, stateFile);

        assertDiscovered(root, 7L);
        assertDiscovered(checkpoint, 7L);
        assertDiscovered(checkpoint.resolve("_metadata"), 7L);
        assertDiscovered(manifest, 7L);
    }

    private static void assertDiscovered(java.nio.file.Path path, long checkpointId) {
        InspectCatalogDiscovery.Result result =
                InspectCatalogDiscovery.discover(
                        path.toUri().toString(), CobbleConnectorStorageOptions.empty());
        assertEquals("checkpoint", result.sourceKind);
        assertEquals(checkpointId, result.checkpoints.get(0).id);
        assertEquals(1, result.checkpoints.get(0).operators.size());
    }

    private static void writePayload(java.nio.file.Path stateFile, java.nio.file.Path manifest)
            throws Exception {
        ShardSnapshot shard = new ShardSnapshot();
        shard.dbId = "db-7";
        shard.snapshotId = 7L;
        shard.manifestPath = manifest.toUri().toString();
        ShardSnapshot.Range range = new ShardSnapshot.Range();
        range.start = 0;
        range.end = 3;
        shard.ranges.add(range);
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(stateFile))) {
            CobbleSnapshotMetadataCodec.write(
                    new CobbleSnapshotMetadataPayload(shard, false, Collections.emptyList(), null),
                    new DataOutputViewStreamWrapper(output));
        }
    }

    private static void writeMetadata(java.nio.file.Path checkpoint, java.nio.file.Path stateFile)
            throws Exception {
        IncrementalRemoteKeyedStateHandle handle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        new KeyGroupRange(0, 3),
                        7L,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new FileStateHandle(new Path(stateFile.toUri()), Files.size(stateFile)));
        OperatorState operator = new OperatorState(new OperatorID(1L, 2L), 1, 4);
        operator.putState(
                0,
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(
                                StateObjectCollection.<KeyedStateHandle>singleton(handle))
                        .build());
        CheckpointMetadata metadata =
                new CheckpointMetadata(
                        7L, Collections.singletonList(operator), Collections.emptyList());
        try (DataOutputStream output =
                new DataOutputStream(Files.newOutputStream(checkpoint.resolve("_metadata")))) {
            Checkpoints.storeCheckpointMetadata(metadata, output);
        }
    }
}
