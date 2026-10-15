package io.cobble.flink.inspect.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleSnapshotMetadataCodec;
import io.cobble.flink.common.CobbleSnapshotMetadataPayload;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
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
import java.lang.reflect.InvocationTargetException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

        java.nio.file.Path newer = Files.createDirectory(root.resolve("chk-8"));
        java.nio.file.Path newerState = newer.resolve("task-state");
        writePayload(newerState, manifest);
        writeMetadata(newer, newerState, 8L);
        assertDiscovered(root, 8L);
    }

    private static void assertDiscovered(java.nio.file.Path path, long checkpointId) {
        InspectCatalogDiscovery.Result result =
                InspectCatalogDiscovery.discover(
                        path.toUri().toString(), CobbleConnectorStorageOptions.empty());
        assertEquals("checkpoint", result.sourceKind);
        assertEquals(checkpointId, result.checkpoints.get(0).id);
        assertEquals(1, result.checkpoints.get(0).operators.size());
    }

    @Test
    void predicatesUseOneStatusAndOnlyMissingPathsBecomeFalse() throws Exception {
        AtomicInteger statuses = new AtomicInteger();
        AtomicReference<IOException> failure = new AtomicReference<>();
        LocalFileSystem delegate = new LocalFileSystem();
        FileSystem fs =
                new LocalFileSystem() {
                    @Override
                    public boolean exists(Path path) {
                        throw new AssertionError("redundant exists");
                    }

                    @Override
                    public FileStatus getFileStatus(Path path) throws IOException {
                        statuses.incrementAndGet();
                        if (failure.get() != null) throw failure.get();
                        return delegate.getFileStatus(path);
                    }
                };
        java.nio.file.Path file = tempDir.resolve("metadata");
        Files.write(file, new byte[] {1});
        assertTrue(predicate("fileExists", fs, new Path(file.toUri())));
        assertTrue(predicate("directoryExists", fs, new Path(tempDir.toUri())));
        assertEquals(2, statuses.get());
        assertFalse(predicate("fileExists", fs, new Path(tempDir.resolve("absent").toUri())));
        failure.set(new SocketTimeoutException("timed out"));
        InspectInputException error =
                assertThrows(
                        InspectInputException.class,
                        () -> predicate("directoryExists", fs, new Path(tempDir.toUri())));
        assertTrue(error.getMessage().contains("timed out"));
        failure.set(new IOException("permission denied"));
        assertThrows(
                InspectInputException.class,
                () -> predicate("fileExists", fs, new Path(file.toUri())));
    }

    private static boolean predicate(String name, FileSystem fs, Path path) throws Exception {
        java.lang.reflect.Method method =
                InspectCatalogDiscovery.class.getDeclaredMethod(name, FileSystem.class, Path.class);
        method.setAccessible(true);
        try {
            return (Boolean) method.invoke(null, fs, path);
        } catch (InvocationTargetException error) {
            if (error.getCause() instanceof RuntimeException)
                throw (RuntimeException) error.getCause();
            throw error;
        }
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
                    new CobbleSnapshotMetadataPayload(
                            Collections.emptyList(), shard, false, Collections.emptyList(), null),
                    new DataOutputViewStreamWrapper(output));
        }
    }

    private static void writeMetadata(java.nio.file.Path checkpoint, java.nio.file.Path stateFile)
            throws Exception {
        writeMetadata(checkpoint, stateFile, 7L);
    }

    private static void writeMetadata(
            java.nio.file.Path checkpoint, java.nio.file.Path stateFile, long checkpointId)
            throws Exception {
        IncrementalRemoteKeyedStateHandle handle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        new KeyGroupRange(0, 3),
                        checkpointId,
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
                        checkpointId, Collections.singletonList(operator), Collections.emptyList());
        try (DataOutputStream output =
                new DataOutputStream(Files.newOutputStream(checkpoint.resolve("_metadata")))) {
            Checkpoints.storeCheckpointMetadata(metadata, output);
        }
    }
}
