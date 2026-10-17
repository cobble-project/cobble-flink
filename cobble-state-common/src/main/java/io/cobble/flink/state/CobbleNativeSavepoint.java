package io.cobble.flink.state;

import io.cobble.Config;
import io.cobble.ExportSnapshotHandle;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleFlinkFileSystemResolver;
import io.cobble.flink.common.CobbleFlinkStorageConfig;
import io.cobble.flink.common.CobbleSnapshotVolumeRoots;
import io.cobble.structured.Db;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** Owns one independent export until Flink successfully publishes its savepoint metadata. */
final class CobbleNativeSavepoint implements Closeable {
    private final ExportSnapshotHandle handle;
    private final Config.VolumeDescriptor destination;
    private final CobbleFlinkStorageConfig storage;
    private boolean published;
    private boolean closed;

    CobbleNativeSavepoint(
            Db db,
            long snapshotId,
            CheckpointOptions options,
            CobbleFlinkStorageConfig storage)
            throws IOException {
        Path savepointDirectory =
                AbstractFsCheckpointStorageAccess.decodePathFromReference(
                        options.getTargetLocation());
        Path directory = new Path(savepointDirectory, "cobble");
        destination =
                Config.VolumeDescriptor.singleVolume(
                        CobbleFlinkStorageConfig.nativePath(directory.toUri()));
        storage.fill(destination);
        this.storage = storage;
        handle = db.exportSnapshot(snapshotId, destination);
    }

    ShardSnapshot await(long timeoutMillis) throws Exception {
        return handle.future().get(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    List<String> volumeDirectories() {
        return CobbleSnapshotVolumeRoots.unique(Collections.singletonList(destination.baseDir));
    }

    synchronized void markPublished() throws IOException {
        if (closed) {
            throw new IOException("Cobble savepoint export was cancelled before publication.");
        }
        published = true;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (published) {
            handle.close();
            return;
        }
        handle.cancel();
        // Cancellation completes only after native writers stop. Do not delete an active target.
        ShardSnapshot snapshot;
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    snapshot = handle.future().get();
                    break;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                } catch (ExecutionException ignored) {
                    return; // Native failure/cancellation has already cleaned its own target.
                }
            }
            Path directory = new Path(new Path(destination.baseDir), snapshot.dbId);
            FileSystem fileSystem =
                    CobbleFlinkFileSystemResolver.resolve(
                            directory.toString(),
                            storage.resolve(
                                    destination.baseDir, CobbleConnectorStorageOptions.empty()));
            directory = directory.makeQualified(fileSystem);
            Path manifestDirectory = new Path(snapshot.manifestPath).getParent();
            if (manifestDirectory == null || !directory.equals(manifestDirectory.getParent())) {
                throw new IOException("Invalid Cobble export directory: " + snapshot.manifestPath);
            }
            if (!fileSystem.delete(directory, true) && fileSystem.exists(directory)) {
                throw new IOException("Failed to remove unpublished Cobble export " + directory);
            }
        } finally {
            handle.close();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
