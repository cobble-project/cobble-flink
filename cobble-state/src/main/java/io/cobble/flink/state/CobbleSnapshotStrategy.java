package io.cobble.flink.state;

import io.cobble.ShardSnapshot;
import io.cobble.structured.Db;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Flink snapshot strategy that captures Cobble shard snapshots into checkpoint metadata streams.
 */
final class CobbleSnapshotStrategy
        implements SnapshotStrategy<
                KeyedStateHandle, CobbleSnapshotStrategy.CobbleSnapshotResources> {

    private final Db cobbleDb;
    private final KeyGroupRange keyGroupRange;
    private final Supplier<Boolean> hasRegisteredState;
    private final UUID backendIdentifier;
    private final Map<Long, TrackedSnapshot> trackedSnapshots;

    CobbleSnapshotStrategy(
            Db cobbleDb, KeyGroupRange keyGroupRange, Supplier<Boolean> hasRegisteredState) {
        this.cobbleDb = cobbleDb;
        this.keyGroupRange = keyGroupRange;
        this.hasRegisteredState = hasRegisteredState;
        this.backendIdentifier = UUID.randomUUID();
        this.trackedSnapshots = new ConcurrentHashMap<>();
    }

    @Override
    public CobbleSnapshotResources syncPrepareResources(long checkpointId) {
        if (!hasRegisteredState.get()) {
            return CobbleSnapshotResources.empty();
        }
        return new CobbleSnapshotResources(cobbleDb, cobbleDb.asyncSnapshot());
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            CobbleSnapshotResources snapshotResources,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {
        if (snapshotResources.isEmpty()) {
            return registry -> SnapshotResult.empty();
        }
        return registry ->
                materializeSnapshot(snapshotResources, checkpointId, streamFactory, registry);
    }

    void notifyCheckpointComplete(long checkpointId) {
        // Snapshot retention is established as soon as the shard snapshot is materialized.
    }

    void notifyCheckpointAborted(long checkpointId) {
        TrackedSnapshot trackedSnapshot = trackedSnapshots.remove(checkpointId);
        if (trackedSnapshot != null) {
            expireNativeSnapshot(checkpointId, trackedSnapshot.snapshotId);
        }
    }

    void notifyCheckpointSubsumed(long checkpointId) {
        List<Map.Entry<Long, TrackedSnapshot>> snapshotsToExpire = new ArrayList<>();
        for (Map.Entry<Long, TrackedSnapshot> entry : trackedSnapshots.entrySet()) {
            if (entry.getKey() <= checkpointId) {
                snapshotsToExpire.add(entry);
            }
        }
        // Subsumed notifications act as a watermark because older checkpoint callbacks can be lost.
        snapshotsToExpire.sort(Comparator.comparingLong(Map.Entry::getKey));
        for (Map.Entry<Long, TrackedSnapshot> entry : snapshotsToExpire) {
            if (trackedSnapshots.remove(entry.getKey(), entry.getValue())) {
                expireNativeSnapshot(entry.getKey(), entry.getValue().snapshotId);
            }
        }
    }

    boolean hasTrackedSnapshot(long checkpointId) {
        return trackedSnapshots.containsKey(checkpointId);
    }

    void close() {
        trackedSnapshots.clear();
    }

    private SnapshotResult<KeyedStateHandle> materializeSnapshot(
            CobbleSnapshotResources snapshotResources,
            long checkpointId,
            CheckpointStreamFactory streamFactory,
            CloseableRegistry snapshotCloseableRegistry)
            throws Exception {
        CheckpointStreamWithResultProvider streamProvider =
                CheckpointStreamWithResultProvider.createSimpleStream(
                        CheckpointedStateScope.EXCLUSIVE, streamFactory);
        snapshotCloseableRegistry.registerCloseable(streamProvider);

        SnapshotResult<StreamStateHandle> streamResult = null;
        boolean success = false;
        try {
            ShardSnapshot shardSnapshot = snapshotResources.awaitSnapshot();
            CobbleSnapshotMetadata.fromShardSnapshot(shardSnapshot)
                    .write(
                            new DataOutputViewStreamWrapper(
                                    streamProvider.getCheckpointOutputStream()));

            if (!snapshotCloseableRegistry.unregisterCloseable(streamProvider)) {
                throw new IOException("Cobble snapshot stream already unregistered.");
            }

            streamResult = streamProvider.closeAndFinalizeCheckpointStreamResult();
            StreamStateHandle metaHandle = streamResult.getJobManagerOwnedSnapshot();
            if (metaHandle == null) {
                throw new IOException(
                        "Cobble checkpoint "
                                + checkpointId
                                + " produced no metadata stream handle.");
            }

            trackedSnapshots.put(checkpointId, new TrackedSnapshot(shardSnapshot.snapshotId));
            snapshotResources.markPublished();
            success = true;

            return SnapshotResult.of(
                    new IncrementalRemoteKeyedStateHandle(
                            backendIdentifier,
                            keyGroupRange,
                            checkpointId,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            metaHandle,
                            metaHandle.getStateSize()));
        } finally {
            if (!success) {
                if (streamResult != null) {
                    StateUtil.discardStateObjectQuietly(streamResult);
                } else if (snapshotCloseableRegistry.unregisterCloseable(streamProvider)) {
                    streamProvider.close();
                }
            }
        }
    }

    private void expireNativeSnapshot(long checkpointId, long snapshotId) {
        if (!cobbleDb.expireSnapshot(snapshotId)) {
            throw new IllegalStateException(
                    "Failed to expire Cobble shard snapshot "
                            + snapshotId
                            + " for checkpoint "
                            + checkpointId
                            + '.');
        }
    }

    /** Resources created in the synchronous phase and cleaned up if async publication fails. */
    static final class CobbleSnapshotResources implements SnapshotResources {

        private static final CobbleSnapshotResources EMPTY =
                new CobbleSnapshotResources(null, CompletableFuture.completedFuture(null));

        private final Db cobbleDb;
        private final CompletableFuture<ShardSnapshot> snapshotFuture;
        private final AtomicBoolean released;
        private final AtomicBoolean published;
        private final AtomicBoolean retained;

        private volatile ShardSnapshot completedSnapshot;

        private CobbleSnapshotResources(
                Db cobbleDb, CompletableFuture<ShardSnapshot> snapshotFuture) {
            this.cobbleDb = cobbleDb;
            this.snapshotFuture = snapshotFuture;
            this.released = new AtomicBoolean(false);
            this.published = new AtomicBoolean(false);
            this.retained = new AtomicBoolean(false);
            this.snapshotFuture.whenComplete(
                    (snapshot, error) -> {
                        completedSnapshot = snapshot;
                        if (error == null
                                && snapshot != null
                                && released.get()
                                && !published.get()
                                && cobbleDb != null) {
                            try {
                                cobbleDb.expireSnapshot(snapshot.snapshotId);
                            } catch (RuntimeException ignored) {
                                // best effort cleanup after async publication failure
                            }
                        }
                    });
        }

        static CobbleSnapshotResources empty() {
            return EMPTY;
        }

        boolean isEmpty() {
            return cobbleDb == null;
        }

        ShardSnapshot awaitSnapshot() throws Exception {
            try {
                ShardSnapshot snapshot = snapshotFuture.get();
                retainSnapshot(snapshot);
                return snapshot;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                throw new IOException("Cobble shard snapshot failed.", cause);
            }
        }

        void markPublished() {
            published.set(true);
        }

        @Override
        public void release() {
            released.set(true);
            if (completedSnapshot != null && !published.get() && cobbleDb != null) {
                try {
                    cobbleDb.expireSnapshot(completedSnapshot.snapshotId);
                } catch (RuntimeException ignored) {
                    // best effort cleanup after async publication failure
                }
            }
        }

        private void retainSnapshot(ShardSnapshot snapshot) throws IOException {
            if (snapshot == null || cobbleDb == null || !retained.compareAndSet(false, true)) {
                return;
            }
            if (!cobbleDb.retainSnapshot(snapshot.snapshotId)) {
                retained.set(false);
                throw new IOException(
                        "Failed to retain Cobble shard snapshot " + snapshot.snapshotId + '.');
            }
        }
    }

    private static final class TrackedSnapshot {
        private final long snapshotId;

        private TrackedSnapshot(long snapshotId) {
            this.snapshotId = snapshotId;
        }
    }
}
