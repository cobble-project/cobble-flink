package io.cobble.flink.state;

import io.cobble.CancelledError;
import io.cobble.PendingSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.structured.Db;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointBoundKeyedStateHandle;
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
    private final Supplier<Boolean> hasCobbleTimers;
    private final Supplier<StateInspectSchemaStore> schemaStoreSupplier;
    private final UUID backendIdentifier;
    private final Map<Long, TrackedSnapshot> trackedSnapshots;
    private final Map<Long, CobbleSnapshotResources> pendingSnapshots;

    CobbleSnapshotStrategy(
            Db cobbleDb,
            KeyGroupRange keyGroupRange,
            Supplier<Boolean> hasRegisteredState,
            Supplier<Boolean> hasCobbleTimers,
            Supplier<StateInspectSchemaStore> schemaStoreSupplier) {
        this.cobbleDb = cobbleDb;
        this.keyGroupRange = keyGroupRange;
        this.hasRegisteredState = hasRegisteredState;
        this.hasCobbleTimers = hasCobbleTimers;
        this.schemaStoreSupplier = schemaStoreSupplier;
        this.backendIdentifier = UUID.randomUUID();
        this.trackedSnapshots = new ConcurrentHashMap<>();
        this.pendingSnapshots = new ConcurrentHashMap<>();
    }

    @Override
    public CobbleSnapshotResources syncPrepareResources(long checkpointId) {
        if (!hasRegisteredState.get()) {
            return CobbleSnapshotResources.empty();
        }
        PendingSnapshot<ShardSnapshot> pendingSnapshot = cobbleDb.startAsyncSnapshot();
        CobbleSnapshotResources snapshotResources =
                new CobbleSnapshotResources(
                        cobbleDb, pendingSnapshot.snapshotId(), pendingSnapshot.future());
        pendingSnapshots.put(checkpointId, snapshotResources);
        return snapshotResources;
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
        CobbleSnapshotResources pendingSnapshot = pendingSnapshots.remove(checkpointId);
        if (pendingSnapshot != null) {
            pendingSnapshot.cleanupAfterAbort(checkpointId);
        }
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

    Long snapshotIdForCheckpoint(long checkpointId) {
        TrackedSnapshot trackedSnapshot = trackedSnapshots.get(checkpointId);
        if (trackedSnapshot != null) {
            return trackedSnapshot.snapshotId;
        }
        CobbleSnapshotResources pendingSnapshot = pendingSnapshots.get(checkpointId);
        return pendingSnapshot == null ? null : pendingSnapshot.snapshotId;
    }

    void close() {
        for (Map.Entry<Long, CobbleSnapshotResources> entry : pendingSnapshots.entrySet()) {
            try {
                entry.getValue().cleanupAfterAbort(entry.getKey());
            } catch (RuntimeException ignored) {
                // best effort cleanup during backend shutdown
            }
        }
        trackedSnapshots.clear();
        pendingSnapshots.clear();
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
            CobbleSnapshotMetadata.fromShardSnapshot(
                            shardSnapshot, hasCobbleTimers.get(), schemaStoreSupplier.get())
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
            pendingSnapshots.remove(checkpointId);
            snapshotResources.markPublished();
            success = true;

            return SnapshotResult.of(
                    new ReportedIncrementalRemoteKeyedStateHandle(
                            backendIdentifier,
                            keyGroupRange,
                            checkpointId,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            metaHandle,
                            metaHandle.getStateSize() + shardSnapshot.incrementalDataSizeBytes,
                            metaHandle.getStateSize() + shardSnapshot.dataSizeBytes));
        } finally {
            if (!success) {
                pendingSnapshots.remove(checkpointId);
            }
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
                new CobbleSnapshotResources(null, -1L, CompletableFuture.completedFuture(null));

        private final Db cobbleDb;
        private final long snapshotId;
        private final CompletableFuture<ShardSnapshot> snapshotFuture;
        private final AtomicBoolean released;
        private final AtomicBoolean published;
        private final AtomicBoolean retained;
        private final AtomicBoolean cleanupHandled;

        private CobbleSnapshotResources(
                Db cobbleDb, long snapshotId, CompletableFuture<ShardSnapshot> snapshotFuture) {
            this.cobbleDb = cobbleDb;
            this.snapshotId = snapshotId;
            this.snapshotFuture = snapshotFuture;
            this.released = new AtomicBoolean(false);
            this.published = new AtomicBoolean(false);
            this.retained = new AtomicBoolean(false);
            this.cleanupHandled = new AtomicBoolean(false);
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
                if (cause instanceof CancelledError) {
                    throw new IOException("Cobble shard snapshot cancelled.", cause);
                }
                throw new IOException("Cobble shard snapshot failed.", cause);
            }
        }

        void markPublished() {
            published.set(true);
        }

        void cleanupAfterAbort(long checkpointId) {
            if (!cleanupHandled.compareAndSet(false, true) || snapshotId < 0 || cobbleDb == null) {
                return;
            }
            final boolean cancelled;
            try {
                cancelled = cobbleDb.cancelSnapshot(snapshotId);
            } catch (RuntimeException e) {
                throw new IllegalStateException(
                        "Failed to cancel Cobble shard snapshot "
                                + snapshotId
                                + " for checkpoint "
                                + checkpointId
                                + '.',
                        e);
            }
            if (!cancelled) {
                try {
                    cobbleDb.expireSnapshot(snapshotId);
                } catch (RuntimeException e) {
                    throw new IllegalStateException(
                            "Failed to expire Cobble shard snapshot "
                                    + snapshotId
                                    + " for checkpoint "
                                    + checkpointId
                                    + '.',
                            e);
                }
            }
        }

        @Override
        public void release() {
            released.set(true);
            if (snapshotId >= 0
                    && !published.get()
                    && cobbleDb != null
                    && cleanupHandled.compareAndSet(false, true)) {
                try {
                    if (!cobbleDb.cancelSnapshot(snapshotId)) {
                        cobbleDb.expireSnapshot(snapshotId);
                    }
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

    private static final class ReportedIncrementalRemoteKeyedStateHandle
            extends IncrementalRemoteKeyedStateHandle {

        private static final long serialVersionUID = 1L;

        private final long reportedStateSize;

        private ReportedIncrementalRemoteKeyedStateHandle(
                UUID backendIdentifier,
                KeyGroupRange keyGroupRange,
                long checkpointId,
                List<HandleAndLocalPath> sharedState,
                List<HandleAndLocalPath> privateState,
                StreamStateHandle metaStateHandle,
                long persistedSizeOfThisCheckpoint,
                long reportedStateSize) {
            super(
                    backendIdentifier,
                    keyGroupRange,
                    checkpointId,
                    sharedState,
                    privateState,
                    metaStateHandle,
                    persistedSizeOfThisCheckpoint);
            this.reportedStateSize = reportedStateSize;
        }

        @Override
        public long getStateSize() {
            return reportedStateSize;
        }

        @Override
        public CheckpointBoundKeyedStateHandle rebound(long checkpointId) {
            return new ReportedIncrementalRemoteKeyedStateHandle(
                    getBackendIdentifier(),
                    getKeyGroupRange(),
                    checkpointId,
                    getSharedState(),
                    getPrivateState(),
                    getMetaStateHandle(),
                    getCheckpointedSize(),
                    reportedStateSize);
        }
    }

    private static final class TrackedSnapshot {
        private final long snapshotId;

        private TrackedSnapshot(long snapshotId) {
            this.snapshotId = snapshotId;
        }
    }
}
