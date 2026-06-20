package io.cobble.flink.state;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoint;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Completed checkpoint store wrapper that materializes Cobble global snapshots on the JM side. */
final class CobbleCompletedCheckpointStore implements CompletedCheckpointStore {

    private static final Logger LOG = LoggerFactory.getLogger(CobbleCompletedCheckpointStore.class);

    private final CompletedCheckpointStore delegate;
    private final Map<String, OperatorCoordinatorHandle> coordinatorsByOperatorDirectory;

    CobbleCompletedCheckpointStore(CompletedCheckpointStore delegate) throws Exception {
        this.delegate = delegate;
        this.coordinatorsByOperatorDirectory = new HashMap<>();
        recoverManagedSnapshots();
    }

    @Override
    public CompletedCheckpoint addCheckpointAndSubsumeOldestOne(
            CompletedCheckpoint checkpoint,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup)
            throws Exception {
        CheckpointsCleaner cobbleCleaner = wrapCleaner(checkpointsCleaner);
        CompletedCheckpoint subsumed =
                delegate.addCheckpointAndSubsumeOldestOne(checkpoint, cobbleCleaner, postCleanup);
        Map<String, OperatorSnapshotData> snapshotDataByOperator =
                collectCobbleShardSnapshotsByOperator(checkpoint);
        materializeGlobalSnapshot(checkpoint, snapshotDataByOperator);
        copyManifestIntoCheckpointDirectory(checkpoint, snapshotDataByOperator);
        updateInspectSchemaRegistry(checkpoint, snapshotDataByOperator);
        cleanupSubsumedCheckpoint(subsumed);
        return subsumed;
    }

    @Override
    public void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner)
            throws Exception {
        Exception failure = null;
        List<CompletedCheckpoint> checkpoints = null;
        try {
            checkpoints = delegate.getAllCheckpoints();
        } catch (Exception e) {
            failure = ExceptionUtils.firstOrSuppressed(e, failure);
        }
        if (checkpoints != null) {
            try {
                cleanupDiscardedCheckpointManifestCopiesBeforeDelegateShutdown(
                        checkpoints, jobStatus);
            } catch (Exception e) {
                failure = ExceptionUtils.firstOrSuppressed(e, failure);
            }
        }
        try {
            delegate.shutdown(jobStatus, wrapCleaner(checkpointsCleaner));
        } catch (Exception e) {
            failure = ExceptionUtils.firstOrSuppressed(e, failure);
        }

        if (checkpoints != null) {
            try {
                cleanupShutdownSnapshots(checkpoints, jobStatus);
            } catch (Exception e) {
                failure = ExceptionUtils.firstOrSuppressed(e, failure);
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
        return delegate.getAllCheckpoints();
    }

    @Override
    public int getNumberOfRetainedCheckpoints() {
        return delegate.getNumberOfRetainedCheckpoints();
    }

    @Override
    public int getMaxNumberOfRetainedCheckpoints() {
        return delegate.getMaxNumberOfRetainedCheckpoints();
    }

    @Override
    public boolean requiresExternalizedCheckpoints() {
        return delegate.requiresExternalizedCheckpoints();
    }

    @Override
    public SharedStateRegistry getSharedStateRegistry() {
        return delegate.getSharedStateRegistry();
    }

    private void materializeGlobalSnapshot(
            CompletedCheckpoint checkpoint,
            Map<String, OperatorSnapshotData> snapshotDataByOperator)
            throws Exception {
        for (OperatorSnapshotData snapshotData : snapshotDataByOperator.values()) {
            OperatorCoordinatorHandle coordinatorHandle =
                    getOrCreateCoordinator(
                            checkpoint.getExternalPointer(), snapshotData.operatorIdHex);
            DbCoordinator coordinator = coordinatorHandle.coordinator;
            coordinator.materializeGlobalSnapshot(
                    snapshotData.totalBuckets,
                    checkpoint.getCheckpointID(),
                    snapshotData.shardSnapshots);
            coordinator.retainSnapshot(checkpoint.getCheckpointID());
            coordinatorHandle.managedSnapshotIds.add(checkpoint.getCheckpointID());
        }
    }

    private void copyManifestIntoCheckpointDirectory(
            CompletedCheckpoint checkpoint,
            Map<String, OperatorSnapshotData> snapshotDataByOperator)
            throws Exception {
        for (OperatorSnapshotData snapshotData : snapshotDataByOperator.values()) {
            copyPath(
                    CobblePathUtils.cobbleGlobalSnapshotManifestPath(
                            checkpoint.getExternalPointer(),
                            snapshotData.operatorIdHex,
                            checkpoint.getCheckpointID()),
                    CobblePathUtils.checkpointGlobalSnapshotManifestCopyPath(
                            checkpoint.getExternalPointer(), snapshotData.operatorIdHex));
        }
    }

    private void cleanupSubsumedCheckpoint(CompletedCheckpoint subsumedCheckpoint)
            throws Exception {
        if (subsumedCheckpoint != null && subsumedCheckpoint.shouldBeDiscardedOnSubsume()) {
            cleanupCheckpointSnapshotFiles(subsumedCheckpoint);
        }
    }

    private void cleanupDiscardedCheckpointManifestCopiesBeforeDelegateShutdown(
            List<CompletedCheckpoint> checkpoints, JobStatus jobStatus) throws Exception {
        Exception failure = null;
        for (CompletedCheckpoint checkpoint : checkpoints) {
            if (!checkpoint.shouldBeDiscardedOnShutdown(jobStatus)) {
                continue;
            }
            try {
                // Flink owns the checkpoint directory and may remove it during delegate.shutdown().
                // Cobble only places a copied global-snapshot manifest inside that directory so
                // restore can find JM-side metadata. Remove that copy first; otherwise Flink's
                // storage-location discard can fail with "directory is not empty" before our
                // normal Cobble snapshot cleanup gets a turn.
                deleteCheckpointManifestCopies(checkpoint);
            } catch (Exception e) {
                failure = ExceptionUtils.firstOrSuppressed(e, failure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void cleanupShutdownSnapshots(
            List<CompletedCheckpoint> checkpoints, JobStatus jobStatus) throws Exception {
        Map<String, Boolean> retainedOperatorDirectories = new HashMap<>();

        for (CompletedCheckpoint checkpoint : checkpoints) {
            Map<String, OperatorSnapshotData> snapshotDataByOperator =
                    collectCobbleShardSnapshotsByOperator(checkpoint);
            if (checkpoint.shouldBeDiscardedOnShutdown(jobStatus)) {
                cleanupCheckpointSnapshotFiles(checkpoint);
            } else {
                for (OperatorSnapshotData snapshotData : snapshotDataByOperator.values()) {
                    retainedOperatorDirectories.put(
                            snapshotData.operatorDirectory(checkpoint.getExternalPointer()),
                            Boolean.TRUE);
                }
            }
        }

        closeCoordinators();
        for (OperatorCoordinatorHandle coordinatorHandle :
                coordinatorsByOperatorDirectory.values()) {
            if (!retainedOperatorDirectories.containsKey(coordinatorHandle.operatorDirectory)) {
                CobblePathUtils.deletePathQuietly(coordinatorHandle.operatorDirectory);
            }
        }
    }

    private void cleanupCheckpointSnapshotFiles(CompletedCheckpoint checkpoint) throws Exception {
        Map<String, OperatorSnapshotData> snapshotDataByOperator =
                collectCobbleShardSnapshotsByOperator(checkpoint);
        for (OperatorSnapshotData snapshotData : snapshotDataByOperator.values()) {
            expireManagedSnapshot(
                    checkpoint.getExternalPointer(),
                    snapshotData.operatorIdHex,
                    checkpoint.getCheckpointID());
        }
        deleteCheckpointManifestCopies(checkpoint, snapshotDataByOperator);
    }

    private void deleteCheckpointManifestCopies(CompletedCheckpoint checkpoint) throws Exception {
        deleteCheckpointManifestCopies(
                checkpoint, collectCobbleShardSnapshotsByOperator(checkpoint));
    }

    private void deleteCheckpointManifestCopies(
            CompletedCheckpoint checkpoint,
            Map<String, OperatorSnapshotData> snapshotDataByOperator)
            throws Exception {
        for (OperatorSnapshotData snapshotData : snapshotDataByOperator.values()) {
            CobblePathUtils.deletePathQuietly(
                    CobblePathUtils.checkpointGlobalSnapshotManifestCopyPath(
                            checkpoint.getExternalPointer(), snapshotData.operatorIdHex));
        }
    }

    private CheckpointsCleaner wrapCleaner(CheckpointsCleaner checkpointsCleaner) {
        return new CobbleCheckpointsCleaner(
                checkpointsCleaner, this::deleteCheckpointManifestCopies);
    }

    private void recoverManagedSnapshots() throws Exception {
        Map<String, RecoveredOperatorSnapshotState> recoveredByOperatorDirectory =
                new LinkedHashMap<>();

        for (CompletedCheckpoint checkpoint : delegate.getAllCheckpoints()) {
            Map<String, OperatorSnapshotData> snapshotDataByOperator =
                    collectCobbleShardSnapshotsByOperator(checkpoint);
            for (OperatorSnapshotData snapshotData : snapshotDataByOperator.values()) {
                String operatorDirectory =
                        snapshotData.operatorDirectory(checkpoint.getExternalPointer());
                RecoveredOperatorSnapshotState recoveredState =
                        recoveredByOperatorDirectory.computeIfAbsent(
                                operatorDirectory,
                                ignored ->
                                        new RecoveredOperatorSnapshotState(
                                                snapshotData.operatorIdHex,
                                                checkpoint.getExternalPointer(),
                                                operatorDirectory));
                recoveredState.retainedCheckpointIds.add(checkpoint.getCheckpointID());
            }
        }

        for (RecoveredOperatorSnapshotState recoveredState :
                recoveredByOperatorDirectory.values()) {
            OperatorCoordinatorHandle coordinatorHandle =
                    getOrCreateCoordinator(
                            recoveredState.checkpointExternalPointer, recoveredState.operatorIdHex);
            for (GlobalSnapshot snapshot : coordinatorHandle.coordinator.listGlobalSnapshots()) {
                coordinatorHandle.managedSnapshotIds.add(snapshot.id);
            }
            for (Long checkpointId : recoveredState.retainedCheckpointIds) {
                if (coordinatorHandle.coordinator.retainSnapshot(checkpointId)) {
                    coordinatorHandle.managedSnapshotIds.add(checkpointId);
                }
            }
        }
    }

    private Map<String, OperatorSnapshotData> collectCobbleShardSnapshotsByOperator(
            CompletedCheckpoint checkpoint) throws IOException {
        Map<String, OperatorSnapshotData> snapshotDataByOperator = new LinkedHashMap<>();
        for (OperatorState operatorState : checkpoint.getOperatorStates().values()) {
            String operatorIdHex = operatorState.getOperatorID().toHexString();
            Map<String, ShardSnapshot> shardSnapshotsById = new LinkedHashMap<>();
            List<StateInspectSchemaStore> subtaskSchemaStores = new ArrayList<>();
            for (OperatorSubtaskState subtaskState : operatorState.getStates()) {
                collectCobbleShardSnapshots(
                        subtaskState.getManagedKeyedState(),
                        shardSnapshotsById,
                        subtaskSchemaStores);
                collectCobbleShardSnapshots(
                        subtaskState.getRawKeyedState(), shardSnapshotsById, subtaskSchemaStores);
            }
            if (!shardSnapshotsById.isEmpty()) {
                snapshotDataByOperator.put(
                        operatorIdHex,
                        new OperatorSnapshotData(
                                operatorIdHex,
                                Math.max(
                                        operatorState.getMaxParallelism(),
                                        calculateTotalBuckets(
                                                new ArrayList<>(shardSnapshotsById.values()))),
                                new ArrayList<>(shardSnapshotsById.values()),
                                subtaskSchemaStores));
            }
        }
        return snapshotDataByOperator;
    }

    private void collectCobbleShardSnapshots(
            Iterable<KeyedStateHandle> keyedStateHandles,
            Map<String, ShardSnapshot> shardSnapshotsById,
            List<StateInspectSchemaStore> subtaskSchemaStores)
            throws IOException {
        for (KeyedStateHandle keyedStateHandle : keyedStateHandles) {
            if (!(keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle)) {
                continue;
            }

            IncrementalRemoteKeyedStateHandle incrementalHandle =
                    (IncrementalRemoteKeyedStateHandle) keyedStateHandle;
            if (incrementalHandle.getMetaStateHandle() == null) {
                continue;
            }

            CobbleSnapshotMetadata metadata;
            try (org.apache.flink.core.fs.FSDataInputStream inputStream =
                    incrementalHandle.getMetaStateHandle().openInputStream()) {
                metadata =
                        CobbleSnapshotMetadata.readIfPresent(
                                new DataInputViewStreamWrapper(inputStream));
            }
            if (metadata == null) {
                continue;
            }

            ShardSnapshot shardSnapshot = metadata.shardSnapshot();
            shardSnapshotsById.put(
                    shardSnapshot.dbId + ':' + shardSnapshot.snapshotId, shardSnapshot);

            StateInspectSchemaStore subtaskStore = metadata.schemaStore();
            if (subtaskStore != null && !subtaskStore.isEmpty()) {
                subtaskSchemaStores.add(subtaskStore);
            }
        }
    }

    private int calculateTotalBuckets(List<ShardSnapshot> shardSnapshots) {
        int totalBuckets = 0;
        for (ShardSnapshot shardSnapshot : shardSnapshots) {
            for (ShardSnapshot.Range range : shardSnapshot.ranges) {
                totalBuckets = Math.max(totalBuckets, range.end + 1);
            }
        }
        if (totalBuckets <= 0) {
            throw new IllegalStateException(
                    "Cobble shard snapshots did not report any bucket range.");
        }
        return totalBuckets;
    }

    static StateInspectSchemaStore mergeSchemaStores(List<StateInspectSchemaStore> stores) {
        if (stores.isEmpty()) {
            return StateInspectSchemaStore.empty();
        }
        Map<String, StateInspectSchema> merged = new LinkedHashMap<>();
        Map<String, StateInspectSemanticSchema> mergedSemanticSchemas = new LinkedHashMap<>();
        for (StateInspectSchemaStore store : stores) {
            for (StateInspectSchema schema : store.schemas()) {
                StateInspectSchema existing = merged.putIfAbsent(schemaKey(schema), schema);
                if (existing != null && !existing.equals(schema)) {
                    LOG.warn(
                            "Inspect schema mismatch for {} '{}' across subtasks: "
                                    + "keeping first registration, discarding divergent schema.",
                            schema.stateKind(),
                            schema.stateName());
                }
            }
            for (Map.Entry<String, StateInspectSemanticSchema> semanticSchema :
                    store.semanticSchemas().entrySet()) {
                StateInspectSemanticSchema existing =
                        mergedSemanticSchemas.putIfAbsent(
                                semanticSchema.getKey(), semanticSchema.getValue());
                if (existing != null && !existing.equals(semanticSchema.getValue())) {
                    LOG.warn(
                            "Inspect semantic schema mismatch for state '{}' across subtasks: "
                                    + "keeping first registration, discarding divergent schema.",
                            semanticSchema.getKey());
                }
            }
        }
        return merged.isEmpty()
                ? StateInspectSchemaStore.empty()
                : new StateInspectSchemaStore(
                        new ArrayList<>(merged.values()), mergedSemanticSchemas);
    }

    private static String schemaKey(StateInspectSchema schema) {
        return schema.stateKind().name() + ":" + schema.stateName();
    }

    private Config createCoordinatorConfig(String checkpointExternalPointer, String operatorIdHex) {
        String externalPointer = checkpointExternalPointer;
        if (externalPointer == null || externalPointer.trim().isEmpty()) {
            throw new IllegalStateException(
                    "Cobble HA wrapper requires an external checkpoint pointer to place the "
                            + "global snapshot alongside Flink checkpoint state.");
        }

        Config config = new Config().numColumns(1).totalBuckets(1);
        config.snapshotRetention = null;
        // The coordinator only needs a root volume. Bucket ownership still comes from the shard
        // snapshots and the explicit totalBuckets argument passed during materialization.
        config.addVolume(
                CobblePathUtils.cobbleOperatorSnapshotDirectory(externalPointer, operatorIdHex));
        return config;
    }

    private OperatorCoordinatorHandle getOrCreateCoordinator(
            String checkpointExternalPointer, String operatorIdHex) {
        String operatorDirectory =
                CobblePathUtils.cobbleOperatorSnapshotDirectory(
                        checkpointExternalPointer, operatorIdHex);
        OperatorCoordinatorHandle handle = coordinatorsByOperatorDirectory.get(operatorDirectory);
        if (handle != null) {
            return handle;
        }

        DbCoordinator coordinator =
                DbCoordinator.open(
                        createCoordinatorConfig(checkpointExternalPointer, operatorIdHex));
        OperatorCoordinatorHandle coordinatorHandle =
                new OperatorCoordinatorHandle(
                        operatorIdHex, operatorDirectory, coordinator, checkpointExternalPointer);
        coordinatorsByOperatorDirectory.put(operatorDirectory, coordinatorHandle);
        return coordinatorHandle;
    }

    private void expireManagedSnapshot(
            String checkpointExternalPointer, String operatorIdHex, long checkpointId)
            throws Exception {
        OperatorCoordinatorHandle coordinatorHandle =
                getOrCreateCoordinator(checkpointExternalPointer, operatorIdHex);
        coordinatorHandle.managedSnapshotIds.remove(checkpointId);
        if (coordinatorHandle.coordinator.expireSnapshot(checkpointId)) {
            return;
        }

        GlobalSnapshot currentSnapshot = coordinatorHandle.coordinator.loadCurrentGlobalSnapshot();
        if (currentSnapshot != null && currentSnapshot.id == checkpointId) {
            return;
        }

        CobblePathUtils.deletePathQuietly(
                CobblePathUtils.cobbleGlobalSnapshotManifestPath(
                        checkpointExternalPointer, operatorIdHex, checkpointId));
    }

    private void closeCoordinators() throws Exception {
        Exception failure = null;
        for (OperatorCoordinatorHandle coordinatorHandle :
                coordinatorsByOperatorDirectory.values()) {
            try {
                coordinatorHandle.coordinator.close();
            } catch (Exception e) {
                failure = ExceptionUtils.firstOrSuppressed(e, failure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void copyPath(String sourcePath, String targetPath) throws Exception {
        Path source = new Path(sourcePath);
        Path target = new Path(targetPath);
        target.getFileSystem().mkdirs(target.getParent());
        try (FSDataInputStream input = source.getFileSystem().open(source);
                FSDataOutputStream output =
                        target.getFileSystem().create(target, FileSystem.WriteMode.OVERWRITE)) {
            byte[] buffer = new byte[8 * 1024];
            int bytesRead;
            while ((bytesRead = input.read(buffer)) >= 0) {
                output.write(buffer, 0, bytesRead);
            }
        }
    }

    private void updateInspectSchemaRegistry(
            CompletedCheckpoint checkpoint,
            Map<String, OperatorSnapshotData> snapshotDataByOperator) {
        for (OperatorSnapshotData snapshotData : snapshotDataByOperator.values()) {
            StateInspectSchemaStore store = snapshotData.schemaStore;
            if (store == null || store.isEmpty()) {
                continue;
            }
            try {
                OperatorCoordinatorHandle coordinatorHandle =
                        getOrCreateCoordinator(
                                checkpoint.getExternalPointer(), snapshotData.operatorIdHex);
                coordinatorHandle.inspectSchemaRegistry.writeIfChanged(
                        checkpoint.getCheckpointID(), store);
            } catch (Exception e) {
                LOG.warn(
                        "Failed to update Cobble inspect schema registry for checkpoint {}"
                                + " operator {}: {}",
                        checkpoint.getCheckpointID(),
                        snapshotData.operatorIdHex,
                        e.getMessage());
            }
        }
    }

    private static final class OperatorSnapshotData {
        private final String operatorIdHex;
        private final int totalBuckets;
        private final List<ShardSnapshot> shardSnapshots;
        private final StateInspectSchemaStore schemaStore;

        private OperatorSnapshotData(
                String operatorIdHex,
                int totalBuckets,
                List<ShardSnapshot> shardSnapshots,
                List<StateInspectSchemaStore> subtaskSchemaStores) {
            this.operatorIdHex = operatorIdHex;
            this.totalBuckets = totalBuckets;
            this.shardSnapshots = shardSnapshots;
            this.schemaStore = mergeSchemaStores(subtaskSchemaStores);
        }

        private String operatorDirectory(String checkpointExternalPointer) {
            return CobblePathUtils.cobbleOperatorSnapshotDirectory(
                    checkpointExternalPointer, operatorIdHex);
        }

        private static StateInspectSchemaStore mergeSchemaStores(
                List<StateInspectSchemaStore> stores) {
            return CobbleCompletedCheckpointStore.mergeSchemaStores(stores);
        }
    }

    private static final class OperatorCoordinatorHandle {
        private final String operatorIdHex;
        private final String operatorDirectory;
        private final DbCoordinator coordinator;
        private final Set<Long> managedSnapshotIds;
        private final CobbleInspectSchemaRegistry inspectSchemaRegistry;

        private OperatorCoordinatorHandle(
                String operatorIdHex,
                String operatorDirectory,
                DbCoordinator coordinator,
                String checkpointExternalPointer) {
            this.operatorIdHex = operatorIdHex;
            this.operatorDirectory = operatorDirectory;
            this.coordinator = coordinator;
            this.managedSnapshotIds = new TreeSet<>();
            this.inspectSchemaRegistry =
                    new CobbleInspectSchemaRegistry(checkpointExternalPointer, operatorIdHex);
        }
    }

    private static final class RecoveredOperatorSnapshotState {
        private final String operatorIdHex;
        private final String checkpointExternalPointer;
        private final String operatorDirectory;
        private final Set<Long> retainedCheckpointIds;

        private RecoveredOperatorSnapshotState(
                String operatorIdHex, String checkpointExternalPointer, String operatorDirectory) {
            this.operatorIdHex = operatorIdHex;
            this.checkpointExternalPointer = checkpointExternalPointer;
            this.operatorDirectory = operatorDirectory;
            this.retainedCheckpointIds = new TreeSet<>();
        }
    }

    @FunctionalInterface
    private interface CompletedCheckpointCleanupAction {
        void cleanup(CompletedCheckpoint checkpoint) throws Exception;
    }

    private static final class CobbleCheckpointsCleaner extends CheckpointsCleaner {
        private final CheckpointsCleaner delegate;
        private final CompletedCheckpointCleanupAction preDiscardCleanup;
        private final List<CompletedCheckpoint> subsumedCheckpoints;

        private CobbleCheckpointsCleaner(
                CheckpointsCleaner delegate, CompletedCheckpointCleanupAction preDiscardCleanup) {
            this.delegate = delegate;
            this.preDiscardCleanup = preDiscardCleanup;
            this.subsumedCheckpoints = new ArrayList<>();
        }

        @Override
        public void cleanCheckpoint(
                Checkpoint checkpoint,
                boolean shouldDiscard,
                Runnable postCleanAction,
                Executor executor) {
            try {
                maybeRunPreDiscardCleanup(checkpoint, shouldDiscard);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to clean Cobble checkpoint manifest copy before checkpoint discard",
                        e);
            }
            delegate.cleanCheckpoint(checkpoint, shouldDiscard, postCleanAction, executor);
        }

        @Override
        public void addSubsumedCheckpoint(CompletedCheckpoint completedCheckpoint) {
            subsumedCheckpoints.add(completedCheckpoint);
        }

        @Override
        public void cleanSubsumedCheckpoints(
                long upTo, Set<Long> stillInUse, Runnable postCleanAction, Executor executor) {
            java.util.Iterator<CompletedCheckpoint> iterator = subsumedCheckpoints.iterator();
            while (iterator.hasNext()) {
                CompletedCheckpoint checkpoint = iterator.next();
                if (checkpoint.getCheckpointID() < upTo
                        && !stillInUse.contains(checkpoint.getCheckpointID())) {
                    try {
                        cleanCheckpoint(
                                checkpoint,
                                checkpoint.shouldBeDiscardedOnSubsume(),
                                postCleanAction,
                                executor);
                        iterator.remove();
                    } catch (Exception e) {
                        LOG.warn(
                                "Failed to discard Cobble subsumed checkpoint {}.",
                                checkpoint.getCheckpointID(),
                                e);
                    }
                }
            }
        }

        @Override
        public void cleanCheckpointOnFailedStoring(
                CompletedCheckpoint completedCheckpoint, Executor executor) {
            delegate.cleanCheckpointOnFailedStoring(completedCheckpoint, executor);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            subsumedCheckpoints.clear();
            return delegate.closeAsync();
        }

        private void maybeRunPreDiscardCleanup(Checkpoint checkpoint, boolean shouldDiscard)
                throws Exception {
            if (!shouldDiscard || !(checkpoint instanceof CompletedCheckpoint)) {
                return;
            }
            preDiscardCleanup.cleanup((CompletedCheckpoint) checkpoint);
        }
    }
}
