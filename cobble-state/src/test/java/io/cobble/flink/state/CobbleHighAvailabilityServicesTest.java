package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TestingCompletedCheckpointStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.TestTaskStateManagerBuilder;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RunnableFuture;

class CobbleHighAvailabilityServicesTest {
    private static final JobVertexID TEST_JOB_VERTEX_ID =
            JobVertexID.fromHexString("22222222222222222222222222222222");

    @Test
    void wrappedCheckpointStoreMaterializesAndCleansGlobalSnapshots(@TempDir Path tempDir)
            throws Exception {
        JobID jobId = new JobID();
        OperatorID operatorId1 = new OperatorID();
        OperatorID operatorId2 = new OperatorID();
        String checkpointDirectory = tempDir.resolve("checkpoints").toString();
        List<CompletedCheckpoint> retainedCheckpoints = new ArrayList<>();

        TestingCompletedCheckpointStore innerStore = createInnerStore(retainedCheckpoints, 1);

        KeyedStateHandle checkpointHandle1;
        KeyedStateHandle checkpointHandle2;
        try (TestBackendContext context =
                createBackendContext(
                        tempDir.resolve("backend"), checkpointDirectory, Collections.emptyList())) {
            CobbleKeyedStateBackend<Integer> backend = context.backend;
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>("ha-state", StringSerializer.INSTANCE);

            backend.setCurrentKey(1);
            backend.getPartitionedState("ha-ns", StringSerializer.INSTANCE, descriptor)
                    .update("value-1");
            checkpointHandle1 = runCheckpointSnapshot(backend, 101L);

            backend.setCurrentKey(1);
            backend.getPartitionedState("ha-ns", StringSerializer.INSTANCE, descriptor)
                    .update("value-2");
            checkpointHandle2 = runCheckpointSnapshot(backend, 102L);
        }

        CompletedCheckpoint completedCheckpoint1 =
                createCompletedCheckpoint(
                        jobId,
                        101L,
                        checkpointHandle1,
                        tempDir.resolve("completed-checkpoints").resolve("chk-101"),
                        operatorId1,
                        operatorId2);
        CompletedCheckpoint completedCheckpoint2 =
                createCompletedCheckpoint(
                        jobId,
                        102L,
                        checkpointHandle2,
                        tempDir.resolve("completed-checkpoints").resolve("chk-102"),
                        operatorId1,
                        operatorId2);

        CobbleCompletedCheckpointStore store = new CobbleCompletedCheckpointStore(innerStore);
        try (DbCoordinator checkpoint1Coordinator1 =
                        DbCoordinator.open(
                                createCoordinatorConfig(completedCheckpoint1, operatorId1));
                DbCoordinator checkpoint1Coordinator2 =
                        DbCoordinator.open(
                                createCoordinatorConfig(completedCheckpoint1, operatorId2));
                DbCoordinator checkpoint2Coordinator1 =
                        DbCoordinator.open(
                                createCoordinatorConfig(completedCheckpoint2, operatorId1));
                DbCoordinator checkpoint2Coordinator2 =
                        DbCoordinator.open(
                                createCoordinatorConfig(completedCheckpoint2, operatorId2))) {
            store.addCheckpointAndSubsumeOldestOne(
                    completedCheckpoint1, new CheckpointsCleaner(), () -> {});
            assertEquals(1, checkpoint1Coordinator1.listGlobalSnapshots().size());
            assertEquals(101L, checkpoint1Coordinator1.loadCurrentGlobalSnapshot().id);
            assertEquals(1, checkpoint1Coordinator2.listGlobalSnapshots().size());
            assertEquals(101L, checkpoint1Coordinator2.loadCurrentGlobalSnapshot().id);
            assertTrue(
                    new org.apache.flink.core.fs.Path(
                                    CobblePathUtils.checkpointGlobalSnapshotManifestCopyPath(
                                            completedCheckpoint1.getExternalPointer(),
                                            operatorId1.toHexString()))
                            .getFileSystem()
                            .exists(
                                    new org.apache.flink.core.fs.Path(
                                            CobblePathUtils
                                                    .checkpointGlobalSnapshotManifestCopyPath(
                                                            completedCheckpoint1
                                                                    .getExternalPointer(),
                                                            operatorId1.toHexString()))));
            assertTrue(
                    new org.apache.flink.core.fs.Path(
                                    CobblePathUtils.checkpointGlobalSnapshotManifestCopyPath(
                                            completedCheckpoint1.getExternalPointer(),
                                            operatorId2.toHexString()))
                            .getFileSystem()
                            .exists(
                                    new org.apache.flink.core.fs.Path(
                                            CobblePathUtils
                                                    .checkpointGlobalSnapshotManifestCopyPath(
                                                            completedCheckpoint1
                                                                    .getExternalPointer(),
                                                            operatorId2.toHexString()))));

            store.addCheckpointAndSubsumeOldestOne(
                    completedCheckpoint2, new CheckpointsCleaner(), () -> {});
            List<GlobalSnapshot> snapshots1 = checkpoint2Coordinator1.listGlobalSnapshots();
            assertEquals(1, snapshots1.size());
            assertEquals(102L, snapshots1.get(0).id);
            List<GlobalSnapshot> snapshots2 = checkpoint2Coordinator2.listGlobalSnapshots();
            assertEquals(1, snapshots2.size());
            assertEquals(102L, snapshots2.get(0).id);
            assertTrue(
                    !new org.apache.flink.core.fs.Path(completedCheckpoint1.getExternalPointer())
                            .getFileSystem()
                            .exists(
                                    new org.apache.flink.core.fs.Path(
                                            completedCheckpoint1.getExternalPointer())));
            assertTrue(
                    !new org.apache.flink.core.fs.Path(
                                    CobblePathUtils.cobbleGlobalSnapshotManifestPath(
                                            completedCheckpoint1.getExternalPointer(),
                                            operatorId1.toHexString(),
                                            101L))
                            .getFileSystem()
                            .exists(
                                    new org.apache.flink.core.fs.Path(
                                            CobblePathUtils.cobbleGlobalSnapshotManifestPath(
                                                    completedCheckpoint1.getExternalPointer(),
                                                    operatorId1.toHexString(),
                                                    101L))));
            assertTrue(
                    !new org.apache.flink.core.fs.Path(
                                    CobblePathUtils.cobbleGlobalSnapshotManifestPath(
                                            completedCheckpoint1.getExternalPointer(),
                                            operatorId2.toHexString(),
                                            101L))
                            .getFileSystem()
                            .exists(
                                    new org.apache.flink.core.fs.Path(
                                            CobblePathUtils.cobbleGlobalSnapshotManifestPath(
                                                    completedCheckpoint1.getExternalPointer(),
                                                    operatorId2.toHexString(),
                                                    101L))));

            List<CompletedCheckpoint> wrappedCheckpoints = store.getAllCheckpoints();
            assertEquals(1, wrappedCheckpoints.size());
            assertEquals(102L, wrappedCheckpoints.get(0).getCheckpointID());

            store.shutdown(JobStatus.FINISHED, new CheckpointsCleaner());
            assertTrue(
                    !new org.apache.flink.core.fs.Path(
                                    CobblePathUtils.cobbleOperatorSnapshotDirectory(
                                            completedCheckpoint2.getExternalPointer(),
                                            operatorId1.toHexString()))
                            .getFileSystem()
                            .exists(
                                    new org.apache.flink.core.fs.Path(
                                            CobblePathUtils.cobbleOperatorSnapshotDirectory(
                                                    completedCheckpoint2.getExternalPointer(),
                                                    operatorId1.toHexString()))));
            assertTrue(
                    !new org.apache.flink.core.fs.Path(
                                    CobblePathUtils.cobbleOperatorSnapshotDirectory(
                                            completedCheckpoint2.getExternalPointer(),
                                            operatorId2.toHexString()))
                            .getFileSystem()
                            .exists(
                                    new org.apache.flink.core.fs.Path(
                                            CobblePathUtils.cobbleOperatorSnapshotDirectory(
                                                    completedCheckpoint2.getExternalPointer(),
                                                    operatorId2.toHexString()))));
        }
    }

    @Test
    void wrappedCheckpointStoreRecoversExistingManifestsAcrossRestart(@TempDir Path tempDir)
            throws Exception {
        JobID jobId = new JobID();
        OperatorID operatorId1 = new OperatorID();
        OperatorID operatorId2 = new OperatorID();
        String checkpointDirectory = tempDir.resolve("checkpoints").toString();
        List<CompletedCheckpoint> retainedCheckpoints = new ArrayList<>();

        TestingCompletedCheckpointStore innerStore = createInnerStore(retainedCheckpoints, 2);

        KeyedStateHandle checkpointHandle1;
        KeyedStateHandle checkpointHandle2;
        KeyedStateHandle checkpointHandle3;
        try (TestBackendContext context =
                createBackendContext(
                        tempDir.resolve("backend-recovery"),
                        checkpointDirectory,
                        Collections.emptyList())) {
            CobbleKeyedStateBackend<Integer> backend = context.backend;
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>("ha-recovery-state", StringSerializer.INSTANCE);

            backend.setCurrentKey(1);
            backend.getPartitionedState("ha-recovery-ns", StringSerializer.INSTANCE, descriptor)
                    .update("value-1");
            checkpointHandle1 = runCheckpointSnapshot(backend, 201L);

            backend.setCurrentKey(1);
            backend.getPartitionedState("ha-recovery-ns", StringSerializer.INSTANCE, descriptor)
                    .update("value-2");
            checkpointHandle2 = runCheckpointSnapshot(backend, 202L);

            backend.setCurrentKey(1);
            backend.getPartitionedState("ha-recovery-ns", StringSerializer.INSTANCE, descriptor)
                    .update("value-3");
            checkpointHandle3 = runCheckpointSnapshot(backend, 203L);
        }

        CompletedCheckpoint completedCheckpoint1 =
                createCompletedCheckpoint(
                        jobId,
                        201L,
                        checkpointHandle1,
                        tempDir.resolve("restart-checkpoints").resolve("chk-201"),
                        operatorId1,
                        operatorId2);
        CompletedCheckpoint completedCheckpoint2 =
                createCompletedCheckpoint(
                        jobId,
                        202L,
                        checkpointHandle2,
                        tempDir.resolve("restart-checkpoints").resolve("chk-202"),
                        operatorId1,
                        operatorId2);
        CompletedCheckpoint completedCheckpoint3 =
                createCompletedCheckpoint(
                        jobId,
                        203L,
                        checkpointHandle3,
                        tempDir.resolve("restart-checkpoints").resolve("chk-203"),
                        operatorId1,
                        operatorId2);

        CobbleCompletedCheckpointStore firstStore = new CobbleCompletedCheckpointStore(innerStore);
        firstStore.addCheckpointAndSubsumeOldestOne(
                completedCheckpoint1, new CheckpointsCleaner(), () -> {});
        firstStore.addCheckpointAndSubsumeOldestOne(
                completedCheckpoint2, new CheckpointsCleaner(), () -> {});

        try (DbCoordinator restartedCoordinator1 =
                        DbCoordinator.open(
                                createCoordinatorConfig(completedCheckpoint2, operatorId1));
                DbCoordinator restartedCoordinator2 =
                        DbCoordinator.open(
                                createCoordinatorConfig(completedCheckpoint2, operatorId2))) {
            assertSnapshotIds(restartedCoordinator1.listGlobalSnapshots(), 201L, 202L);
            assertSnapshotIds(restartedCoordinator2.listGlobalSnapshots(), 201L, 202L);
        }

        CobbleCompletedCheckpointStore recoveredStore =
                new CobbleCompletedCheckpointStore(innerStore);
        recoveredStore.addCheckpointAndSubsumeOldestOne(
                completedCheckpoint3, new CheckpointsCleaner(), () -> {});

        try (DbCoordinator finalCoordinator1 =
                        DbCoordinator.open(
                                createCoordinatorConfig(completedCheckpoint3, operatorId1));
                DbCoordinator finalCoordinator2 =
                        DbCoordinator.open(
                                createCoordinatorConfig(completedCheckpoint3, operatorId2))) {
            assertSnapshotIds(finalCoordinator1.listGlobalSnapshots(), 202L, 203L);
            assertEquals(203L, finalCoordinator1.loadCurrentGlobalSnapshot().id);
            assertSnapshotIds(finalCoordinator2.listGlobalSnapshots(), 202L, 203L);
            assertEquals(203L, finalCoordinator2.loadCurrentGlobalSnapshot().id);
        }

        assertTrue(
                !new org.apache.flink.core.fs.Path(
                                CobblePathUtils.cobbleGlobalSnapshotManifestPath(
                                        completedCheckpoint1.getExternalPointer(),
                                        operatorId1.toHexString(),
                                        201L))
                        .getFileSystem()
                        .exists(
                                new org.apache.flink.core.fs.Path(
                                        CobblePathUtils.cobbleGlobalSnapshotManifestPath(
                                                completedCheckpoint1.getExternalPointer(),
                                                operatorId1.toHexString(),
                                                201L))));
        assertTrue(
                !new org.apache.flink.core.fs.Path(
                                CobblePathUtils.cobbleGlobalSnapshotManifestPath(
                                        completedCheckpoint1.getExternalPointer(),
                                        operatorId2.toHexString(),
                                        201L))
                        .getFileSystem()
                        .exists(
                                new org.apache.flink.core.fs.Path(
                                        CobblePathUtils.cobbleGlobalSnapshotManifestPath(
                                                completedCheckpoint1.getExternalPointer(),
                                                operatorId2.toHexString(),
                                                201L))));
        assertTrue(
                new org.apache.flink.core.fs.Path(
                                CobblePathUtils.checkpointGlobalSnapshotManifestCopyPath(
                                        completedCheckpoint2.getExternalPointer(),
                                        operatorId1.toHexString()))
                        .getFileSystem()
                        .exists(
                                new org.apache.flink.core.fs.Path(
                                        CobblePathUtils.checkpointGlobalSnapshotManifestCopyPath(
                                                completedCheckpoint2.getExternalPointer(),
                                                operatorId1.toHexString()))));
        assertTrue(
                new org.apache.flink.core.fs.Path(
                                CobblePathUtils.checkpointGlobalSnapshotManifestCopyPath(
                                        completedCheckpoint3.getExternalPointer(),
                                        operatorId2.toHexString()))
                        .getFileSystem()
                        .exists(
                                new org.apache.flink.core.fs.Path(
                                        CobblePathUtils.checkpointGlobalSnapshotManifestCopyPath(
                                                completedCheckpoint3.getExternalPointer(),
                                                operatorId2.toHexString()))));
    }

    @Test
    void wrappedCheckpointStoreCleansPreviousManifestAfterRestoreContinuesCheckpointing(
            @TempDir Path tempDir) throws Exception {
        JobID jobId = new JobID();
        OperatorID operatorId = new OperatorID();
        String checkpointDirectory = tempDir.resolve("restore-checkpoints").toString();
        List<CompletedCheckpoint> retainedCheckpoints = new ArrayList<>();
        TestingCompletedCheckpointStore innerStore = createInnerStore(retainedCheckpoints, 1);

        int restoredKey = findKeyForGroupRange(KeyGroupRange.of(0, 15));
        KeyedStateHandle initialHandle;
        KeyedStateHandle restoredHandle;
        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("restore-continue-state", StringSerializer.INSTANCE);

        try (TestBackendContext sourceContext =
                createBackendContext(
                        tempDir.resolve("restore-source"),
                        checkpointDirectory,
                        Collections.emptyList(),
                        KeyGroupRange.of(0, 15))) {
            CobbleKeyedStateBackend<Integer> backend = sourceContext.backend;
            backend.setCurrentKey(restoredKey);
            backend.getPartitionedState(
                            "restore-continue-ns", StringSerializer.INSTANCE, descriptor)
                    .update("value-before-restore");
            initialHandle = runCheckpointSnapshot(backend, 301L);
        }

        try (TestBackendContext restoredContext =
                createBackendContext(
                        tempDir.resolve("restore-target"),
                        checkpointDirectory,
                        Collections.singletonList(initialHandle),
                        KeyGroupRange.of(0, 15))) {
            CobbleKeyedStateBackend<Integer> backend = restoredContext.backend;
            backend.setCurrentKey(restoredKey);
            assertEquals(
                    "value-before-restore",
                    backend.getPartitionedState(
                                    "restore-continue-ns", StringSerializer.INSTANCE, descriptor)
                            .value());

            backend.setCurrentKey(restoredKey);
            backend.getPartitionedState(
                            "restore-continue-ns", StringSerializer.INSTANCE, descriptor)
                    .update("value-after-restore");
            restoredHandle = runCheckpointSnapshot(backend, 302L);
        }

        CompletedCheckpoint completedCheckpoint1 =
                createCompletedCheckpoint(
                        jobId,
                        301L,
                        tempDir.resolve("restore-completed").resolve("chk-301"),
                        operatorId,
                        1,
                        initialHandle);
        CompletedCheckpoint completedCheckpoint2 =
                createCompletedCheckpoint(
                        jobId,
                        302L,
                        tempDir.resolve("restore-completed").resolve("chk-302"),
                        operatorId,
                        1,
                        restoredHandle);

        CobbleCompletedCheckpointStore initialStore =
                new CobbleCompletedCheckpointStore(innerStore);
        initialStore.addCheckpointAndSubsumeOldestOne(
                completedCheckpoint1, new CheckpointsCleaner(), () -> {});

        CobbleCompletedCheckpointStore recoveredStore =
                new CobbleCompletedCheckpointStore(innerStore);
        recoveredStore.addCheckpointAndSubsumeOldestOne(
                completedCheckpoint2, new CheckpointsCleaner(), () -> {});

        try (DbCoordinator coordinator =
                DbCoordinator.open(createCoordinatorConfig(completedCheckpoint2, operatorId))) {
            assertEquals(1, coordinator.listGlobalSnapshots().size());
            assertEquals(302L, coordinator.loadCurrentGlobalSnapshot().id);
        }

        assertFalse(
                new org.apache.flink.core.fs.Path(
                                CobblePathUtils.cobbleGlobalSnapshotManifestPath(
                                        completedCheckpoint1.getExternalPointer(),
                                        operatorId.toHexString(),
                                        301L))
                        .getFileSystem()
                        .exists(
                                new org.apache.flink.core.fs.Path(
                                        CobblePathUtils.cobbleGlobalSnapshotManifestPath(
                                                completedCheckpoint1.getExternalPointer(),
                                                operatorId.toHexString(),
                                                301L))));
        assertFalse(
                new org.apache.flink.core.fs.Path(
                                CobblePathUtils.checkpointGlobalSnapshotManifestCopyPath(
                                        completedCheckpoint1.getExternalPointer(),
                                        operatorId.toHexString()))
                        .getFileSystem()
                        .exists(
                                new org.apache.flink.core.fs.Path(
                                        CobblePathUtils.checkpointGlobalSnapshotManifestCopyPath(
                                                completedCheckpoint1.getExternalPointer(),
                                                operatorId.toHexString()))));
    }

    @Test
    void wrappedCheckpointStoreCleansPreviousManifestAfterRescaleContinuesCheckpointing(
            @TempDir Path tempDir) throws Exception {
        JobID jobId = new JobID();
        OperatorID operatorId = new OperatorID();
        String checkpointDirectory = tempDir.resolve("rescale-checkpoints").toString();
        List<CompletedCheckpoint> retainedCheckpoints = new ArrayList<>();
        TestingCompletedCheckpointStore innerStore = createInnerStore(retainedCheckpoints, 1);

        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("rescale-continue-state", StringSerializer.INSTANCE);
        int leftKey = findKeyForGroupRange(KeyGroupRange.of(0, 7));
        int rightKey = findKeyForGroupRange(KeyGroupRange.of(8, 15));

        KeyedStateHandle leftHandle;
        KeyedStateHandle rightHandle;
        KeyedStateHandle mergedHandle;

        try (TestBackendContext leftContext =
                createBackendContext(
                        tempDir.resolve("rescale-left"),
                        checkpointDirectory,
                        Collections.emptyList(),
                        KeyGroupRange.of(0, 7))) {
            CobbleKeyedStateBackend<Integer> backend = leftContext.backend;
            backend.setCurrentKey(leftKey);
            backend.getPartitionedState(
                            "rescale-continue-ns", StringSerializer.INSTANCE, descriptor)
                    .update("left-value");
            leftHandle = runCheckpointSnapshot(backend, 401L);
        }

        try (TestBackendContext rightContext =
                createBackendContext(
                        tempDir.resolve("rescale-right"),
                        checkpointDirectory,
                        Collections.emptyList(),
                        KeyGroupRange.of(8, 15))) {
            CobbleKeyedStateBackend<Integer> backend = rightContext.backend;
            backend.setCurrentKey(rightKey);
            backend.getPartitionedState(
                            "rescale-continue-ns", StringSerializer.INSTANCE, descriptor)
                    .update("right-value");
            rightHandle = runCheckpointSnapshot(backend, 401L);
        }

        try (TestBackendContext mergedContext =
                createBackendContext(
                        tempDir.resolve("rescale-merged"),
                        checkpointDirectory,
                        Arrays.asList(leftHandle, rightHandle),
                        KeyGroupRange.of(0, 15))) {
            CobbleKeyedStateBackend<Integer> backend = mergedContext.backend;
            ValueState<String> state =
                    backend.getPartitionedState(
                            "rescale-continue-ns", StringSerializer.INSTANCE, descriptor);

            backend.setCurrentKey(leftKey);
            assertEquals("left-value", state.value());
            backend.setCurrentKey(rightKey);
            assertEquals("right-value", state.value());

            backend.setCurrentKey(rightKey);
            state.update("right-value-updated");
            mergedHandle = runCheckpointSnapshot(backend, 402L);
        }

        CompletedCheckpoint completedCheckpoint1 =
                createCompletedCheckpoint(
                        jobId,
                        401L,
                        tempDir.resolve("rescale-completed").resolve("chk-401"),
                        operatorId,
                        2,
                        leftHandle,
                        rightHandle);
        CompletedCheckpoint completedCheckpoint2 =
                createCompletedCheckpoint(
                        jobId,
                        402L,
                        tempDir.resolve("rescale-completed").resolve("chk-402"),
                        operatorId,
                        1,
                        mergedHandle);

        CobbleCompletedCheckpointStore initialStore =
                new CobbleCompletedCheckpointStore(innerStore);
        initialStore.addCheckpointAndSubsumeOldestOne(
                completedCheckpoint1, new CheckpointsCleaner(), () -> {});

        CobbleCompletedCheckpointStore recoveredStore =
                new CobbleCompletedCheckpointStore(innerStore);
        recoveredStore.addCheckpointAndSubsumeOldestOne(
                completedCheckpoint2, new CheckpointsCleaner(), () -> {});

        try (DbCoordinator coordinator =
                DbCoordinator.open(createCoordinatorConfig(completedCheckpoint2, operatorId))) {
            assertEquals(1, coordinator.listGlobalSnapshots().size());
            assertEquals(402L, coordinator.loadCurrentGlobalSnapshot().id);
        }

        assertFalse(
                new org.apache.flink.core.fs.Path(
                                CobblePathUtils.cobbleGlobalSnapshotManifestPath(
                                        completedCheckpoint1.getExternalPointer(),
                                        operatorId.toHexString(),
                                        401L))
                        .getFileSystem()
                        .exists(
                                new org.apache.flink.core.fs.Path(
                                        CobblePathUtils.cobbleGlobalSnapshotManifestPath(
                                                completedCheckpoint1.getExternalPointer(),
                                                operatorId.toHexString(),
                                                401L))));
        assertFalse(
                new org.apache.flink.core.fs.Path(
                                CobblePathUtils.checkpointGlobalSnapshotManifestCopyPath(
                                        completedCheckpoint1.getExternalPointer(),
                                        operatorId.toHexString()))
                        .getFileSystem()
                        .exists(
                                new org.apache.flink.core.fs.Path(
                                        CobblePathUtils.checkpointGlobalSnapshotManifestCopyPath(
                                                completedCheckpoint1.getExternalPointer(),
                                                operatorId.toHexString()))));
    }

    @Test
    void haFactoryWrapsDelegateCheckpointRecoveryFactory(@TempDir Path tempDir) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(
                org.apache.flink.configuration.HighAvailabilityOptions.HA_MODE,
                CobbleHighAvailabilityServicesFactory.class.getName());
        configuration.setString(CobbleHighAvailabilityOptions.DELEGATE_HA_TYPE, "NONE");

        HighAvailabilityServices services =
                new CobbleHighAvailabilityServicesFactory()
                        .createHAServices(configuration, Runnable::run);
        try {
            assertInstanceOf(CobbleHighAvailabilityServices.class, services);
            assertInstanceOf(
                    CobbleCheckpointRecoveryFactory.class, services.getCheckpointRecoveryFactory());
        } finally {
            services.close();
        }
    }

    private CompletedCheckpoint createCompletedCheckpoint(
            JobID jobId,
            long checkpointId,
            KeyedStateHandle keyedStateHandle,
            Path externalPointer,
            OperatorID... operatorIds) {
        java.util.Map<OperatorID, OperatorState> operatorStates = new java.util.LinkedHashMap<>();
        for (OperatorID operatorId : operatorIds) {
            OperatorSubtaskState subtaskState =
                    OperatorSubtaskState.builder().setManagedKeyedState(keyedStateHandle).build();
            OperatorState operatorState = new OperatorState(operatorId, 1, 16);
            operatorState.putState(0, subtaskState);
            operatorStates.put(operatorId, operatorState);
        }
        return new CompletedCheckpoint(
                jobId,
                checkpointId,
                checkpointId,
                checkpointId + 1,
                operatorStates,
                Collections.emptyList(),
                CheckpointProperties.forCheckpoint(
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                new TestCompletedCheckpointStorageLocation(
                        new org.apache.flink.runtime.state.memory.ByteStreamStateHandle(
                                "metadata-" + checkpointId, new byte[0]),
                        externalPointer.toString()),
                null);
    }

    private CompletedCheckpoint createCompletedCheckpoint(
            JobID jobId,
            long checkpointId,
            Path externalPointer,
            OperatorID operatorId,
            int operatorParallelism,
            KeyedStateHandle... keyedStateHandles) {
        java.util.Map<OperatorID, OperatorState> operatorStates = new java.util.LinkedHashMap<>();
        OperatorState operatorState = new OperatorState(operatorId, operatorParallelism, 16);
        for (int subtaskIndex = 0; subtaskIndex < keyedStateHandles.length; subtaskIndex++) {
            operatorState.putState(
                    subtaskIndex,
                    OperatorSubtaskState.builder()
                            .setManagedKeyedState(keyedStateHandles[subtaskIndex])
                            .build());
        }
        operatorStates.put(operatorId, operatorState);
        return new CompletedCheckpoint(
                jobId,
                checkpointId,
                checkpointId,
                checkpointId + 1,
                operatorStates,
                Collections.emptyList(),
                CheckpointProperties.forCheckpoint(
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                new TestCompletedCheckpointStorageLocation(
                        new org.apache.flink.runtime.state.memory.ByteStreamStateHandle(
                                "metadata-" + checkpointId, new byte[0]),
                        externalPointer.toString()),
                null);
    }

    private Config createCoordinatorConfig(CompletedCheckpoint checkpoint, OperatorID operatorId) {
        Config config = new Config().numColumns(1).totalBuckets(1);
        config.snapshotRetention = null;
        config.addVolume(
                CobblePathUtils.cobbleOperatorSnapshotDirectory(
                        checkpoint.getExternalPointer(), operatorId.toHexString()));
        return config;
    }

    private static void deleteCheckpointDirectory(String externalPointer) {
        try {
            CobblePathUtils.deletePathQuietly(
                    CobblePathUtils.normalizeStorageDirectory(externalPointer));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to delete checkpoint directory " + externalPointer, e);
        }
    }

    private TestBackendContext createBackendContext(
            Path tempDir, String checkpointDirectory, Collection<KeyedStateHandle> stateHandles)
            throws Exception {
        return createBackendContext(
                tempDir, checkpointDirectory, stateHandles, KeyGroupRange.of(0, 15));
    }

    private TestBackendContext createBackendContext(
            Path tempDir,
            String checkpointDirectory,
            Collection<KeyedStateHandle> stateHandles,
            KeyGroupRange keyGroupRange)
            throws Exception {
        Path configuredLocalDir = tempDir.resolve("configured-local-dir");
        Path taskManagerWorkingDir = tempDir.resolve("tm-working-dir");

        Configuration configuration = new Configuration();
        configuration.set(CobbleOptions.LOCAL_DIRECTORIES, configuredLocalDir.toString());
        configuration.set(CobbleOptions.WRITE_BUFFER_RATIO, 0.25d);
        configuration.set(CobbleOptions.MEMTABLE_BUFFER_COUNT, 4);
        configuration.set(CobbleOptions.DIRECT_IO_BUFFER_SIZE, MemorySize.parse("8kb"));
        configuration.set(CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE, 128);
        configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDirectory);

        CobbleStateBackend backend =
                new CobbleStateBackend(false).configure(configuration, getClass().getClassLoader());
        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setTaskName("cobble-ha-test-task")
                        .setJobVertexID(TEST_JOB_VERTEX_ID)
                        .setManagedMemorySize(
                                org.apache.flink.configuration.MemorySize.ofMebiBytes(128)
                                        .getBytes())
                        .setTaskManagerRuntimeInfo(
                                new TestingTaskManagerRuntimeInfo(
                                        new Configuration(), taskManagerWorkingDir.toFile()))
                        .setTaskStateManager(new TestTaskStateManagerBuilder().build())
                        .build();

        AbstractKeyedStateBackend<Integer> keyedStateBackend = null;
        try {
            TaskKvStateRegistry kvStateRegistry = environment.getTaskKvStateRegistry();
            keyedStateBackend =
                    backend.createKeyedStateBackend(
                            environment,
                            environment.getJobID(),
                            "ha-test-operator",
                            IntSerializer.INSTANCE,
                            16,
                            keyGroupRange,
                            kvStateRegistry,
                            TtlTimeProvider.DEFAULT,
                            environment.getMetricGroup(),
                            stateHandles,
                            new CloseableRegistry(),
                            0.5d);
            return new TestBackendContext(
                    environment, (CobbleKeyedStateBackend<Integer>) keyedStateBackend);
        } catch (Exception e) {
            if (keyedStateBackend != null) {
                keyedStateBackend.close();
            }
            environment.close();
            throw e;
        }
    }

    private static KeyedStateHandle runCheckpointSnapshot(
            CobbleKeyedStateBackend<Integer> backend, long checkpointId) throws Exception {
        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                backend.snapshot(
                        checkpointId,
                        System.currentTimeMillis(),
                        new MemCheckpointStreamFactory(1024 * 1024),
                        org.apache.flink.runtime.checkpoint.CheckpointOptions
                                .forCheckpointWithDefaultLocation());
        snapshotFuture.run();
        return snapshotFuture.get().getJobManagerOwnedSnapshot();
    }

    private static int findKeyForGroupRange(KeyGroupRange keyGroupRange) {
        for (int candidate = 0; candidate < 10_000; candidate++) {
            int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(candidate, 16);
            if (keyGroupRange.contains(keyGroup)) {
                return candidate;
            }
        }
        throw new IllegalStateException(
                "Could not find test key for key-group range " + keyGroupRange);
    }

    private static TestingCompletedCheckpointStore createInnerStore(
            List<CompletedCheckpoint> retainedCheckpoints, int maxRetainedCheckpoints) {
        return TestingCompletedCheckpointStore.builder()
                .withAddCheckpointAndSubsumeOldestOneFunction(
                        (checkpoint, cleaner, postCleanup) -> {
                            CompletedCheckpoint subsumed =
                                    retainedCheckpoints.size() >= maxRetainedCheckpoints
                                            ? retainedCheckpoints.remove(0)
                                            : null;
                            if (subsumed != null) {
                                deleteCheckpointDirectory(subsumed.getExternalPointer());
                            }
                            retainedCheckpoints.add(checkpoint);
                            return subsumed;
                        })
                .withShutdownConsumer(
                        (jobStatus, cleaner) -> {
                            for (CompletedCheckpoint checkpoint : retainedCheckpoints) {
                                deleteCheckpointDirectory(checkpoint.getExternalPointer());
                            }
                            retainedCheckpoints.clear();
                        })
                .withGetAllCheckpointsSupplier(() -> new ArrayList<>(retainedCheckpoints))
                .withGetNumberOfRetainedCheckpointsSupplier(retainedCheckpoints::size)
                .withGetMaxNumberOfRetainedCheckpointsSupplier(() -> maxRetainedCheckpoints)
                .withRequiresExternalizedCheckpointsSupplier(() -> false)
                .withGetSharedStateRegistrySupplier(
                        org.apache.flink.runtime.state.SharedStateRegistryImpl::new)
                .build();
    }

    private static void assertSnapshotIds(
            List<GlobalSnapshot> snapshots, long firstSnapshotId, long secondSnapshotId) {
        assertEquals(2, snapshots.size());
        assertEquals(firstSnapshotId, snapshots.get(0).id);
        assertEquals(secondSnapshotId, snapshots.get(1).id);
    }

    private static final class TestBackendContext implements AutoCloseable {
        private final MockEnvironment environment;
        private final CobbleKeyedStateBackend<Integer> backend;

        private TestBackendContext(
                MockEnvironment environment, CobbleKeyedStateBackend<Integer> backend) {
            this.environment = environment;
            this.backend = backend;
        }

        @Override
        public void close() throws Exception {
            backend.close();
            environment.close();
        }
    }
}
