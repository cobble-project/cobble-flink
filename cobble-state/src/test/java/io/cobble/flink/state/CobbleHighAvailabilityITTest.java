package io.cobble.flink.state;

import static org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForCheckpoint;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

class CobbleHighAvailabilityITTest {

    @Test
    void streamingJobMaterializesPerOperatorSnapshotManifests(@TempDir Path tempDir)
            throws Exception {
        Path checkpointRoot = tempDir.resolve("checkpoints");
        Path localStateRoot = tempDir.resolve("local-state");

        Configuration clusterConfiguration = new Configuration();
        clusterConfiguration.setString(
                HighAvailabilityOptions.HA_MODE,
                CobbleHighAvailabilityServicesFactory.class.getName());
        clusterConfiguration.setString(CobbleHighAvailabilityOptions.DELEGATE_HA_TYPE, "NONE");
        clusterConfiguration.setString(JobManagerOptions.ADDRESS, "localhost");
        clusterConfiguration.setInteger(JobManagerOptions.PORT, 6123);
        clusterConfiguration.setString(RestOptions.ADDRESS, "localhost");
        clusterConfiguration.setInteger(RestOptions.PORT, 8081);
        clusterConfiguration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointRoot.toUri().toString());

        SnapshotObservation firstObservation;
        Path restoreCheckpoint;

        MiniClusterWithClientResource firstCluster = createCluster(clusterConfiguration);
        firstCluster.before();
        try {
            JobGraph firstJobGraph =
                    createJobGraph(checkpointRoot, localStateRoot.resolve("first"), 2, null);
            JobID firstJobId = firstJobGraph.getJobID();

            firstCluster.getClusterClient().submitJob(firstJobGraph).get(30, TimeUnit.SECONDS);
            waitForAllTaskRunning(firstCluster.getMiniCluster(), firstJobId, false);
            waitForCheckpoint(firstJobId, firstCluster.getMiniCluster(), 2);

            firstObservation =
                    waitForSnapshotArtifacts(checkpointRoot, 2L, 2, Duration.ofSeconds(60));
            assertTrue(firstObservation.latestCompletedCheckpointId() >= 2L);
            assertTrue(firstObservation.maxManifestCopiesPerCheckpoint >= 2);
            assertTrue(firstObservation.maxOperatorDirectories >= 2);

            restoreCheckpoint = firstObservation.latestCompletedCheckpointPath();

            firstCluster.getClusterClient().cancel(firstJobId).get(30, TimeUnit.SECONDS);
        } finally {
            firstCluster.after();
        }

        MiniClusterWithClientResource secondCluster = createCluster(clusterConfiguration);
        secondCluster.before();
        try {
            JobGraph rescaledJobGraph =
                    createJobGraph(
                            checkpointRoot,
                            localStateRoot.resolve("second"),
                            4,
                            restoreCheckpoint.toUri().toString());
            JobID rescaledJobId = rescaledJobGraph.getJobID();

            secondCluster.getClusterClient().submitJob(rescaledJobGraph).get(30, TimeUnit.SECONDS);
            waitForAllTaskRunning(secondCluster.getMiniCluster(), rescaledJobId, false);
            waitForCheckpoint(rescaledJobId, secondCluster.getMiniCluster(), 2);

            SnapshotObservation rescaledObservation =
                    waitForSnapshotArtifacts(
                            checkpointRoot,
                            firstObservation.latestCompletedCheckpointId() + 1L,
                            2,
                            Duration.ofSeconds(60));
            assertTrue(
                    rescaledObservation.latestCompletedCheckpointId()
                            > firstObservation.latestCompletedCheckpointId());
            assertTrue(rescaledObservation.maxManifestCopiesPerCheckpoint >= 2);
            assertTrue(rescaledObservation.maxOperatorDirectories >= 2);

            secondCluster.getClusterClient().cancel(rescaledJobId).get(30, TimeUnit.SECONDS);
        } finally {
            secondCluster.after();
        }
    }

    private MiniClusterWithClientResource createCluster(Configuration clusterConfiguration) {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(clusterConfiguration)
                        .setNumberTaskManagers(2)
                        .setNumberSlotsPerTaskManager(2)
                        .build());
    }

    private JobGraph createJobGraph(
            Path checkpointRoot, Path localStateRoot, int parallelism, String restorePath) {
        Configuration jobConfiguration = new Configuration();
        jobConfiguration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointRoot.toUri().toString());
        jobConfiguration.set(CobbleOptions.LOCAL_DIRECTORIES, localStateRoot.toString());
        jobConfiguration.set(CobbleOptions.WRITE_BUFFER_RATIO, 0.25d);
        jobConfiguration.set(CobbleOptions.MEMTABLE_BUFFER_COUNT, 4);
        jobConfiguration.set(CobbleOptions.DIRECT_IO_BUFFER_SIZE, MemorySize.parse("8kb"));
        jobConfiguration.set(CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE, 128);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(jobConfiguration);
        env.setParallelism(parallelism);
        env.enableCheckpointing(5_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1_000L);
        env.getCheckpointConfig().setCheckpointTimeout(180_000L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(noRestart());
        env.setStateBackend(
                new CobbleStateBackend().configure(jobConfiguration, getClass().getClassLoader()));

        env.addSource(new ContinuousSequenceSource())
                .name("continuous-source")
                .uid("continuous-source")
                .keyBy(value -> value % 128L)
                .map(new CountingStateMap("first-state"))
                .name("first-stateful-operator")
                .uid("first-stateful-operator")
                .setMaxParallelism(128)
                .keyBy(value -> value % 64L)
                .map(new CountingStateMap("second-state"))
                .name("second-stateful-operator")
                .uid("second-stateful-operator")
                .setMaxParallelism(128)
                .addSink(new DiscardingSink<>());

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        if (restorePath != null) {
            jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(restorePath));
        }
        return jobGraph;
    }

    private static SnapshotObservation waitForSnapshotArtifacts(
            Path checkpointRoot,
            long minimumLatestCompletedCheckpointId,
            int minimumOperatorDirectories,
            Duration timeout)
            throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        SnapshotObservation lastObservation = SnapshotObservation.empty();
        while (System.nanoTime() < deadline) {
            lastObservation = observeSnapshotArtifacts(checkpointRoot);
            if (!lastObservation.completedCheckpointPaths.isEmpty()
                    && lastObservation.latestCompletedCheckpointId()
                            >= minimumLatestCompletedCheckpointId
                    && lastObservation.maxManifestCopiesPerCheckpoint >= minimumOperatorDirectories
                    && lastObservation.maxOperatorDirectories >= minimumOperatorDirectories) {
                return lastObservation;
            }
            Thread.sleep(200L);
        }

        throw new AssertionError(
                "Timed out waiting for Cobble HA checkpoint artifacts. Observed checkpoints="
                        + lastObservation.completedCheckpointPaths
                        + ", latestCompletedCheckpointId="
                        + lastObservation.latestCompletedCheckpointIdOrDefault(-1L)
                        + ", maxManifestCopiesPerCheckpoint="
                        + lastObservation.maxManifestCopiesPerCheckpoint
                        + ", maxOperatorDirectories="
                        + lastObservation.maxOperatorDirectories);
    }

    private static SnapshotObservation observeSnapshotArtifacts(Path checkpointRoot)
            throws IOException {
        if (!Files.exists(checkpointRoot)) {
            return SnapshotObservation.empty();
        }

        Set<Path> completedCheckpointPaths = new LinkedHashSet<>();
        int maxManifestCopiesPerCheckpoint = 0;
        int maxOperatorDirectories = 0;

        try (Stream<Path> paths = Files.walk(checkpointRoot)) {
            for (Path path : (Iterable<Path>) paths::iterator) {
                if (!Files.isDirectory(path)) {
                    continue;
                }

                String fileName = path.getFileName() == null ? "" : path.getFileName().toString();
                if (isCompletedCheckpointDirectory(path, fileName)) {
                    completedCheckpointPaths.add(path);
                    maxManifestCopiesPerCheckpoint =
                            Math.max(maxManifestCopiesPerCheckpoint, countManifestCopies(path));
                } else if ("cobble".equals(fileName)) {
                    maxOperatorDirectories =
                            Math.max(maxOperatorDirectories, countOperatorDirectories(path));
                }
            }
        }

        return new SnapshotObservation(
                completedCheckpointPaths, maxManifestCopiesPerCheckpoint, maxOperatorDirectories);
    }

    private static int countManifestCopies(Path checkpointDirectory) throws IOException {
        try (Stream<Path> children = Files.list(checkpointDirectory)) {
            return (int)
                    children.filter(Files::isRegularFile)
                            .map(Path::getFileName)
                            .map(Path::toString)
                            .filter(name -> name.startsWith("COBBLE-SNAPSHOT-"))
                            .count();
        }
    }

    private static int countOperatorDirectories(Path cobbleDirectory) throws IOException {
        try (Stream<Path> children = Files.list(cobbleDirectory)) {
            return (int) children.filter(Files::isDirectory).count();
        }
    }

    private static boolean isCompletedCheckpointDirectory(Path path, String fileName) {
        return fileName.startsWith("chk-")
                && Files.exists(path.resolve(AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME));
    }

    private static long checkpointId(Path checkpointPath) {
        String fileName =
                checkpointPath.getFileName() == null ? "" : checkpointPath.getFileName().toString();
        if (!fileName.startsWith("chk-")) {
            throw new IllegalArgumentException(
                    "Checkpoint path does not use the expected chk-<id> format: " + checkpointPath);
        }
        return Long.parseLong(fileName.substring("chk-".length()));
    }

    private static final class ContinuousSequenceSource extends RichParallelSourceFunction<Long> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            long nextValue = getRuntimeContext().getIndexOfThisSubtask();
            long step = getRuntimeContext().getNumberOfParallelSubtasks();
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(nextValue);
                    nextValue += step;
                }
                Thread.sleep(1L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class CountingStateMap extends RichMapFunction<Long, Long> {
        private final String stateName;
        private transient ValueState<Long> countState;

        private CountingStateMap(String stateName) {
            this.stateName = stateName;
        }

        @Override
        public Long map(Long value) throws Exception {
            if (countState == null) {
                countState =
                        getRuntimeContext()
                                .getState(new ValueStateDescriptor<>(stateName, Long.class));
            }
            Long current = countState.value();
            long updated = current == null ? 1L : current + 1L;
            countState.update(updated);
            return value + updated;
        }
    }

    private static final class SnapshotObservation {
        private final Set<Path> completedCheckpointPaths;
        private final int maxManifestCopiesPerCheckpoint;
        private final int maxOperatorDirectories;

        private SnapshotObservation(
                Set<Path> completedCheckpointPaths,
                int maxManifestCopiesPerCheckpoint,
                int maxOperatorDirectories) {
            this.completedCheckpointPaths = completedCheckpointPaths;
            this.maxManifestCopiesPerCheckpoint = maxManifestCopiesPerCheckpoint;
            this.maxOperatorDirectories = maxOperatorDirectories;
        }

        private Path latestCompletedCheckpointPath() {
            return completedCheckpointPaths.stream()
                    .max(Comparator.comparingLong(CobbleHighAvailabilityITTest::checkpointId))
                    .orElseThrow(
                            () -> new AssertionError("No completed checkpoint directory found."));
        }

        private long latestCompletedCheckpointId() {
            return checkpointId(latestCompletedCheckpointPath());
        }

        private long latestCompletedCheckpointIdOrDefault(long defaultValue) {
            return completedCheckpointPaths.isEmpty()
                    ? defaultValue
                    : latestCompletedCheckpointId();
        }

        private static SnapshotObservation empty() {
            return new SnapshotObservation(new LinkedHashSet<>(), 0, 0);
        }
    }
}
