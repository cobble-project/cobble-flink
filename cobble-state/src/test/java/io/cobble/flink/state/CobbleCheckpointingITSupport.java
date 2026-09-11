package io.cobble.flink.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public final class CobbleCheckpointingITSupport {
    static final int NUM_TASK_MANAGERS = 2;
    static final int NUM_TASK_SLOTS = 2;
    static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;
    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration JOB_TRANSITION_TIMEOUT = Duration.ofSeconds(60);
    private static final long POLL_INTERVAL_MILLIS = 50L;

    private CobbleCheckpointingITSupport() {}

    static MiniClusterWithClientResource createCluster(Configuration configuration) {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(configuration)
                        .setNumberTaskManagers(NUM_TASK_MANAGERS)
                        .setNumberSlotsPerTaskManager(NUM_TASK_SLOTS)
                        .build());
    }

    static Configuration createJobConfiguration(Path localStateDirectory) {
        Configuration configuration = new Configuration();
        configuration.set(CobbleOptions.LOCAL_DIRECTORIES, localStateDirectory.toString());
        configuration.set(CobbleOptions.MEMTABLE_BUFFER_RATIO, 0.25d);
        configuration.set(CobbleOptions.MEMTABLE_BUFFER_COUNT, 4);
        configuration.set(CobbleOptions.DIRECT_IO_BUFFER_SIZE, MemorySize.parse("8kb"));
        configuration.set(CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE, 128);
        return configuration;
    }

    static StreamExecutionEnvironment createEnvironment(Path localStateDirectory) {
        Configuration configuration = createJobConfiguration(localStateDirectory);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(PARALLELISM);
        env.setStateBackend(
                new CobbleStateBackend()
                        .configure(
                                configuration,
                                CobbleCheckpointingITSupport.class.getClassLoader()));
        return env;
    }

    public static void submitJobAndWaitForRunning(
            MiniClusterWithClientResource cluster, JobGraph jobGraph) throws Exception {
        cluster.getClusterClient()
                .submitJob(jobGraph)
                .get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        waitForAllTasksRunning(
                cluster.getMiniCluster(), jobGraph.getJobID(), JOB_TRANSITION_TIMEOUT);
    }

    public static String triggerCheckpointAndWait(MiniCluster miniCluster, JobID jobId)
            throws Exception {
        long previousCheckpointId = latestCompletedCheckpointId(miniCluster, jobId);
        String checkpointPath =
                miniCluster
                        .triggerCheckpoint(jobId)
                        .get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        waitForJobCondition(
                miniCluster,
                jobId,
                () -> latestCompletedCheckpointId(miniCluster, jobId) > previousCheckpointId,
                JOB_TRANSITION_TIMEOUT,
                "a checkpoint newer than " + previousCheckpointId);
        return checkpointPath;
    }

    public static void waitForCompletedCheckpointCount(
            MiniCluster miniCluster, JobID jobId, long expectedCount, Duration timeout)
            throws Exception {
        waitForJobCondition(
                miniCluster,
                jobId,
                () -> completedCheckpointCount(miniCluster, jobId) >= expectedCount,
                timeout,
                expectedCount + " completed checkpoint(s)");
    }

    public static void waitForCheckpointArtifacts(
            MiniCluster miniCluster, JobID jobId, Path checkpointRoot, Duration timeout)
            throws Exception {
        waitForJobCondition(
                miniCluster,
                jobId,
                () -> checkpointArtifactsReady(checkpointRoot),
                timeout,
                "checkpoint manifest and inspect schema artifacts");
    }

    public static void cancelJobAndWait(MiniClusterWithClientResource cluster, JobID jobId)
            throws Exception {
        cluster.getClusterClient()
                .cancel(jobId)
                .get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        waitForJobStatus(
                cluster.getMiniCluster(), jobId, JobStatus.CANCELED, JOB_TRANSITION_TIMEOUT);
    }

    public static void waitForJobFinished(MiniClusterWithClientResource cluster, JobID jobId)
            throws Exception {
        waitForJobFinished(cluster, jobId, JOB_TRANSITION_TIMEOUT);
    }

    public static void waitForJobFinished(
            MiniClusterWithClientResource cluster, JobID jobId, Duration timeout) throws Exception {
        cluster.getClusterClient()
                .requestJobResult(jobId)
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        waitForJobStatus(cluster.getMiniCluster(), jobId, JobStatus.FINISHED, timeout);
    }

    public static void waitForAllTasksRunning(
            MiniCluster miniCluster, JobID jobId, Duration timeout) throws Exception {
        waitForJobCondition(
                miniCluster,
                jobId,
                () -> {
                    AccessExecutionGraph graph = miniCluster.getExecutionGraph(jobId).get();
                    return graph.getState() == JobStatus.RUNNING
                            && graph.getAllVertices().values().stream()
                                    .flatMap(vertex -> Arrays.stream(vertex.getTaskVertices()))
                                    .allMatch(
                                            task ->
                                                    task.getExecutionState()
                                                            == ExecutionState.RUNNING);
                },
                timeout,
                "all tasks to enter RUNNING");
    }

    public static void waitForJobCondition(
            MiniCluster miniCluster,
            JobID jobId,
            CheckedBooleanSupplier condition,
            Duration timeout,
            String description)
            throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            AccessExecutionGraph graph = miniCluster.getExecutionGraph(jobId).get();
            if (graph.getState().isGloballyTerminalState()) {
                Throwable failure =
                        graph.getFailureInfo() == null
                                ? null
                                : graph.getFailureInfo().getException();
                throw new AssertionError(
                        "Job "
                                + jobId
                                + " entered "
                                + graph.getState()
                                + " while waiting for "
                                + description
                                + ".",
                        failure);
            }
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MILLIS);
        }
        throw new AssertionError(
                "Timed out after " + timeout + " waiting for " + description + " on job " + jobId);
    }

    public static void waitForJobStatus(
            MiniCluster miniCluster, JobID jobId, JobStatus expected, Duration timeout)
            throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            JobStatus actual = miniCluster.getJobStatus(jobId).get();
            if (actual == expected) {
                return;
            }
            if (actual.isGloballyTerminalState()) {
                throw new AssertionError(
                        "Job " + jobId + " entered " + actual + " instead of " + expected + ".");
            }
            Thread.sleep(POLL_INTERVAL_MILLIS);
        }
        throw new AssertionError(
                "Timed out after "
                        + timeout
                        + " waiting for job "
                        + jobId
                        + " to enter "
                        + expected);
    }

    private static long latestCompletedCheckpointId(MiniCluster miniCluster, JobID jobId)
            throws Exception {
        AccessExecutionGraph graph = miniCluster.getExecutionGraph(jobId).get();
        if (graph.getCheckpointStatsSnapshot() == null
                || graph.getCheckpointStatsSnapshot().getHistory() == null
                || graph.getCheckpointStatsSnapshot().getHistory().getLatestCompletedCheckpoint()
                        == null) {
            return -1L;
        }
        return graph.getCheckpointStatsSnapshot()
                .getHistory()
                .getLatestCompletedCheckpoint()
                .getCheckpointId();
    }

    private static long completedCheckpointCount(MiniCluster miniCluster, JobID jobId)
            throws Exception {
        AccessExecutionGraph graph = miniCluster.getExecutionGraph(jobId).get();
        return graph.getCheckpointStatsSnapshot() == null
                ? 0L
                : graph.getCheckpointStatsSnapshot().getCounts().getNumberOfCompletedCheckpoints();
    }

    private static boolean checkpointArtifactsReady(Path checkpointRoot) throws Exception {
        if (!Files.exists(checkpointRoot)) {
            return false;
        }
        boolean manifestFound = false;
        boolean schemaEventFound = false;
        try (Stream<Path> paths = Files.walk(checkpointRoot)) {
            for (Path path : (Iterable<Path>) paths::iterator) {
                String name = path.getFileName() == null ? "" : path.getFileName().toString();
                manifestFound |= name.startsWith("COBBLE-SNAPSHOT-") && name.endsWith("-MANIFEST");
                schemaEventFound |=
                        Files.isRegularFile(path)
                                && path.getParent() != null
                                && path.getParent().getFileName() != null
                                && "events".equals(path.getParent().getFileName().toString());
            }
        }
        return manifestFound && schemaEventFound;
    }

    @FunctionalInterface
    public interface CheckedBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }
}
