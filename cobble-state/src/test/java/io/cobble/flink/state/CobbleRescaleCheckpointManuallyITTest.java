package io.cobble.flink.state;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getLatestCompletedCheckpointPath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class CobbleRescaleCheckpointManuallyITTest {
    private static final int NUMBER_OF_KEYS = 42;
    private static final int INITIAL_NUMBER_OF_ELEMENTS = 1_000;
    private static final int RESTORED_NUMBER_OF_ELEMENTS = 500;
    private static final int MAX_PARALLELISM = 13;

    @Test
    void restoresKeyedStateCorrectlyAfterScaleOut(@TempDir Path tempDir) throws Exception {
        runRescaleTest(tempDir, 3, 4);
    }

    @Test
    void restoresKeyedStateCorrectlyAfterScaleIn(@TempDir Path tempDir) throws Exception {
        runRescaleTest(tempDir, 4, 3);
    }

    private void runRescaleTest(Path tempDir, int initialParallelism, int restoredParallelism)
            throws Exception {
        MiniClusterWithClientResource cluster =
                CobbleCheckpointingITSupport.createCluster(new Configuration());

        cluster.before();
        try {
            String checkpointPath =
                    runJobAndGetCheckpoint(
                            cluster.getMiniCluster(),
                            tempDir,
                            initialParallelism,
                            INITIAL_NUMBER_OF_ELEMENTS);

            restoreAndAssert(
                    cluster.getMiniCluster(),
                    tempDir,
                    checkpointPath,
                    restoredParallelism,
                    INITIAL_NUMBER_OF_ELEMENTS + RESTORED_NUMBER_OF_ELEMENTS);
        } finally {
            cluster.after();
        }
    }

    private String runJobAndGetCheckpoint(
            MiniCluster miniCluster, Path tempDir, int parallelism, int numberOfElementsPerKey)
            throws Exception {
        DefiniteKeySource.reset();
        CollectingSink.clear();

        JobGraph jobGraph =
                createJobGraph(
                        tempDir.resolve("local-state-initial"),
                        tempDir.resolve("checkpoints"),
                        parallelism,
                        numberOfElementsPerKey,
                        numberOfElementsPerKey,
                        true);
        JobID jobId = jobGraph.getJobID();

        miniCluster.submitJob(jobGraph).get(30, TimeUnit.SECONDS);
        waitForAllTaskRunning(miniCluster, jobId, false);
        waitForCondition(
                () -> DefiniteKeySource.finishedSubtasks() == parallelism,
                Duration.ofSeconds(30),
                "all source subtasks finished emitting");
        waitForCondition(
                () -> completedCheckpoints(miniCluster, jobId) >= 2L,
                Duration.ofSeconds(30),
                "at least two completed checkpoints");

        miniCluster.cancelJob(jobId).get(30, TimeUnit.SECONDS);

        Optional<String> checkpointPath = getLatestCompletedCheckpointPath(jobId, miniCluster);
        if (!checkpointPath.isPresent()) {
            throw new AssertionError("No completed checkpoint found for initial job.");
        }
        return checkpointPath.get();
    }

    private void restoreAndAssert(
            MiniCluster miniCluster,
            Path tempDir,
            String checkpointPath,
            int restoredParallelism,
            int expectedElementsPerKey)
            throws Exception {
        DefiniteKeySource.reset();
        CollectingSink.clear();

        JobGraph restoredJobGraph =
                createJobGraph(
                        tempDir.resolve("local-state-restored"),
                        tempDir.resolve("checkpoints"),
                        restoredParallelism,
                        RESTORED_NUMBER_OF_ELEMENTS,
                        expectedElementsPerKey,
                        false);
        restoredJobGraph.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(checkpointPath, false, RestoreMode.CLAIM));

        miniCluster.submitJob(restoredJobGraph).get(30, TimeUnit.SECONDS);
        miniCluster.requestJobResult(restoredJobGraph.getJobID()).get(180, TimeUnit.SECONDS);

        Set<Tuple2<Integer, Integer>> expected = new HashSet<>();
        for (int key = 0; key < NUMBER_OF_KEYS; key++) {
            int keyGroup =
                    org.apache.flink.runtime.state.KeyGroupRangeAssignment.assignToKeyGroup(
                            key, MAX_PARALLELISM);
            int operatorIndex =
                    org.apache.flink.runtime.state.KeyGroupRangeAssignment
                            .computeOperatorIndexForKeyGroup(
                                    MAX_PARALLELISM, restoredParallelism, keyGroup);
            expected.add(Tuple2.of(operatorIndex, key * expectedElementsPerKey));
        }

        assertEquals(expected, CollectingSink.snapshot());
    }

    private JobGraph createJobGraph(
            Path localStateDirectory,
            Path checkpointDirectory,
            int parallelism,
            int numberOfElementsPerKey,
            int expectedElementsPerKey,
            boolean waitForCancellationAfterEmission) {
        Configuration configuration =
                CobbleCheckpointingITSupport.createJobConfiguration(localStateDirectory);
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDirectory.toUri().toString());

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);
        env.getConfig().setMaxParallelism(MAX_PARALLELISM);
        env.enableCheckpointing(100L);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setStateBackend(
                new CobbleStateBackend().configure(configuration, getClass().getClassLoader()));

        env.addSource(
                        new DefiniteKeySource(
                                NUMBER_OF_KEYS,
                                numberOfElementsPerKey,
                                waitForCancellationAfterEmission))
                .name("definite-key-source")
                .uid("definite-key-source")
                .keyBy(new IdentityKeySelector())
                .flatMap(new SubtaskIndexFlatMapper(expectedElementsPerKey))
                .name("subtask-index-flatmap")
                .uid("subtask-index-flatmap")
                .addSink(new CollectingSink())
                .name("collecting-sink")
                .uid("collecting-sink");

        return env.getStreamGraph().getJobGraph();
    }

    private void waitForCondition(
            CheckedBooleanSupplier condition, Duration timeout, String description)
            throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50L);
        }

        throw new AssertionError("Timed out waiting for " + description + ".");
    }

    private long completedCheckpoints(MiniCluster miniCluster, JobID jobId) throws Exception {
        return miniCluster
                .getArchivedExecutionGraph(jobId)
                .get()
                .getCheckpointStatsSnapshot()
                .getCounts()
                .getNumberOfCompletedCheckpoints();
    }

    @FunctionalInterface
    private interface CheckedBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }

    private static final class DefiniteKeySource extends RichParallelSourceFunction<Integer> {
        private static final AtomicInteger FINISHED_SUBTASKS = new AtomicInteger();

        private final int numberOfKeys;
        private final int numberOfElementsPerKey;
        private final boolean waitForCancellationAfterEmission;
        private volatile boolean running = true;

        private DefiniteKeySource(
                int numberOfKeys,
                int numberOfElementsPerKey,
                boolean waitForCancellationAfterEmission) {
            this.numberOfKeys = numberOfKeys;
            this.numberOfElementsPerKey = numberOfElementsPerKey;
            this.waitForCancellationAfterEmission = waitForCancellationAfterEmission;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();

            for (int round = 0; running && round < numberOfElementsPerKey; round++) {
                synchronized (ctx.getCheckpointLock()) {
                    for (int key = subtaskIndex; key < numberOfKeys; key += parallelism) {
                        ctx.collect(key);
                    }
                }
            }

            FINISHED_SUBTASKS.incrementAndGet();
            while (running && waitForCancellationAfterEmission) {
                Thread.sleep(10L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        private static void reset() {
            FINISHED_SUBTASKS.set(0);
        }

        private static int finishedSubtasks() {
            return FINISHED_SUBTASKS.get();
        }
    }

    private static final class SubtaskIndexFlatMapper
            extends RichFlatMapFunction<Integer, Tuple2<Integer, Integer>> {
        private final int expectedElementsPerKey;
        private transient ValueState<Integer> counter;
        private transient ValueState<Integer> sum;

        private SubtaskIndexFlatMapper(int expectedElementsPerKey) {
            this.expectedElementsPerKey = expectedElementsPerKey;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            counter =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("counter", Integer.class));
            sum = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Integer.class));
        }

        @Override
        public void flatMap(Integer value, Collector<Tuple2<Integer, Integer>> out)
                throws Exception {
            Integer currentCount = counter.value();
            int updatedCount = currentCount == null ? 1 : currentCount + 1;
            counter.update(updatedCount);

            Integer currentSum = sum.value();
            int updatedSum = currentSum == null ? value : currentSum + value;
            sum.update(updatedSum);

            if (updatedCount == expectedElementsPerKey) {
                out.collect(Tuple2.of(getRuntimeContext().getIndexOfThisSubtask(), updatedSum));
            }
        }
    }

    private static final class IdentityKeySelector implements KeySelector<Integer, Integer> {
        @Override
        public Integer getKey(Integer value) {
            return value;
        }
    }

    private static final class CollectingSink extends RichSinkFunction<Tuple2<Integer, Integer>> {
        private static final Set<Tuple2<Integer, Integer>> RESULTS = ConcurrentHashMap.newKeySet();

        @Override
        public void invoke(Tuple2<Integer, Integer> value) {
            RESULTS.add(Tuple2.of(value.f0, value.f1));
        }

        private static void clear() {
            RESULTS.clear();
        }

        private static Set<Tuple2<Integer, Integer>> snapshot() {
            return new HashSet<>(RESULTS);
        }
    }
}
