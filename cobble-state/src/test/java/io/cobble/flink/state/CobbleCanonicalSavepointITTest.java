package io.cobble.flink.state;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.StateBackend;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MiniCluster IT tests for canonical savepoint creation and restore through a real Flink cluster.
 *
 * <p>Tests the full lifecycle: submit a Cobble job → trigger a canonical savepoint via the cluster
 * API → restore at a different parallelism (or into a different backend) → verify state
 * correctness.
 *
 * <p><b>Source design:</b> The source emits a fixed set of keys, writes state, then blocks on a
 * {@link CountDownLatch}. It does NOT terminate — the savepoint is triggered while the job is still
 * running, eliminating the race condition between source completion and savepoint trigger.
 *
 * <p><b>Restore verification:</b> The restore job uses a probe-only topology: it reads the same
 * fixed keys, queries state values without writing, and outputs them to a collecting sink. No
 * {@code add}/{@code update} calls on the restored state — the assertions see exactly what was
 * restored, not "restore + new writes."
 */
class CobbleCanonicalSavepointITTest {

    private static final int NUM_KEYS = 20;
    private static final int MAX_PARALLELISM = 16;

    @Test
    void canonicalSavepointRoundTripWithRescaleOut(@TempDir Path tempDir) throws Exception {
        runRescaleTest(tempDir, 2, 4, false);
    }

    @Test
    void canonicalSavepointRoundTripWithRescaleIn(@TempDir Path tempDir) throws Exception {
        runRescaleTest(tempDir, 4, 2, false);
    }

    @Test
    void canonicalSavepointRestoresIntoRocksDBBackend(@TempDir Path tempDir) throws Exception {
        runRescaleTest(tempDir, 2, 2, true);
    }

    // =====================================================================================
    //  Test driver
    // =====================================================================================

    private void runRescaleTest(
            Path tempDir,
            int initialParallelism,
            int restoredParallelism,
            boolean useRocksDbForRestore)
            throws Exception {
        MiniClusterWithClientResource cluster =
                CobbleCheckpointingITSupport.createCluster(new Configuration());

        cluster.before();
        try {
            // Phase 1: submit job, write state, trigger canonical savepoint.
            String savepointPath =
                    triggerCanonicalSavepoint(
                            cluster.getMiniCluster(), tempDir, initialParallelism);

            // Phase 2: restore at new parallelism (or new backend) with a probe-only topology.
            restoreAndProbe(
                    cluster.getMiniCluster(),
                    tempDir,
                    savepointPath,
                    restoredParallelism,
                    useRocksDbForRestore);
        } finally {
            cluster.after();
        }
    }

    // =====================================================================================
    //  Phase 1: write state + trigger savepoint
    // =====================================================================================

    private String triggerCanonicalSavepoint(MiniCluster miniCluster, Path tempDir, int parallelism)
            throws Exception {
        BlockingKeySource.reset();
        CollectingSink.clear();

        JobGraph jobGraph =
                createWriteJobGraph(
                        tempDir.resolve("local-state-write"),
                        tempDir.resolve("checkpoints"),
                        parallelism);
        JobID jobId = jobGraph.getJobID();

        miniCluster.submitJob(jobGraph).get(30, TimeUnit.SECONDS);
        waitForAllTaskRunning(miniCluster, jobId, false);

        // Wait until all source subtasks have emitted their keys and written state.
        waitForCondition(
                () -> BlockingKeySource.finishedSubtasks() == parallelism,
                Duration.ofSeconds(30),
                "all source subtasks finished emitting");

        // Trigger a canonical savepoint. cancelJob=true cancels the job after the savepoint
        // completes — no additional cancel call needed.
        CompletableFuture<String> savepointFuture =
                miniCluster.triggerSavepoint(
                        jobId,
                        tempDir.resolve("savepoint").toString(),
                        true,
                        SavepointFormatType.CANONICAL);
        String savepointPath = savepointFuture.get(60, TimeUnit.SECONDS);
        assertNotNull(savepointPath, "savepoint path must not be null");

        // Wait for the job to reach terminal state (CANCELLED via triggerSavepoint).
        miniCluster.requestJobResult(jobId).get(60, TimeUnit.SECONDS);

        return savepointPath;
    }

    private JobGraph createWriteJobGraph(
            Path localStateDirectory, Path checkpointDirectory, int parallelism) {
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

        env.addSource(new BlockingKeySource(NUM_KEYS))
                .name("blocking-key-source")
                .uid("blocking-key-source")
                .keyBy(new IdentityKeySelector())
                .flatMap(new StateWriterFlatMapper())
                .name("state-writer")
                .uid("state-writer")
                .addSink(new DiscardingSink<>())
                .name("discard")
                .uid("discard");

        return env.getStreamGraph().getJobGraph();
    }

    // =====================================================================================
    //  Phase 2: probe-only restore
    // =====================================================================================

    private void restoreAndProbe(
            MiniCluster miniCluster,
            Path tempDir,
            String savepointPath,
            int restoredParallelism,
            boolean useRocksDbForRestore)
            throws Exception {
        CollectingSink.clear();

        JobGraph restoredJobGraph =
                createProbeJobGraph(
                        tempDir.resolve("local-state-restore"),
                        restoredParallelism,
                        useRocksDbForRestore);
        restoredJobGraph.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(savepointPath, false, RestoreMode.CLAIM));

        miniCluster.submitJob(restoredJobGraph).get(30, TimeUnit.SECONDS);
        miniCluster.requestJobResult(restoredJobGraph.getJobID()).get(120, TimeUnit.SECONDS);

        // Verify: every key should have been probed and output its state values.
        Map<Integer, Tuple2<Integer, Integer>> results = CollectingSink.snapshot();
        assertEquals(NUM_KEYS, results.size(), "all keys should be probed after restore");

        for (int key = 0; key < NUM_KEYS; key++) {
            Tuple2<Integer, Integer> state = results.get(key);
            assertNotNull(state, "missing probe result for key " + key);
            // ValueState: count was set to the key value by the write job.
            assertEquals(key, state.f0, "ValueState value mismatch for key " + key);
            // ListState: should contain one element (the key value).
            assertEquals(1, state.f1, "ListState size mismatch for key " + key);
        }
    }

    private JobGraph createProbeJobGraph(
            Path localStateDirectory, int parallelism, boolean useRocksDbForRestore)
            throws Exception {
        Configuration configuration =
                CobbleCheckpointingITSupport.createJobConfiguration(localStateDirectory);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);
        env.getConfig().setMaxParallelism(MAX_PARALLELISM);
        env.setRestartStrategy(RestartStrategies.noRestart());

        StateBackend stateBackend;
        if (useRocksDbForRestore) {
            // Restore into RocksDB instead of Cobble — proves canonical savepoint portability.
            stateBackend = new RocksDBStateBackend(localStateDirectory.resolve("rocksdb").toUri());
        } else {
            stateBackend =
                    new CobbleStateBackend().configure(configuration, getClass().getClassLoader());
        }
        env.setStateBackend(stateBackend);

        // Probe topology: emit each key once, read state (no writes), output values.
        env.addSource(new ProbeKeySource(NUM_KEYS))
                .name("probe-key-source")
                .uid("blocking-key-source") // same UID as write job for state mapping
                .keyBy(new IdentityKeySelector())
                .flatMap(new StateProbeFlatMapper())
                .name("state-writer") // same UID as write job for state mapping
                .uid("state-writer")
                .addSink(new CollectingSink())
                .name("collecting-sink")
                .uid("discard"); // same UID as write job for state mapping

        return env.getStreamGraph().getJobGraph();
    }

    // =====================================================================================
    //  Utilities
    // =====================================================================================

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

    @FunctionalInterface
    private interface CheckedBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }

    // =====================================================================================
    //  Source / operators
    // =====================================================================================

    /**
     * A source that emits each key once (partitioned by subtask), then blocks on a latch. The job
     * stays running until the savepoint cancels it.
     */
    private static final class BlockingKeySource extends RichParallelSourceFunction<Integer> {
        private static final AtomicInteger FINISHED_SUBTASKS = new AtomicInteger();

        private final int numberOfKeys;
        private volatile boolean running = true;

        BlockingKeySource(int numberOfKeys) {
            this.numberOfKeys = numberOfKeys;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();

            for (int key = subtaskIndex; key < numberOfKeys; key += parallelism) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(key);
                }
            }

            FINISHED_SUBTASKS.incrementAndGet();
            // Block until the savepoint cancels the job.
            while (running) {
                Thread.sleep(10L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        static void reset() {
            FINISHED_SUBTASKS.set(0);
        }

        static int finishedSubtasks() {
            return FINISHED_SUBTASKS.get();
        }
    }

    /**
     * A source for the probe job that emits each key once and terminates. It uses the same UID as
     * the write job's source so that operator state is correctly mapped during restore.
     */
    private static final class ProbeKeySource extends RichParallelSourceFunction<Integer> {
        private final int numberOfKeys;
        private volatile boolean running = true;

        ProbeKeySource(int numberOfKeys) {
            this.numberOfKeys = numberOfKeys;
        }

        @Override
        public void run(SourceContext<Integer> ctx) {
            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();

            for (int key = subtaskIndex; key < numberOfKeys; key += parallelism) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(key);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * Writes ValueState (count) and ListState (append) for each key. Each key is emitted exactly
     * once by the source, so the final state is: count = key value, list size = 1.
     */
    private static final class StateWriterFlatMapper extends RichFlatMapFunction<Integer, Void> {
        private transient ValueState<Integer> count;
        private transient ListState<Integer> list;

        @Override
        public void open(Configuration parameters) throws Exception {
            count =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("count", Integer.class));
            list =
                    getRuntimeContext()
                            .getListState(new ListStateDescriptor<>("list", Integer.class));
        }

        @Override
        public void flatMap(Integer value, Collector<Void> out) throws Exception {
            count.update(value);
            list.add(value);
        }
    }

    /**
     * Probe-only operator: reads ValueState and ListState without writing. Outputs (key, count,
     * listSize) for verification.
     */
    private static final class StateProbeFlatMapper
            extends RichFlatMapFunction<Integer, Tuple2<Integer, Tuple2<Integer, Integer>>> {
        private transient ValueState<Integer> count;
        private transient ListState<Integer> list;

        @Override
        public void open(Configuration parameters) throws Exception {
            count =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("count", Integer.class));
            list =
                    getRuntimeContext()
                            .getListState(new ListStateDescriptor<>("list", Integer.class));
        }

        @Override
        public void flatMap(Integer value, Collector<Tuple2<Integer, Tuple2<Integer, Integer>>> out)
                throws Exception {
            Integer countVal = count.value();
            int listSize = 0;
            for (Integer ignored : list.get()) {
                listSize++;
            }
            out.collect(Tuple2.of(value, Tuple2.of(countVal == null ? 0 : countVal, listSize)));
        }
    }

    private static final class IdentityKeySelector implements KeySelector<Integer, Integer> {
        @Override
        public Integer getKey(Integer value) {
            return value;
        }
    }

    private static final class DiscardingSink<T> extends RichSinkFunction<T> {
        @Override
        public void invoke(T value) {}
    }

    private static final class CollectingSink
            extends RichSinkFunction<Tuple2<Integer, Tuple2<Integer, Integer>>> {
        private static final Map<Integer, Tuple2<Integer, Integer>> RESULTS =
                new ConcurrentHashMap<>();

        @Override
        public void invoke(Tuple2<Integer, Tuple2<Integer, Integer>> value) {
            RESULTS.put(value.f0, value.f1);
        }

        static void clear() {
            RESULTS.clear();
        }

        static Map<Integer, Tuple2<Integer, Integer>> snapshot() {
            return new java.util.HashMap<>(RESULTS);
        }
    }
}
