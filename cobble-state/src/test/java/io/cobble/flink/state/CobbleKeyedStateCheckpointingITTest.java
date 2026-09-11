package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

class CobbleKeyedStateCheckpointingITTest {
    private static final int NUM_STRINGS = 10_000;
    private static final int NUM_KEYS = 40;
    private static final int SEQUENTIAL_FAILURES = 3;
    private static final Duration STEP_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration LOCAL_STATE_CLEANUP_TIMEOUT = Duration.ofSeconds(30);
    private static final Map<String, TestControl> CONTROLS = new ConcurrentHashMap<>();

    @Test
    void recoversKeyedStateExactlyOnceAfterFailure(@TempDir Path tempDir) throws Exception {
        runControlledFailoverTest(tempDir, 1);
    }

    @Test
    void recoversAllStatefulSubtasksAcrossThreeSequentialFailures(@TempDir Path tempDir)
            throws Exception {
        runControlledFailoverTest(tempDir, SEQUENTIAL_FAILURES);
    }

    private void runControlledFailoverTest(Path tempDir, int numberOfFailures) throws Exception {
        String controlId = UUID.randomUUID().toString();
        TestControl control = new TestControl(CobbleCheckpointingITSupport.PARALLELISM);
        CONTROLS.put(controlId, control);

        Path localStateRoot = tempDir.resolve("local-state");
        MiniClusterWithClientResource cluster =
                CobbleCheckpointingITSupport.createCluster(new Configuration());
        cluster.before();
        try {
            StreamExecutionEnvironment env =
                    CobbleCheckpointingITSupport.createEnvironment(localStateRoot);
            configureCheckpointing(env, tempDir);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(numberOfFailures, 0L));

            JobGraph jobGraph = createJobGraph(env, controlId, numberOfFailures + 1);
            JobID jobId = jobGraph.getJobID();
            CobbleCheckpointingITSupport.submitJobAndWaitForRunning(cluster, jobGraph);

            waitForSourcePhase(cluster, jobId, control, 0);
            CobbleCheckpointingITSupport.triggerCheckpointAndWait(cluster.getMiniCluster(), jobId);

            for (int failure = 1; failure <= numberOfFailures; failure++) {
                control.releasePhaseAndRequestFailure(failure);
                waitForFailure(cluster, jobId, control, failure);
                waitForRecovery(cluster, jobId, control, failure);
                CobbleCheckpointingITSupport.waitForAllTasksRunning(
                        cluster.getMiniCluster(), jobId, STEP_TIMEOUT);
                waitForSourcePhase(cluster, jobId, control, failure);
                CobbleCheckpointingITSupport.triggerCheckpointAndWait(
                        cluster.getMiniCluster(), jobId);
            }

            control.finishSources();
            waitForSourceFunctionsFinished(cluster, jobId, control);
            CobbleCheckpointingITSupport.triggerCheckpointAndWait(cluster.getMiniCluster(), jobId);
            CobbleCheckpointingITSupport.waitForJobFinished(cluster, jobId);
        } finally {
            try {
                cluster.after();
            } finally {
                CONTROLS.remove(controlId);
            }
        }

        assertEquals(numberOfFailures, control.observedFailures());
        assertEquals(
                Collections.nCopies(numberOfFailures, 0), new ArrayList<>(control.failingSubtasks));
        assertRestoredOnEverySubtask(control.mapRecoveries, numberOfFailures);
        assertRestoredOnEverySubtask(control.sinkRecoveries, numberOfFailures);
        assertFinalSumsAndCounts(control);
        assertLocalStateRootIsEmpty(localStateRoot);
    }

    private static void configureCheckpointing(StreamExecutionEnvironment env, Path tempDir) {
        // Checkpointing must be enabled for manual triggers. The long interval prevents an
        // unsolicited periodic checkpoint from changing the test's recovery point.
        env.enableCheckpointing(Duration.ofHours(1).toMillis());
        Configuration checkpointConfiguration = new Configuration();
        checkpointConfiguration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        checkpointConfiguration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                tempDir.resolve("checkpoints").toUri().toString());
        env.configure(checkpointConfiguration);
    }

    private static JobGraph createJobGraph(
            StreamExecutionEnvironment env, String controlId, int phaseCount) {
        env.addSource(new ControlledIntegerSource(controlId, NUM_STRINGS, phaseCount))
                .name("controlled-source")
                .uid("controlled-source")
                .keyBy(new IdentityKeySelector<>())
                .map(new ControlledFailingPartitionedSum(controlId))
                .name("sum")
                .uid("sum")
                .keyBy(value -> value.f0)
                .addSink(new CounterSink(controlId))
                .name("counter-sink")
                .uid("counter-sink");
        return env.getStreamGraph().getJobGraph();
    }

    private static void waitForSourcePhase(
            MiniClusterWithClientResource cluster,
            JobID jobId,
            TestControl control,
            int expectedPhase)
            throws Exception {
        CobbleCheckpointingITSupport.waitForJobCondition(
                cluster.getMiniCluster(),
                jobId,
                () -> control.allSourcesReached(expectedPhase),
                STEP_TIMEOUT,
                "all source subtasks to reach phase " + expectedPhase);
    }

    private static void waitForFailure(
            MiniClusterWithClientResource cluster,
            JobID jobId,
            TestControl control,
            int expectedFailures)
            throws Exception {
        CobbleCheckpointingITSupport.waitForJobCondition(
                cluster.getMiniCluster(),
                jobId,
                () -> control.observedFailures() == expectedFailures,
                STEP_TIMEOUT,
                "controlled failure " + expectedFailures);
    }

    private static void waitForRecovery(
            MiniClusterWithClientResource cluster,
            JobID jobId,
            TestControl control,
            int expectedRecoveries)
            throws Exception {
        CobbleCheckpointingITSupport.waitForJobCondition(
                cluster.getMiniCluster(),
                jobId,
                () ->
                        restoredOnEverySubtask(control.mapRecoveries, expectedRecoveries)
                                && restoredOnEverySubtask(
                                        control.sinkRecoveries, expectedRecoveries),
                STEP_TIMEOUT,
                "all stateful subtasks to restore for attempt " + expectedRecoveries);
    }

    private static void waitForSourceFunctionsFinished(
            MiniClusterWithClientResource cluster, JobID jobId, TestControl control)
            throws Exception {
        CobbleCheckpointingITSupport.waitForJobCondition(
                cluster.getMiniCluster(),
                jobId,
                control::allSourceFunctionsFinished,
                STEP_TIMEOUT,
                "all controlled source functions to finish");
    }

    private static boolean restoredOnEverySubtask(
            Map<Integer, AtomicInteger> recoveriesBySubtask, int expectedRestarts) {
        if (recoveriesBySubtask.size() != CobbleCheckpointingITSupport.PARALLELISM) {
            return false;
        }
        for (int subtask = 0; subtask < CobbleCheckpointingITSupport.PARALLELISM; subtask++) {
            AtomicInteger recoveries = recoveriesBySubtask.get(subtask);
            if (recoveries == null || recoveries.get() < expectedRestarts) {
                return false;
            }
        }
        return true;
    }

    private static void assertFinalSumsAndCounts(TestControl control) {
        assertEquals(NUM_KEYS, control.finalSums.size());
        assertEquals(NUM_KEYS, control.finalCounts.size());

        for (Map.Entry<Integer, Long> sum : control.finalSums.entrySet()) {
            assertEquals(
                    sum.getKey().longValue() * NUM_STRINGS / NUM_KEYS, sum.getValue().longValue());
        }
        for (Long count : control.finalCounts.values()) {
            assertEquals(NUM_STRINGS / NUM_KEYS, count.longValue());
        }
    }

    private static void assertRestoredOnEverySubtask(
            Map<Integer, AtomicInteger> recoveriesBySubtask, int expectedRestarts) {
        assertTrue(
                restoredOnEverySubtask(recoveriesBySubtask, expectedRestarts),
                "not every stateful subtask restored " + expectedRestarts + " times");
        recoveriesBySubtask.forEach(
                (subtask, recoveries) ->
                        assertEquals(expectedRestarts, recoveries.get(), "subtask " + subtask));
    }

    private static TestControl control(String controlId) {
        TestControl control = CONTROLS.get(controlId);
        if (control == null) {
            throw new IllegalStateException("Missing test control " + controlId);
        }
        return control;
    }

    private static final class TestControl {
        private final AtomicInteger requestedFailures = new AtomicInteger();
        private final AtomicInteger observedFailures = new AtomicInteger();
        private final AtomicInteger attempt = new AtomicInteger();
        private final AtomicIntegerArray sourceAttempts;
        private final AtomicIntegerArray sourcePhases;
        private final AtomicIntegerArray finishedSourceAttempts;
        private final ConcurrentLinkedQueue<Integer> failingSubtasks =
                new ConcurrentLinkedQueue<>();
        private final Map<Integer, AtomicInteger> mapRecoveries = new ConcurrentHashMap<>();
        private final Map<Integer, AtomicInteger> sinkRecoveries = new ConcurrentHashMap<>();
        private final Map<Integer, Long> finalSums = new ConcurrentHashMap<>();
        private final Map<Integer, Long> finalCounts = new ConcurrentHashMap<>();

        private int allowedPhase;
        private boolean sourcesMayFinish;

        private TestControl(int sourceParallelism) {
            this.sourceAttempts = new AtomicIntegerArray(sourceParallelism);
            this.sourcePhases = new AtomicIntegerArray(sourceParallelism);
            this.finishedSourceAttempts = new AtomicIntegerArray(sourceParallelism);
            for (int subtask = 0; subtask < sourceParallelism; subtask++) {
                sourceAttempts.set(subtask, -1);
                sourcePhases.set(subtask, -1);
                finishedSourceAttempts.set(subtask, -1);
            }
        }

        private synchronized void releasePhaseAndRequestFailure(int phase) {
            requestedFailures.set(phase);
            allowedPhase = phase;
            notifyAll();
        }

        private synchronized boolean observeFailure(int subtask) {
            int requested = requestedFailures.get();
            if (requested != observedFailures.get() + 1) {
                return false;
            }
            observedFailures.incrementAndGet();
            failingSubtasks.add(subtask);
            attempt.incrementAndGet();
            notifyAll();
            return true;
        }

        private synchronized boolean awaitPhase(
                int phase, int sourceAttempt, BooleanSupplier sourceRunning)
                throws InterruptedException {
            while (sourceRunning.getAsBoolean()
                    && !sourcesMayFinish
                    && attempt.get() == sourceAttempt
                    && allowedPhase < phase) {
                wait();
            }
            return sourceRunning.getAsBoolean()
                    && !sourcesMayFinish
                    && attempt.get() == sourceAttempt;
        }

        private void markSourcePhase(int subtask, int sourceAttempt, int phase) {
            if (sourceAttempt == attempt.get()) {
                sourceAttempts.set(subtask, sourceAttempt);
                sourcePhases.accumulateAndGet(subtask, phase, Math::max);
            }
        }

        private boolean allSourcesReached(int phase) {
            int currentAttempt = attempt.get();
            for (int subtask = 0; subtask < sourcePhases.length(); subtask++) {
                if (sourceAttempts.get(subtask) != currentAttempt
                        || sourcePhases.get(subtask) < phase) {
                    return false;
                }
            }
            return true;
        }

        private void markSourceFinished(int subtask, int sourceAttempt) {
            if (sourceAttempt == attempt.get()) {
                finishedSourceAttempts.set(subtask, sourceAttempt);
            }
        }

        private boolean allSourceFunctionsFinished() {
            int currentAttempt = attempt.get();
            for (int subtask = 0; subtask < finishedSourceAttempts.length(); subtask++) {
                if (finishedSourceAttempts.get(subtask) != currentAttempt) {
                    return false;
                }
            }
            return true;
        }

        private synchronized void finishSources() {
            sourcesMayFinish = true;
            notifyAll();
        }

        private synchronized void wakeSources() {
            notifyAll();
        }

        private int observedFailures() {
            return observedFailures.get();
        }
    }

    private static final class ControlledIntegerSource extends RichParallelSourceFunction<Integer>
            implements ListCheckpointed<Integer> {
        private final String controlId;
        private final int numElements;
        private final int phaseCount;
        private volatile boolean running = true;
        private int lastEmitted = -1;
        private transient int sourceAttempt;

        private ControlledIntegerSource(String controlId, int numElements, int phaseCount) {
            this.controlId = controlId;
            this.numElements = numElements;
            this.phaseCount = phaseCount;
        }

        @Override
        public void open(Configuration parameters) {
            sourceAttempt = control(controlId).attempt.get();
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            TestControl control = control(controlId);
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            int step = getRuntimeContext().getNumberOfParallelSubtasks();
            int nextElement = lastEmitted >= 0 ? lastEmitted + step : subtask;

            while (running && nextElement < numElements) {
                int phase =
                        (int)
                                Math.min(
                                        (long) nextElement * phaseCount / numElements,
                                        phaseCount - 1);
                if (phase > 0) {
                    control.markSourcePhase(subtask, sourceAttempt, phase - 1);
                }
                if (!control.awaitPhase(phase, sourceAttempt, () -> running)) {
                    return;
                }
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(nextElement % NUM_KEYS);
                    lastEmitted = nextElement;
                }
                nextElement += step;
            }

            control.markSourcePhase(subtask, sourceAttempt, phaseCount - 1);
            while (running && control.awaitPhase(phaseCount, sourceAttempt, () -> running)) {
                // The test releases source completion after the final recovery checkpoint.
            }
            control.markSourceFinished(subtask, sourceAttempt);
        }

        @Override
        public void cancel() {
            running = false;
            TestControl control = CONTROLS.get(controlId);
            if (control != null) {
                control.wakeSources();
            }
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) {
            return Collections.singletonList(lastEmitted);
        }

        @Override
        public void restoreState(List<Integer> state) {
            assertEquals(1, state.size());
            lastEmitted = state.get(0);
        }
    }

    private static final class ControlledFailingPartitionedSum
            extends RichMapFunction<Integer, Tuple2<Integer, Long>>
            implements ListCheckpointed<Integer> {
        private final String controlId;
        private transient ValueState<Long> sum;

        private ControlledFailingPartitionedSum(String controlId) {
            this.controlId = controlId;
        }

        @Override
        public void open(Configuration parameters) throws IOException {
            sum = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Long.class));
        }

        @Override
        public Tuple2<Integer, Long> map(Integer value) throws Exception {
            TestControl control = control(controlId);
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            if (subtask == 0 && control.observeFailure(subtask)) {
                throw new Exception(
                        "intentional controlled test failure " + control.observedFailures());
            }

            Long oldSum = sum.value();
            long currentSum = (oldSum == null ? 0L : oldSum) + value;
            sum.update(currentSum);
            control.finalSums.put(value, currentSum);
            return Tuple2.of(value, currentSum);
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) {
            return Collections.emptyList();
        }

        @Override
        public void restoreState(List<Integer> state) {
            assertTrue(state.isEmpty());
            TestControl control = control(controlId);
            control.mapRecoveries
                    .computeIfAbsent(
                            getRuntimeContext().getIndexOfThisSubtask(),
                            ignored -> new AtomicInteger())
                    .incrementAndGet();
        }
    }

    private static final class CounterSink extends RichSinkFunction<Tuple2<Integer, Long>>
            implements ListCheckpointed<Integer> {
        private final String controlId;
        private transient ValueState<NonSerializableLong> countStateA;
        private transient ValueState<Long> countStateB;

        private CounterSink(String controlId) {
            this.controlId = controlId;
        }

        @Override
        public void open(Configuration parameters) throws IOException {
            countStateA =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("a", NonSerializableLong.class));
            countStateB = getRuntimeContext().getState(new ValueStateDescriptor<>("b", Long.class));
        }

        @Override
        public void invoke(Tuple2<Integer, Long> value) throws Exception {
            NonSerializableLong currentA = countStateA.value();
            Long currentB = countStateB.value();
            long countA = currentA == null ? 0L : currentA.value;
            long countB = currentB == null ? 0L : currentB;
            assertEquals(countA, countB);

            long updated = countA + 1L;
            countStateA.update(NonSerializableLong.of(updated));
            countStateB.update(updated);
            control(controlId).finalCounts.put(value.f0, updated);
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) {
            return Collections.emptyList();
        }

        @Override
        public void restoreState(List<Integer> state) {
            assertTrue(state.isEmpty());
            TestControl control = control(controlId);
            control.sinkRecoveries
                    .computeIfAbsent(
                            getRuntimeContext().getIndexOfThisSubtask(),
                            ignored -> new AtomicInteger())
                    .incrementAndGet();
        }
    }

    private static void assertLocalStateRootIsEmpty(Path localStateRoot) throws Exception {
        long deadline = System.nanoTime() + LOCAL_STATE_CLEANUP_TIMEOUT.toNanos();
        List<Path> leftovers;
        do {
            leftovers = localStateChildren(localStateRoot);
            if (leftovers.isEmpty()) {
                return;
            }
            Thread.sleep(100L);
        } while (System.nanoTime() < deadline);

        assertEquals(Collections.emptyList(), leftovers, "attempt-local Cobble DB files remain");
    }

    private static List<Path> localStateChildren(Path localStateRoot) throws IOException {
        if (!Files.exists(localStateRoot)) {
            return Collections.emptyList();
        }
        try (Stream<Path> paths = Files.walk(localStateRoot)) {
            List<Path> children = new ArrayList<>();
            paths.filter(path -> !path.equals(localStateRoot)).forEach(children::add);
            return children;
        }
    }

    private static final class IdentityKeySelector<T> implements KeySelector<T, T> {
        @Override
        public T getKey(T value) {
            return value;
        }
    }

    private static final class NonSerializableLong {
        private final long value;

        private NonSerializableLong(long value) {
            this.value = value;
        }

        private static NonSerializableLong of(long value) {
            return new NonSerializableLong(value);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj
                    || (obj instanceof NonSerializableLong
                            && ((NonSerializableLong) obj).value == value);
        }

        @Override
        public int hashCode() {
            return Long.hashCode(value);
        }
    }
}
