package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

class CobbleKeyedStateCheckpointingITTest {
    private static final int NUM_STRINGS = 10_000;
    private static final int NUM_KEYS = 40;
    private static final int FAILURE_POSITION = 1_800;
    private static final int SEQUENTIAL_FAILURES = 3;
    private static final Duration JOB_TIMEOUT = Duration.ofSeconds(180);
    private static final Duration LOCAL_STATE_CLEANUP_TIMEOUT = Duration.ofSeconds(30);
    private static final Map<Integer, Long> FINAL_SUMS = new ConcurrentHashMap<>();

    @Test
    void recoversKeyedStateExactlyOnceAfterFailure(@TempDir Path tempDir) throws Exception {
        resetSharedState();

        MiniClusterWithClientResource cluster =
                CobbleCheckpointingITSupport.createCluster(new Configuration());
        cluster.before();
        try {
            StreamExecutionEnvironment env =
                    CobbleCheckpointingITSupport.createEnvironment(tempDir.resolve("local-state"));
            configureCheckpointing(env, tempDir);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));

            submitAndAwait(
                    cluster,
                    createJobGraph(env, false, new OnceFailingPartitionedSum(FAILURE_POSITION)));
        } finally {
            cluster.after();
        }

        assertEquals(
                CobbleCheckpointingITSupport.PARALLELISM,
                OnceFailingPartitionedSum.RECOVERY_COUNTER.get());
        assertFinalSumsAndCounts();
    }

    @Test
    void recoversAllStatefulSubtasksAcrossThreeSequentialFailures(@TempDir Path tempDir)
            throws Exception {
        resetSharedState();

        Path localStateRoot = tempDir.resolve("local-state");
        MiniClusterWithClientResource cluster =
                CobbleCheckpointingITSupport.createCluster(new Configuration());
        cluster.before();
        try {
            StreamExecutionEnvironment env =
                    CobbleCheckpointingITSupport.createEnvironment(localStateRoot);
            configureCheckpointing(env, tempDir);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(SEQUENTIAL_FAILURES, 0L));

            submitAndAwait(
                    cluster, createJobGraph(env, true, new ThreeTimesFailingPartitionedSum()));
        } finally {
            cluster.after();
        }

        assertEquals(SEQUENTIAL_FAILURES, ThreeTimesFailingPartitionedSum.FAILURES.get());
        assertEquals(
                Collections.nCopies(SEQUENTIAL_FAILURES, 0),
                new ArrayList<>(ThreeTimesFailingPartitionedSum.FAILING_SUBTASKS));
        assertRestoredOnEverySubtask(
                ThreeTimesFailingPartitionedSum.RECOVERIES_BY_SUBTASK, SEQUENTIAL_FAILURES);
        assertRestoredOnEverySubtask(CounterSink.RECOVERIES_BY_SUBTASK, SEQUENTIAL_FAILURES);
        assertFinalSumsAndCounts();
        assertLocalStateRootIsEmpty(localStateRoot);
    }

    private static void configureCheckpointing(StreamExecutionEnvironment env, Path tempDir) {
        env.enableCheckpointing(500L);
        Configuration checkpointConfiguration = new Configuration();
        checkpointConfiguration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        checkpointConfiguration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                tempDir.resolve("checkpoints").toUri().toString());
        env.configure(checkpointConfiguration);
    }

    private static JobGraph createJobGraph(
            StreamExecutionEnvironment env,
            boolean waitForCheckpointAfterRestore,
            RichMapFunction<Integer, Tuple2<Integer, Long>> sumFunction) {
        env.addSource(
                        new IntGeneratingSourceFunction(
                                NUM_STRINGS / 2, NUM_STRINGS / 4, waitForCheckpointAfterRestore))
                .name("source-one")
                .uid("source-one")
                .union(
                        env.addSource(
                                new IntGeneratingSourceFunction(
                                        NUM_STRINGS / 2,
                                        NUM_STRINGS / 4,
                                        waitForCheckpointAfterRestore)))
                .keyBy(new IdentityKeySelector<>())
                .map(sumFunction)
                .name("sum")
                .uid("sum")
                .keyBy(value -> value.f0)
                .addSink(new CounterSink())
                .name("counter-sink")
                .uid("counter-sink");
        return env.getStreamGraph().getJobGraph();
    }

    private static void submitAndAwait(MiniClusterWithClientResource cluster, JobGraph jobGraph)
            throws Exception {
        cluster.getClusterClient().submitJob(jobGraph).get(30, TimeUnit.SECONDS);
        cluster.getClusterClient()
                .requestJobResult(jobGraph.getJobID())
                .get(JOB_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }

    private static void assertFinalSumsAndCounts() {
        assertEquals(NUM_KEYS, FINAL_SUMS.size());
        assertEquals(NUM_KEYS, CounterSink.ALL_COUNTS.size());

        for (Map.Entry<Integer, Long> sum : FINAL_SUMS.entrySet()) {
            assertEquals(
                    sum.getKey().longValue() * NUM_STRINGS / NUM_KEYS, sum.getValue().longValue());
        }
        for (Long count : CounterSink.ALL_COUNTS.values()) {
            assertEquals(NUM_STRINGS / NUM_KEYS, count.longValue());
        }
    }

    private static void assertRestoredOnEverySubtask(
            Map<Integer, AtomicInteger> recoveriesBySubtask, int expectedRestarts) {
        assertEquals(CobbleCheckpointingITSupport.PARALLELISM, recoveriesBySubtask.size());
        for (int subtask = 0; subtask < CobbleCheckpointingITSupport.PARALLELISM; subtask++) {
            AtomicInteger recoveries = recoveriesBySubtask.get(subtask);
            assertTrue(recoveries != null, "subtask " + subtask + " was not restored");
            assertEquals(expectedRestarts, recoveries.get(), "subtask " + subtask);
        }
    }

    private static void resetSharedState() {
        FINAL_SUMS.clear();
        OnceFailingPartitionedSum.RECOVERY_COUNTER.set(0L);
        ThreeTimesFailingPartitionedSum.FAILURES.set(0);
        ThreeTimesFailingPartitionedSum.FAILING_SUBTASKS.clear();
        ThreeTimesFailingPartitionedSum.RECOVERIES_BY_SUBTASK.clear();
        CounterSink.ALL_COUNTS.clear();
        CounterSink.RECOVERIES_BY_SUBTASK.clear();
    }

    private static final class IntGeneratingSourceFunction
            extends RichParallelSourceFunction<Integer>
            implements ListCheckpointed<Integer>, CheckpointListener {
        private final int numElements;
        private final int checkpointLatestAt;
        private final boolean waitForCheckpointAfterRestore;
        private volatile boolean running = true;
        private int lastEmitted = -1;
        private boolean checkpointHappened;

        private IntGeneratingSourceFunction(
                int numElements, int checkpointLatestAt, boolean waitForCheckpointAfterRestore) {
            this.numElements = numElements;
            this.checkpointLatestAt = checkpointLatestAt;
            this.waitForCheckpointAfterRestore = waitForCheckpointAfterRestore;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            Object checkpointLock = ctx.getCheckpointLock();
            int step = getRuntimeContext().getNumberOfParallelSubtasks();
            int nextElement =
                    lastEmitted >= 0
                            ? lastEmitted + step
                            : getRuntimeContext().getIndexOfThisSubtask();

            while (running && nextElement < numElements) {
                if (!checkpointHappened) {
                    if (nextElement < checkpointLatestAt) {
                        Thread.sleep(1L);
                    } else {
                        synchronized (this) {
                            while (!checkpointHappened) {
                                wait();
                            }
                        }
                    }
                }

                synchronized (checkpointLock) {
                    ctx.collect(nextElement % NUM_KEYS);
                    lastEmitted = nextElement;
                }
                nextElement += step;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) {
            return Collections.singletonList(lastEmitted);
        }

        @Override
        public void restoreState(List<Integer> state) {
            assertEquals(1, state.size());
            lastEmitted = state.get(0);
            checkpointHappened = !waitForCheckpointAfterRestore;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            synchronized (this) {
                checkpointHappened = true;
                notifyAll();
            }
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}
    }

    private static final class OnceFailingPartitionedSum
            extends RichMapFunction<Integer, Tuple2<Integer, Long>>
            implements ListCheckpointed<Integer> {
        private static final AtomicLong RECOVERY_COUNTER = new AtomicLong();

        private final int failurePosition;
        private int count;
        private boolean shouldFail = true;
        private transient ValueState<Long> sum;

        private OnceFailingPartitionedSum(int failurePosition) {
            this.failurePosition = failurePosition;
        }

        @Override
        public void open(Configuration parameters) throws IOException {
            sum = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Long.class));
        }

        @Override
        public Tuple2<Integer, Long> map(Integer value) throws Exception {
            if (shouldFail && count++ >= failurePosition) {
                shouldFail = false;
                throw new Exception("intentional test failure");
            }

            Long oldSum = sum.value();
            long currentSum = (oldSum == null ? 0L : oldSum) + value;
            sum.update(currentSum);
            FINAL_SUMS.put(value, currentSum);
            return Tuple2.of(value, currentSum);
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) {
            assertEquals(1, state.size());
            RECOVERY_COUNTER.incrementAndGet();
            count = state.get(0);
            shouldFail = false;
        }
    }

    private static final class ThreeTimesFailingPartitionedSum
            extends RichMapFunction<Integer, Tuple2<Integer, Long>>
            implements ListCheckpointed<Integer>, CheckpointListener {
        private static final AtomicInteger FAILURES = new AtomicInteger();
        private static final ConcurrentLinkedQueue<Integer> FAILING_SUBTASKS =
                new ConcurrentLinkedQueue<>();
        private static final Map<Integer, AtomicInteger> RECOVERIES_BY_SUBTASK =
                new ConcurrentHashMap<>();

        private transient ValueState<Long> sum;
        private boolean checkpointCompleted;
        private boolean failedThisAttempt;

        @Override
        public void open(Configuration parameters) throws IOException {
            sum = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Long.class));
        }

        @Override
        public Tuple2<Integer, Long> map(Integer value) throws Exception {
            if (getRuntimeContext().getIndexOfThisSubtask() == 0
                    && checkpointCompleted
                    && !failedThisAttempt
                    && FAILURES.get() < SEQUENTIAL_FAILURES) {
                failedThisAttempt = true;
                int failure = FAILURES.incrementAndGet();
                if (failure <= SEQUENTIAL_FAILURES) {
                    FAILING_SUBTASKS.add(0);
                    throw new Exception("intentional sequential test failure " + failure);
                }
            }

            Long oldSum = sum.value();
            long currentSum = (oldSum == null ? 0L : oldSum) + value;
            sum.update(currentSum);
            FINAL_SUMS.put(value, currentSum);
            return Tuple2.of(value, currentSum);
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) {
            return Collections.emptyList();
        }

        @Override
        public void restoreState(List<Integer> state) {
            assertTrue(state.isEmpty());
            RECOVERIES_BY_SUBTASK
                    .computeIfAbsent(
                            getRuntimeContext().getIndexOfThisSubtask(),
                            ignored -> new AtomicInteger())
                    .incrementAndGet();
            checkpointCompleted = false;
            failedThisAttempt = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            checkpointCompleted = true;
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}
    }

    private static final class CounterSink extends RichSinkFunction<Tuple2<Integer, Long>>
            implements ListCheckpointed<Integer> {
        private static final Map<Integer, Long> ALL_COUNTS = new ConcurrentHashMap<>();
        private static final Map<Integer, AtomicInteger> RECOVERIES_BY_SUBTASK =
                new ConcurrentHashMap<>();

        private transient ValueState<NonSerializableLong> countStateA;
        private transient ValueState<Long> countStateB;

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
            ALL_COUNTS.put(value.f0, updated);
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) {
            return Collections.emptyList();
        }

        @Override
        public void restoreState(List<Integer> state) {
            assertTrue(state.isEmpty());
            RECOVERIES_BY_SUBTASK
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
