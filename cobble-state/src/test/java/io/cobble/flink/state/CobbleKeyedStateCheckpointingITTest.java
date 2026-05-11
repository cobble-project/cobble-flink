package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class CobbleKeyedStateCheckpointingITTest {
    private static final int NUM_STRINGS = 10_000;
    private static final int NUM_KEYS = 40;
    private static final int FAILURE_POSITION = 1_800;

    @Test
    void recoversKeyedStateExactlyOnceAfterFailure(@TempDir Path tempDir) throws Exception {
        resetSharedState();

        MiniClusterWithClientResource cluster =
                CobbleCheckpointingITSupport.createCluster(new Configuration());
        cluster.before();
        try {
            StreamExecutionEnvironment env =
                    CobbleCheckpointingITSupport.createEnvironment(tempDir.resolve("local-state"));
            env.enableCheckpointing(500L);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));
            Configuration checkpointConfiguration = new Configuration();
            checkpointConfiguration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            checkpointConfiguration.set(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                    tempDir.resolve("checkpoints").toUri().toString());
            env.configure(checkpointConfiguration);

            env.addSource(new IntGeneratingSourceFunction(NUM_STRINGS / 2, NUM_STRINGS / 4))
                    .name("source-one")
                    .uid("source-one")
                    .union(
                            env.addSource(
                                    new IntGeneratingSourceFunction(
                                            NUM_STRINGS / 2, NUM_STRINGS / 4)))
                    .keyBy(new IdentityKeySelector<>())
                    .map(new OnceFailingPartitionedSum(FAILURE_POSITION))
                    .name("sum")
                    .uid("sum")
                    .keyBy(value -> value.f0)
                    .addSink(new CounterSink())
                    .name("counter-sink")
                    .uid("counter-sink");

            org.apache.flink.runtime.jobgraph.JobGraph jobGraph =
                    env.getStreamGraph().getJobGraph();
            cluster.getClusterClient().submitJob(jobGraph).get(30, TimeUnit.SECONDS);
            cluster.getClusterClient()
                    .requestJobResult(jobGraph.getJobID())
                    .get(180, TimeUnit.SECONDS);
        } finally {
            cluster.after();
        }

        assertEquals(
                CobbleCheckpointingITSupport.PARALLELISM,
                OnceFailingPartitionedSum.RECOVERY_COUNTER.get());
        assertEquals(NUM_KEYS, OnceFailingPartitionedSum.ALL_SUMS.size());
        assertEquals(NUM_KEYS, CounterSink.ALL_COUNTS.size());

        for (Map.Entry<Integer, Long> sum : OnceFailingPartitionedSum.ALL_SUMS.entrySet()) {
            assertEquals(
                    sum.getKey().longValue() * NUM_STRINGS / NUM_KEYS, sum.getValue().longValue());
        }
        for (Long count : CounterSink.ALL_COUNTS.values()) {
            assertEquals(NUM_STRINGS / NUM_KEYS, count.longValue());
        }
    }

    private static void resetSharedState() {
        OnceFailingPartitionedSum.ALL_SUMS.clear();
        OnceFailingPartitionedSum.RECOVERY_COUNTER.set(0L);
        CounterSink.ALL_COUNTS.clear();
    }

    private static final class IntGeneratingSourceFunction
            extends RichParallelSourceFunction<Integer>
            implements ListCheckpointed<Integer>, CheckpointListener {
        private final int numElements;
        private final int checkpointLatestAt;
        private volatile boolean running = true;
        private int lastEmitted = -1;
        private boolean checkpointHappened;

        private IntGeneratingSourceFunction(int numElements, int checkpointLatestAt) {
            this.numElements = numElements;
            this.checkpointLatestAt = checkpointLatestAt;
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
            checkpointHappened = true;
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
        private static final Map<Integer, Long> ALL_SUMS = new ConcurrentHashMap<>();
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
            ALL_SUMS.put(value, currentSum);
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

    private static final class CounterSink extends RichSinkFunction<Tuple2<Integer, Long>> {
        private static final Map<Integer, Long> ALL_COUNTS = new ConcurrentHashMap<>();

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
