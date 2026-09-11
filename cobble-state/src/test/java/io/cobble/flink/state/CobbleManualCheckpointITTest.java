package io.cobble.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

class CobbleManualCheckpointITTest {

    @Test
    void manualCheckpointWorksWithoutPeriodicCheckpointingUsingJobManagerStorage(
            @TempDir Path tempDir) throws Exception {
        runManualCheckpointTest(tempDir, StorageMode.JOB_MANAGER, false);
    }

    @Test
    void manualCheckpointWorksWithoutPeriodicCheckpointingUsingFileSystemStorage(
            @TempDir Path tempDir) throws Exception {
        runManualCheckpointTest(tempDir, StorageMode.FILE_SYSTEM, false);
    }

    @Test
    void manualCheckpointWorksWithPeriodicCheckpointingUsingJobManagerStorage(@TempDir Path tempDir)
            throws Exception {
        runManualCheckpointTest(tempDir, StorageMode.JOB_MANAGER, true);
    }

    @Test
    void manualCheckpointWorksWithPeriodicCheckpointingUsingFileSystemStorage(@TempDir Path tempDir)
            throws Exception {
        runManualCheckpointTest(tempDir, StorageMode.FILE_SYSTEM, true);
    }

    private void runManualCheckpointTest(Path tempDir, StorageMode storageMode, boolean periodic)
            throws Exception {
        MiniClusterWithClientResource cluster =
                CobbleCheckpointingITSupport.createCluster(new Configuration());

        cluster.before();
        try {
            StreamExecutionEnvironment env =
                    CobbleCheckpointingITSupport.createEnvironment(tempDir.resolve("local-state"));
            env.setRestartStrategy(RestartStrategies.noRestart());
            if (periodic) {
                env.enableCheckpointing(500L);
            }
            configureCheckpointStorage(env, storageMode, tempDir.resolve("checkpoints"));

            env.addSource(new ContinuousIntegerSource())
                    .name("continuous-source")
                    .uid("continuous-source")
                    .keyBy(value -> value % CobbleCheckpointingITSupport.PARALLELISM)
                    .flatMap(new StatefulMapper())
                    .name("stateful-mapper")
                    .uid("stateful-mapper")
                    .addSink(new DiscardingSink<>());

            org.apache.flink.runtime.jobgraph.JobGraph jobGraph =
                    env.getStreamGraph().getJobGraph();
            CobbleCheckpointingITSupport.submitJobAndWaitForRunning(cluster, jobGraph);

            if (periodic) {
                CobbleCheckpointingITSupport.waitForCompletedCheckpointCount(
                        cluster.getMiniCluster(), jobGraph.getJobID(), 1, Duration.ofSeconds(30));
            }

            CobbleCheckpointingITSupport.triggerCheckpointAndWait(
                    cluster.getMiniCluster(), jobGraph.getJobID());
            CobbleCheckpointingITSupport.cancelJobAndWait(cluster, jobGraph.getJobID());
        } finally {
            cluster.after();
        }
    }

    private void configureCheckpointStorage(
            StreamExecutionEnvironment env, StorageMode storageMode, Path checkpointDirectory)
            throws Exception {
        Configuration configuration = new Configuration();
        switch (storageMode) {
            case JOB_MANAGER:
                configuration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");
                env.configure(configuration);
                return;
            case FILE_SYSTEM:
                configuration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
                configuration.set(
                        CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                        checkpointDirectory.toUri().toString());
                env.configure(configuration);
                return;
        }
        throw new IllegalArgumentException("Unsupported storage mode: " + storageMode);
    }

    private enum StorageMode {
        JOB_MANAGER,
        FILE_SYSTEM
    }

    private static final class ContinuousIntegerSource extends RichParallelSourceFunction<Integer> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            int nextValue = getRuntimeContext().getIndexOfThisSubtask();
            int step = getRuntimeContext().getNumberOfParallelSubtasks();
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

    private static final class StatefulMapper extends RichFlatMapFunction<Integer, Long> {
        private transient ValueState<Long> count;

        @Override
        public void open(Configuration parameters) throws Exception {
            count =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "count", BasicTypeInfo.LONG_TYPE_INFO));
        }

        @Override
        public void flatMap(Integer value, Collector<Long> out) throws Exception {
            long updated = (count.value() == null ? 0L : count.value()) + value;
            count.update(updated);
            out.collect(updated);
        }
    }
}
