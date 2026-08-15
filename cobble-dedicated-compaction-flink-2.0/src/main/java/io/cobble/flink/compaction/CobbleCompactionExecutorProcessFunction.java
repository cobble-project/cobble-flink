package io.cobble.flink.compaction;

import io.cobble.DedicatedCompactionExecutor;
import io.cobble.flink.common.CobbleLoader;

import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;

import java.nio.file.Paths;

/** Executes queued compaction plans in parallel Flink 2.x subtasks. */
public final class CobbleCompactionExecutorProcessFunction
        implements OneInputStreamProcessFunction<CobbleCompactionPlan, String> {
    private static final long serialVersionUID = 1L;

    private final String configPath;
    private transient DedicatedCompactionExecutor executor;
    private transient CobbleCompactionMetrics.ExecutorMetrics metrics;

    public CobbleCompactionExecutorProcessFunction(String configPath) {
        if (configPath == null || configPath.trim().isEmpty()) {
            throw new IllegalArgumentException("configPath must not be blank");
        }
        this.configPath = configPath;
    }

    @Override
    public void open(NonPartitionedContext<String> context) {
        CobbleLoader.ensureCobbleLoaded();
        executor = DedicatedCompactionExecutor.open(Paths.get(configPath));
        metrics = new CobbleCompactionMetrics.ExecutorMetrics(context.getMetricGroup());
    }

    @Override
    public void processRecord(
            CobbleCompactionPlan plan,
            Collector<String> output,
            PartitionedContext<String> context) {
        long startedNanos = System.nanoTime();
        metrics.executionStarted();
        try {
            DedicatedCompactionExecutor.Outcome outcome = executor.execute(plan.toNativePlan());
            metrics.executionSucceeded(
                    outcome, CobbleCompactionMetrics.elapsedMillis(startedNanos));
            output.collect(outcome.name());
        } catch (RuntimeException | LinkageError error) {
            metrics.executionFailed(CobbleCompactionMetrics.elapsedMillis(startedNanos));
            throw error;
        } finally {
            metrics.executionFinished();
        }
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.close();
            executor = null;
        }
    }
}
