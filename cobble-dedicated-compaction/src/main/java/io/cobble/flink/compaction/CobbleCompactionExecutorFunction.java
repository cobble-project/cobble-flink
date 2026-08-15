package io.cobble.flink.compaction;

import io.cobble.DedicatedCompactionExecutor;
import io.cobble.flink.common.CobbleLoader;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.nio.file.Paths;

/** Executes queued compaction plans in parallel Flink 1.x subtasks. */
public final class CobbleCompactionExecutorFunction
        extends RichMapFunction<CobbleCompactionPlan, String> {
    private static final long serialVersionUID = 1L;

    private final String configPath;
    private transient DedicatedCompactionExecutor executor;
    private transient CobbleCompactionMetrics.ExecutorMetrics metrics;

    public CobbleCompactionExecutorFunction(String configPath) {
        if (configPath == null || configPath.trim().isEmpty()) {
            throw new IllegalArgumentException("configPath must not be blank");
        }
        this.configPath = configPath;
    }

    @Override
    public void open(Configuration parameters) {
        CobbleLoader.ensureCobbleLoaded();
        executor = DedicatedCompactionExecutor.open(Paths.get(configPath));
        metrics = new CobbleCompactionMetrics.ExecutorMetrics(getRuntimeContext().getMetricGroup());
    }

    @Override
    public String map(CobbleCompactionPlan plan) {
        long startedNanos = System.nanoTime();
        metrics.executionStarted();
        try {
            DedicatedCompactionExecutor.Outcome outcome = executor.execute(plan.toNativePlan());
            metrics.executionSucceeded(
                    outcome, CobbleCompactionMetrics.elapsedMillis(startedNanos));
            return outcome.name();
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
