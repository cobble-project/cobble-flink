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
    }

    @Override
    public String map(CobbleCompactionPlan plan) {
        DedicatedCompactionExecutor.Outcome outcome = executor.execute(plan.toNativePlan());
        return outcome.name();
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.close();
            executor = null;
        }
    }
}
