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
    }

    @Override
    public void processRecord(
            CobbleCompactionPlan plan,
            Collector<String> output,
            PartitionedContext<String> context) {
        DedicatedCompactionExecutor.Outcome outcome = executor.execute(plan.toNativePlan());
        output.collect(outcome.name());
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.close();
            executor = null;
        }
    }
}
