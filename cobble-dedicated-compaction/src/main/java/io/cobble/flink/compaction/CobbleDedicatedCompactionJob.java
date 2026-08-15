package io.cobble.flink.compaction;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

/** Flink 1.x entry point for continuous Cobble dedicated compaction. */
public final class CobbleDedicatedCompactionJob {
    private CobbleDedicatedCompactionJob() {}

    public static void main(String[] args) throws Exception {
        CobbleCompactionJobOptions options = CobbleCompactionJobOptions.parse(args);
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        configure(environment, options);
        environment.execute("Cobble Dedicated Compaction");
    }

    static void configure(
            StreamExecutionEnvironment environment, CobbleCompactionJobOptions options) {
        environment
                .fromSource(
                        options.source(),
                        WatermarkStrategy.<CobbleCompactionPlan>noWatermarks(),
                        "Cobble compaction monitor")
                .setParallelism(1)
                .rebalance()
                .map(new CobbleCompactionExecutorFunction(options.configPath()))
                .name("Cobble compaction executor")
                .setParallelism(options.parallelism())
                .addSink(new DiscardingSink<String>())
                .name("Cobble compaction result sink")
                .setParallelism(options.parallelism());
    }
}
