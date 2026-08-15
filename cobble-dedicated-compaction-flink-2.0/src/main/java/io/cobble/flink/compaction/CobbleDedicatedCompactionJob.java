package io.cobble.flink.compaction;

import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.api.connector.dsv2.WrappedSource;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

/** Flink 2.x entry point for continuous Cobble dedicated compaction. */
public final class CobbleDedicatedCompactionJob {
    private CobbleDedicatedCompactionJob() {}

    public static void main(String[] args) throws Exception {
        CobbleCompactionJobOptions options = CobbleCompactionJobOptions.parse(args);
        ExecutionEnvironment environment = ExecutionEnvironment.getInstance();
        environment
                .fromSource(
                        new WrappedSource<CobbleCompactionPlan>(options.source()),
                        "Cobble compaction monitor")
                .withParallelism(1)
                .shuffle()
                .process(new CobbleCompactionExecutorProcessFunction(options.configPath()))
                .withName("Cobble compaction executor")
                .withParallelism(options.parallelism())
                .toSink(new WrappedSink<String>(new DiscardingSink<String>()))
                .withName("Cobble compaction result sink")
                .withParallelism(options.parallelism());
        environment.execute("Cobble Dedicated Compaction");
    }
}
