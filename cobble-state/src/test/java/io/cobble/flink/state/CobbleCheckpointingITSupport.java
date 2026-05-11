package io.cobble.flink.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import java.nio.file.Path;

final class CobbleCheckpointingITSupport {
    static final int NUM_TASK_MANAGERS = 2;
    static final int NUM_TASK_SLOTS = 2;
    static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;

    private CobbleCheckpointingITSupport() {}

    static MiniClusterWithClientResource createCluster(Configuration configuration) {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(configuration)
                        .setNumberTaskManagers(NUM_TASK_MANAGERS)
                        .setNumberSlotsPerTaskManager(NUM_TASK_SLOTS)
                        .build());
    }

    static Configuration createJobConfiguration(Path localStateDirectory) {
        Configuration configuration = new Configuration();
        configuration.set(CobbleOptions.LOCAL_DIRECTORIES, localStateDirectory.toString());
        configuration.set(CobbleOptions.WRITE_BUFFER_RATIO, 0.25d);
        configuration.set(CobbleOptions.MEMTABLE_BUFFER_COUNT, 4);
        configuration.set(CobbleOptions.DIRECT_IO_BUFFER_SIZE, MemorySize.parse("8kb"));
        configuration.set(CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE, 128);
        return configuration;
    }

    static StreamExecutionEnvironment createEnvironment(Path localStateDirectory) {
        Configuration configuration = createJobConfiguration(localStateDirectory);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(PARALLELISM);
        env.setStateBackend(
                new CobbleStateBackend()
                        .configure(
                                configuration,
                                CobbleCheckpointingITSupport.class.getClassLoader()));
        return env;
    }
}
