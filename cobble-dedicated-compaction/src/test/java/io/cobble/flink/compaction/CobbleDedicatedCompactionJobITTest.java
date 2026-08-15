package io.cobble.flink.compaction;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.cobble.Config;
import io.cobble.CounterMetricValue;
import io.cobble.Db;
import io.cobble.MetricSample;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

class CobbleDedicatedCompactionJobITTest {
    @Test
    void flinkJobCompactsExactCobbleDatabase() throws Exception {
        Path root = Files.createTempDirectory("cobble-flink-dedicated-");
        Config config = dedicatedConfig(root.resolve("data"));
        runCompactionJob(
                root,
                config,
                new String[] {"--path", root.resolve("data").resolve("${dbId}").toString()});
    }

    @Test
    void flinkJobDiscoversStateFromCheckpointJobDirectory() throws Exception {
        Path root = Files.createTempDirectory("cobble-flink-dedicated-job-");
        Path checkpointRoot = root.resolve("checkpoints");
        Path jobDirectory = checkpointRoot.resolve("0123456789abcdef");
        Path dataRoot = jobDirectory.resolve("shared/op_state/data");
        runCompactionJob(
                root,
                dedicatedConfig(dataRoot),
                dedicatedConfig(checkpointRoot),
                new String[] {"--path", jobDirectory.toString()});
    }

    @Test
    void flinkJobDiscoversStateFromCheckpointParentDirectory() throws Exception {
        Path root = Files.createTempDirectory("cobble-flink-dedicated-root-");
        Path checkpointRoot = root.resolve("checkpoints");
        Path dataRoot = checkpointRoot.resolve("0123456789abcdef/shared/op_state/data");
        runCompactionJob(
                root,
                dedicatedConfig(dataRoot),
                dedicatedConfig(checkpointRoot),
                new String[] {"--path", checkpointRoot.toString()});
    }

    private static void runCompactionJob(Path root, Config writerConfig, String[] discoveryArgs)
            throws Exception {
        runCompactionJob(root, writerConfig, writerConfig, discoveryArgs);
    }

    private static void runCompactionJob(
            Path root, Config writerConfig, Config compactorConfig, String[] discoveryArgs)
            throws Exception {
        Path configPath = root.resolve("compactor.json");
        Files.createDirectories(Paths.get(writerConfig.volumes.get(0).baseDir));
        Files.write(configPath, compactorConfig.toJson().getBytes(StandardCharsets.UTF_8));
        byte[] value = new byte[1024];

        try (Db db = Db.open(writerConfig)) {
            for (int index = 0; index < 80; index++) {
                db.put(
                        0,
                        String.format("key-%08d", index).getBytes(StandardCharsets.UTF_8),
                        0,
                        value);
            }

            String[] args = new String[discoveryArgs.length + 6];
            args[0] = "--config";
            args[1] = configPath.toString();
            for (int index = 0; index < discoveryArgs.length; index++) {
                args[index + 2] = discoveryArgs[index].replace("${dbId}", db.id());
            }
            int offset = discoveryArgs.length + 2;
            args[offset] = "--poll-interval-ms";
            args[offset + 1] = "20";
            args[offset + 2] = "--parallelism";
            args[offset + 3] = "2";
            CobbleCompactionJobOptions options = CobbleCompactionJobOptions.parse(args);
            StreamExecutionEnvironment environment =
                    StreamExecutionEnvironment.createLocalEnvironment(2);
            CobbleDedicatedCompactionJob.configure(environment, options);
            JobClient job = environment.executeAsync("Cobble dedicated compaction test");
            try {
                waitForCompaction(db);
            } finally {
                cancelIfRunning(job);
            }

            assertArrayEquals(value, db.get(0, "key-00000000".getBytes(StandardCharsets.UTF_8), 0));
        }
    }

    private static void cancelIfRunning(JobClient job) throws Exception {
        try {
            job.cancel().get();
        } catch (IllegalStateException error) {
            if (error.getMessage() == null || !error.getMessage().contains("MiniCluster")) {
                throw error;
            }
        }
    }

    private static Config dedicatedConfig(Path root) {
        Config config = new Config().addVolume(root.toString()).numColumns(1).totalBuckets(1);
        config.memtableCapacity = 8 * 1024;
        config.memtableBufferCount = 2;
        config.l0FileLimit = 2;
        config.baseFileSize = 4 * 1024;
        config.l1BaseBytes = 8 * 1024;
        config.levelSizeMultiplier = 2;
        config.maxLevel = 4;
        config.blockCacheSize = 0;
        config.compactionMode = Config.CompactionMode.DEDICATED;
        config.runtimeManifestMode = Config.RuntimeManifestMode.AUTO;
        config.compactionDedicatedPollIntervalMs = 20L;
        config.compactionOrphanMinAgeMs = 30_000L;
        return config;
    }

    private static void waitForCompaction(Db db) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            for (MetricSample metric : db.metrics()) {
                if ("compactions_total".equals(metric.name())
                        && metric.value() instanceof CounterMetricValue
                        && ((CounterMetricValue) metric.value()).value() > 0L) {
                    return;
                }
            }
            Thread.sleep(50L);
        }
        fail("Flink compaction job did not complete a compaction before timeout");
    }
}
