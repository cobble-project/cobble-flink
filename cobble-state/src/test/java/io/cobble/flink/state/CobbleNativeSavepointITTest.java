package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/** Real checkpoint/savepoint lifecycle with no remaining source checkpoint or local files. */
class CobbleNativeSavepointITTest {
    private static final int KEYS = 256;
    private static final Set<Integer> ACKNOWLEDGED = ConcurrentHashMap.newKeySet();

    @Test
    void independentNativeSavepointsRestoreAtSameAndDifferentParallelism(@TempDir Path root)
            throws Exception {
        MiniClusterWithClientResource cluster =
                CobbleCheckpointingITSupport.createCluster(new Configuration());
        cluster.before();
        try {
            JobGraph writer = graph(root.resolve("writer"), 2, true);
            submitAndAwaitData(cluster, writer);
            CobbleCheckpointingITSupport.triggerCheckpointAndWait(
                    cluster.getMiniCluster(), writer.getJobID());
            String sameParallelismSavepoint =
                    cluster.getMiniCluster()
                            .triggerSavepoint(
                                    writer.getJobID(),
                                    root.resolve("savepoints").toString(),
                                    false,
                                    SavepointFormatType.NATIVE)
                            .get(120, TimeUnit.SECONDS);
            String rescaledSavepoint =
                    cluster.getMiniCluster()
                            .triggerSavepoint(
                                    writer.getJobID(),
                                    root.resolve("savepoints").toString(),
                                    true,
                                    SavepointFormatType.NATIVE)
                            .get(120, TimeUnit.SECONDS);
            CobbleCheckpointingITSupport.waitForJobStatus(
                    cluster.getMiniCluster(),
                    writer.getJobID(),
                    JobStatus.CANCELED,
                    Duration.ofSeconds(60));
            deleteTree(root.resolve("writer"));
            for (int parallelism : new int[] {2, 3}) {
                String savepoint = parallelism == 2 ? sameParallelismSavepoint : rescaledSavepoint;
                assertExportMetadata(savepoint);
                Map<Path, byte[]> original =
                        hashFiles(Path.of(java.net.URI.create(savepoint)).resolve("cobble"));
                assertFalse(
                        original.isEmpty(),
                        "savepoint must contain independently exported native files");
                assertTrue(
                        original.keySet().stream()
                                .anyMatch(path -> path.toString().endsWith(".cb")));
                Path restoredRoot = root.resolve("restore-" + parallelism);
                JobGraph restored = graph(restoredRoot, parallelism, false);
                restored.setSavepointRestoreSettings(
                        SavepointRestoreSettings.forPath(savepoint, false, RestoreMode.CLAIM));
                submitAndAwaitData(cluster, restored);
                for (Map.Entry<Path, byte[]> file : original.entrySet()) {
                    assertArrayEquals(
                            file.getValue(),
                            java.security.MessageDigest.getInstance("SHA-256")
                                    .digest(Files.readAllBytes(file.getKey())),
                            "restore changed export: " + file.getKey());
                }
                CobbleCheckpointingITSupport.triggerCheckpointAndWait(
                        cluster.getMiniCluster(), restored.getJobID());
                // A restored backend must also be able to re-export from its own adopted files.
                String next =
                        cluster.getMiniCluster()
                                .triggerSavepoint(
                                        restored.getJobID(),
                                        root.resolve("reexports").toString(),
                                        true,
                                        SavepointFormatType.NATIVE)
                                .get(120, TimeUnit.SECONDS);
                assertNotEquals(savepoint, next);
                CobbleCheckpointingITSupport.waitForJobStatus(
                        cluster.getMiniCluster(),
                        restored.getJobID(),
                        JobStatus.CANCELED,
                        Duration.ofSeconds(60));
                deleteTree(restoredRoot);
                assertFalse(
                        hashFiles(Path.of(java.net.URI.create(next)).resolve("cobble")).isEmpty());
            }
        } finally {
            cluster.after();
        }
    }

    private static void submitAndAwaitData(MiniClusterWithClientResource cluster, JobGraph graph)
            throws Exception {
        ACKNOWLEDGED.clear();
        CobbleCheckpointingITSupport.submitJobAndWaitForRunning(cluster, graph);
        CobbleCheckpointingITSupport.waitForJobCondition(
                cluster.getMiniCluster(),
                graph.getJobID(),
                () -> ACKNOWLEDGED.size() == KEYS,
                Duration.ofSeconds(60),
                "all operator state writes/reads acknowledged");
    }

    private static JobGraph graph(Path root, int parallelism, boolean write) {
        Configuration config =
                CobbleCheckpointingITSupport.createJobConfiguration(root.resolve("local"));
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                root.resolve("checkpoints").toUri().toString());
        config.set(CobbleOptions.FIX_PER_SLOT_MEMORY_SIZE, MemorySize.parse("16mb"));
        config.set(CobbleOptions.MEMTABLE_BUFFER_RATIO, 0.02);
        config.set(CobbleOptions.VALUE_SEPARATION_THRESHOLD, MemorySize.parse("64b"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(parallelism);
        env.getConfig().setMaxParallelism(16);
        env.enableCheckpointing(60000L);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setStateBackend(
                new CobbleStateBackend()
                        .configure(config, CobbleNativeSavepointITTest.class.getClassLoader()));
        env.addSource(new Keys())
                .uid("keys")
                .keyBy(key -> key)
                .flatMap(new StateAccess(write))
                .uid("state")
                .addSink(new Discard())
                .uid("sink");
        return env.getStreamGraph().getJobGraph();
    }

    private static final class Keys extends RichParallelSourceFunction<Integer> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Integer> context) throws Exception {
            for (int key = getRuntimeContext().getIndexOfThisSubtask();
                    key < KEYS;
                    key += getRuntimeContext().getNumberOfParallelSubtasks()) {
                synchronized (context.getCheckpointLock()) {
                    context.collect(key);
                }
            }
            while (running) Thread.sleep(10);
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class StateAccess extends RichFlatMapFunction<Integer, Void> {
        private final boolean write;
        private transient ValueState<byte[]> value;
        private transient MapState<Integer, byte[]> map;
        private transient ListState<byte[]> list;

        StateAccess(boolean write) {
            this.write = write;
        }

        @Override
        public void open(Configuration config) {
            value = getRuntimeContext().getState(new ValueStateDescriptor<>("value", byte[].class));
            map =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>("map", Integer.class, byte[].class));
            list =
                    getRuntimeContext()
                            .getListState(new ListStateDescriptor<>("list", byte[].class));
        }

        @Override
        public void flatMap(Integer key, Collector<Void> collector) throws Exception {
            byte[] expected = new byte[4096];
            Arrays.fill(expected, (byte) key.intValue());
            if (write) {
                value.update(expected);
                map.put(1, expected);
                map.put(2, expected);
                list.add(expected);
                list.add(expected);
            }
            assertArrayEquals(expected, value.value());
            assertArrayEquals(expected, map.get(1));
            assertArrayEquals(expected, map.get(2));
            int count = 0;
            for (byte[] entry : list.get()) {
                assertArrayEquals(expected, entry);
                count++;
            }
            assertEquals(2, count);
            ACKNOWLEDGED.add(
                    key); // This runs downstream after all state operations, not in the source.
        }
    }

    private static final class Discard extends RichSinkFunction<Void> {}

    private static Map<Path, byte[]> hashFiles(Path root) throws Exception {
        Map<Path, byte[]> result = new HashMap<>();
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : (Iterable<Path>) paths.filter(Files::isRegularFile)::iterator) {
                result.put(
                        path,
                        java.security.MessageDigest.getInstance("SHA-256")
                                .digest(Files.readAllBytes(path)));
            }
        }
        return result;
    }

    private static void assertExportMetadata(String savepoint) throws Exception {
        Path directory = Path.of(java.net.URI.create(savepoint));
        org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata checkpoint;
        try (java.io.DataInputStream input =
                new java.io.DataInputStream(Files.newInputStream(directory.resolve("_metadata")))) {
            checkpoint =
                    org.apache.flink.runtime.checkpoint.Checkpoints.loadCheckpointMetadata(
                            input, CobbleNativeSavepointITTest.class.getClassLoader(), savepoint);
        }
        int shards = 0;
        for (org.apache.flink.runtime.checkpoint.OperatorState operator :
                checkpoint.getOperatorStates()) {
            for (org.apache.flink.runtime.checkpoint.OperatorSubtaskState subtask :
                    operator.getStates()) {
                for (org.apache.flink.runtime.state.KeyedStateHandle state :
                        subtask.getManagedKeyedState()) {
                    org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle handle =
                            (org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle)
                                    state;
                    try (org.apache.flink.core.fs.FSDataInputStream stream =
                            handle.getMetaStateHandle().openInputStream()) {
                        CobbleSnapshotMetadata metadata =
                                CobbleSnapshotMetadata.read(
                                        new org.apache.flink.core.memory.DataInputViewStreamWrapper(
                                                stream));
                        assertTrue(metadata.exportedSavepoint());
                        assertTrue(
                                Path.of(java.net.URI.create(metadata.shardSnapshot().manifestPath))
                                        .startsWith(directory));
                        assertTrue(
                                metadata.volumeDirectories().stream()
                                        .noneMatch(path -> path.contains("/writer/")));
                        shards++;
                    }
                }
            }
        }
        assertEquals(2, shards);
    }

    private static void deleteTree(Path root) throws Exception {
        org.apache.flink.util.FileUtils.deleteDirectory(root.toFile());
    }
}
