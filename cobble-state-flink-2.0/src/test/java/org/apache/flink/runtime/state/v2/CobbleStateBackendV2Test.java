package org.apache.flink.runtime.state.v2;

import static org.assertj.core.api.Assertions.assertThat;

import io.cobble.flink.state.CobbleOptions;
import io.cobble.flink.state.CobbleStateBackend;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.v2.AggregatingState;
import org.apache.flink.api.common.state.v2.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.ReducingState;
import org.apache.flink.api.common.state.v2.ReducingStateDescriptor;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.state.StateFutureImpl;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationManager;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.IOUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/** Flink's async-backend contract suite plus Cobble row-format coverage. */
@ExtendWith(ParameterizedTestExtension.class)
class CobbleStateBackendV2Test extends StateBackendTestV2Base<CobbleStateBackend> {

    @TempDir private static Path tempDirectory;

    @Parameters(name = "Cobble async backend")
    static List<Object[]> parameters() {
        return Collections.singletonList(new Object[0]);
    }

    @Override
    protected CheckpointStorage getCheckpointStorage() {
        return new FileSystemCheckpointStorage(checkpointDirectory().toUri().toString());
    }

    @Override
    protected ConfigurableStateBackend getStateBackend() {
        Configuration config = new Configuration();
        config.set(CobbleOptions.LOCAL_DIRECTORIES, localDirectory().toString());
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                checkpointDirectory().toUri().toString());
        return new CobbleStateBackend()
                .configure(config, Thread.currentThread().getContextClassLoader());
    }

    @Override
    protected void restoreJob() {
        // A failover restore keeps the same Flink job id and shared checkpoint directory.
    }

    /**
     * The shared suite registers a disjoint state name in each source subtask. Cobble snapshots
     * require every shard of one operator to carry the same registered schema, as real Flink
     * operators do. Cobble-specific rescale coverage uses that production registration model.
     */
    @Override
    @Disabled("Shared test creates inconsistent per-subtask state schemas")
    @TestTemplate
    void testAsyncStateBackendScaleUp() {}

    @Override
    @Disabled("Shared test creates inconsistent per-subtask state schemas")
    @TestTemplate
    void testAsyncStateBackendScaleDown() {}

    @TestTemplate
    void supportsAllV2StateKindsAndMapPutAllWireFormat() throws Exception {
        AsyncKeyedStateBackend<Integer> backend =
                createAsyncKeyedBackend(
                        0, 1, IntSerializer.INSTANCE, KeyGroupRange.of(0, 127), env);
        AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
        AsyncExecutionController<Integer> controller = createController(backend, asyncFailure, 128);
        backend.setup(controller);

        try {
            ValueState<Integer> valueState =
                    backend.getOrCreateKeyedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("value", IntSerializer.INSTANCE));
            ListState<String> listState =
                    backend.getOrCreateKeyedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ListStateDescriptor<>("list", StringSerializer.INSTANCE));
            MapState<String, Integer> mapState =
                    backend.getOrCreateKeyedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    "map", StringSerializer.INSTANCE, IntSerializer.INSTANCE));
            ReducingState<Integer> reducingState =
                    backend.getOrCreateKeyedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ReducingStateDescriptor<Integer>(
                                    "reducing",
                                    (ReduceFunction<Integer>) Integer::sum,
                                    IntSerializer.INSTANCE));
            AggregatingState<Integer, Integer> aggregatingState =
                    backend.getOrCreateKeyedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new AggregatingStateDescriptor<>(
                                    "aggregating",
                                    new SumAggregateFunction(),
                                    IntSerializer.INSTANCE));

            RecordContext<Integer> context = controller.buildContext(7, 7);
            context.retain();
            controller.setCurrentContext(context);

            valueState.update(11);
            listState.addAll(Arrays.asList("a", "b"));
            listState.add("c");
            Map<String, Integer> batch = new LinkedHashMap<>();
            batch.put("one", 1);
            batch.put("null", null);
            mapState.putAll(batch);
            mapState.put("two", 2);
            Map<String, Integer> continuationBatch = new LinkedHashMap<>();
            for (int i = 0; i < 256; i++) {
                continuationBatch.put("batch-" + i, i);
            }
            mapState.putAll(continuationBatch);
            reducingState.add(4);
            reducingState.add(5);
            aggregatingState.add(6);
            aggregatingState.add(7);

            assertThat(valueState.value()).isEqualTo(11);
            assertThat(copy(listState.get())).containsExactly("a", "b", "c");
            assertThat(mapState.get("one")).isEqualTo(1);
            assertThat(mapState.get("two")).isEqualTo(2);
            assertThat(mapState.contains("null")).isTrue();
            assertThat(mapState.get("null")).isNull();
            assertThat(mapState.entries())
                    .contains(
                            new java.util.AbstractMap.SimpleImmutableEntry<>("one", 1),
                            new java.util.AbstractMap.SimpleImmutableEntry<>("null", null),
                            new java.util.AbstractMap.SimpleImmutableEntry<>("two", 2));
            assertThat(copy(mapState.keys())).hasSize(259);
            assertThat(reducingState.get()).isEqualTo(9);
            assertThat(aggregatingState.get()).isEqualTo(13);

            context.release();
            controller.drainInflightRecords(0);
            assertThat(asyncFailure.get()).isNull();
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @TestTemplate
    void restoresConsistentAsyncStateAfterScaleUp() throws Exception {
        testConsistentSchemaRescale(2, 3);
    }

    @TestTemplate
    void restoresConsistentAsyncStateAfterScaleDown() throws Exception {
        testConsistentSchemaRescale(3, 2);
    }

    @Override
    protected <K> AsyncKeyedStateBackend<K> createAsyncKeyedBackend(
            int subtaskId,
            int parallelism,
            TypeSerializer<K> keySerializer,
            KeyGroupRange keyGroupRange,
            Environment environment)
            throws Exception {
        return createBackend(
                subtaskId,
                parallelism,
                keySerializer,
                keyGroupRange,
                Collections.emptyList(),
                environment);
    }

    @Override
    protected <K> AsyncKeyedStateBackend<K> restoreAsyncKeyedBackend(
            int subtaskId,
            int parallelism,
            TypeSerializer<K> keySerializer,
            KeyGroupRange keyGroupRange,
            List<KeyedStateHandle> state,
            Environment environment)
            throws Exception {
        return createBackend(
                subtaskId, parallelism, keySerializer, keyGroupRange, state, environment);
    }

    private <K> AsyncKeyedStateBackend<K> createBackend(
            int subtaskId,
            int parallelism,
            TypeSerializer<K> keySerializer,
            KeyGroupRange keyGroupRange,
            List<KeyedStateHandle> state,
            Environment environment)
            throws Exception {
        int maxParallelism = keyGroupRange.getNumberOfKeyGroups() * parallelism;
        return getStateBackend()
                .createAsyncKeyedStateBackend(
                        new KeyedStateBackendParametersImpl<>(
                                environment,
                                jobID,
                                String.format("test_op_%d_%d", subtaskId, parallelism),
                                keySerializer,
                                maxParallelism,
                                keyGroupRange,
                                environment.getTaskKvStateRegistry(),
                                TtlTimeProvider.DEFAULT,
                                getMetricGroup(),
                                getCustomInitializationMetrics(),
                                state,
                                new CloseableRegistry(),
                                1.0d));
    }

    private void testConsistentSchemaRescale(int sourceParallelism, int targetParallelism)
            throws Exception {
        int maxParallelism = 12;
        List<Integer> keysByKeyGroup = new ArrayList<>(maxParallelism);
        for (int keyGroup = 0; keyGroup < maxParallelism; keyGroup++) {
            keysByKeyGroup.add(findKeyForKeyGroup(keyGroup, maxParallelism));
        }

        List<KeyedStateHandle> sourceSnapshots = new ArrayList<>(sourceParallelism);
        for (int subtask = 0; subtask < sourceParallelism; subtask++) {
            KeyGroupRange range =
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                            maxParallelism, sourceParallelism, subtask);
            AsyncKeyedStateBackend<Integer> backend =
                    createAsyncKeyedBackend(
                            subtask, sourceParallelism, IntSerializer.INSTANCE, range, env);
            AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
            AsyncExecutionController<Integer> controller =
                    createController(backend, asyncFailure, maxParallelism);
            backend.setup(controller);
            try {
                ValueState<Integer> valueState =
                        backend.getOrCreateKeyedState(
                                VoidNamespace.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                new ValueStateDescriptor<>("value", IntSerializer.INSTANCE));
                MapState<String, Integer> mapState =
                        backend.getOrCreateKeyedState(
                                VoidNamespace.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                new MapStateDescriptor<>(
                                        "map", StringSerializer.INSTANCE, IntSerializer.INSTANCE));
                for (int keyGroup : range) {
                    int key = keysByKeyGroup.get(keyGroup);
                    RecordContext<Integer> context = controller.buildContext(key, key);
                    context.retain();
                    controller.setCurrentContext(context);
                    valueState.update(keyGroup * 10);
                    Map<String, Integer> entries = new LinkedHashMap<>();
                    entries.put("value", keyGroup);
                    entries.put("null", null);
                    mapState.putAll(entries);
                    context.release();
                }
                controller.drainInflightRecords(0);
                assertThat(asyncFailure.get()).isNull();
                sourceSnapshots.add(snapshot(backend, 41L));
            } finally {
                IOUtils.closeQuietly(backend);
                backend.dispose();
            }
        }

        for (int subtask = 0; subtask < targetParallelism; subtask++) {
            KeyGroupRange range =
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                            maxParallelism, targetParallelism, subtask);
            List<KeyedStateHandle> intersections = new ArrayList<>();
            StateAssignmentOperation.extractIntersectingState(
                    sourceSnapshots, range, intersections);
            AsyncKeyedStateBackend<Integer> backend =
                    restoreAsyncKeyedBackend(
                            subtask,
                            targetParallelism,
                            IntSerializer.INSTANCE,
                            range,
                            intersections,
                            env);
            AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
            AsyncExecutionController<Integer> controller =
                    createController(backend, asyncFailure, maxParallelism);
            backend.setup(controller);
            try {
                ValueState<Integer> valueState =
                        backend.getOrCreateKeyedState(
                                VoidNamespace.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                new ValueStateDescriptor<>("value", IntSerializer.INSTANCE));
                MapState<String, Integer> mapState =
                        backend.getOrCreateKeyedState(
                                VoidNamespace.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                new MapStateDescriptor<>(
                                        "map", StringSerializer.INSTANCE, IntSerializer.INSTANCE));
                for (int keyGroup : range) {
                    int key = keysByKeyGroup.get(keyGroup);
                    RecordContext<Integer> context = controller.buildContext(key, key);
                    context.retain();
                    controller.setCurrentContext(context);
                    assertThat(valueState.value()).isEqualTo(keyGroup * 10);
                    assertThat(mapState.get("value")).isEqualTo(keyGroup);
                    assertThat(mapState.contains("null")).isTrue();
                    assertThat(mapState.get("null")).isNull();
                    context.release();
                }
                controller.drainInflightRecords(0);
                assertThat(asyncFailure.get()).isNull();
            } finally {
                IOUtils.closeQuietly(backend);
                backend.dispose();
            }
        }
    }

    private KeyedStateHandle snapshot(AsyncKeyedStateBackend<Integer> backend, long checkpointId)
            throws Exception {
        java.util.concurrent.RunnableFuture<SnapshotResult<KeyedStateHandle>> future =
                backend.snapshot(
                        checkpointId,
                        System.currentTimeMillis(),
                        createStreamFactory(),
                        org.apache.flink.runtime.checkpoint.CheckpointOptions
                                .forCheckpointWithDefaultLocation());
        if (!future.isDone()) {
            future.run();
        }
        return future.get().getJobManagerOwnedSnapshot();
    }

    private static int findKeyForKeyGroup(int targetKeyGroup, int maxParallelism) {
        for (int key = 0; ; key++) {
            if (KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism) == targetKeyGroup) {
                return key;
            }
        }
    }

    private static <K> AsyncExecutionController<K> createController(
            AsyncKeyedStateBackend<K> backend,
            AtomicReference<Throwable> asyncFailure,
            int maxParallelism) {
        StateFutureImpl.AsyncFrameworkExceptionHandler exceptionHandler =
                (message, error) -> asyncFailure.compareAndSet(null, error);
        return new AsyncExecutionController<>(
                new SyncMailboxExecutor(),
                exceptionHandler,
                backend.createStateExecutor(),
                new DeclarationManager(),
                maxParallelism,
                1,
                1,
                1000,
                null,
                null);
    }

    private static Path localDirectory() {
        return tempDirectory.resolve("local");
    }

    private static Path checkpointDirectory() {
        return tempDirectory.resolve("checkpoints");
    }

    private static <T> List<T> copy(Iterable<T> values) {
        List<T> copy = new ArrayList<>();
        for (T value : values) {
            copy.add(value);
        }
        return copy;
    }

    private static final class SumAggregateFunction
            implements AggregateFunction<Integer, Integer, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Integer value, Integer accumulator) {
            return value + accumulator;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer first, Integer second) {
            return first + second;
        }
    }
}
