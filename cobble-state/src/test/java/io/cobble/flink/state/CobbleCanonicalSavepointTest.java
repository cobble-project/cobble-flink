package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SavepointSnapshotStrategy;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.RunnableFuture;

/**
 * End-to-end tests for canonical savepoint CREATION from a Cobble backend.
 *
 * <p>These tests create state in a Cobble backend, call {@code backend.savepoint()} to produce a
 * {@link KeyGroupsSavepointStateHandle}, then restore it into a fresh Cobble backend and verify all
 * state values match. This validates the full round-trip: Cobble state → canonical savepoint bytes
 * → Cobble restore.
 *
 * <p>This class does NOT extend {@code CobbleStateBackendTest} (which would inherit all ~150 parent
 * {@code @Test} methods under JUnit 5). Instead it composes a {@code CobbleStateBackendTest}
 * instance to access its package-private helper methods.
 */
class CobbleCanonicalSavepointTest {

    private static final Configuration EMPTY_CONFIG = new Configuration();

    /** Composed delegate for accessing shared package-private helpers (no @Test inheritance). */
    private final CobbleStateBackendTest helpers = new CobbleStateBackendTest();

    // =====================================================================================
    //  Basic round-trip: VALUE state
    // =====================================================================================

    @Test
    void savepointProducesKeyGroupsSavepointStateHandle(@TempDir Path tempDir) throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);

        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.emptyList(),
                        KeyGroupRange.of(keyGroup, keyGroup),
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            backend.setCurrentKey(key);
            ValueState<String> valueState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("value", StringSerializer.INSTANCE));
            valueState.update("hello-cobble");

            KeyedStateHandle handle = runCobbleSavepoint(backend);
            assertTrue(handle instanceof KeyGroupsSavepointStateHandle);
        }
    }

    // =====================================================================================
    //  Round-trip: VALUE + LIST + MAP (including present-null map value)
    // =====================================================================================

    @Test
    void roundTripValueListMapState(@TempDir Path tempDir) throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupRange range = KeyGroupRange.of(keyGroup, keyGroup);

        KeyedStateHandle savepoint;
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("source"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.emptyList(),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            backend.setCurrentKey(key);

            ValueState<String> valueState =
                    CobbleStateBackendTest.valueState(backend, "value", "ns");
            ListState<String> listState = CobbleStateBackendTest.listState(backend, "list", "ns");
            MapState<String, String> mapState =
                    CobbleStateBackendTest.mapState(backend, "map", "ns");

            valueState.update("v1");
            listState.addAll(Arrays.asList("a", "b", "c"));
            mapState.put("mk1", "mv1");
            mapState.put("mk-null", null);

            savepoint = runCobbleSavepoint(backend);
        }

        // Restore into a fresh backend.
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("target"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            backend.setCurrentKey(key);

            assertEquals("v1", CobbleStateBackendTest.valueState(backend, "value", "ns").value());
            assertEquals(
                    Arrays.asList("a", "b", "c"),
                    CobbleStateBackendTest.toList(
                            CobbleStateBackendTest.listState(backend, "list", "ns").get()));

            MapState<String, String> map = CobbleStateBackendTest.mapState(backend, "map", "ns");
            assertEquals("mv1", map.get("mk1"));
            assertTrue(map.contains("mk-null"));
            assertNull(map.get("mk-null"));
        }
    }

    // =====================================================================================
    //  Round-trip: REDUCING state
    // =====================================================================================

    @Test
    void roundTripReducingState(@TempDir Path tempDir) throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupRange range = KeyGroupRange.of(keyGroup, keyGroup);

        KeyedStateHandle savepoint;
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("source"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.emptyList(),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            backend.setCurrentKey(key);
            ReducingState<Integer> reducing =
                    CobbleStateBackendTest.reducingState(backend, "reducing", "ns");
            reducing.add(10);
            reducing.add(20);
            reducing.add(30);

            savepoint = runCobbleSavepoint(backend);
        }

        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("target"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            backend.setCurrentKey(key);
            assertEquals(60, CobbleStateBackendTest.reducingState(backend, "reducing", "ns").get());
        }
    }

    // =====================================================================================
    //  Round-trip: AGGREGATING state
    // =====================================================================================

    @Test
    void roundTripAggregatingState(@TempDir Path tempDir) throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupRange range = KeyGroupRange.of(keyGroup, keyGroup);

        KeyedStateHandle savepoint;
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("source"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.emptyList(),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            backend.setCurrentKey(key);
            AggregatingState<Integer, String> aggregating =
                    CobbleStateBackendTest.aggregatingState(backend, "aggregating", "ns");
            aggregating.add(3);
            aggregating.add(4);

            savepoint = runCobbleSavepoint(backend);
        }

        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("target"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            backend.setCurrentKey(key);
            assertEquals(
                    "sum=7",
                    CobbleStateBackendTest.aggregatingState(backend, "aggregating", "ns").get());
        }
    }

    // =====================================================================================
    //  Round-trip: all 5 KV state kinds in one savepoint
    // =====================================================================================

    @Test
    void roundTripAllStateKinds(@TempDir Path tempDir) throws Exception {
        int key = CobbleStateBackendTest.findKeyForGroup(5);
        KeyGroupRange range = KeyGroupRange.of(0, 15);

        KeyedStateHandle savepoint;
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("source"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.emptyList(),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            backend.setCurrentKey(key);

            ValueState<String> value = CobbleStateBackendTest.valueState(backend, "v", "ns");
            ListState<String> list = CobbleStateBackendTest.listState(backend, "l", "ns");
            MapState<String, String> map = CobbleStateBackendTest.mapState(backend, "m", "ns");
            ReducingState<Integer> reducing =
                    CobbleStateBackendTest.reducingState(backend, "r", "ns");
            AggregatingState<Integer, String> agg =
                    CobbleStateBackendTest.aggregatingState(backend, "a", "ns");

            value.update("val");
            list.addAll(Arrays.asList("x", "y"));
            map.put("k", "vv");
            reducing.add(100);
            reducing.add(200);
            agg.add(5);
            agg.add(6);

            savepoint = runCobbleSavepoint(backend);
        }

        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("target"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            backend.setCurrentKey(key);

            assertEquals("val", CobbleStateBackendTest.valueState(backend, "v", "ns").value());
            assertEquals(
                    Arrays.asList("x", "y"),
                    CobbleStateBackendTest.toList(
                            CobbleStateBackendTest.listState(backend, "l", "ns").get()));
            assertEquals("vv", CobbleStateBackendTest.mapState(backend, "m", "ns").get("k"));
            assertEquals(300, CobbleStateBackendTest.reducingState(backend, "r", "ns").get());
            assertEquals(
                    "sum=11", CobbleStateBackendTest.aggregatingState(backend, "a", "ns").get());
        }
    }

    // =====================================================================================
    //  Round-trip: timer state (native + overlay)
    // =====================================================================================

    @Test
    void roundTripTimerState(@TempDir Path tempDir) throws Exception {
        int keyGroup = 5;
        int key = CobbleStateBackendTest.findKeyForGroup(keyGroup);
        KeyGroupRange range = KeyGroupRange.of(0, 15);

        KeyedStateHandle savepoint;
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("source"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.emptyList(),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            TimerSerializer<Integer, VoidNamespace> timerSerializer =
                    new TimerSerializer<>(IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE);

            @SuppressWarnings("unchecked")
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, VoidNamespace>> queue =
                    backend.create("timer", timerSerializer);

            // Add two timers. Both go to native storage initially.
            TimerHeapInternalTimer<Integer, VoidNamespace> timer1 =
                    new TimerHeapInternalTimer<>(100L, key, VoidNamespace.INSTANCE);
            TimerHeapInternalTimer<Integer, VoidNamespace> timer2 =
                    new TimerHeapInternalTimer<>(200L, key, VoidNamespace.INSTANCE);
            queue.add(timer1);
            queue.add(timer2);

            // Peek triggers ensureLoaded → reload → pollBatchDirect, which moves timers from
            // native storage into the in-memory overlay. The savepoint must capture the overlay
            // snapshot in the sync phase and not re-read the live queue in the async phase.
            backend.setCurrentKey(key);
            assertEquals(timer1, queue.peek());

            // Call savepoint() (sync phase) — overlay timers are frozen here.
            SavepointResources<Integer> savepointResources =
                    ((CheckpointableKeyedStateBackend<Integer>) backend).savepoint();
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                    new SnapshotStrategyRunner<>(
                                    "Cobble canonical savepoint",
                                    new SavepointSnapshotStrategy<>(
                                            savepointResources.getSnapshotResources()),
                                    new CloseableRegistry(),
                                    savepointResources.getPreferredSnapshotExecutionType())
                            .snapshot(
                                    1L,
                                    System.currentTimeMillis(),
                                    new MemCheckpointStreamFactory(1024 * 1024),
                                    CheckpointOptions.alignedNoTimeout(
                                            SavepointType.savepoint(SavepointFormatType.CANONICAL),
                                            CheckpointStorageLocationReference.getDefault()));

            // Add a third timer AFTER savepoint() returned but BEFORE the async writer runs.
            // This timer must NOT appear in the savepoint.
            queue.add(new TimerHeapInternalTimer<>(50L, key, VoidNamespace.INSTANCE));

            // Now run the async writer.
            snapshotFuture.run();
            SnapshotResult<KeyedStateHandle> snapshotResult = snapshotFuture.get();
            savepoint = snapshotResult.getJobManagerOwnedSnapshot();
        }

        // Restore into a fresh backend.
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("target"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            TimerSerializer<Integer, VoidNamespace> timerSerializer =
                    new TimerSerializer<>(IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE);

            @SuppressWarnings("unchecked")
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, VoidNamespace>> queue =
                    backend.create("timer", timerSerializer);

            // Both overlay timers should be present. The post-savepoint timer (50L) must NOT be.
            TimerHeapInternalTimer<Integer, VoidNamespace> first = queue.poll();
            assertEquals(100L, first.getTimestamp());
            assertEquals(key, first.getKey());

            TimerHeapInternalTimer<Integer, VoidNamespace> second = queue.poll();
            assertEquals(200L, second.getTimestamp());
            assertEquals(key, second.getKey());

            assertNull(queue.poll());
        }
    }

    // =====================================================================================
    //  Empty backend → SnapshotResult.empty()
    // =====================================================================================

    @Test
    void savepointFromEmptyBackend(@TempDir Path tempDir) throws Exception {
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.emptyList(),
                        KeyGroupRange.of(0, 15),
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            KeyedStateHandle handle = runCobbleSavepoint(backend);
            // Empty savepoint produces a null JM-owned handle (SnapshotResult.empty()).
            assertNull(handle);
        }
    }

    // =====================================================================================
    //  Stable view: post-savepoint writes are excluded
    // =====================================================================================

    @Test
    void savepointStableViewExcludesPostSavepointWrites(@TempDir Path tempDir) throws Exception {
        int key = CobbleStateBackendTest.findKeyForGroup(5);
        KeyGroupRange range = KeyGroupRange.of(0, 15);

        KeyedStateHandle savepoint;
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("source"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.emptyList(),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            backend.setCurrentKey(key);
            ValueState<String> value = CobbleStateBackendTest.valueState(backend, "v", "ns");
            value.update("before-savepoint");

            // Call savepoint() (sync phase) but do NOT run the async writer yet.
            SavepointResources<Integer> savepointResources =
                    ((CheckpointableKeyedStateBackend<Integer>) backend).savepoint();
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                    new SnapshotStrategyRunner<>(
                                    "Cobble canonical savepoint",
                                    new SavepointSnapshotStrategy<>(
                                            savepointResources.getSnapshotResources()),
                                    new CloseableRegistry(),
                                    savepointResources.getPreferredSnapshotExecutionType())
                            .snapshot(
                                    1L,
                                    System.currentTimeMillis(),
                                    new MemCheckpointStreamFactory(1024 * 1024),
                                    CheckpointOptions.alignedNoTimeout(
                                            SavepointType.savepoint(SavepointFormatType.CANONICAL),
                                            CheckpointStorageLocationReference.getDefault()));

            // Write more data AFTER savepoint() returned but BEFORE the async writer runs.
            value.update("after-savepoint");

            // Now run the async writer — it must see the pre-savepoint data.
            snapshotFuture.run();
            SnapshotResult<KeyedStateHandle> snapshotResult = snapshotFuture.get();
            savepoint = snapshotResult.getJobManagerOwnedSnapshot();
        }

        // Restore: should see "before-savepoint", not "after-savepoint".
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("target"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            backend.setCurrentKey(key);
            assertEquals(
                    "before-savepoint",
                    CobbleStateBackendTest.valueState(backend, "v", "ns").value());
        }
    }

    // =====================================================================================
    //  Multiple key groups
    // =====================================================================================

    @Test
    void roundTripMultipleKeyGroups(@TempDir Path tempDir) throws Exception {
        int key1 = CobbleStateBackendTest.findKeyForGroup(3);
        int key2 = CobbleStateBackendTest.findKeyForGroup(10);
        KeyGroupRange range = KeyGroupRange.of(0, 15);

        KeyedStateHandle savepoint;
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("source"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.emptyList(),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;

            backend.setCurrentKey(key1);
            CobbleStateBackendTest.valueState(backend, "v", "ns").update("k1");

            backend.setCurrentKey(key2);
            CobbleStateBackendTest.valueState(backend, "v", "ns").update("k2");

            savepoint = runCobbleSavepoint(backend);
        }

        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("target"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;

            backend.setCurrentKey(key1);
            assertEquals("k1", CobbleStateBackendTest.valueState(backend, "v", "ns").value());

            backend.setCurrentKey(key2);
            assertEquals("k2", CobbleStateBackendTest.valueState(backend, "v", "ns").value());
        }
    }

    // =====================================================================================
    //  State exists in one key group but not another (unknown CF handling)
    // =====================================================================================

    @Test
    void savepointStateAbsentInSomeKeyGroups(@TempDir Path tempDir) throws Exception {
        // Use a wide range. State "v" only has data in key group 3 (key1).
        // key2 is in key group 10 with no data for "v" — the scan for that (keyGroup, state) pair
        // should yield nothing, but the savepoint must still succeed.
        int key1 = CobbleStateBackendTest.findKeyForGroup(3);
        KeyGroupRange range = KeyGroupRange.of(0, 15);

        KeyedStateHandle savepoint;
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("source"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.emptyList(),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            backend.setCurrentKey(key1);
            CobbleStateBackendTest.valueState(backend, "v", "ns").update("only-here");

            savepoint = runCobbleSavepoint(backend);
        }

        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("target"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            backend.setCurrentKey(key1);
            assertEquals(
                    "only-here", CobbleStateBackendTest.valueState(backend, "v", "ns").value());
        }
    }

    // =====================================================================================
    //  Helper: run a Cobble canonical savepoint and return the JM-owned handle
    // =====================================================================================

    /**
     * Runs a canonical savepoint from the given Cobble backend and returns the job-manager-owned
     * {@link KeyedStateHandle}. Returns {@code null} for an empty backend (matching {@code
     * SnapshotResult.empty()}).
     */
    private static KeyedStateHandle runCobbleSavepoint(CobbleKeyedStateBackend<Integer> backend)
            throws Exception {
        SavepointResources<Integer> savepointResources =
                ((CheckpointableKeyedStateBackend<Integer>) backend).savepoint();
        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                new SnapshotStrategyRunner<>(
                                "Cobble canonical savepoint",
                                new SavepointSnapshotStrategy<>(
                                        savepointResources.getSnapshotResources()),
                                new CloseableRegistry(),
                                savepointResources.getPreferredSnapshotExecutionType())
                        .snapshot(
                                1L,
                                System.currentTimeMillis(),
                                new MemCheckpointStreamFactory(1024 * 1024),
                                CheckpointOptions.alignedNoTimeout(
                                        SavepointType.savepoint(SavepointFormatType.CANONICAL),
                                        CheckpointStorageLocationReference.getDefault()));
        snapshotFuture.run();
        SnapshotResult<KeyedStateHandle> snapshotResult = snapshotFuture.get();
        return snapshotResult.getJobManagerOwnedSnapshot();
    }
}
