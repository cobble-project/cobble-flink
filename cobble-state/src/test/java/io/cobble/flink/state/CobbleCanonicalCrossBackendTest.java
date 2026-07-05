package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
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
import java.util.List;
import java.util.concurrent.RunnableFuture;

/**
 * Cross-backend compatibility tests for canonical savepoints produced by Cobble.
 *
 * <p>These tests create state in a Cobble backend, produce a canonical savepoint via {@code
 * savepoint()}, then restore it into RocksDB and Heap backends — verifying that Cobble-generated
 * canonical savepoints are portable across Flink backends. The reverse direction (RocksDB → Cobble)
 * is already covered by {@link CobbleCanonicalRescaleTest} and {@link
 * CobbleCanonicalTimerRestoreTest}.
 *
 * <p>This class does NOT extend {@code CobbleStateBackendTest} (which would inherit all ~150 parent
 * {@code @Test} methods under JUnit 5). Instead it composes a {@code CobbleStateBackendTest}
 * instance to access its package-private helper methods.
 */
class CobbleCanonicalCrossBackendTest {

    private static final Configuration EMPTY_CONFIG = new Configuration();
    private static final String TIMER_SERVICE_NAME = "cross-backend-timers";
    private static final String ET_QUEUE = "_timer_state/event_" + TIMER_SERVICE_NAME;

    /** Composed delegate for accessing shared package-private helpers (no @Test inheritance). */
    private final CobbleStateBackendTest helpers = new CobbleStateBackendTest();

    // =====================================================================================
    //  Cobble → RocksDB: all 5 state kinds
    // =====================================================================================

    @Test
    void cobbleSavepointRestoresIntoRocksDB(@TempDir Path tempDir) throws Exception {
        int key1 = CobbleStateBackendTest.findKeyForGroup(3);
        int key2 = CobbleStateBackendTest.findKeyForGroup(10);
        KeyGroupRange range = KeyGroupRange.of(0, 15);

        KeyedStateHandle savepoint =
                helpers.createCobbleCanonicalAllStateKindsSavepoint(tempDir, key1, key2, range);

        try (CobbleStateBackendTest.RocksDbBackendContext ctx =
                helpers.createRocksDbBackendForRestore(
                        tempDir.resolve("rocksdb-restore"),
                        range,
                        Collections.singletonList(savepoint))) {
            AbstractKeyedStateBackend<Integer> backend = ctx.backend;
            CobbleStateBackendTest.assertAllStateKindsPresent(
                    backend,
                    key1,
                    "ns-a",
                    "value-k1-ns-a",
                    Arrays.asList("k1-a-left", "k1-a-right"),
                    "k1-a",
                    "map-k1-a",
                    "k1-null",
                    3,
                    "sum=7");
            CobbleStateBackendTest.assertAllStateKindsPresent(
                    backend,
                    key1,
                    "ns-b",
                    "value-k1-ns-b",
                    Arrays.asList("k1-b-only"),
                    "k1-b",
                    "map-k1-b",
                    null,
                    5,
                    "sum=11");
            CobbleStateBackendTest.assertAllStateKindsPresent(
                    backend,
                    key2,
                    "ns-a",
                    "value-k2-ns-a",
                    Arrays.asList("k2-a-left", "k2-a-right"),
                    "k2-a",
                    "map-k2-a",
                    "k2-null",
                    30,
                    "sum=70");
        }
    }

    // =====================================================================================
    //  Cobble → Heap: all 5 state kinds
    // =====================================================================================

    @Test
    void cobbleSavepointRestoresIntoHeap(@TempDir Path tempDir) throws Exception {
        int key1 = CobbleStateBackendTest.findKeyForGroup(3);
        int key2 = CobbleStateBackendTest.findKeyForGroup(10);
        KeyGroupRange range = KeyGroupRange.of(0, 15);

        KeyedStateHandle savepoint =
                helpers.createCobbleCanonicalAllStateKindsSavepoint(tempDir, key1, key2, range);

        try (CobbleStateBackendTest.HeapBackendContext ctx =
                helpers.createHeapBackendForRestore(range, Collections.singletonList(savepoint))) {
            AbstractKeyedStateBackend<Integer> backend = ctx.backend;
            CobbleStateBackendTest.assertAllStateKindsPresent(
                    backend,
                    key1,
                    "ns-a",
                    "value-k1-ns-a",
                    Arrays.asList("k1-a-left", "k1-a-right"),
                    "k1-a",
                    "map-k1-a",
                    "k1-null",
                    3,
                    "sum=7");
            CobbleStateBackendTest.assertAllStateKindsPresent(
                    backend,
                    key1,
                    "ns-b",
                    "value-k1-ns-b",
                    Arrays.asList("k1-b-only"),
                    "k1-b",
                    "map-k1-b",
                    null,
                    5,
                    "sum=11");
            CobbleStateBackendTest.assertAllStateKindsPresent(
                    backend,
                    key2,
                    "ns-a",
                    "value-k2-ns-a",
                    Arrays.asList("k2-a-left", "k2-a-right"),
                    "k2-a",
                    "map-k2-a",
                    "k2-null",
                    30,
                    "sum=70");
        }
    }

    // =====================================================================================
    //  Cobble → RocksDB: rescale (key-group intersection)
    // =====================================================================================

    @Test
    void cobbleSavepointRescaleIntoRocksDB(@TempDir Path tempDir) throws Exception {
        int keyLeft = CobbleStateBackendTest.findKeyForGroup(2);
        int keyRight = CobbleStateBackendTest.findKeyForGroup(10);
        KeyGroupRange fullRange = KeyGroupRange.of(0, 15);

        KeyedStateHandle savepoint =
                helpers.createCobbleCanonicalAllStateKindsSavepoint(
                        tempDir, keyLeft, keyRight, fullRange);
        KeyGroupsSavepointStateHandle canonical = (KeyGroupsSavepointStateHandle) savepoint;

        // Subtask 0: range [0,7]. keyLeft (group 2) is in range; keyRight (group 10) is not.
        KeyedStateHandle subtask0Handle = canonical.getIntersection(KeyGroupRange.of(0, 7));
        try (CobbleStateBackendTest.RocksDbBackendContext ctx =
                helpers.createRocksDbBackendForRestore(
                        tempDir.resolve("rocksdb-sub0"),
                        KeyGroupRange.of(0, 7),
                        Collections.singletonList(subtask0Handle))) {
            AbstractKeyedStateBackend<Integer> backend = ctx.backend;
            CobbleStateBackendTest.assertAllStateKindsPresent(
                    backend,
                    keyLeft,
                    "ns-a",
                    "value-k1-ns-a",
                    Arrays.asList("k1-a-left", "k1-a-right"),
                    "k1-a",
                    "map-k1-a",
                    "k1-null",
                    3,
                    "sum=7");
            // keyRight (group 10) is outside [0,7]; verify absence with an in-range unseeded key.
            CobbleStateBackendTest.assertKeyAbsent(
                    backend, CobbleStateBackendTest.findKeyForGroup(5));
        }

        // Subtask 1: range [8,15]. keyRight (group 10) is in range; keyLeft (group 2) is not.
        KeyedStateHandle subtask1Handle = canonical.getIntersection(KeyGroupRange.of(8, 15));
        try (CobbleStateBackendTest.RocksDbBackendContext ctx =
                helpers.createRocksDbBackendForRestore(
                        tempDir.resolve("rocksdb-sub1"),
                        KeyGroupRange.of(8, 15),
                        Collections.singletonList(subtask1Handle))) {
            AbstractKeyedStateBackend<Integer> backend = ctx.backend;
            CobbleStateBackendTest.assertAllStateKindsPresent(
                    backend,
                    keyRight,
                    "ns-a",
                    "value-k2-ns-a",
                    Arrays.asList("k2-a-left", "k2-a-right"),
                    "k2-a",
                    "map-k2-a",
                    "k2-null",
                    30,
                    "sum=70");
            CobbleStateBackendTest.assertKeyAbsent(
                    backend, CobbleStateBackendTest.findKeyForGroup(12));
        }
    }

    // =====================================================================================
    //  Cobble → RocksDB: timers (VoidNamespace)
    // =====================================================================================

    @Test
    void cobbleTimerSavepointRestoresIntoRocksDB(@TempDir Path tempDir) throws Exception {
        int keyA = CobbleStateBackendTest.findKeyForGroup(3);
        int keyB = CobbleStateBackendTest.findKeyForGroup(7);
        KeyGroupRange range = KeyGroupRange.of(0, 15);

        KeyedStateHandle savepoint =
                helpers.createCobbleCanonicalTimerSavepoint(
                        tempDir,
                        range,
                        TIMER_SERVICE_NAME,
                        (backend, queue) -> {
                            backend.setCurrentKey(keyA);
                            queue.add(
                                    new TimerHeapInternalTimer<>(
                                            100L, keyA, VoidNamespace.INSTANCE));
                            backend.setCurrentKey(keyB);
                            queue.add(
                                    new TimerHeapInternalTimer<>(
                                            200L, keyB, VoidNamespace.INSTANCE));
                        });

        try (CobbleStateBackendTest.RocksDbBackendContext ctx =
                helpers.createRocksDbBackendForRestore(
                        tempDir.resolve("rocksdb-timer-restore"),
                        range,
                        Collections.singletonList(savepoint))) {
            AbstractKeyedStateBackend<Integer> backend = ctx.backend;

            // Read timers through the RocksDB priority queue (same API as Cobble's create()).
            @SuppressWarnings("unchecked")
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, VoidNamespace>> queue =
                    (KeyGroupedInternalPriorityQueue<
                                    TimerHeapInternalTimer<Integer, VoidNamespace>>)
                            ((CheckpointableKeyedStateBackend<Integer>) backend)
                                    .create(
                                            ET_QUEUE,
                                            new TimerSerializer<>(
                                                    IntSerializer.INSTANCE,
                                                    VoidNamespaceSerializer.INSTANCE));

            List<TimerHeapInternalTimer<Integer, VoidNamespace>> timers = pollAll(queue);
            assertEquals(2, timers.size());
            assertEquals(100L, timers.get(0).getTimestamp());
            assertEquals(keyA, timers.get(0).getKey());
            assertEquals(200L, timers.get(1).getTimestamp());
            assertEquals(keyB, timers.get(1).getKey());
        }
    }

    // =====================================================================================
    //  Closure: Cobble → RocksDB → Cobble
    // =====================================================================================

    @Test
    void cobbleSavepointRoundTripThroughRocksDB(@TempDir Path tempDir) throws Exception {
        int key1 = CobbleStateBackendTest.findKeyForGroup(3);
        int key2 = CobbleStateBackendTest.findKeyForGroup(10);
        KeyGroupRange range = KeyGroupRange.of(0, 15);

        // Phase 1: Cobble → canonical savepoint.
        KeyedStateHandle cobbleSavepoint =
                helpers.createCobbleCanonicalAllStateKindsSavepoint(tempDir, key1, key2, range);

        // Phase 2: Restore into RocksDB, then produce a RocksDB canonical savepoint.
        KeyedStateHandle rocksDbSavepoint;
        try (CobbleStateBackendTest.RocksDbBackendContext ctx =
                helpers.createRocksDbBackendForRestore(
                        tempDir.resolve("rocksdb-phase2"),
                        range,
                        Collections.singletonList(cobbleSavepoint))) {
            AbstractKeyedStateBackend<Integer> backend = ctx.backend;
            rocksDbSavepoint = runRocksDbSavepoint(backend);
        }

        // Phase 3: Restore the RocksDB savepoint back into Cobble and verify.
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("cobble-phase3"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(rocksDbSavepoint),
                        range,
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            CobbleStateBackendTest.assertAllStateKindsPresent(
                    backend,
                    key1,
                    "ns-a",
                    "value-k1-ns-a",
                    Arrays.asList("k1-a-left", "k1-a-right"),
                    "k1-a",
                    "map-k1-a",
                    "k1-null",
                    3,
                    "sum=7");
            CobbleStateBackendTest.assertAllStateKindsPresent(
                    backend,
                    key2,
                    "ns-a",
                    "value-k2-ns-a",
                    Arrays.asList("k2-a-left", "k2-a-right"),
                    "k2-a",
                    "map-k2-a",
                    "k2-null",
                    30,
                    "sum=70");
        }
    }

    // =====================================================================================
    //  Present-null MAP: Cobble → RocksDB and Cobble → Heap
    // =====================================================================================

    @Test
    void cobbleSavepointWithPresentNullMapRestoresIntoRocksDB(@TempDir Path tempDir)
            throws Exception {
        int key = CobbleStateBackendTest.findKeyForGroup(5);
        KeyGroupRange range = KeyGroupRange.of(0, 15);

        KeyedStateHandle savepoint;
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("cobble-source"),
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
            MapState<String, String> map =
                    CobbleStateBackendTest.mapState(backend, "map-null-test", "ns");
            map.put("present-null", null);
            map.put("present-value", "hello");
            savepoint = CobbleStateBackendTest.runCobbleSavepoint(backend);
        }

        try (CobbleStateBackendTest.RocksDbBackendContext ctx =
                helpers.createRocksDbBackendForRestore(
                        tempDir.resolve("rocksdb-restore"),
                        range,
                        Collections.singletonList(savepoint))) {
            AbstractKeyedStateBackend<Integer> backend = ctx.backend;
            backend.setCurrentKey(key);
            MapState<String, String> map =
                    CobbleStateBackendTest.mapState(backend, "map-null-test", "ns");
            assertTrue(map.contains("present-null"), "present-null key should exist");
            assertNull(map.get("present-null"), "present-null value should be null");
            assertTrue(map.contains("present-value"));
            assertEquals("hello", map.get("present-value"));
        }
    }

    @Test
    void cobbleSavepointWithPresentNullMapRestoresIntoHeap(@TempDir Path tempDir) throws Exception {
        int key = CobbleStateBackendTest.findKeyForGroup(5);
        KeyGroupRange range = KeyGroupRange.of(0, 15);

        KeyedStateHandle savepoint;
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("cobble-source"),
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
            MapState<String, String> map =
                    CobbleStateBackendTest.mapState(backend, "map-null-test", "ns");
            map.put("present-null", null);
            map.put("present-value", "hello");
            savepoint = CobbleStateBackendTest.runCobbleSavepoint(backend);
        }

        try (CobbleStateBackendTest.HeapBackendContext ctx =
                helpers.createHeapBackendForRestore(range, Collections.singletonList(savepoint))) {
            AbstractKeyedStateBackend<Integer> backend = ctx.backend;
            backend.setCurrentKey(key);
            MapState<String, String> map =
                    CobbleStateBackendTest.mapState(backend, "map-null-test", "ns");
            assertTrue(map.contains("present-null"), "present-null key should exist");
            assertNull(map.get("present-null"), "present-null value should be null");
            assertTrue(map.contains("present-value"));
            assertEquals("hello", map.get("present-value"));
        }
    }

    // =====================================================================================
    // Helpers
    // =====================================================================================

    /** Runs a canonical savepoint from a RocksDB (or any) keyed state backend. */
    @SuppressWarnings("unchecked")
    private static KeyedStateHandle runRocksDbSavepoint(AbstractKeyedStateBackend<Integer> backend)
            throws Exception {
        SavepointResources<Integer> savepointResources =
                ((CheckpointableKeyedStateBackend<Integer>) backend).savepoint();
        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                new SnapshotStrategyRunner<>(
                                "RocksDB canonical savepoint",
                                new SavepointSnapshotStrategy<>(
                                        savepointResources.getSnapshotResources()),
                                new org.apache.flink.core.fs.CloseableRegistry(),
                                savepointResources.getPreferredSnapshotExecutionType())
                        .snapshot(
                                1L,
                                System.currentTimeMillis(),
                                new MemCheckpointStreamFactory(1024 * 1024),
                                org.apache.flink.runtime.checkpoint.CheckpointOptions
                                        .alignedNoTimeout(
                                                org.apache.flink.runtime.checkpoint.SavepointType
                                                        .savepoint(
                                                                org.apache.flink.core.execution
                                                                        .SavepointFormatType
                                                                        .CANONICAL),
                                                org.apache.flink.runtime.state
                                                        .CheckpointStorageLocationReference
                                                        .getDefault()));
        snapshotFuture.run();
        SnapshotResult<KeyedStateHandle> snapshotResult = snapshotFuture.get();
        return snapshotResult.getJobManagerOwnedSnapshot();
    }

    private static <N> List<TimerHeapInternalTimer<Integer, N>> pollAll(
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, N>> queue) {
        List<TimerHeapInternalTimer<Integer, N>> drained = new java.util.ArrayList<>();
        TimerHeapInternalTimer<Integer, N> timer;
        while ((timer = queue.poll()) != null) {
            drained.add(timer);
        }
        return drained;
    }
}
