package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Rescale-restore integration tests for canonical RocksDB savepoints. Verifies that a RocksDB
 * canonical savepoint created at one parallelism can be restored into Cobble backends at a
 * different parallelism, that each subtask imports only its own KeyGroupRange, and that the full
 * RocksDB-canonical → Cobble → checkpoint → Cobble-restore closure works.
 *
 * <p>Flink's {@code StateAssignmentOperation.extractIntersectingState} calls {@code
 * KeyGroupsSavepointStateHandle.getIntersection(targetRange)} in the JobManager before any handle
 * reaches a backend. Since that JM step does not run in a unit test, these tests call {@code
 * getIntersection} directly — the exact same method — to simulate what each target subtask would
 * receive. No new production code is required: Cobble's {@code CanonicalSavepointRestoreOperation}
 * delegates key-group iteration to Flink's {@code FullSnapshotRestoreOperation}, which only yields
 * in-range groups.
 *
 * <p>This class does NOT extend {@code CobbleStateBackendTest} (which would inherit all 153
 * parent @Test methods under JUnit 5). Instead it composes a {@code CobbleStateBackendTest}
 * instance to access its package-private helper methods.
 */
class CobbleCanonicalRescaleTest {

    private static final Configuration EMPTY_CONFIG = new Configuration();

    /** Composed delegate for accessing shared package-private helpers (no @Test inheritance). */
    private final CobbleStateBackendTest helpers = new CobbleStateBackendTest();

    // =====================================================================================
    // Scale-out: parallelism 1 → 2 / 4
    // =====================================================================================

    @Test
    void scaleOutFromParallelism1To2(@TempDir Path tempDir) throws Exception {
        int keyLeft = CobbleStateBackendTest.findKeyForGroup(2);
        int keyRight = CobbleStateBackendTest.findKeyForGroup(10);
        KeyGroupRange fullRange = KeyGroupRange.of(0, 15);

        KeyedStateHandle savepoint =
                helpers.createRocksDbCanonicalAllStateKindsSavepoint(
                        tempDir, keyLeft, keyRight, fullRange);
        KeyGroupsSavepointStateHandle canonical = (KeyGroupsSavepointStateHandle) savepoint;

        // Subtask 0: range [0,7]. The JM would intersect the handle down to [0,7].
        KeyedStateHandle subtask0Handle = canonical.getIntersection(KeyGroupRange.of(0, 7));
        try (CobbleStateBackendTest.TestBackendContext ctx =
                subtaskBackend(tempDir, "subtask-0", subtask0Handle, 0, 7)) {
            // keyLeft (group 2) is in [0,7] — present with all state kinds.
            assertAllStateKindsPresent(
                    ctx.cobbleBackend,
                    keyLeft,
                    "ns-a",
                    "value-k1-ns-a",
                    Arrays.asList("k1-a-left", "k1-a-right"),
                    "k1-a",
                    "map-k1-a",
                    "k1-null",
                    3,
                    "sum=7");
            assertAllStateKindsPresent(
                    ctx.cobbleBackend,
                    keyLeft,
                    "ns-b",
                    "value-k1-ns-b",
                    Arrays.asList("k1-b-only"),
                    "k1-b",
                    "map-k1-b",
                    null,
                    5,
                    "sum=11");
            // keyRight (group 10) is outside [0,7]; getIntersection already excluded its groups.
            // We cannot setCurrentKey to an out-of-range key, so we verify absence with an
            // in-range key that was never seeded.
            int unseededInRange = CobbleStateBackendTest.findKeyForGroup(5);
            assertKeyAbsent(ctx.cobbleBackend, unseededInRange);
        }

        // Subtask 1: range [8,15].
        KeyedStateHandle subtask1Handle = canonical.getIntersection(KeyGroupRange.of(8, 15));
        try (CobbleStateBackendTest.TestBackendContext ctx =
                subtaskBackend(tempDir, "subtask-1", subtask1Handle, 8, 15)) {
            assertAllStateKindsPresent(
                    ctx.cobbleBackend,
                    keyRight,
                    "ns-a",
                    "value-k2-ns-a",
                    Arrays.asList("k2-a-left", "k2-a-right"),
                    "k2-a",
                    "map-k2-a",
                    "k2-null",
                    30,
                    "sum=70");
            // keyLeft (group 2) is outside [8,15]; verify absence with an in-range unseeded key.
            int unseededInRange = CobbleStateBackendTest.findKeyForGroup(12);
            assertKeyAbsent(ctx.cobbleBackend, unseededInRange);
        }
    }

    @Test
    void scaleOutFromParallelism1To4(@TempDir Path tempDir) throws Exception {
        // keyA → group 1 ([0,3]), keyB → group 9 ([8,11]).
        int keyA = CobbleStateBackendTest.findKeyForGroup(1);
        int keyB = CobbleStateBackendTest.findKeyForGroup(9);
        KeyGroupRange fullRange = KeyGroupRange.of(0, 15);

        KeyedStateHandle savepoint =
                helpers.createRocksDbCanonicalAllStateKindsSavepoint(
                        tempDir, keyA, keyB, fullRange);
        KeyGroupsSavepointStateHandle canonical = (KeyGroupsSavepointStateHandle) savepoint;

        // Subtask 0: [0,3] — should see keyA (group 1) only.
        try (CobbleStateBackendTest.TestBackendContext ctx =
                subtaskBackend(
                        tempDir,
                        "sub-0",
                        canonical.getIntersection(KeyGroupRange.of(0, 3)),
                        0,
                        3)) {
            assertAllStateKindsPresent(
                    ctx.cobbleBackend,
                    keyA,
                    "ns-a",
                    "value-k1-ns-a",
                    Arrays.asList("k1-a-left", "k1-a-right"),
                    "k1-a",
                    "map-k1-a",
                    "k1-null",
                    3,
                    "sum=7");
            // keyB (group 9) is outside [0,3]; verify with an unseeded in-range key.
            assertKeyAbsent(ctx.cobbleBackend, CobbleStateBackendTest.findKeyForGroup(2));
        }

        // Subtask 1: [4,7] — neither keyA (group 1) nor keyB (group 9) is in range.
        try (CobbleStateBackendTest.TestBackendContext ctx =
                subtaskBackend(
                        tempDir,
                        "sub-1",
                        canonical.getIntersection(KeyGroupRange.of(4, 7)),
                        4,
                        7)) {
            assertKeyAbsent(ctx.cobbleBackend, CobbleStateBackendTest.findKeyForGroup(5));
        }

        // Subtask 2: [8,11] — should see keyB (group 9) only.
        try (CobbleStateBackendTest.TestBackendContext ctx =
                subtaskBackend(
                        tempDir,
                        "sub-2",
                        canonical.getIntersection(KeyGroupRange.of(8, 11)),
                        8,
                        11)) {
            assertAllStateKindsPresent(
                    ctx.cobbleBackend,
                    keyB,
                    "ns-a",
                    "value-k2-ns-a",
                    Arrays.asList("k2-a-left", "k2-a-right"),
                    "k2-a",
                    "map-k2-a",
                    "k2-null",
                    30,
                    "sum=70");
            assertKeyAbsent(ctx.cobbleBackend, CobbleStateBackendTest.findKeyForGroup(10));
        }

        // Subtask 3: [12,15] — neither key is in range.
        try (CobbleStateBackendTest.TestBackendContext ctx =
                subtaskBackend(
                        tempDir,
                        "sub-3",
                        canonical.getIntersection(KeyGroupRange.of(12, 15)),
                        12,
                        15)) {
            assertKeyAbsent(ctx.cobbleBackend, CobbleStateBackendTest.findKeyForGroup(13));
        }
    }

    // =====================================================================================
    // Scale-in: parallelism 2 / 4 → 1
    // =====================================================================================

    @Test
    void scaleInFromParallelism2To1(@TempDir Path tempDir) throws Exception {
        int keyLeft = CobbleStateBackendTest.findKeyForGroup(2);
        int keyLeftDummy = CobbleStateBackendTest.findKeyForGroup(3);
        int keyRight = CobbleStateBackendTest.findKeyForGroup(10);
        int keyRightDummy = CobbleStateBackendTest.findKeyForGroup(11);

        // Two savepoints: subtask A at [0,7], subtask B at [8,15].
        // Pass a distinct dummy key2 in the same range so the two seeds don't collide.
        KeyedStateHandle handleA =
                helpers.createRocksDbCanonicalAllStateKindsSavepoint(
                        tempDir.resolve("source-a"), keyLeft, keyLeftDummy, KeyGroupRange.of(0, 7));
        KeyedStateHandle handleB =
                helpers.createRocksDbCanonicalAllStateKindsSavepoint(
                        tempDir.resolve("source-b"),
                        keyRight,
                        keyRightDummy,
                        KeyGroupRange.of(8, 15));

        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("merged"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Arrays.asList(handleA, handleB),
                        KeyGroupRange.of(0, 15),
                        EMPTY_CONFIG)) {
            // Both keys' state should be present after merging two handles into one backend.
            assertAllStateKindsPresent(
                    ctx.cobbleBackend,
                    keyLeft,
                    "ns-a",
                    "value-k1-ns-a",
                    Arrays.asList("k1-a-left", "k1-a-right"),
                    "k1-a",
                    "map-k1-a",
                    "k1-null",
                    3,
                    "sum=7");
            assertAllStateKindsPresent(
                    ctx.cobbleBackend,
                    keyLeft,
                    "ns-b",
                    "value-k1-ns-b",
                    Arrays.asList("k1-b-only"),
                    "k1-b",
                    "map-k1-b",
                    null,
                    5,
                    "sum=11");
            assertAllStateKindsPresent(
                    ctx.cobbleBackend,
                    keyRight,
                    "ns-a",
                    "value-k1-ns-a",
                    Arrays.asList("k1-a-left", "k1-a-right"),
                    "k1-a",
                    "map-k1-a",
                    "k1-null",
                    3,
                    "sum=7");
            assertAllStateKindsPresent(
                    ctx.cobbleBackend,
                    keyRight,
                    "ns-b",
                    "value-k1-ns-b",
                    Arrays.asList("k1-b-only"),
                    "k1-b",
                    "map-k1-b",
                    null,
                    5,
                    "sum=11");
        }
    }

    @Test
    void scaleInFromParallelism4To1(@TempDir Path tempDir) throws Exception {
        int[] keys = {
            CobbleStateBackendTest.findKeyForGroup(1),
            CobbleStateBackendTest.findKeyForGroup(5),
            CobbleStateBackendTest.findKeyForGroup(9),
            CobbleStateBackendTest.findKeyForGroup(13)
        };
        int[] dummyKeys = {
            CobbleStateBackendTest.findKeyForGroup(2),
            CobbleStateBackendTest.findKeyForGroup(6),
            CobbleStateBackendTest.findKeyForGroup(10),
            CobbleStateBackendTest.findKeyForGroup(14)
        };
        KeyGroupRange[] sourceRanges = {
            KeyGroupRange.of(0, 3),
            KeyGroupRange.of(4, 7),
            KeyGroupRange.of(8, 11),
            KeyGroupRange.of(12, 15)
        };

        KeyedStateHandle[] handles = new KeyedStateHandle[4];
        for (int i = 0; i < 4; i++) {
            handles[i] =
                    helpers.createRocksDbCanonicalAllStateKindsSavepoint(
                            tempDir.resolve("source-" + i), keys[i], dummyKeys[i], sourceRanges[i]);
        }

        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("merged"),
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Arrays.asList(handles),
                        KeyGroupRange.of(0, 15),
                        EMPTY_CONFIG)) {
            for (int i = 0; i < 4; i++) {
                assertAllStateKindsPresent(
                        ctx.cobbleBackend,
                        keys[i],
                        "ns-a",
                        "value-k1-ns-a",
                        Arrays.asList("k1-a-left", "k1-a-right"),
                        "k1-a",
                        "map-k1-a",
                        "k1-null",
                        3,
                        "sum=7");
                assertAllStateKindsPresent(
                        ctx.cobbleBackend,
                        keys[i],
                        "ns-b",
                        "value-k1-ns-b",
                        Arrays.asList("k1-b-only"),
                        "k1-b",
                        "map-k1-b",
                        null,
                        5,
                        "sum=11");
            }
        }
    }

    // =====================================================================================
    // Closure: rescale + mutate + Cobble checkpoint + Cobble restore
    // =====================================================================================

    @Test
    void scaleOutThenCheckpointThenRestoreClosure(@TempDir Path tempDir) throws Exception {
        int keyLeft = CobbleStateBackendTest.findKeyForGroup(2);
        int keyRight = CobbleStateBackendTest.findKeyForGroup(10);
        KeyGroupRange fullRange = KeyGroupRange.of(0, 15);

        KeyedStateHandle savepoint =
                helpers.createRocksDbCanonicalAllStateKindsSavepoint(
                        tempDir, keyLeft, keyRight, fullRange);
        KeyGroupsSavepointStateHandle canonical = (KeyGroupsSavepointStateHandle) savepoint;

        String checkpointDirectory = tempDir.resolve("checkpoints").toUri().toString();
        KeyedStateHandle cobbleCheckpoint;

        // Stage 1: restore subtask 0 at [0,7], mutate every state kind, snapshot.
        KeyedStateHandle subtask0Handle = canonical.getIntersection(KeyGroupRange.of(0, 7));
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("imported-subtask-0"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(subtask0Handle),
                        KeyGroupRange.of(0, 7),
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;

            backend.setCurrentKey(keyLeft);
            assertEquals(
                    "value-k1-ns-a",
                    CobbleStateBackendTest.valueState(backend, "value-all", "ns-a").value());

            CobbleStateBackendTest.valueState(backend, "value-all", "ns-a")
                    .update("value-k1-mutated");
            CobbleStateBackendTest.listState(backend, "list-all", "ns-a").add("k1-a-tail");
            MapState<String, String> map =
                    CobbleStateBackendTest.mapState(backend, "map-all", "ns-a");
            map.put("k1-a", "map-k1-a-mutated");
            map.remove("k1-null");
            map.put("k1-new", "map-k1-new");
            CobbleStateBackendTest.reducingState(backend, "reducing-all", "ns-a").add(100);
            CobbleStateBackendTest.aggregatingState(backend, "aggregating-all", "ns-a").add(200);

            cobbleCheckpoint = CobbleStateBackendTest.runCheckpointSnapshot(backend, 401L);
            backend.notifyCheckpointComplete(401L);
        }

        // Stage 2: restore from the Cobble checkpoint at the same [0,7] range.
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("restored-subtask-0"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(cobbleCheckpoint),
                        KeyGroupRange.of(0, 7),
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
            backend.setCurrentKey(keyLeft);

            assertEquals(
                    "value-k1-mutated",
                    CobbleStateBackendTest.valueState(backend, "value-all", "ns-a").value());
            assertEquals(
                    Arrays.asList("k1-a-left", "k1-a-right", "k1-a-tail"),
                    CobbleStateBackendTest.toList(
                            CobbleStateBackendTest.listState(backend, "list-all", "ns-a").get()));
            MapState<String, String> map =
                    CobbleStateBackendTest.mapState(backend, "map-all", "ns-a");
            assertFalse(map.contains("k1-null"));
            assertEquals("map-k1-a-mutated", map.get("k1-a"));
            assertEquals("map-k1-new", map.get("k1-new"));
            assertEquals(
                    Integer.valueOf(103),
                    CobbleStateBackendTest.reducingState(backend, "reducing-all", "ns-a").get());
            assertEquals(
                    "sum=207",
                    CobbleStateBackendTest.aggregatingState(backend, "aggregating-all", "ns-a")
                            .get());
        }
    }

    @Test
    void scaleInThenCheckpointThenRestoreClosure(@TempDir Path tempDir) throws Exception {
        int keyLeft = CobbleStateBackendTest.findKeyForGroup(2);
        int keyLeftDummy = CobbleStateBackendTest.findKeyForGroup(3);
        int keyRight = CobbleStateBackendTest.findKeyForGroup(10);
        int keyRightDummy = CobbleStateBackendTest.findKeyForGroup(11);

        KeyedStateHandle handleA =
                helpers.createRocksDbCanonicalAllStateKindsSavepoint(
                        tempDir.resolve("source-a"), keyLeft, keyLeftDummy, KeyGroupRange.of(0, 7));
        KeyedStateHandle handleB =
                helpers.createRocksDbCanonicalAllStateKindsSavepoint(
                        tempDir.resolve("source-b"),
                        keyRight,
                        keyRightDummy,
                        KeyGroupRange.of(8, 15));

        String checkpointDirectory = tempDir.resolve("checkpoints").toUri().toString();
        KeyedStateHandle cobbleCheckpoint;

        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("merged"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Arrays.asList(handleA, handleB),
                        KeyGroupRange.of(0, 15),
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;

            // keyLeft was seeded as key1 (reducing=3, aggregating=sum=7).
            backend.setCurrentKey(keyLeft);
            CobbleStateBackendTest.valueState(backend, "value-all", "ns-a")
                    .update("value-left-mutated");
            CobbleStateBackendTest.reducingState(backend, "reducing-all", "ns-a").add(100);
            CobbleStateBackendTest.aggregatingState(backend, "aggregating-all", "ns-a").add(200);

            backend.setCurrentKey(keyRight);
            CobbleStateBackendTest.valueState(backend, "value-all", "ns-a")
                    .update("value-right-mutated");
            CobbleStateBackendTest.reducingState(backend, "reducing-all", "ns-a").add(1000);
            CobbleStateBackendTest.aggregatingState(backend, "aggregating-all", "ns-a").add(2000);

            cobbleCheckpoint = CobbleStateBackendTest.runCheckpointSnapshot(backend, 501L);
            backend.notifyCheckpointComplete(501L);
        }

        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("restored"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(cobbleCheckpoint),
                        KeyGroupRange.of(0, 15),
                        EMPTY_CONFIG)) {
            CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;

            backend.setCurrentKey(keyLeft);
            assertEquals(
                    "value-left-mutated",
                    CobbleStateBackendTest.valueState(backend, "value-all", "ns-a").value());
            assertEquals(
                    Integer.valueOf(103),
                    CobbleStateBackendTest.reducingState(backend, "reducing-all", "ns-a").get());
            assertEquals(
                    "sum=207",
                    CobbleStateBackendTest.aggregatingState(backend, "aggregating-all", "ns-a")
                            .get());

            backend.setCurrentKey(keyRight);
            assertEquals(
                    "value-right-mutated",
                    CobbleStateBackendTest.valueState(backend, "value-all", "ns-a").value());
            assertEquals(
                    Integer.valueOf(1003),
                    CobbleStateBackendTest.reducingState(backend, "reducing-all", "ns-a").get());
            assertEquals(
                    "sum=2007",
                    CobbleStateBackendTest.aggregatingState(backend, "aggregating-all", "ns-a")
                            .get());
        }
    }

    // =====================================================================================
    // Helpers
    // =====================================================================================

    private CobbleStateBackendTest.TestBackendContext subtaskBackend(
            Path tempDir, String name, KeyedStateHandle handle, int start, int end)
            throws Exception {
        return helpers.createBackendContext(
                tempDir.resolve(name),
                false,
                null,
                null,
                TtlTimeProvider.DEFAULT,
                false,
                Collections.singletonList(handle),
                KeyGroupRange.of(start, end),
                EMPTY_CONFIG);
    }

    /**
     * Asserts all 5 state kinds are present for the given key+namespace with the expected values,
     * including present-null MapState semantics.
     *
     * @param nullMapKey the map key whose value should be present-null, or {@code null} to skip the
     *     present-null check (e.g. for ns-b which has no null entry)
     */
    private void assertAllStateKindsPresent(
            CobbleKeyedStateBackend<Integer> backend,
            int key,
            String namespace,
            String expectedValue,
            List<String> expectedList,
            String mapKey,
            String expectedMapValue,
            String nullMapKey,
            int expectedReducing,
            String expectedAggregating)
            throws Exception {
        backend.setCurrentKey(key);

        assertEquals(
                expectedValue,
                CobbleStateBackendTest.valueState(backend, "value-all", namespace).value());
        assertEquals(
                expectedList,
                CobbleStateBackendTest.toList(
                        CobbleStateBackendTest.listState(backend, "list-all", namespace).get()));

        MapState<String, String> map =
                CobbleStateBackendTest.mapState(backend, "map-all", namespace);
        if (nullMapKey != null) {
            assertTrue(
                    map.contains(nullMapKey),
                    "present-null map key '" + nullMapKey + "' should exist");
            assertNull(map.get(nullMapKey), "present-null map value should be null");
        }
        assertTrue(map.contains(mapKey));
        assertEquals(expectedMapValue, map.get(mapKey));

        assertEquals(
                Integer.valueOf(expectedReducing),
                CobbleStateBackendTest.reducingState(backend, "reducing-all", namespace).get());
        assertEquals(
                expectedAggregating,
                CobbleStateBackendTest.aggregatingState(backend, "aggregating-all", namespace)
                        .get());
    }

    /**
     * Asserts that an in-range key has no state at all (all state kinds return null/empty/absent).
     * The key must belong to the backend's KeyGroupRange.
     */
    private void assertKeyAbsent(CobbleKeyedStateBackend<Integer> backend, int key)
            throws Exception {
        backend.setCurrentKey(key);
        assertNull(CobbleStateBackendTest.valueState(backend, "value-all", "ns-a").value());
        assertNull(CobbleStateBackendTest.listState(backend, "list-all", "ns-a").get());
        assertFalse(CobbleStateBackendTest.mapState(backend, "map-all", "ns-a").contains("k1-a"));
        assertFalse(CobbleStateBackendTest.mapState(backend, "map-all", "ns-a").contains("k2-a"));
        assertNull(CobbleStateBackendTest.reducingState(backend, "reducing-all", "ns-a").get());
        assertNull(
                CobbleStateBackendTest.aggregatingState(backend, "aggregating-all", "ns-a").get());
    }
}
