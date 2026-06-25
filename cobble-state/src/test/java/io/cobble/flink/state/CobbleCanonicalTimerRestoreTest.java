package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManagerImpl;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Integration tests verifying that real RocksDB canonical savepoints — produced by {@code
 * RocksDBKeyedStateBackend.savepoint()} with timers registered through Flink's genuine {@code
 * InternalTimerService} — restore correctly into Cobble.
 *
 * <p>Unlike the hand-crafted {@code createCanonicalTimerSavepoint} fixtures (which build the
 * priority-queue bytes by hand), these tests use {@code
 * CobbleStateBackendTest.createRocksDbCanonicalTimerSavepoint} to spin up a real RocksDB backend,
 * register event-time and processing-time timers via {@code registerEventTimeTimer} / {@code
 * registerProcessingTimeTimer}, and let {@code savepoint()} emit the canonical bytes. The resulting
 * savepoint contains two {@code PRIORITY_QUEUE} states named {@code _timer_state/processing_<svc>}
 * and {@code _timer_state/event_<svc>} — exactly the format Cobble's {@code
 * CanonicalSavepointRestoreOperation.RestoredPriorityQueueState} imports.
 *
 * <p>This class does NOT extend {@code CobbleStateBackendTest} (which would inherit all 153
 * parent @Test methods under JUnit 5). Instead it composes a {@code CobbleStateBackendTest}
 * instance to access its package-private helper methods.
 */
class CobbleCanonicalTimerRestoreTest {

    private static final Configuration EMPTY_CONFIG = new Configuration();
    private static final String TIMER_SERVICE_NAME = "user-timers";
    private static final String ET_QUEUE = "_timer_state/event_" + TIMER_SERVICE_NAME;
    private static final String PT_QUEUE = "_timer_state/processing_" + TIMER_SERVICE_NAME;

    /** Composed delegate for accessing shared package-private helpers (no @Test inheritance). */
    private final CobbleStateBackendTest helpers = new CobbleStateBackendTest();

    // =====================================================================================
    // Event-time timers
    // =====================================================================================

    @Test
    void restoresRealRocksDbCanonicalEventTimeTimers(@TempDir Path tempDir) throws Exception {
        int keyA = CobbleStateBackendTest.findKeyForGroup(3);
        int keyB = CobbleStateBackendTest.findKeyForGroup(7);

        KeyedStateHandle savepoint =
                helpers.createRocksDbCanonicalTimerSavepoint(
                        tempDir,
                        KeyGroupRange.of(0, 15),
                        TIMER_SERVICE_NAME,
                        (backend, timerService) -> {
                            backend.setCurrentKey(keyA);
                            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 100L);
                            backend.setCurrentKey(keyB);
                            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 200L);
                        });

        try (CobbleStateBackendTest.TestBackendContext ctx =
                subtaskBackend(tempDir, "restore-full", savepoint, 0, 15)) {
            // Event-time timers restore in ascending-timestamp order (TimerSerializer sign-bit
            // flips the timestamp so byte-lexicographic order matches priority order).
            List<TimerHeapInternalTimer<Integer, VoidNamespace>> etTimers =
                    pollAll(etQueue(ctx.cobbleBackend));
            assertEquals(2, etTimers.size());
            assertEquals(100L, etTimers.get(0).getTimestamp());
            assertEquals(keyA, etTimers.get(0).getKey());
            assertEquals(200L, etTimers.get(1).getTimestamp());
            assertEquals(keyB, etTimers.get(1).getKey());

            // ET/PT separation: no processing-time timers were registered.
            assertTrue(pollAll(ptQueue(ctx.cobbleBackend)).isEmpty());
        }
    }

    // =====================================================================================
    // Processing-time timers
    // =====================================================================================

    @Test
    void restoresRealRocksDbCanonicalProcessingTimeTimers(@TempDir Path tempDir) throws Exception {
        int keyA = CobbleStateBackendTest.findKeyForGroup(2);
        int keyB = CobbleStateBackendTest.findKeyForGroup(11);

        KeyedStateHandle savepoint =
                helpers.createRocksDbCanonicalTimerSavepoint(
                        tempDir,
                        KeyGroupRange.of(0, 15),
                        TIMER_SERVICE_NAME,
                        (backend, timerService) -> {
                            backend.setCurrentKey(keyA);
                            timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, 1000L);
                            backend.setCurrentKey(keyB);
                            timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, 3000L);
                        });

        try (CobbleStateBackendTest.TestBackendContext ctx =
                subtaskBackend(tempDir, "restore-full", savepoint, 0, 15)) {
            List<TimerHeapInternalTimer<Integer, VoidNamespace>> ptTimers =
                    pollAll(ptQueue(ctx.cobbleBackend));
            assertEquals(2, ptTimers.size());
            assertEquals(1000L, ptTimers.get(0).getTimestamp());
            assertEquals(keyA, ptTimers.get(0).getKey());
            assertEquals(3000L, ptTimers.get(1).getTimestamp());
            assertEquals(keyB, ptTimers.get(1).getKey());

            // No event-time timers were registered.
            assertTrue(pollAll(etQueue(ctx.cobbleBackend)).isEmpty());
        }
    }

    // =====================================================================================
    // Multi-key / multi-key-group ordering
    // =====================================================================================

    @Test
    void restoresRealRocksDbCanonicalTimersAcrossKeysAndKeyGroups(@TempDir Path tempDir)
            throws Exception {
        // Two keys in two different key-groups, each with two distinct ET timestamps. This covers
        // multi-key and multi-key-group behavior; namespace isolation is exercised separately by
        // restoresRealRocksDbCanonicalTimersIsolatedByNamespace.
        int keyA = CobbleStateBackendTest.findKeyForGroup(1);
        int keyB = CobbleStateBackendTest.findKeyForGroup(14);
        long[] tsA = {150L, 500L};
        long[] tsB = {50L, 900L};

        KeyedStateHandle savepoint =
                helpers.createRocksDbCanonicalTimerSavepoint(
                        tempDir,
                        KeyGroupRange.of(0, 15),
                        TIMER_SERVICE_NAME,
                        (backend, timerService) -> {
                            backend.setCurrentKey(keyA);
                            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, tsA[0]);
                            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, tsA[1]);
                            backend.setCurrentKey(keyB);
                            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, tsB[0]);
                            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, tsB[1]);
                        });

        try (CobbleStateBackendTest.TestBackendContext ctx =
                subtaskBackend(tempDir, "restore-full", savepoint, 0, 15)) {
            List<TimerHeapInternalTimer<Integer, VoidNamespace>> timers =
                    pollAll(etQueue(ctx.cobbleBackend));
            // All four timers present, in ascending-timestamp order regardless of key-group.
            assertEquals(4, timers.size());
            assertEquals(50L, timers.get(0).getTimestamp());
            assertEquals(keyB, timers.get(0).getKey());
            assertEquals(150L, timers.get(1).getTimestamp());
            assertEquals(keyA, timers.get(1).getKey());
            assertEquals(500L, timers.get(2).getTimestamp());
            assertEquals(keyA, timers.get(2).getKey());
            assertEquals(900L, timers.get(3).getTimestamp());
            assertEquals(keyB, timers.get(3).getKey());
        }
    }

    // =====================================================================================
    // Namespace isolation
    // =====================================================================================

    @Test
    void restoresRealRocksDbCanonicalTimersIsolatedByNamespace(@TempDir Path tempDir)
            throws Exception {
        // The same key is registered under two distinct namespaces ("ns-a", "ns-b") with different
        // event-time timestamps. After restore both timers must be present, each carrying its own
        // namespace — timers registered under one namespace must not leak into the other.
        int key = CobbleStateBackendTest.findKeyForGroup(6);
        String nsA = "ns-a";
        String nsB = "ns-b";

        KeyedStateHandle savepoint =
                helpers.createRocksDbCanonicalTimerSavepoint(
                        tempDir,
                        KeyGroupRange.of(0, 15),
                        TIMER_SERVICE_NAME,
                        StringSerializer.INSTANCE,
                        (backend, timerService) -> {
                            backend.setCurrentKey(key);
                            timerService.registerEventTimeTimer(nsA, 100L);
                            timerService.registerEventTimeTimer(nsB, 200L);
                        });

        try (CobbleStateBackendTest.TestBackendContext ctx =
                subtaskBackend(tempDir, "restore-ns", savepoint, 0, 15)) {
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, String>> queue =
                    ctx.cobbleBackend.create(
                            ET_QUEUE,
                            new TimerSerializer<>(
                                    IntSerializer.INSTANCE, StringSerializer.INSTANCE));
            List<TimerHeapInternalTimer<Integer, String>> timers = pollAll(queue);

            // Both timers survive, in ascending-timestamp order, each with its own namespace.
            assertEquals(2, timers.size());
            assertEquals(100L, timers.get(0).getTimestamp());
            assertEquals(key, timers.get(0).getKey());
            assertEquals(nsA, timers.get(0).getNamespace());
            assertEquals(200L, timers.get(1).getTimestamp());
            assertEquals(key, timers.get(1).getKey());
            assertEquals(nsB, timers.get(1).getNamespace());
        }
    }

    // =====================================================================================
    // Poll does not resurrect consumed timers
    // =====================================================================================

    @Test
    void canonicalTimerRestorePollDoesNotResurrectConsumedTimers(@TempDir Path tempDir)
            throws Exception {
        int key = CobbleStateBackendTest.findKeyForGroup(5);

        KeyedStateHandle savepoint =
                helpers.createRocksDbCanonicalTimerSavepoint(
                        tempDir,
                        KeyGroupRange.of(0, 15),
                        TIMER_SERVICE_NAME,
                        (backend, timerService) -> {
                            backend.setCurrentKey(key);
                            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 42L);
                        });

        try (CobbleStateBackendTest.TestBackendContext ctx =
                subtaskBackend(tempDir, "restore-full", savepoint, 0, 15)) {
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, VoidNamespace>> queue =
                    etQueue(ctx.cobbleBackend);

            // The restored timer polls out exactly once.
            TimerHeapInternalTimer<Integer, VoidNamespace> first = queue.poll();
            assertEquals(42L, first.getTimestamp());
            assertEquals(key, first.getKey());
            assertNull(queue.poll(), "consumed timer must not reappear");

            // A freshly-added timer is independently pollable and does not resurrect the old one.
            queue.add(new TimerHeapInternalTimer<>(88L, key, VoidNamespace.INSTANCE));
            TimerHeapInternalTimer<Integer, VoidNamespace> second = queue.poll();
            assertEquals(88L, second.getTimestamp());
            assertEquals(key, second.getKey());
            assertNull(queue.poll(), "consumed timer must not reappear");
        }
    }

    // =====================================================================================
    // Canonical import → Cobble checkpoint → Cobble restore closure
    // =====================================================================================

    @Test
    void canonicalTimerRestoreSurvivesCobbleCheckpointAndRestoreClosure(@TempDir Path tempDir)
            throws Exception {
        int keyA = CobbleStateBackendTest.findKeyForGroup(4);
        int keyB = CobbleStateBackendTest.findKeyForGroup(9);

        KeyedStateHandle canonicalSavepoint =
                helpers.createRocksDbCanonicalTimerSavepoint(
                        tempDir,
                        KeyGroupRange.of(0, 15),
                        TIMER_SERVICE_NAME,
                        (backend, timerService) -> {
                            backend.setCurrentKey(keyA);
                            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 100L);
                        });

        // Cobble-native checkpoints are incremental handles that reference local files, so both
        // phases must share a checkpoint directory (mirrors the rescale closure test).
        String checkpointDirectory = tempDir.resolve("checkpoints").toUri().toString();
        KeyGroupRange fullRange = KeyGroupRange.of(0, 15);
        KeyedStateHandle cobbleCheckpoint;
        KeyGroupsStateHandle timerRawKeyedState;

        // Phase 1: restore the RocksDB canonical savepoint into Cobble, then drive a Cobble-native
        // checkpoint that captures both the keyed state and the timer overlay (via Flink's legacy
        // synchronous timer snapshot path — Cobble returns true from
        // requiresLegacySynchronousTimerSnapshots, so timers are exported through
        // InternalTimeServiceManager.snapshotToRawKeyedState, not the Cobble DB).
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("restore-canonical"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(canonicalSavepoint),
                        fullRange,
                        EMPTY_CONFIG)) {
            InternalTimeServiceManagerImpl<Integer> timerManager =
                    newTimerManager(ctx.cobbleBackend);
            InternalTimerService<VoidNamespace> timerService =
                    timerManager.getInternalTimerService(
                            TIMER_SERVICE_NAME,
                            IntSerializer.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new NoOpTriggerable());

            // The canonical-imported timer is present. forEachEventTimeTimer is non-destructive
            // (it iterates the queue) and forces the native queue to be prefetched into the
            // in-memory overlay that the legacy timer snapshot exports.
            List<Long> restoredTimestamps = new ArrayList<>();
            timerService.forEachEventTimeTimer((ns, ts) -> restoredTimestamps.add(ts));
            assertEquals(Collections.singletonList(100L), restoredTimestamps);

            // Register a new ET timer on the Cobble side, then re-trigger the prefetch so the
            // new timer also lands in the overlay before the snapshot.
            ctx.cobbleBackend.setCurrentKey(keyB);
            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 200L);
            timerService.forEachEventTimeTimer((ns, ts) -> {});

            cobbleCheckpoint = CobbleStateBackendTest.runCheckpointSnapshot(ctx.cobbleBackend, 2L);
            ctx.cobbleBackend.notifyCheckpointComplete(2L);
            timerRawKeyedState = snapshotTimerRawKeyedState(timerManager, fullRange, "closure");
        }

        // Phase 2: restore the Cobble keyed-state checkpoint, then rebuild the timer service from
        // the raw keyed state stream — both the original canonical-imported timer and the
        // newly-added timer must survive the closure.
        try (CobbleStateBackendTest.TestBackendContext ctx =
                helpers.createBackendContext(
                        tempDir.resolve("restore-cobble"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(cobbleCheckpoint),
                        fullRange,
                        EMPTY_CONFIG)) {
            KeyContext kc = keyContext(ctx.cobbleBackend);
            InternalTimeServiceManagerImpl<Integer> timerManager =
                    InternalTimeServiceManagerImpl.create(
                            (CheckpointableKeyedStateBackend<Integer>) ctx.cobbleBackend,
                            getClass().getClassLoader(),
                            kc,
                            new TestProcessingTimeService(),
                            rawKeyedStateInputs(timerRawKeyedState));
            InternalTimerService<VoidNamespace> timerService =
                    timerManager.getInternalTimerService(
                            TIMER_SERVICE_NAME,
                            IntSerializer.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new NoOpTriggerable());

            // forEachEventTimeTimer iterates in ascending-timestamp order and sets the current key
            // on the context before each callback, so we capture both timestamp and key.
            List<TimerHeapInternalTimer<Integer, VoidNamespace>> timers = new ArrayList<>();
            timerService.forEachEventTimeTimer(
                    (ns, ts) ->
                            timers.add(
                                    new TimerHeapInternalTimer<>(
                                            ts, ctx.cobbleBackend.getCurrentKey(), ns)));
            assertEquals(2, timers.size());
            assertEquals(100L, timers.get(0).getTimestamp());
            assertEquals(keyA, timers.get(0).getKey());
            assertEquals(200L, timers.get(1).getTimestamp());
            assertEquals(keyB, timers.get(1).getKey());
        }
    }

    // =====================================================================================
    // Key-group intersection (rescale)
    // =====================================================================================

    @Test
    void canonicalTimerRestoreRespectsKeyGroupIntersection(@TempDir Path tempDir) throws Exception {
        // Pick keys whose key-groups fall on opposite sides of the [0,7] / [8,15] split.
        int keyLow = CobbleStateBackendTest.findKeyForGroup(3); // in [0,7]
        int keyHigh = CobbleStateBackendTest.findKeyForGroup(12); // in [8,15]

        KeyedStateHandle savepoint =
                helpers.createRocksDbCanonicalTimerSavepoint(
                        tempDir,
                        KeyGroupRange.of(0, 15),
                        TIMER_SERVICE_NAME,
                        (backend, timerService) -> {
                            backend.setCurrentKey(keyLow);
                            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 100L);
                            backend.setCurrentKey(keyHigh);
                            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 200L);
                        });

        // Simulate the JobManager's StateAssignmentOperation.extractIntersectingState by calling
        // KeyGroupsSavepointStateHandle.getIntersection directly — the exact same method Flink
        // uses to narrow a savepoint handle down to a subtask's key-group range.
        KeyGroupsSavepointStateHandle canonical = (KeyGroupsSavepointStateHandle) savepoint;
        KeyedStateHandle subtask0Handle = canonical.getIntersection(KeyGroupRange.of(0, 7));

        try (CobbleStateBackendTest.TestBackendContext ctx =
                subtaskBackend(tempDir, "subtask-0", subtask0Handle, 0, 7)) {
            List<TimerHeapInternalTimer<Integer, VoidNamespace>> timers =
                    pollAll(etQueue(ctx.cobbleBackend));
            // Only the timer whose key-group is in [0,7] survives; keyHigh (group 12) is absent.
            assertEquals(1, timers.size());
            assertEquals(100L, timers.get(0).getTimestamp());
            assertEquals(keyLow, timers.get(0).getKey());
            assertEquals(
                    3,
                    KeyGroupRangeAssignment.assignToKeyGroup(timers.get(0).getKey(), 16),
                    "restored timer must belong to the intersected range");
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

    private static KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, VoidNamespace>>
            etQueue(CobbleKeyedStateBackend<Integer> backend) {
        return backend.create(
                ET_QUEUE,
                new TimerSerializer<>(IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE));
    }

    private static KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, VoidNamespace>>
            ptQueue(CobbleKeyedStateBackend<Integer> backend) {
        return backend.create(
                PT_QUEUE,
                new TimerSerializer<>(IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE));
    }

    private static <N> List<TimerHeapInternalTimer<Integer, N>> pollAll(
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, N>> queue) {
        List<TimerHeapInternalTimer<Integer, N>> drained = new ArrayList<>();
        TimerHeapInternalTimer<Integer, N> timer;
        while ((timer = queue.poll()) != null) {
            drained.add(timer);
        }
        return drained;
    }

    /**
     * Builds an {@link InternalTimeServiceManagerImpl} over a freshly-restored Cobble backend with
     * no prior timer state. {@code CobbleKeyedStateBackend} is not itself a {@link KeyContext}, so
     * a thin adapter delegates {@code setCurrentKey}/{@code getCurrentKey} to the backend.
     */
    private static InternalTimeServiceManagerImpl<Integer> newTimerManager(
            CobbleKeyedStateBackend<Integer> backend) throws Exception {
        return InternalTimeServiceManagerImpl.create(
                (CheckpointableKeyedStateBackend<Integer>) backend,
                CobbleCanonicalTimerRestoreTest.class.getClassLoader(),
                keyContext(backend),
                new TestProcessingTimeService(),
                Collections.emptyList());
    }

    private static KeyContext keyContext(CobbleKeyedStateBackend<Integer> backend) {
        return new KeyContext() {
            @Override
            public void setCurrentKey(Object key) {
                backend.setCurrentKey((Integer) key);
            }

            @Override
            public Object getCurrentKey() {
                return backend.getCurrentKey();
            }
        };
    }

    /**
     * Snapshots timer state through Flink's legacy synchronous timer path ({@code
     * InternalTimeServiceManagerImpl.snapshotToRawKeyedState}). {@code
     * KeyedStateCheckpointOutputStream.closeAndGetHandle()} is package-private in {@code
     * org.apache.flink.runtime.state}, so reflection bridges the single call.
     */
    private static KeyGroupsStateHandle snapshotTimerRawKeyedState(
            InternalTimeServiceManagerImpl<Integer> manager,
            KeyGroupRange keyGroupRange,
            String operatorName)
            throws Exception {
        org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream rawStream =
                new org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream(
                        new MemCheckpointStreamFactory(1024 * 1024)
                                .createCheckpointStateOutputStream(
                                        org.apache.flink.runtime.state.CheckpointedStateScope
                                                .EXCLUSIVE),
                        keyGroupRange);
        manager.snapshotToRawKeyedState(rawStream, operatorName);
        Method closeAndGetHandle =
                org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream.class
                        .getDeclaredMethod("closeAndGetHandle");
        closeAndGetHandle.setAccessible(true);
        return (KeyGroupsStateHandle) closeAndGetHandle.invoke(rawStream);
    }

    /**
     * Builds the {@link KeyGroupStatePartitionStreamProvider} iterable that {@code
     * InternalTimeServiceManagerImpl.create} consumes to restore timers, by slicing each
     * key-group's byte range out of a {@link KeyGroupsStateHandle}.
     */
    private static Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs(
            KeyGroupsStateHandle handle) {
        List<KeyGroupStatePartitionStreamProvider> providers = new ArrayList<>();
        if (handle == null) {
            return providers;
        }
        KeyGroupRange range = handle.getKeyGroupRange();
        long[] offsets = new long[range.getNumberOfKeyGroups()];
        int i = 0;
        for (int keyGroup : range) {
            offsets[i++] = handle.getOffsetForKeyGroup(keyGroup);
        }
        try {
            org.apache.flink.core.fs.FSDataInputStream in = handle.openInputStream();
            int idx = 0;
            for (int keyGroup : range) {
                long start = offsets[idx];
                long end = (idx + 1 < offsets.length) ? offsets[idx + 1] : -1;
                in.seek(start);
                byte[] bytes;
                try (java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream()) {
                    long limit = (end == -1) ? Long.MAX_VALUE : end;
                    long pos = start;
                    byte[] chunk = new byte[4096];
                    while (pos < limit) {
                        int toRead = (int) Math.min(chunk.length, limit - pos);
                        int read = in.read(chunk, 0, toRead);
                        if (read == -1) {
                            break;
                        }
                        buffer.write(chunk, 0, read);
                        pos += read;
                    }
                    bytes = buffer.toByteArray();
                }
                providers.add(
                        new KeyGroupStatePartitionStreamProvider(
                                new java.io.ByteArrayInputStream(bytes), keyGroup));
                idx++;
            }
            in.close();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to read timer raw keyed state", e);
        }
        return providers;
    }

    /** A no-op {@link Triggerable} so the timer service can be constructed without firing. */
    private static final class NoOpTriggerable implements Triggerable<Integer, VoidNamespace> {
        @Override
        public void onEventTime(
                org.apache.flink.streaming.api.operators.InternalTimer<Integer, VoidNamespace>
                        timer) {}

        @Override
        public void onProcessingTime(
                org.apache.flink.streaming.api.operators.InternalTimer<Integer, VoidNamespace>
                        timer) {}
    }
}
