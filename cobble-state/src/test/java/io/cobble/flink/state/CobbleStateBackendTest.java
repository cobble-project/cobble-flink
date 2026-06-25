package io.cobble.flink.state;

import static org.apache.flink.runtime.state.FullSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.runtime.state.FullSnapshotUtil.setMetaDataFollowsFlagInKey;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.CancelledError;
import io.cobble.Config;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.StateKind;
import io.cobble.structured.Schema;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SavepointSnapshotStrategy;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.TestTaskStateManagerBuilder;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.ttl.MockTtlTimeProvider;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.RunnableFuture;

/** Tests for {@link CobbleStateBackend}. */
class CobbleStateBackendTest {
    private static final JobVertexID TEST_JOB_VERTEX_ID =
            JobVertexID.fromHexString("11111111111111111111111111111111");
    private static final String CHECKPOINT_SCOPE = "op_test-operator";

    @Test
    void createsBackendAndLoadsNativeLibrary() {
        assertDoesNotThrow(() -> new CobbleStateBackend());
    }

    @Test
    void restoresCanonicalSavepointValueListAndMapState(@TempDir Path tempDir) throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupsSavepointStateHandle savepoint = createCanonicalSavepoint(key, keyGroup);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);

            ValueState<String> valueState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("value", StringSerializer.INSTANCE));
            ListState<String> listState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ListStateDescriptor<>("list", StringSerializer.INSTANCE));
            MapState<String, String> mapState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    "map", StringSerializer.INSTANCE, StringSerializer.INSTANCE));

            assertEquals("value-42", valueState.value());
            assertEquals(Arrays.asList("left", "right"), toList(listState.get()));
            assertEquals("map-value", mapState.get("map-key"));
        }
    }

    @Test
    void restoresRocksDbCanonicalSavepointValueListAndMapState(@TempDir Path tempDir)
            throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyedStateHandle savepoint = createRocksDbCanonicalSavepoint(tempDir, key, keyGroup);
        assertInstanceOf(KeyGroupsSavepointStateHandle.class, savepoint);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);

            ValueState<String> valueState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("value", StringSerializer.INSTANCE));
            ListState<String> listState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ListStateDescriptor<>("list", StringSerializer.INSTANCE));
            MapState<String, String> mapState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    "map", StringSerializer.INSTANCE, StringSerializer.INSTANCE));

            assertEquals("value-42", valueState.value());
            assertEquals(Arrays.asList("left", "right"), toList(listState.get()));
            assertEquals("map-value", mapState.get("map-key"));
        }
    }

    @Test
    void restoresRealRocksDbCanonicalSavepointAllStateKindsAndRoundTripsCobbleCheckpoint(
            @TempDir Path tempDir) throws Exception {
        int key1 = findKeyForGroup(3);
        int key2 = findKeyForGroup(11);
        KeyGroupRange keyGroupRange = KeyGroupRange.of(3, 11);
        KeyedStateHandle savepoint =
                createRocksDbCanonicalAllStateKindsSavepoint(tempDir, key1, key2, keyGroupRange);
        assertInstanceOf(KeyGroupsSavepointStateHandle.class, savepoint);

        String checkpointDirectory = tempDir.resolve("checkpoints").toUri().toString();
        KeyedStateHandle cobbleCheckpoint;

        try (TestBackendContext imported =
                createBackendContext(
                        tempDir.resolve("imported-all-kinds"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        keyGroupRange)) {
            CobbleKeyedStateBackend<Integer> backend = imported.cobbleBackend;

            ValueState<String> value = valueState(backend, "value-all", "ns-a");
            ListState<String> list = listState(backend, "list-all", "ns-a");
            MapState<String, String> map = mapState(backend, "map-all", "ns-a");
            ReducingState<Integer> reducing = reducingState(backend, "reducing-all", "ns-a");
            AggregatingState<Integer, String> aggregating =
                    aggregatingState(backend, "aggregating-all", "ns-a");

            backend.setCurrentKey(key1);
            assertEquals("value-k1-ns-a", value.value());
            assertEquals(Arrays.asList("k1-a-left", "k1-a-right"), toList(list.get()));
            assertTrue(map.contains("k1-null"));
            assertNull(map.get("k1-null"));
            assertTrue(map.contains("k1-a"));
            assertEquals("map-k1-a", map.get("k1-a"));
            assertEquals(Integer.valueOf(3), reducing.get());
            assertEquals("sum=7", aggregating.get());

            backend.setCurrentKey(key2);
            assertEquals("value-k2-ns-a", value.value());
            assertEquals(Arrays.asList("k2-a-left", "k2-a-right"), toList(list.get()));
            assertTrue(map.contains("k2-null"));
            assertNull(map.get("k2-null"));
            assertTrue(map.contains("k2-a"));
            assertEquals("map-k2-a", map.get("k2-a"));
            assertEquals(Integer.valueOf(30), reducing.get());
            assertEquals("sum=70", aggregating.get());

            ValueState<String> valueNsB = valueState(backend, "value-all", "ns-b");
            ListState<String> listNsB = listState(backend, "list-all", "ns-b");
            MapState<String, String> mapNsB = mapState(backend, "map-all", "ns-b");
            ReducingState<Integer> reducingNsB = reducingState(backend, "reducing-all", "ns-b");
            AggregatingState<Integer, String> aggregatingNsB =
                    aggregatingState(backend, "aggregating-all", "ns-b");
            backend.setCurrentKey(key1);
            assertEquals("value-k1-ns-b", valueNsB.value());
            assertEquals(Arrays.asList("k1-b-only"), toList(listNsB.get()));
            assertEquals("map-k1-b", mapNsB.get("k1-b"));
            assertEquals(Integer.valueOf(5), reducingNsB.get());
            assertEquals("sum=11", aggregatingNsB.get());
            backend.setCurrentKey(key2);
            assertNull(valueNsB.value());
            assertNull(listNsB.get());
            assertFalse(mapNsB.contains("k1-b"));
            assertNull(reducingNsB.get());
            assertNull(aggregatingNsB.get());

            // Mutate every state kind after import. This verifies the imported DB is fully
            // writable,
            // not merely readable, before producing a native Cobble checkpoint. Re-fetch ns-a after
            // the ns-b checks above because getPartitionedState mutates the cached state's current
            // namespace as a side effect.
            value = valueState(backend, "value-all", "ns-a");
            list = listState(backend, "list-all", "ns-a");
            map = mapState(backend, "map-all", "ns-a");
            reducing = reducingState(backend, "reducing-all", "ns-a");
            aggregating = aggregatingState(backend, "aggregating-all", "ns-a");

            backend.setCurrentKey(key1);
            value.update("value-k1-ns-a-mutated");
            list.add("k1-a-tail");
            map.put("k1-a", "map-k1-a-mutated");
            map.put("k1-extra", "map-k1-extra");
            reducing.add(4);
            aggregating.add(8);

            backend.setCurrentKey(key2);
            value.update("value-k2-ns-a-mutated");
            list.addAll(Arrays.asList("k2-a-third", "k2-a-fourth"));
            map.put("k2-a", "map-k2-a-mutated");
            map.remove("k2-null");
            reducing.add(40);
            aggregating.add(80);

            valueNsB = valueState(backend, "value-all", "ns-b");
            listNsB = listState(backend, "list-all", "ns-b");
            mapNsB = mapState(backend, "map-all", "ns-b");
            reducingNsB = reducingState(backend, "reducing-all", "ns-b");
            aggregatingNsB = aggregatingState(backend, "aggregating-all", "ns-b");

            backend.setCurrentKey(key1);
            valueNsB.clear();
            listNsB.clear();
            mapNsB.put("k1-b-extra", "map-k1-b-extra");
            reducingNsB.add(6);
            aggregatingNsB.add(12);

            cobbleCheckpoint = runCheckpointSnapshot(backend, 301L);
            backend.notifyCheckpointComplete(301L);
        }

        try (TestBackendContext restored =
                createBackendContext(
                        tempDir.resolve("restored-all-kinds"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(cobbleCheckpoint),
                        keyGroupRange)) {
            CobbleKeyedStateBackend<Integer> backend = restored.cobbleBackend;

            ValueState<String> value = valueState(backend, "value-all", "ns-a");
            ListState<String> list = listState(backend, "list-all", "ns-a");
            MapState<String, String> map = mapState(backend, "map-all", "ns-a");
            ReducingState<Integer> reducing = reducingState(backend, "reducing-all", "ns-a");
            AggregatingState<Integer, String> aggregating =
                    aggregatingState(backend, "aggregating-all", "ns-a");

            backend.setCurrentKey(key1);
            assertEquals("value-k1-ns-a-mutated", value.value());
            assertEquals(Arrays.asList("k1-a-left", "k1-a-right", "k1-a-tail"), toList(list.get()));
            assertTrue(map.contains("k1-null"));
            assertNull(map.get("k1-null"));
            assertEquals("map-k1-a-mutated", map.get("k1-a"));
            assertEquals("map-k1-extra", map.get("k1-extra"));
            assertEquals(Integer.valueOf(7), reducing.get());
            assertEquals("sum=15", aggregating.get());

            backend.setCurrentKey(key2);
            assertEquals("value-k2-ns-a-mutated", value.value());
            assertEquals(
                    Arrays.asList("k2-a-left", "k2-a-right", "k2-a-third", "k2-a-fourth"),
                    toList(list.get()));
            assertFalse(map.contains("k2-null"));
            assertEquals("map-k2-a-mutated", map.get("k2-a"));
            assertEquals(Integer.valueOf(70), reducing.get());
            assertEquals("sum=150", aggregating.get());

            ValueState<String> valueNsB = valueState(backend, "value-all", "ns-b");
            ListState<String> listNsB = listState(backend, "list-all", "ns-b");
            MapState<String, String> mapNsB = mapState(backend, "map-all", "ns-b");
            ReducingState<Integer> reducingNsB = reducingState(backend, "reducing-all", "ns-b");
            AggregatingState<Integer, String> aggregatingNsB =
                    aggregatingState(backend, "aggregating-all", "ns-b");
            backend.setCurrentKey(key1);
            assertNull(valueNsB.value());
            assertNull(listNsB.get());
            assertEquals("map-k1-b", mapNsB.get("k1-b"));
            assertEquals("map-k1-b-extra", mapNsB.get("k1-b-extra"));
            assertEquals(Integer.valueOf(11), reducingNsB.get());
            assertEquals("sum=23", aggregatingNsB.get());

            backend.setCurrentKey(key2);
            assertNull(valueNsB.value());
            assertNull(listNsB.get());
            assertFalse(mapNsB.contains("k1-b"));
            assertNull(reducingNsB.get());
            assertNull(aggregatingNsB.get());
        }
    }

    @Test
    void restoresCanonicalSavepointFromCompressedStream(@TempDir Path tempDir) throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        // Hand-authored canonical stream wrapped with SnappyStreamCompressionDecorator, so that the
        // FullSnapshotRestoreOperation exercises its compressed-read path.
        KeyGroupsSavepointStateHandle savepoint = createCanonicalSavepoint(key, keyGroup, true);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);

            ValueState<String> valueState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("value", StringSerializer.INSTANCE));
            ListState<String> listState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ListStateDescriptor<>("list", StringSerializer.INSTANCE));
            MapState<String, String> mapState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    "map", StringSerializer.INSTANCE, StringSerializer.INSTANCE));

            assertEquals("value-42", valueState.value());
            assertEquals(Arrays.asList("left", "right"), toList(listState.get()));
            assertEquals("map-value", mapState.get("map-key"));
        }
    }

    @Test
    void rejectsMixOfCanonicalAndNativeRestoreHandles(@TempDir Path tempDir) throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupsSavepointStateHandle canonical = createCanonicalSavepoint(key, keyGroup);
        // A second handle that is not a KeyGroupsSavepointStateHandle. We do not need it to be
        // readable: the mixed-handle preflight must reject before any I/O on either handle.
        KeyedStateHandle nativeHandle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        KeyGroupRange.of(keyGroup, keyGroup),
                        1L,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new ByteStreamStateHandle("dummy-native-meta", new byte[0]));

        Path configuredLocalDir = tempDir.resolve("configured-local-dir");
        UnsupportedOperationException error =
                assertThrows(
                        UnsupportedOperationException.class,
                        () ->
                                createBackendContext(
                                                tempDir,
                                                false,
                                                null,
                                                null,
                                                TtlTimeProvider.DEFAULT,
                                                false,
                                                Arrays.asList(canonical, nativeHandle),
                                                KeyGroupRange.of(keyGroup, keyGroup))
                                        .close());
        // The error message must name the actual conflicting handle classes so operators can find
        // the offending source. It must also identify both sides as canonical / non-canonical.
        assertTrue(
                error.getMessage().contains("canonical savepoint"),
                "expected message to mention canonical savepoint, got: " + error.getMessage());
        assertTrue(
                error.getMessage().contains(KeyGroupsSavepointStateHandle.class.getName()),
                "expected message to list canonical handle class, got: " + error.getMessage());
        assertTrue(
                error.getMessage().contains(IncrementalRemoteKeyedStateHandle.class.getName()),
                "expected message to list the conflicting non-canonical handle class, got: "
                        + error.getMessage());
        // P1: even when the preflight rejects before any DB is opened, no on-disk artifact must be
        // left behind under the configured LOCAL_DIRECTORIES root. createVolumeLayout() and any
        // earlier filesystem step that runs before the preflight must be inside the same failure
        // cleanup.
        Path[] leakedInstanceDirs = listLeakedInstanceDirs(configuredLocalDir);
        assertEquals(
                0,
                leakedInstanceDirs.length,
                "Mixed-handle preflight must not leave a backend instance directory behind. "
                        + "Found leaked: "
                        + Arrays.toString(leakedInstanceDirs));
    }

    @Test
    void canonicalImportSupportsSubsequentCobbleCheckpoint(@TempDir Path tempDir) throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupsSavepointStateHandle savepoint = createCanonicalSavepoint(key, keyGroup);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);
            // Touch every imported state kind so the backend marks them registered.
            backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("value", StringSerializer.INSTANCE))
                    .value();
            backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ListStateDescriptor<>("list", StringSerializer.INSTANCE))
                    .get();
            backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    "map", StringSerializer.INSTANCE, StringSerializer.INSTANCE))
                    .entries();

            // A canonical-imported backend must be able to produce its own native Cobble
            // checkpoint,
            // proving the new DB is fully usable (not a read-only import target).
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                    backend.snapshot(
                            1L,
                            System.currentTimeMillis(),
                            new MemCheckpointStreamFactory(1024 * 1024),
                            CheckpointOptions.forCheckpointWithDefaultLocation());
            snapshotFuture.run();
            SnapshotResult<KeyedStateHandle> snapshotResult = snapshotFuture.get();
            assertNotNull(snapshotResult.getJobManagerOwnedSnapshot());
            backend.notifyCheckpointComplete(1L);
        }
    }

    @Test
    void failedCanonicalImportLeavesNoUsableDb(@TempDir Path tempDir) throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        // A handle that survives mixed-handle preflight (all KeyGroupsSavepointStateHandle) but
        // fails inside FullSnapshotRestoreOperation because its metadata header is truncated.
        KeyGroupsSavepointStateHandle malformed =
                new KeyGroupsSavepointStateHandle(
                        new KeyGroupRangeOffsets(
                                KeyGroupRange.of(keyGroup, keyGroup), new long[] {0L}),
                        new ByteStreamStateHandle(
                                "malformed-canonical-savepoint", new byte[] {0, 1, 2, 3}));

        Path configuredLocalDir = tempDir.resolve("configured-local-dir");
        assertThrows(
                Exception.class,
                () ->
                        createBackendContext(
                                        tempDir,
                                        false,
                                        null,
                                        null,
                                        TtlTimeProvider.DEFAULT,
                                        false,
                                        Collections.singletonList(malformed),
                                        KeyGroupRange.of(keyGroup, keyGroup))
                                .close());
        // The builder's createBackendResources finally block must delete the newly-allocated
        // instance directory (the per-subtask job_*/<subtask> tree) on failure so a retried restore
        // starts from a clean slate. The configured LOCAL_DIRECTORIES root is owned by Flink and
        // may exist (or not) depending on prior backends; it is not the failed-import's to delete.
        Path[] leakedInstanceDirs = listLeakedInstanceDirs(configuredLocalDir);
        assertEquals(
                0,
                leakedInstanceDirs.length,
                "Failed canonical import must not leave a backend instance directory behind. "
                        + "Found leaked: "
                        + Arrays.toString(leakedInstanceDirs));
    }

    @Test
    void partialCanonicalImportFailureRemovesEverything(@TempDir Path tempDir) throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        // A canonical stream whose first ValueState entry is fully readable, but the second entry's
        // length prefix is truncated. FullSnapshotRestoreOperation will hand the first entry to our
        // import operation (which calls db.put), then fail on the second entry while parsing its
        // length-prefixed byte array. The builder's failure cleanup must still remove the whole
        // instance directory tree.
        KeyGroupsSavepointStateHandle savepoint =
                createPartiallyValidCanonicalSavepoint(key, keyGroup);
        Path configuredLocalDir = tempDir.resolve("configured-local-dir");

        assertThrows(
                Exception.class,
                () ->
                        createBackendContext(
                                        tempDir,
                                        false,
                                        null,
                                        null,
                                        TtlTimeProvider.DEFAULT,
                                        false,
                                        Collections.singletonList(savepoint),
                                        KeyGroupRange.of(keyGroup, keyGroup))
                                .close());
        Path[] leakedInstanceDirs = listLeakedInstanceDirs(configuredLocalDir);
        assertEquals(
                0,
                leakedInstanceDirs.length,
                "Failed canonical import after partial row write must not leave a backend instance "
                        + "directory behind. Found leaked: "
                        + Arrays.toString(leakedInstanceDirs));
    }

    @Test
    void deleteInstanceDirectoriesPreservesWrapperWithSibling(@TempDir Path tempDir)
            throws Exception {
        // Direct unit test for the conservative wrapper deletion contract: when the job/op wrapper
        // contains another subtask's directory, the failed-import cleanup must remove only the
        // current instance directory and leave the wrapper (and its sibling) intact. Driving this
        // through the full backend flow is unreliable because each MockEnvironment generates a
        // distinct JobID, so two failing restores never share a wrapper.
        File wrapper = tempDir.resolve("job_test/op_test").toFile();
        File mySubtask = new File(wrapper, "subtask-0");
        File peerSubtask = new File(wrapper, "subtask-1");
        assertTrue(mySubtask.mkdirs(), "setup: create my subtask dir");
        assertTrue(peerSubtask.mkdirs(), "setup: create peer subtask dir");

        CobbleKeyedStateBackendBuilder.deleteInstanceDirectories(mySubtask);

        assertFalse(mySubtask.exists(), "my subtask dir must be deleted");
        assertTrue(peerSubtask.exists(), "peer subtask dir must survive");
        assertTrue(wrapper.exists(), "wrapper must survive while a sibling exists");
    }

    @Test
    void deleteInstanceDirectoriesRemovesEmptyWrapper(@TempDir Path tempDir) throws Exception {
        File wrapper = tempDir.resolve("job_test/op_test").toFile();
        File mySubtask = new File(wrapper, "subtask-0");
        assertTrue(mySubtask.mkdirs(), "setup: create my subtask dir");

        CobbleKeyedStateBackendBuilder.deleteInstanceDirectories(mySubtask);

        assertFalse(mySubtask.exists(), "my subtask dir must be deleted");
        assertFalse(wrapper.exists(), "empty wrapper must be deleted");
        assertTrue(
                wrapper.getParentFile().exists(),
                "LOCAL_DIRECTORIES root (wrapper's parent) is shared and must never be removed");
    }

    @Test
    void canonicalImportCobbleCheckpointCobbleRestoreRoundTrip(@TempDir Path tempDir)
            throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupsSavepointStateHandle savepoint = createCanonicalSavepoint(key, keyGroup);

        // Stage 1: import the canonical savepoint into a Cobble backend and produce a native Cobble
        // checkpoint. The checkpoint directory is persistent and lives outside both backend
        // instance directories, so the manifest survives the first backend being closed.
        String checkpointDirectory = tempDir.resolve("checkpoints").toUri().toString();
        KeyedStateHandle cobbleCheckpoint;
        try (TestBackendContext imported =
                createBackendContext(
                        tempDir.resolve("imported"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = imported.cobbleBackend;
            backend.setCurrentKey(key);

            // Touch every imported state so descriptors register and the schema is committed before
            // we snapshot.
            assertEquals(
                    "value-42",
                    backend.getPartitionedState(
                                    org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    new ValueStateDescriptor<>("value", StringSerializer.INSTANCE))
                            .value());
            assertEquals(
                    Arrays.asList("left", "right"),
                    toList(
                            backend.getPartitionedState(
                                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            new ListStateDescriptor<>(
                                                    "list", StringSerializer.INSTANCE))
                                    .get()));
            assertEquals(
                    "map-value",
                    backend.getPartitionedState(
                                    org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    new MapStateDescriptor<>(
                                            "map",
                                            StringSerializer.INSTANCE,
                                            StringSerializer.INSTANCE))
                            .get("map-key"));

            cobbleCheckpoint = runCheckpointSnapshot(backend, 1L);
            backend.notifyCheckpointComplete(1L);
        }

        // Stage 2: start a brand-new Cobble backend, restored from the Cobble checkpoint produced
        // above (not from the original canonical savepoint), and verify every previously-imported
        // value is observable. This is the full canonical -> Cobble checkpoint -> Cobble restore
        // round-trip the reviewer required for Task 1.
        try (TestBackendContext restored =
                createBackendContext(
                        tempDir.resolve("restored"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(cobbleCheckpoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = restored.cobbleBackend;
            backend.setCurrentKey(key);

            assertEquals(
                    "value-42",
                    backend.getPartitionedState(
                                    org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    new ValueStateDescriptor<>("value", StringSerializer.INSTANCE))
                            .value());
            assertEquals(
                    Arrays.asList("left", "right"),
                    toList(
                            backend.getPartitionedState(
                                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            new ListStateDescriptor<>(
                                                    "list", StringSerializer.INSTANCE))
                                    .get()));
            assertEquals(
                    "map-value",
                    backend.getPartitionedState(
                                    org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    new MapStateDescriptor<>(
                                            "map",
                                            StringSerializer.INSTANCE,
                                            StringSerializer.INSTANCE))
                            .get("map-key"));
        }
    }

    /**
     * Builds a canonical-savepoint stream with one valid VALUE entry followed by a truncated entry
     * (only a half-written {@code int} length prefix). FullSnapshotRestoreOperation will hand the
     * first entry to the import operation, which writes one row to the new Cobble DB; the second
     * entry's deserialize will then throw EOF, leaving a partially-imported DB that the builder
     * must clean up entirely.
     */
    private static KeyGroupsSavepointStateHandle createPartiallyValidCanonicalSavepoint(
            int key, int keyGroup) throws Exception {
        List<StateMetaInfoSnapshot> stateMetaInfos =
                Collections.singletonList(
                        new RegisteredKeyValueStateBackendMetaInfo<>(
                                        org.apache.flink.api.common.state.StateDescriptor.Type
                                                .VALUE,
                                        "value",
                                        VoidNamespaceSerializer.INSTANCE,
                                        StringSerializer.INSTANCE)
                                .snapshot());
        DataOutputSerializer output = new DataOutputSerializer(512);
        new KeyedBackendSerializationProxy<>(IntSerializer.INSTANCE, stateMetaInfos, false)
                .write(output);

        long keyGroupOffset = output.length();
        DataOutputSerializer body = new DataOutputSerializer(128);
        body.writeShort(0);
        // First entry: a fully-formed Value row, so it is actually imported into the new DB. We
        // signal END_OF_KEY_GROUP_MARK as the following-state-id, which would normally terminate
        // the key group; but we keep writing more bytes after it. We instead use a regular
        // following-state-id so the framework attempts another read, which is where we inject the
        // truncation below.
        body.writeBoolean(false); // dummy padding to allow downstream truncation alignment
        body.setPosition(body.length() - 1); // drop the padding byte we just wrote
        writeCanonicalEntry(
                body,
                canonicalKey(keyGroup, key, null),
                CobbleStateKeySerializer.serialize(StringSerializer.INSTANCE, "value-42"),
                0); // following-state-id = 0 keeps the framework reading another entry
        // Truncated second entry: write only 2 bytes where an int (4 bytes) length prefix is
        // expected by BytePrimitiveArraySerializer.deserialize, forcing an EOFException after the
        // first row has already been persisted.
        body.writeByte(0);
        body.writeByte(0);

        output.write(body.getCopyOfBuffer());

        KeyGroupRange range = KeyGroupRange.of(keyGroup, keyGroup);
        return new KeyGroupsSavepointStateHandle(
                new KeyGroupRangeOffsets(range, new long[] {keyGroupOffset}),
                new ByteStreamStateHandle(
                        "partially-valid-canonical-savepoint", output.getCopyOfBuffer()));
    }

    /**
     * Returns the per-subtask {@code job_<id>} instance directory trees still living under {@code
     * localDirRoot}. The builder must delete those when a restore fails; the {@code localDirRoot}
     * itself is owned by Flink configuration and may exist either way.
     */
    private static Path[] listLeakedInstanceDirs(Path localDirRoot) throws IOException {
        if (!java.nio.file.Files.isDirectory(localDirRoot)) {
            return new Path[0];
        }
        try (java.util.stream.Stream<Path> entries = java.nio.file.Files.list(localDirRoot)) {
            return entries.filter(java.nio.file.Files::isDirectory)
                    .filter(p -> p.getFileName().toString().startsWith("job_"))
                    .sorted()
                    .toArray(Path[]::new);
        }
    }

    @Test
    void restoresCanonicalSavepointTimerState(@TempDir Path tempDir) throws Exception {
        int keyGroup = 3;
        TimerHeapInternalTimer<Integer, org.apache.flink.runtime.state.VoidNamespace> timer =
                new TimerHeapInternalTimer<>(
                        123L,
                        findKeyForGroup(keyGroup),
                        org.apache.flink.runtime.state.VoidNamespace.INSTANCE);
        KeyGroupsSavepointStateHandle savepoint = createCanonicalTimerSavepoint(keyGroup, timer);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            KeyGroupedInternalPriorityQueue<
                            TimerHeapInternalTimer<
                                    Integer, org.apache.flink.runtime.state.VoidNamespace>>
                    queue =
                            context.cobbleBackend.create(
                                    "timer",
                                    new TimerSerializer<>(
                                            IntSerializer.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE));
            assertEquals(timer, queue.poll());
            assertNull(queue.poll());
        }
    }

    @Test
    void canonicalRestoreRejectsIncompatibleValueSerializerBeforeDataRead(@TempDir Path tempDir)
            throws Exception {
        // The savepoint metadata declares the "value" state as String. The running job re-registers
        // the same state name with an IntSerializer; the backend must reject the descriptor before
        // any column family is created or any row is read.
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupsSavepointStateHandle savepoint = createCanonicalSavepoint(key, keyGroup);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);

            // Snapshot the schema BEFORE registering the mis-typed descriptor; the failure path
            // must not leave any new column family behind.
            Set<String> columnFamiliesBefore =
                    backend.getCobbleDb().currentSchema().columnFamilies().keySet();

            Exception error =
                    assertThrows(
                            Exception.class,
                            () ->
                                    backend.getPartitionedState(
                                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            new ValueStateDescriptor<>(
                                                    "value", IntSerializer.INSTANCE)));
            assertTrue(
                    hasCause(error, org.apache.flink.util.StateMigrationException.class),
                    "expected StateMigrationException cause, got: " + error);
            String message = collectMessages(error);
            assertTrue(
                    message.contains("value serializer"),
                    "error must name the offending role, got: " + message);
            assertTrue(
                    message.contains(StringSerializer.class.getName()),
                    "error must name canonical serializer class, got: " + message);
            assertTrue(
                    message.contains(IntSerializer.class.getName()),
                    "error must name the rejected runtime serializer class, got: " + message);
            assertTrue(message.contains("'value'"), "error must name the state, got: " + message);

            // The schema must be unchanged — no new column family was created for the rejected
            // state. The canonical restore already created the family for "value", but the failed
            // descriptor registration must not add anything else.
            Set<String> columnFamiliesAfter =
                    backend.getCobbleDb().currentSchema().columnFamilies().keySet();
            assertEquals(columnFamiliesBefore, columnFamiliesAfter);

            // Re-registering with the canonical-compatible serializer must still succeed: the
            // failed registration must not have poisoned the registry entry.
            ValueState<String> recovered =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("value", StringSerializer.INSTANCE));
            assertEquals("value-42", recovered.value());
        }
    }

    @Test
    void canonicalRestoreRejectsIncompatibleNamespaceSerializerBeforeDataRead(@TempDir Path tempDir)
            throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupsSavepointStateHandle savepoint = createCanonicalSavepoint(key, keyGroup);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);

            // Wrong namespace serializer class (StringSerializer vs canonical
            // VoidNamespaceSerializer)
            // must be rejected. The value-serializer class agrees, so this isolates the namespace
            // check.
            @SuppressWarnings({"unchecked", "rawtypes"})
            Exception error =
                    assertThrows(
                            Exception.class,
                            () ->
                                    backend.getPartitionedState(
                                            (Object) "any-namespace",
                                            (org.apache.flink.api.common.typeutils.TypeSerializer)
                                                    StringSerializer.INSTANCE,
                                            new ValueStateDescriptor<>(
                                                    "value", StringSerializer.INSTANCE)));
            assertTrue(
                    hasCause(error, org.apache.flink.util.StateMigrationException.class),
                    "expected StateMigrationException cause, got: " + error);
            String message = collectMessages(error);
            assertTrue(
                    message.contains("namespace serializer"),
                    "error must name namespace serializer role, got: " + message);
            assertTrue(
                    message.contains(VoidNamespaceSerializer.class.getName()),
                    "error must name canonical namespace serializer class, got: " + message);
            assertTrue(
                    message.contains(StringSerializer.class.getName()),
                    "error must name rejected namespace serializer class, got: " + message);
        }
    }

    @Test
    void canonicalRestoreRejectsKindMismatchBetweenSavepointAndDescriptor(@TempDir Path tempDir)
            throws Exception {
        // Canonical savepoint registers "value" as VALUE. The job mis-registers it as LIST.
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupsSavepointStateHandle savepoint = createCanonicalSavepoint(key, keyGroup);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);

            Exception error =
                    assertThrows(
                            Exception.class,
                            () ->
                                    backend.getPartitionedState(
                                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            new ListStateDescriptor<>(
                                                    "value", StringSerializer.INSTANCE)));
            assertTrue(
                    hasCause(error, org.apache.flink.util.StateMigrationException.class),
                    "expected StateMigrationException cause, got: " + error);
            String message = collectMessages(error);
            assertTrue(
                    message.contains("kind mismatch") || message.contains("registers"),
                    "error should report state-kind mismatch, got: " + message);
            assertTrue(
                    message.contains("VALUE"),
                    "error must mention canonical VALUE kind: " + message);
            assertTrue(
                    message.contains("LIST"), "error must mention requested LIST kind: " + message);
        }
    }

    @Test
    void canonicalRestoreRejectsIncompatibleTimerElementSerializer(@TempDir Path tempDir)
            throws Exception {
        int keyGroup = 3;
        TimerHeapInternalTimer<Integer, org.apache.flink.runtime.state.VoidNamespace> timer =
                new TimerHeapInternalTimer<>(
                        123L,
                        findKeyForGroup(keyGroup),
                        org.apache.flink.runtime.state.VoidNamespace.INSTANCE);
        KeyGroupsSavepointStateHandle savepoint = createCanonicalTimerSavepoint(keyGroup, timer);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;

            // Pass a raw IntSerializer for the timer element — its class differs from the canonical
            // TimerSerializer, so the registration must fail with a StateMigrationException cause.
            UncheckedIOException error =
                    assertThrows(
                            UncheckedIOException.class,
                            () ->
                                    backend.create(
                                            "timer",
                                            (org.apache.flink.api.common.typeutils.TypeSerializer)
                                                    StringSerializer.INSTANCE));
            assertTrue(
                    hasCause(error, org.apache.flink.util.StateMigrationException.class),
                    "expected StateMigrationException cause, got: " + error);
            String message = collectMessages(error);
            assertTrue(
                    message.contains("timer element serializer"),
                    "error must name timer element serializer role, got: " + message);
            assertTrue(
                    message.contains(TimerSerializer.class.getName()),
                    "error must name canonical TimerSerializer class, got: " + message);
            assertTrue(
                    message.contains(StringSerializer.class.getName()),
                    "error must name rejected runtime serializer class, got: " + message);
            assertTrue(message.contains("'timer'"), "error must name the state, got: " + message);

            // Re-creating with the canonical serializer must still succeed, proving the failed
            // create() did not poison the registry entry.
            KeyGroupedInternalPriorityQueue<
                            TimerHeapInternalTimer<
                                    Integer, org.apache.flink.runtime.state.VoidNamespace>>
                    queue =
                            backend.create(
                                    "timer",
                                    new TimerSerializer<>(
                                            IntSerializer.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE));
            assertEquals(timer, queue.poll());
        }
    }

    @Test
    void canonicalRestoreHandlesSameStateAcrossMultipleHandlesInDifferentOrder(
            @TempDir Path tempDir) throws Exception {
        // Two canonical handles for two different key-groups belonging to the same task. The state
        // name "value" appears in BOTH handles, but the per-handle metadata IDs differ (a vs b),
        // and the second handle places it in a different relative position. The backend must
        // accept the consistent metadata and apply it from any handle, then validate the running
        // descriptor against it.
        int keyGroupA = 3;
        int keyGroupB = 4;
        int keyA = findKeyForGroup(keyGroupA);
        int keyB = findKeyForGroup(keyGroupB);
        KeyGroupsSavepointStateHandle handleA =
                createCanonicalSavepointSingleValueState(keyGroupA, keyA);
        KeyGroupsSavepointStateHandle handleB =
                createCanonicalSavepointSingleValueState(keyGroupB, keyB);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        // Pass the handles deliberately in reverse key-group order to exercise
                        // multi-handle ordering: the restored metadata registry must end up with
                        // a single entry for "value".
                        Arrays.asList(handleB, handleA),
                        KeyGroupRange.of(keyGroupA, keyGroupB))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;

            ValueState<String> valueState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("value", StringSerializer.INSTANCE));

            backend.setCurrentKey(keyA);
            assertEquals("value-" + keyA, valueState.value());
            backend.setCurrentKey(keyB);
            assertEquals("value-" + keyB, valueState.value());
        }
    }

    @Test
    void canonicalRestoreRejectsSameClassValueSerializerWithIncompatibleNestedConfig(
            @TempDir Path tempDir) throws Exception {
        // P1 regression guard: the canonical metadata records a ListSerializer<String>; the running
        // job re-registers the SAME state name with a ListSerializer<Integer>. Both outer
        // serializers share the ListSerializer class, so the obsolete "class equality" check would
        // have wrongly accepted this. The Flink TypeSerializerSnapshot.resolveSchemaCompatibility
        // path must observe the nested element-serializer mismatch and reject as incompatible.
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupsSavepointStateHandle savepoint =
                createCanonicalSavepointSingleValueStateWithSerializer(
                        keyGroup, key, new ListSerializer<>(StringSerializer.INSTANCE));

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);

            Exception error =
                    assertThrows(
                            Exception.class,
                            () ->
                                    backend.getPartitionedState(
                                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            new ValueStateDescriptor<>(
                                                    "value",
                                                    new ListSerializer<>(IntSerializer.INSTANCE))));
            assertTrue(
                    hasCause(error, org.apache.flink.util.StateMigrationException.class),
                    "expected StateMigrationException cause, got: " + error);
            String message = collectMessages(error);
            assertTrue(
                    message.contains("value serializer"),
                    "error must name the offending role, got: " + message);
            assertTrue(
                    message.contains(ListSerializer.class.getName()),
                    "error must mention the outer ListSerializer class on both sides, got: "
                            + message);
            assertTrue(
                    message.contains("incompatible"),
                    "error must explain the resolveSchemaCompatibility outcome, got: " + message);
            assertTrue(message.contains("'value'"), "error must name the state, got: " + message);
        }
    }

    @Test
    void canonicalRestoreRejectsSameClassTimerSerializerWithIncompatibleNestedConfig(
            @TempDir Path tempDir) throws Exception {
        // P1 regression guard for priority-queue / timer state. The canonical metadata's
        // TimerSerializer is constructed over IntSerializer; the running job calls create() with a
        // TimerSerializer constructed over StringSerializer. Outer TimerSerializer class is
        // identical on both sides, so only the nested-config-aware Flink resolveSchemaCompatibility
        // call can detect the mismatch.
        int keyGroup = 3;
        TimerHeapInternalTimer<Integer, org.apache.flink.runtime.state.VoidNamespace> timer =
                new TimerHeapInternalTimer<>(
                        123L,
                        findKeyForGroup(keyGroup),
                        org.apache.flink.runtime.state.VoidNamespace.INSTANCE);
        KeyGroupsSavepointStateHandle savepoint = createCanonicalTimerSavepoint(keyGroup, timer);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;

            UncheckedIOException error =
                    assertThrows(
                            UncheckedIOException.class,
                            () ->
                                    backend.create(
                                            "timer",
                                            new TimerSerializer<>(
                                                    StringSerializer.INSTANCE,
                                                    VoidNamespaceSerializer.INSTANCE)));
            assertTrue(
                    hasCause(error, org.apache.flink.util.StateMigrationException.class),
                    "expected StateMigrationException cause, got: " + error);
            String message = collectMessages(error);
            assertTrue(
                    message.contains("timer element serializer"),
                    "error must name timer element serializer role, got: " + message);
            assertTrue(
                    message.contains(TimerSerializer.class.getName()),
                    "error must mention the outer TimerSerializer class on both sides, got: "
                            + message);
            assertTrue(
                    message.contains("incompatible"),
                    "error must explain the resolveSchemaCompatibility outcome, got: " + message);
            assertTrue(message.contains("'timer'"), "error must name the state, got: " + message);

            // Re-creating with the canonical-compatible serializer must still succeed, proving the
            // failed create did not poison the registry entry or the column family.
            KeyGroupedInternalPriorityQueue<
                            TimerHeapInternalTimer<
                                    Integer, org.apache.flink.runtime.state.VoidNamespace>>
                    queue =
                            backend.create(
                                    "timer",
                                    new TimerSerializer<>(
                                            IntSerializer.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE));
            assertEquals(timer, queue.poll());
        }
    }

    @Test
    void canonicalRestoreRejectsTwoHandlesWithIncompatibleNestedConfigBeforeAnyDbWrite(
            @TempDir Path tempDir) throws Exception {
        // P1 regression guard for the two-phase restore: the same state name "value" appears in
        // two canonical handles. Both record a ListSerializer (same outer class), but the nested
        // element type differs (String vs Integer). The two-phase preflight must observe the
        // incompatibility while still inside phase 1 — before any column family is created or any
        // row is written — and on failure the builder must clean up the entire on-disk instance
        // directory tree under the configured LOCAL_DIRECTORIES root.
        int keyGroupA = 3;
        int keyGroupB = 4;
        int keyA = findKeyForGroup(keyGroupA);
        int keyB = findKeyForGroup(keyGroupB);
        KeyGroupsSavepointStateHandle handleA =
                createCanonicalSavepointSingleValueStateWithSerializer(
                        keyGroupA, keyA, new ListSerializer<>(StringSerializer.INSTANCE));
        KeyGroupsSavepointStateHandle handleB =
                createCanonicalSavepointSingleValueStateWithSerializer(
                        keyGroupB, keyB, new ListSerializer<>(IntSerializer.INSTANCE));

        Path configuredLocalDir = tempDir.resolve("configured-local-dir");
        Exception error =
                assertThrows(
                        Exception.class,
                        () ->
                                createBackendContext(
                                                tempDir,
                                                false,
                                                null,
                                                null,
                                                TtlTimeProvider.DEFAULT,
                                                false,
                                                Arrays.asList(handleA, handleB),
                                                KeyGroupRange.of(keyGroupA, keyGroupB))
                                        .close());
        assertTrue(
                hasCause(error, org.apache.flink.util.StateMigrationException.class),
                "expected StateMigrationException cause, got: " + error);
        String message = collectMessages(error);
        assertTrue(
                message.contains("between canonical savepoint handles"),
                "error must identify the multi-handle preflight as the comparison context, got: "
                        + message);
        assertTrue(
                message.contains(ListSerializer.class.getName()),
                "error must mention the outer ListSerializer class, got: " + message);
        assertTrue(
                message.contains("incompatible"),
                "error must explain the resolveSchemaCompatibility outcome, got: " + message);
        assertTrue(message.contains("'value'"), "error must name the state, got: " + message);

        // Preflight failure must leave the configured local directory pristine — no per-subtask
        // instance directory, no Cobble DB, no column family residue.
        Path[] leakedInstanceDirs = listLeakedInstanceDirs(configuredLocalDir);
        assertEquals(
                0,
                leakedInstanceDirs.length,
                "Preflight rejection of incompatible multi-handle metadata must not leave a backend "
                        + "instance directory behind. Found leaked: "
                        + Arrays.toString(leakedInstanceDirs));
    }

    @Test
    void canonicalRestoreReconfiguresKeySerializerForRunningBackend(@TempDir Path tempDir)
            throws Exception {
        // P1 regression guard: when Flink reports
        // TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer for the KEY
        // serializer, the running backend must end up holding the reconfigured serializer Flink
        // produced — NOT the one the job initially registered. This is the canonical contract of
        // setPreviousSerializerSnapshotForRestoredState: the resulting reconfigured serializer is
        // the one the running pipeline must serialize new keys with.
        //
        // Mechanics: the canonical savepoint records keys with
        // ReconfigurableIntKeySerializer(version="canonical"). The running job registers
        // ReconfigurableIntKeySerializer(version="runtime"). The custom snapshot's
        // resolveSchemaCompatibility(currentSerializer) returns
        // compatibleWithReconfiguredSerializer
        // with a brand-new ReconfigurableIntKeySerializer(version="reconfigured"). After restore,
        // backend.getKeySerializer() MUST be the "reconfigured" instance.
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupsSavepointStateHandle savepoint =
                createCanonicalSavepointSingleValueStateWithKeySerializer(
                        keyGroup,
                        key,
                        new ReconfigurableIntKeySerializer(
                                ReconfigurableIntKeySerializer.CANONICAL_VERSION));

        try (TestBackendContext context =
                createBackendContextWithKeySerializer(
                        tempDir,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup),
                        new ReconfigurableIntKeySerializer(
                                ReconfigurableIntKeySerializer.RUNTIME_VERSION))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;

            // The backend's key serializer must be the reconfigured instance produced by
            // resolveSchemaCompatibility, not the one the job registered. This is the heart of the
            // reviewer's P1: if the import phase ran on a throw-away provider, the builder's
            // provider would never observe the canonical snapshot and would hand the runtime
            // serializer to the backend.
            org.apache.flink.api.common.typeutils.TypeSerializer<Integer> keySerializer =
                    backend.getKeySerializer();
            assertInstanceOf(ReconfigurableIntKeySerializer.class, keySerializer);
            assertEquals(
                    ReconfigurableIntKeySerializer.RECONFIGURED_VERSION,
                    ((ReconfigurableIntKeySerializer) keySerializer).version,
                    "backend must hold the reconfigured key serializer that Flink's "
                            + "TypeSerializerSchemaCompatibility produced");

            // The imported row is still observable end-to-end, proving the import phase actually
            // ran on the builder's provider (the same one whose currentSchemaSerializer() returned
            // the reconfigured key serializer used to construct the backend).
            backend.setCurrentKey(key);
            ValueState<String> valueState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("value", StringSerializer.INSTANCE));
            assertEquals("value-" + key, valueState.value());
        }
    }

    /** Concatenates the messages of {@code error} and every cause in its chain. */
    private static String collectMessages(Throwable error) {
        StringBuilder builder = new StringBuilder();
        Throwable current = error;
        while (current != null) {
            if (current.getMessage() != null) {
                if (builder.length() > 0) {
                    builder.append(" | ");
                }
                builder.append(current.getMessage());
            }
            current = current.getCause();
        }
        return builder.toString();
    }

    /**
     * Builds a canonical savepoint containing only a single {@code value} state for the given
     * key-group/key — used by the multi-handle test to compose two independent canonical handles
     * that share the same state name. The serialized payload mirrors {@code value-<key>}.
     */
    private static KeyGroupsSavepointStateHandle createCanonicalSavepointSingleValueState(
            int keyGroup, int key) throws Exception {
        return createCanonicalSavepointSingleValueStateWithSerializer(
                keyGroup, key, StringSerializer.INSTANCE);
    }

    /**
     * Variant of {@link #createCanonicalSavepointSingleValueState(int, int)} that lets the caller
     * inject a custom value serializer for the recorded canonical metadata. The body bytes are
     * intentionally synthetic and are never read (no test that exercises this helper actually
     * consumes the row — only the preflight metadata is compared), so we can record a
     * non-String-typed serializer without serializing a matching payload.
     */
    private static KeyGroupsSavepointStateHandle
            createCanonicalSavepointSingleValueStateWithSerializer(
                    int keyGroup,
                    int key,
                    org.apache.flink.api.common.typeutils.TypeSerializer<?> valueSerializer)
                    throws Exception {
        List<StateMetaInfoSnapshot> stateMetaInfos =
                Collections.singletonList(
                        new RegisteredKeyValueStateBackendMetaInfo<>(
                                        org.apache.flink.api.common.state.StateDescriptor.Type
                                                .VALUE,
                                        "value",
                                        VoidNamespaceSerializer.INSTANCE,
                                        valueSerializer)
                                .snapshot());
        DataOutputSerializer output = new DataOutputSerializer(256);
        new KeyedBackendSerializationProxy<>(IntSerializer.INSTANCE, stateMetaInfos, false)
                .write(output);

        long keyGroupOffset = output.length();
        output.writeShort(0);
        writeCanonicalEntry(
                output,
                canonicalKey(keyGroup, key, null),
                CobbleStateKeySerializer.serialize(StringSerializer.INSTANCE, "value-" + key),
                END_OF_KEY_GROUP_MARK);

        KeyGroupRange range = KeyGroupRange.of(keyGroup, keyGroup);
        return new KeyGroupsSavepointStateHandle(
                new KeyGroupRangeOffsets(range, new long[] {keyGroupOffset}),
                new ByteStreamStateHandle(
                        "canonical-single-value-kg" + keyGroup, output.getCopyOfBuffer()));
    }

    /**
     * Variant of {@link #createCanonicalSavepointSingleValueState(int, int)} that records a custom
     * KEY serializer in the canonical metadata header. The KEY serializer flows into the
     * KeyedBackendSerializationProxy and its snapshot is what the import phase later hands to the
     * builder's StateSerializerProvider via {@code setPreviousSerializerSnapshotForRestoredState
     * (...)}. The serialized key bytes are produced with {@code IntSerializer.INSTANCE} so the row
     * remains decodable by any wire-compatible runtime key serializer.
     */
    private static KeyGroupsSavepointStateHandle
            createCanonicalSavepointSingleValueStateWithKeySerializer(
                    int keyGroup,
                    int key,
                    org.apache.flink.api.common.typeutils.TypeSerializer<Integer> keySerializer)
                    throws Exception {
        List<StateMetaInfoSnapshot> stateMetaInfos =
                Collections.singletonList(
                        new RegisteredKeyValueStateBackendMetaInfo<>(
                                        org.apache.flink.api.common.state.StateDescriptor.Type
                                                .VALUE,
                                        "value",
                                        VoidNamespaceSerializer.INSTANCE,
                                        StringSerializer.INSTANCE)
                                .snapshot());
        DataOutputSerializer output = new DataOutputSerializer(256);
        new KeyedBackendSerializationProxy<>(keySerializer, stateMetaInfos, false).write(output);

        long keyGroupOffset = output.length();
        output.writeShort(0);
        writeCanonicalEntry(
                output,
                canonicalKey(keyGroup, key, null),
                CobbleStateKeySerializer.serialize(StringSerializer.INSTANCE, "value-" + key),
                END_OF_KEY_GROUP_MARK);

        KeyGroupRange range = KeyGroupRange.of(keyGroup, keyGroup);
        return new KeyGroupsSavepointStateHandle(
                new KeyGroupRangeOffsets(range, new long[] {keyGroupOffset}),
                new ByteStreamStateHandle(
                        "canonical-single-value-custom-key-kg" + keyGroup,
                        output.getCopyOfBuffer()));
    }

    @Test
    void stateBackendSupportsOnlyClaimRestoreMode() {
        assertFalse(new CobbleStateBackend().supportsNoClaimRestoreMode());
    }

    @Test
    void timerServiceFactoryConfigurationFollowsRocksDbStylePrecedence() {
        Configuration heapConfiguration = new Configuration();
        heapConfiguration.setString(CobbleOptions.TIMER_SERVICE_FACTORY.key(), "HEAP");

        CobbleStateBackend defaultBackend = new CobbleStateBackend();
        CobbleStateBackend configuredBackend =
                defaultBackend.configure(heapConfiguration, getClass().getClassLoader());
        assertEquals(
                CobbleStateBackend.PriorityQueueStateType.COBBLE,
                defaultBackend.getPriorityQueueStateType());
        assertEquals(
                CobbleStateBackend.PriorityQueueStateType.HEAP,
                configuredBackend.getPriorityQueueStateType());

        defaultBackend.setPriorityQueueStateType(CobbleStateBackend.PriorityQueueStateType.COBBLE);
        assertEquals(
                CobbleStateBackend.PriorityQueueStateType.COBBLE,
                defaultBackend
                        .configure(heapConfiguration, getClass().getClassLoader())
                        .getPriorityQueueStateType());
    }

    @Test
    void factoryCreatesCobbleStateBackend() throws Exception {
        CobbleStateBackendFactory factory = new CobbleStateBackendFactory();

        assertInstanceOf(
                CobbleStateBackend.class,
                factory.createFromConfig(new Configuration(), getClass().getClassLoader()));
    }

    @Test
    void serializeKeyAndNamespaceUsesKeyLengthSuffixOnly() throws Exception {
        byte[] keyBytes = CobbleStateKeySerializer.serialize(StringSerializer.INSTANCE, "key");
        byte[] namespaceBytes = CobbleStateKeySerializer.serialize(StringSerializer.INSTANCE, "ns");
        CobbleStateKeySerializer.ReusableSerializedKeyBuilder<String, String> builder =
                new CobbleStateKeySerializer.ReusableSerializedKeyBuilder<>(
                        StringSerializer.INSTANCE, StringSerializer.INSTANCE, 64);

        byte[] encoded = builder.buildKeyAndNamespace("key", "ns");

        assertEquals(keyBytes.length + namespaceBytes.length + Integer.BYTES, encoded.length);
        assertByteSegmentEquals(encoded, 0, keyBytes);
        assertByteSegmentEquals(encoded, keyBytes.length, namespaceBytes);
        assertEquals(keyBytes.length, readTrailingInt(encoded, Integer.BYTES));
    }

    @Test
    void serializeMapKeyNamespaceAndUserKeyUsesKeyAndNamespaceLengthSuffixes() throws Exception {
        byte[] keyBytes = CobbleStateKeySerializer.serialize(StringSerializer.INSTANCE, "key");
        byte[] userKeyBytes = CobbleStateKeySerializer.serialize(StringSerializer.INSTANCE, "uk");
        byte[] namespaceBytes = CobbleStateKeySerializer.serialize(StringSerializer.INSTANCE, "ns");
        CobbleStateKeySerializer.ReusableSerializedKeyBuilder<String, String> builder =
                new CobbleStateKeySerializer.ReusableSerializedKeyBuilder<>(
                        StringSerializer.INSTANCE, StringSerializer.INSTANCE, 64);

        byte[] encoded =
                builder.buildMapKeyNamespaceAndUserKey(
                        "key", StringSerializer.INSTANCE, "uk", "ns");

        assertEquals(
                keyBytes.length
                        + namespaceBytes.length
                        + 1
                        + userKeyBytes.length
                        + (Integer.BYTES * 2),
                encoded.length);
        assertByteSegmentEquals(encoded, 0, keyBytes);
        assertByteSegmentEquals(encoded, keyBytes.length, namespaceBytes);
        assertEquals(0, encoded[keyBytes.length + namespaceBytes.length]);
        assertByteSegmentEquals(encoded, keyBytes.length + namespaceBytes.length + 1, userKeyBytes);
        assertEquals(namespaceBytes.length, readTrailingInt(encoded, Integer.BYTES));
        assertEquals(keyBytes.length, readTrailingInt(encoded, Integer.BYTES * 2));
    }

    @Test
    void serializeMapKeyNamespacePrefixContainsOnlyKeyAndNamespaceBytes() throws Exception {
        byte[] keyBytes = CobbleStateKeySerializer.serialize(StringSerializer.INSTANCE, "key");
        byte[] namespaceBytes = CobbleStateKeySerializer.serialize(StringSerializer.INSTANCE, "ns");
        CobbleStateKeySerializer.ReusableSerializedKeyBuilder<String, String> builder =
                new CobbleStateKeySerializer.ReusableSerializedKeyBuilder<>(
                        StringSerializer.INSTANCE, StringSerializer.INSTANCE, 64);

        byte[] encoded = builder.buildMapKeyNamespacePrefix("key", "ns");

        assertEquals(keyBytes.length + namespaceBytes.length, encoded.length);
        assertByteSegmentEquals(encoded, 0, keyBytes);
        assertByteSegmentEquals(encoded, keyBytes.length, namespaceBytes);
    }

    @Test
    void reusableSerializedKeyBuilderReusesSharedBufferUntilKeyChanges() throws Exception {
        CobbleStateKeySerializer.ReusableSerializedKeyBuilder<String, String> builder =
                new CobbleStateKeySerializer.ReusableSerializedKeyBuilder<>(
                        StringSerializer.INSTANCE, StringSerializer.INSTANCE, 64);

        builder.buildKeyAndNamespace("key", "ns-1");
        byte[] sharedBuffer = builder.sharedBuffer();

        builder.buildKeyAndNamespace("key", "ns-2");
        assertSame(sharedBuffer, builder.sharedBuffer());

        builder.buildKeyAndNamespace("other-key", "ns-3");
        assertSame(sharedBuffer, builder.sharedBuffer());
    }

    @Test
    void valueStateSupportsExplicitNullNamespace(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>("null-namespace-state", StringSerializer.INSTANCE);

            backend.setCurrentKey(1);
            ValueState<String> state =
                    backend.getPartitionedState("initial", StringSerializer.INSTANCE, descriptor);
            @SuppressWarnings("unchecked")
            InternalKvState<Integer, String, String> internalState =
                    (InternalKvState<Integer, String, String>) state;

            internalState.setCurrentNamespace(null);
            state.update("null-namespace");
            assertEquals("null-namespace", state.value());

            internalState.setCurrentNamespace("regular-namespace");
            state.update("regular-namespace");
            assertEquals("regular-namespace", state.value());

            internalState.setCurrentNamespace(null);
            assertEquals("null-namespace", state.value());
        }
    }

    @Test
    void createKeyedStateBackendUsesLocalPrimaryWhenCheckpointDirectoryIsUnset(
            @TempDir Path tempDir) throws Exception {
        CobbleKeyedStateBackend<Integer> cobbleBackend;
        Path instanceBasePath;

        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            cobbleBackend = context.cobbleBackend;
            instanceBasePath = cobbleBackend.getInstanceBasePath().toPath();

            assertTrue(cobbleBackend.getConfigPath().startsWith(context.configuredLocalDir));
            assertTrue(
                    cobbleBackend.getVolumePath().toPath().startsWith(context.configuredLocalDir));
            assertTrue(cobbleBackend.getCobbleDb().getNativeHandle() != 0L);
            assertThrows(
                    UnsupportedOperationException.class, cobbleBackend::numKeyValueStateEntries);

            assertEquals(1, cobbleBackend.getCobbleConfig().volumes.size());
            assertEquals(
                    CobbleKeyedStateBackendBuilder.normalizeLocalPath(
                            new File(cobbleBackend.getInstanceBasePath(), "cobble-db")),
                    cobbleBackend.getCobbleConfig().volumes.get(0).baseDir);
            assertVolumeKinds(
                    cobbleBackend.getCobbleConfig().volumes.get(0),
                    Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                    Config.VolumeUsageKind.META);

            assertEquals(1, cobbleBackend.getCobbleConfig().numColumns.intValue());
            assertEquals(16, cobbleBackend.getCobbleConfig().totalBuckets.intValue());
            assertEquals(4, cobbleBackend.getCobbleConfig().memtableBufferCount.intValue());
            assertEquals(
                    MemorySize.ofMebiBytes(4).getBytes(),
                    cobbleBackend.getCobbleConfig().memtableCapacity.longValue());
            assertEquals(
                    MemorySize.ofMebiBytes(48).getBytes(),
                    cobbleBackend.getCobbleConfig().blockCacheSize.longValue());
            assertTrue(cobbleBackend.getConfigPath().toFile().isFile());
        }

        assertTrue(cobbleBackend.getCobbleDb().isDisposed());
        assertFalse(instanceBasePath.toFile().exists());
    }

    @Test
    void createVolumeDescriptorsUsesCheckpointDirectoryAsPrimaryAndLocalAsCache(
            @TempDir Path tempDir) {
        File instanceBasePath = tempDir.resolve("instance").toFile();

        List<Config.VolumeDescriptor> volumes =
                CobbleKeyedStateBackendBuilder.createVolumeDescriptors(
                        instanceBasePath, CHECKPOINT_SCOPE, "s3a://bucket/checkpoints", false);

        assertEquals(2, volumes.size());

        Config.VolumeDescriptor checkpointVolume = volumes.get(0);
        assertEquals(
                "s3://bucket/checkpoints/shared/" + CHECKPOINT_SCOPE, checkpointVolume.baseDir);
        assertVolumeKinds(
                checkpointVolume,
                Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                Config.VolumeUsageKind.META,
                Config.VolumeUsageKind.SNAPSHOT);

        Config.VolumeDescriptor localVolume = volumes.get(1);
        assertEquals(
                CobbleKeyedStateBackendBuilder.normalizeLocalPath(
                        new File(instanceBasePath, "cobble-db")),
                localVolume.baseDir);
        assertVolumeKinds(localVolume, Config.VolumeUsageKind.CACHE);
    }

    @Test
    void createVolumeDescriptorsCanUseLocalAsHighPriorityPrimaryWithCheckpointDirectory(
            @TempDir Path tempDir) {
        File instanceBasePath = tempDir.resolve("instance").toFile();

        List<Config.VolumeDescriptor> volumes =
                CobbleKeyedStateBackendBuilder.createVolumeDescriptors(
                        instanceBasePath, CHECKPOINT_SCOPE, "s3p://bucket/checkpoints", true);

        assertEquals(2, volumes.size());
        assertEquals("s3://bucket/checkpoints/shared/" + CHECKPOINT_SCOPE, volumes.get(0).baseDir);
        assertVolumeKinds(volumes.get(1), Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH);
    }

    @Test
    void createVolumeDescriptorsKeepsFileCheckpointDirectoryAsFileUrl(@TempDir Path tempDir) {
        File instanceBasePath = tempDir.resolve("instance").toFile();
        String checkpointDirectory = tempDir.resolve("checkpoints").toUri().toString();

        List<Config.VolumeDescriptor> volumes =
                CobbleKeyedStateBackendBuilder.createVolumeDescriptors(
                        instanceBasePath, CHECKPOINT_SCOPE, checkpointDirectory, false);

        assertEquals(
                CobbleKeyedStateBackendBuilder.normalizeLocalPath(
                        tempDir.resolve("checkpoints")
                                .resolve("shared")
                                .resolve(CHECKPOINT_SCOPE)
                                .toFile()),
                volumes.get(0).baseDir);
    }

    @Test
    void createVolumeDescriptorsCopiesS3CheckpointCredentialsFromFlinkConfig(
            @TempDir Path tempDir) {
        File instanceBasePath = tempDir.resolve("instance").toFile();
        Configuration configuration = new Configuration();
        configuration.setString("s3.access-key", "test-access-id");
        configuration.setString("s3.secret-key", "test-secret-key");

        List<Config.VolumeDescriptor> volumes =
                CobbleKeyedStateBackendBuilder.createVolumeDescriptors(
                        instanceBasePath,
                        CHECKPOINT_SCOPE,
                        "s3://127.0.0.1:9000/cobble-test/checkpoints?endpoint_scheme=http&region=us-east-1",
                        false,
                        configuration);

        Config.VolumeDescriptor checkpointVolume = volumes.get(0);
        assertEquals("test-access-id", checkpointVolume.accessId);
        assertEquals("test-secret-key", checkpointVolume.secretKey);
        assertNotNull(checkpointVolume.customOptions);
        assertEquals("true", checkpointVolume.customOptions.get("disable_config_load"));
        assertEquals("true", checkpointVolume.customOptions.get("disable_ec2_metadata"));
        assertNull(checkpointVolume.customOptions.get("endpoint"));
    }

    @Test
    void createVolumeDescriptorsCopiesS3EndpointSettingsFromFlinkConfig(@TempDir Path tempDir) {
        File instanceBasePath = tempDir.resolve("instance").toFile();
        Configuration configuration = new Configuration();
        configuration.setString("s3.endpoint", "http://127.0.0.1:9000");
        configuration.setString("s3.path.style.access", "true");

        List<Config.VolumeDescriptor> volumes =
                CobbleKeyedStateBackendBuilder.createVolumeDescriptors(
                        instanceBasePath,
                        CHECKPOINT_SCOPE,
                        "s3://cobble-test/checkpoints",
                        false,
                        configuration);

        Config.VolumeDescriptor checkpointVolume = volumes.get(0);
        assertNotNull(checkpointVolume.customOptions);
        assertEquals("http://127.0.0.1:9000", checkpointVolume.customOptions.get("endpoint"));
        assertEquals("false", checkpointVolume.customOptions.get("enable_virtual_host_style"));
        assertEquals("us-east-1", checkpointVolume.customOptions.get("region"));
    }

    @Test
    void createVolumeDescriptorsCopiesOssCheckpointCredentialsFromFlinkConfig(
            @TempDir Path tempDir) {
        File instanceBasePath = tempDir.resolve("instance").toFile();
        Configuration configuration = new Configuration();
        configuration.setString("fs.oss.accessKeyId", "oss-access-id");
        configuration.setString("fs.oss.accessKeySecret", "oss-secret-key");
        configuration.setString("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");

        List<Config.VolumeDescriptor> volumes =
                CobbleKeyedStateBackendBuilder.createVolumeDescriptors(
                        instanceBasePath,
                        CHECKPOINT_SCOPE,
                        "oss://example-bucket/checkpoints",
                        false,
                        configuration);

        Config.VolumeDescriptor checkpointVolume = volumes.get(0);
        assertEquals("oss-access-id", checkpointVolume.accessId);
        assertEquals("oss-secret-key", checkpointVolume.secretKey);
        assertNotNull(checkpointVolume.customOptions);
        assertEquals(
                "https://oss-cn-hangzhou.aliyuncs.com",
                checkpointVolume.customOptions.get("endpoint"));
    }

    @Test
    void timerStateUsesCobblePriorityQueueImplementation(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            TestTimerElementSerializer timerSerializer = new TestTimerElementSerializer();

            KeyGroupedInternalPriorityQueue<TestTimerElement> queue =
                    backend.create("timer-state", timerSerializer);
            assertInstanceOf(CobbleTimerPriorityQueue.class, queue);
            CobbleTimerPriorityQueue<TestTimerElement> cobbleQueue =
                    (CobbleTimerPriorityQueue<TestTimerElement>) queue;
            assertEquals(1, cobbleQueue.bucketForKeyGroup(1));
            assertEquals(2, cobbleQueue.bucketForKeyGroup(2));
            assertTrue(backend.requiresLegacySynchronousTimerSnapshots(null));

            TestTimerElement later = new TestTimerElement(20L, 1);
            TestTimerElement earlier = new TestTimerElement(10L, 2);

            queue.add(later);
            queue.add(earlier);

            assertTrue(queue.getSubsetForKeyGroup(1).isEmpty());
            assertTrue(queue.getSubsetForKeyGroup(2).isEmpty());
            assertEquals(earlier, queue.peek());

            backend.setCurrentKey(2);
            assertEquals(earlier, queue.poll());

            backend.setCurrentKey(1);
            assertEquals(later, queue.poll());
            assertTrue(queue.isEmpty());
        }
    }

    @Test
    void timerStateRegistersInspectSchemaForFlinkTimerSerializer(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.create(
                    "event-time-timers",
                    new TimerSerializer<>(
                            StringSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE));

            StateInspectSchema schema =
                    backend.getStateInspectSchemas().get("timer:event-time-timers");
            assertNotNull(schema);
            assertEquals(StateKind.TIMER, schema.stateKind());
            assertEquals("event-time-timers", schema.stateName());
            assertEquals("__cobble_timer__event-time-timers", schema.columnFamily());
            assertEquals(
                    StringSerializer.class.getName(), schema.keySerializer().serializerClassName());
            assertEquals(
                    VoidNamespaceSerializer.class.getName(),
                    schema.namespaceSerializer().serializerClassName());
        }
    }

    @Test
    void timerStateCanUseHeapPriorityQueueImplementation(@TempDir Path tempDir) throws Exception {
        Configuration overrides = new Configuration();
        overrides.set(
                CobbleOptions.TIMER_SERVICE_FACTORY,
                CobbleStateBackend.PriorityQueueStateType.HEAP);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.emptyList(),
                        KeyGroupRange.of(0, 15),
                        overrides)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            KeyGroupedInternalPriorityQueue<TestTimerElement> queue =
                    backend.create("heap-timer-state", new TestTimerElementSerializer());
            TestTimerElement later = new TestTimerElement(20L, findKeyForGroup(1));
            TestTimerElement earlier = new TestTimerElement(10L, findKeyForGroup(2));

            assertFalse(queue instanceof CobbleTimerPriorityQueue);
            assertFalse(
                    backend.getCobbleDb()
                            .currentSchema()
                            .columnFamilies()
                            .containsKey(
                                    CobblePriorityQueueSetFactory.timerQueueColumnFamilyName(
                                            "heap-timer-state")));
            assertTrue(queue.add(later));
            assertTrue(queue.add(earlier));
            assertEquals(Collections.singleton(later), queue.getSubsetForKeyGroup(1));
            assertEquals(Collections.singleton(earlier), queue.getSubsetForKeyGroup(2));
            assertEquals(earlier, queue.poll());
            assertEquals(later, queue.poll());
            assertTrue(queue.isEmpty());
        }
    }

    @Test
    void rejectsRestoringCobbleTimersIntoHeapQueues(@TempDir Path tempDir) throws Exception {
        String checkpointDirectory = tempDir.resolve("checkpoints").toString();
        KeyedStateHandle snapshotHandle;

        try (TestBackendContext context =
                createBackendContext(tempDir.resolve("source"), false, checkpointDirectory)) {
            KeyGroupedInternalPriorityQueue<TestTimerElement> queue =
                    context.cobbleBackend.create("timer-state", new TestTimerElementSerializer());
            assertTrue(queue.add(new TestTimerElement(10L, findKeyForGroup(2))));
            snapshotHandle = runCheckpointSnapshot(context.cobbleBackend, 87L);
        }

        Configuration heapConfiguration = new Configuration();
        heapConfiguration.set(
                CobbleOptions.TIMER_SERVICE_FACTORY,
                CobbleStateBackend.PriorityQueueStateType.HEAP);
        UnsupportedOperationException error =
                assertThrows(
                        UnsupportedOperationException.class,
                        () ->
                                createBackendContext(
                                        tempDir.resolve("restored"),
                                        false,
                                        checkpointDirectory,
                                        null,
                                        TtlTimeProvider.DEFAULT,
                                        false,
                                        Collections.singletonList(snapshotHandle),
                                        KeyGroupRange.of(0, 15),
                                        heapConfiguration));
        assertTrue(error.getMessage().contains(CobbleOptions.TIMER_SERVICE_FACTORY.key()));
    }

    @Test
    void timerStateRestoresFromCobbleSnapshot(@TempDir Path tempDir) throws Exception {
        String checkpointDirectory = tempDir.resolve("checkpoints").toString();
        KeyedStateHandle snapshotHandle;
        Set<TestTimerElement> keyGroupOneOverlaySnapshot;
        Set<TestTimerElement> keyGroupTwoOverlaySnapshot;

        try (TestBackendContext context =
                createBackendContext(tempDir.resolve("source"), false, checkpointDirectory)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            TestTimerElementSerializer timerSerializer = new TestTimerElementSerializer();
            KeyGroupedInternalPriorityQueue<TestTimerElement> queue =
                    backend.create("timer-state", timerSerializer);

            TestTimerElement first = new TestTimerElement(10L, findKeyForGroup(2));
            TestTimerElement second = new TestTimerElement(20L, findKeyForGroup(1));
            TestTimerElement third = new TestTimerElement(30L, findKeyForGroup(2));

            queue.add(third);
            queue.add(first);
            queue.add(second);

            assertEquals(first, queue.poll());
            keyGroupOneOverlaySnapshot = queue.getSubsetForKeyGroup(1);
            keyGroupTwoOverlaySnapshot = queue.getSubsetForKeyGroup(2);
            snapshotHandle = runCheckpointSnapshot(backend, 88L);
        }

        try (TestBackendContext context =
                createBackendContext(
                        tempDir.resolve("restored"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(snapshotHandle))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            KeyGroupedInternalPriorityQueue<TestTimerElement> restoredQueue =
                    backend.create("timer-state", new TestTimerElementSerializer());

            restoredQueue.addAll(keyGroupOneOverlaySnapshot);
            restoredQueue.addAll(keyGroupTwoOverlaySnapshot);
            assertEquals(2, restoredQueue.size());
            assertEquals(new TestTimerElement(20L, findKeyForGroup(1)), restoredQueue.peek());
            assertEquals(new TestTimerElement(20L, findKeyForGroup(1)), restoredQueue.poll());
            assertEquals(new TestTimerElement(30L, findKeyForGroup(2)), restoredQueue.poll());
            assertTrue(restoredQueue.isEmpty());
        }
    }

    @Test
    void timerStateAcceptsEarlierTimerAfterPolling(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            KeyGroupedInternalPriorityQueue<TestTimerElement> queue =
                    backend.create("timer-state", new TestTimerElementSerializer());
            int key = findKeyForGroup(2);
            TestTimerElement later = new TestTimerElement(20L, key);
            TestTimerElement earlier = new TestTimerElement(10L, key);

            assertTrue(queue.add(later));
            assertEquals(later, queue.poll());
            assertTrue(queue.add(earlier));
            assertEquals(Collections.singleton(earlier), queue.getSubsetForKeyGroup(2));
            assertEquals(earlier, queue.poll());
            assertTrue(queue.getSubsetForKeyGroup(2).isEmpty());
            assertTrue(queue.isEmpty());
        }
    }

    @Test
    void timerStateAcceptsSameTimerAfterPolling(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            KeyGroupedInternalPriorityQueue<TestTimerElement> queue =
                    backend.create("timer-state", new TestTimerElementSerializer());
            TestTimerElement timer = new TestTimerElement(20L, findKeyForGroup(2));

            assertTrue(queue.add(timer));
            assertEquals(timer, queue.poll());
            assertTrue(queue.add(timer));
            assertEquals(Collections.singleton(timer), queue.getSubsetForKeyGroup(2));
            assertEquals(timer, queue.poll());
            assertTrue(queue.isEmpty());
        }
    }

    @Test
    void timerStateRestoresLateTimerFromLegacyOverlaySnapshot(@TempDir Path tempDir)
            throws Exception {
        String checkpointDirectory = tempDir.resolve("checkpoints").toString();
        KeyedStateHandle snapshotHandle;
        Set<TestTimerElement> overlaySnapshot;
        int key = findKeyForGroup(2);
        TestTimerElement consumed = new TestTimerElement(20L, key);
        TestTimerElement late = new TestTimerElement(10L, key);

        try (TestBackendContext context =
                createBackendContext(tempDir.resolve("source"), false, checkpointDirectory)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            KeyGroupedInternalPriorityQueue<TestTimerElement> queue =
                    backend.create("timer-state", new TestTimerElementSerializer());

            assertTrue(queue.add(consumed));
            assertEquals(consumed, queue.poll());
            assertTrue(queue.add(late));
            overlaySnapshot = queue.getSubsetForKeyGroup(2);
            snapshotHandle = runCheckpointSnapshot(backend, 89L);
        }

        try (TestBackendContext context =
                createBackendContext(
                        tempDir.resolve("restored"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(snapshotHandle))) {
            KeyGroupedInternalPriorityQueue<TestTimerElement> restoredQueue =
                    context.cobbleBackend.create("timer-state", new TestTimerElementSerializer());

            restoredQueue.addAll(overlaySnapshot);
            assertEquals(late, restoredQueue.poll());
            assertTrue(restoredQueue.isEmpty());
        }
    }

    @Test
    void createKeyedStateBackendAppliesSelectedCobbleOptionsFromFlinkConfig(@TempDir Path tempDir)
            throws Exception {
        Configuration overrides = new Configuration();
        overrides.set(CobbleOptions.MEMTABLE_TYPE, "skiplist");
        overrides.set(CobbleOptions.COMPACTION_POLICY, "min_overlap");
        overrides.set(CobbleOptions.SST_BLOOM_FILTER_ENABLED, true);
        overrides.set(CobbleOptions.SST_BLOOM_FILTER_BITS_PER_KEY, 15);
        overrides.set(CobbleOptions.SST_PARTITIONED_INDEX_ENABLED, true);
        overrides.set(CobbleOptions.DIRECT_IO_BUFFER_SIZE, MemorySize.parse("8kb"));
        overrides.set(CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE, 128);
        overrides.set(CobbleOptions.LOG_LEVEL, "debug");
        overrides.set(CobbleOptions.LOG_MAX_FILE_SIZE, MemorySize.parse("16mb"));
        overrides.set(CobbleOptions.LOG_KEEP_FILES, 5);
        overrides.set(CobbleOptions.VALUE_SEPARATION_THRESHOLD, MemorySize.parse("4kb"));
        overrides.set(CobbleOptions.SNAPSHOT_RETENTION, 3);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.emptyList(),
                        KeyGroupRange.of(0, 15),
                        overrides)) {
            Config config = context.cobbleBackend.getCobbleConfig();
            assertEquals(Config.MemtableType.SKIPLIST, config.memtableType);
            assertEquals(Config.CompactionPolicyKind.MIN_OVERLAP, config.compactionPolicy);
            assertTrue(config.sstBloomFilterEnabled);
            assertEquals(15, config.sstBloomBitsPerKey.intValue());
            assertTrue(config.sstPartitionedIndex);
            assertEquals(8 * 1024, config.jniDirectBufferSize.intValue());
            assertEquals(128, config.jniDirectBufferPoolSize.intValue());
            assertEquals("DEBUG", config.logLevel);
            assertEquals(16 * 1024 * 1024, config.logMaxFileSize.intValue());
            assertEquals(5, config.logKeepFiles.intValue());
            assertEquals(4 * 1024, config.valueSeparationThreshold.intValue());
            assertEquals(3, config.snapshotRetention.intValue());
            assertEquals(Config.GovernanceMode.NOOP, config.governanceMode);
            assertEquals(1, config.numColumns.intValue());
        }
    }

    @Test
    void shardSnapshotCheckpointUploadsMetadataIntoKeyedStateHandle(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>("snapshot-state", StringSerializer.INSTANCE);

            backend.setCurrentKey(7);
            ValueState<String> state =
                    backend.getPartitionedState(
                            "snapshot-ns", StringSerializer.INSTANCE, descriptor);
            state.update("snapshot-value");

            KeyedStateHandle keyedStateHandle = runCheckpointSnapshot(backend, 41L);
            IncrementalRemoteKeyedStateHandle incrementalHandle =
                    assertInstanceOf(IncrementalRemoteKeyedStateHandle.class, keyedStateHandle);
            assertNotNull(incrementalHandle.getMetaStateHandle());

            CobbleSnapshotMetadata metadata = readSnapshotMetadata(keyedStateHandle);
            ShardSnapshot shardSnapshot = metadata.shardSnapshot();

            assertTrue(shardSnapshot.snapshotId >= 0L);
            assertEquals(backend.getCobbleDb().id(), shardSnapshot.dbId);
            assertFalse(shardSnapshot.manifestPath.isEmpty());
            assertFalse(shardSnapshot.ranges.isEmpty());
            assertTrue(shardSnapshot.columnFamilyIds.containsKey("snapshot-state"));
            assertTrue(shardSnapshot.dataSizeBytes > 0L);
            assertTrue(shardSnapshot.incrementalDataSizeBytes > 0L);
            assertTrue(
                    incrementalHandle.getStateSize()
                            > incrementalHandle.getMetaStateHandle().getStateSize());
            assertEquals(
                    incrementalHandle.getMetaStateHandle().getStateSize()
                            + shardSnapshot.incrementalDataSizeBytes,
                    incrementalHandle.getCheckpointedSize());
            assertTrue(backend.hasTrackedSnapshot(41L));
        }
    }

    @Test
    void completedCheckpointLeavesPublishedShardSnapshotRetainedUntilSubsumed(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>(
                            "retained-snapshot-state", StringSerializer.INSTANCE);

            backend.setCurrentKey(8);
            backend.getPartitionedState(
                            "retained-snapshot-ns", StringSerializer.INSTANCE, descriptor)
                    .update("retained");

            KeyedStateHandle keyedStateHandle = runCheckpointSnapshot(backend, 42L);
            long snapshotId = readSnapshotMetadata(keyedStateHandle).shardSnapshot().snapshotId;

            assertTrue(backend.hasTrackedSnapshot(42L));

            backend.notifyCheckpointComplete(42L);
            assertTrue(backend.hasTrackedSnapshot(42L));

            backend.notifyCheckpointSubsumed(42L);
            assertFalse(backend.hasTrackedSnapshot(42L));
            assertFalse(backend.getCobbleDb().expireSnapshot(snapshotId));
        }
    }

    @Test
    void abortedCheckpointExpiresPublishedShardSnapshot(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>("aborted-snapshot-state", StringSerializer.INSTANCE);

            backend.setCurrentKey(9);
            backend.getPartitionedState(
                            "aborted-snapshot-ns", StringSerializer.INSTANCE, descriptor)
                    .update("aborted");

            KeyedStateHandle keyedStateHandle = runCheckpointSnapshot(backend, 43L);
            long snapshotId = readSnapshotMetadata(keyedStateHandle).shardSnapshot().snapshotId;

            backend.notifyCheckpointAborted(43L);
            assertFalse(backend.hasTrackedSnapshot(43L));
            assertFalse(backend.getCobbleDb().expireSnapshot(snapshotId));
        }
    }

    @Test
    void abortedCheckpointCancelsPendingShardSnapshot(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>("pending-abort-state", StringSerializer.INSTANCE);

            backend.setCurrentKey(10);
            backend.getPartitionedState("pending-abort-ns", StringSerializer.INSTANCE, descriptor)
                    .update("value");

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                    backend.snapshot(
                            44L,
                            System.currentTimeMillis(),
                            new MemCheckpointStreamFactory(1024 * 1024),
                            CheckpointOptions.forCheckpointWithDefaultLocation());
            Long snapshotId = backend.snapshotIdForCheckpoint(44L);
            assertNotNull(snapshotId);

            backend.notifyCheckpointAborted(44L);

            snapshotFuture.run();
            Exception exception = assertThrows(Exception.class, snapshotFuture::get);
            assertFalse(backend.hasTrackedSnapshot(44L));
            assertTrue(hasCause(exception, CancelledError.class));
        }
    }

    @Test
    void subsumedCheckpointClearsAllOlderTrackedSnapshots(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>(
                            "subsumed-watermark-state", StringSerializer.INSTANCE);

            backend.setCurrentKey(11);
            backend.getPartitionedState(
                            "subsumed-watermark-ns", StringSerializer.INSTANCE, descriptor)
                    .update("first");
            long firstSnapshotId =
                    readSnapshotMetadata(runCheckpointSnapshot(backend, 45L))
                            .shardSnapshot()
                            .snapshotId;

            backend.setCurrentKey(12);
            backend.getPartitionedState(
                            "subsumed-watermark-ns", StringSerializer.INSTANCE, descriptor)
                    .update("second");
            long secondSnapshotId =
                    readSnapshotMetadata(runCheckpointSnapshot(backend, 46L))
                            .shardSnapshot()
                            .snapshotId;

            backend.setCurrentKey(13);
            backend.getPartitionedState(
                            "subsumed-watermark-ns", StringSerializer.INSTANCE, descriptor)
                    .update("third");
            long thirdSnapshotId =
                    readSnapshotMetadata(runCheckpointSnapshot(backend, 47L))
                            .shardSnapshot()
                            .snapshotId;

            backend.notifyCheckpointSubsumed(46L);

            assertFalse(backend.hasTrackedSnapshot(45L));
            assertFalse(backend.hasTrackedSnapshot(46L));
            assertTrue(backend.hasTrackedSnapshot(47L));
            assertFalse(backend.getCobbleDb().expireSnapshot(firstSnapshotId));
            assertFalse(backend.getCobbleDb().expireSnapshot(secondSnapshotId));
            assertTrue(backend.getCobbleDb().expireSnapshot(thirdSnapshotId));
        }
    }

    @Test
    void failedSnapshotMaterializationDoesNotPublishTrackedSnapshot(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>("failing-snapshot-state", StringSerializer.INSTANCE);

            backend.setCurrentKey(10);
            backend.getPartitionedState(
                            "failing-snapshot-ns", StringSerializer.INSTANCE, descriptor)
                    .update("value");

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                    backend.snapshot(
                            44L,
                            System.currentTimeMillis(),
                            new FailingCheckpointStreamFactory(),
                            CheckpointOptions.forCheckpointWithDefaultLocation());

            snapshotFuture.run();
            assertThrows(Exception.class, snapshotFuture::get);
            assertFalse(backend.hasTrackedSnapshot(44L));
        }
    }

    @Test
    void restoreFromShardSnapshotReopensStateAndSchema(@TempDir Path tempDir) throws Exception {
        String checkpointDirectory = tempDir.resolve("checkpoints").toString();
        String restoredCheckpointDirectory = tempDir.resolve("restored-checkpoints").toString();
        KeyedStateHandle snapshotHandle;

        try (TestBackendContext context =
                createBackendContext(tempDir.resolve("source"), false, checkpointDirectory)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;

            ValueStateDescriptor<String> valueStateDescriptor =
                    new ValueStateDescriptor<>("restore-value-state", StringSerializer.INSTANCE);
            ListStateDescriptor<String> listStateDescriptor =
                    new ListStateDescriptor<>("restore-list-state", StringSerializer.INSTANCE);
            MapStateDescriptor<String, String> mapStateDescriptor =
                    new MapStateDescriptor<>(
                            "restore-map-state",
                            StringSerializer.INSTANCE,
                            StringSerializer.INSTANCE);

            backend.setCurrentKey(1);
            backend.getPartitionedState(
                            "restore-ns", StringSerializer.INSTANCE, valueStateDescriptor)
                    .update("value-1");

            backend.setCurrentKey(2);
            ListState<String> listState =
                    backend.getPartitionedState(
                            "restore-ns", StringSerializer.INSTANCE, listStateDescriptor);
            listState.add("left");
            listState.add("right");

            backend.setCurrentKey(3);
            MapState<String, String> mapState =
                    backend.getPartitionedState(
                            "restore-ns", StringSerializer.INSTANCE, mapStateDescriptor);
            mapState.put("left", "L");
            mapState.put("right", "R");

            snapshotHandle = runCheckpointSnapshot(backend, 51L);
        }

        try (TestBackendContext context =
                createBackendContext(
                        tempDir.resolve("restored"),
                        false,
                        restoredCheckpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(snapshotHandle))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;

            Schema schema = backend.getCobbleDb().currentSchema();
            assertBytesColumnFamily(schema, "restore-value-state");
            assertBytesColumnFamily(schema, "restore-list-state");
            assertBytesColumnFamily(schema, "restore-map-state");
            assertEquals(3, backend.getCobbleConfig().volumes.size());
            assertVolumeKinds(
                    backend.getCobbleConfig().volumes.get(0),
                    Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                    Config.VolumeUsageKind.META,
                    Config.VolumeUsageKind.SNAPSHOT);
            assertVolumeKinds(
                    backend.getCobbleConfig().volumes.get(1),
                    Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                    Config.VolumeUsageKind.META,
                    Config.VolumeUsageKind.SNAPSHOT);

            ValueStateDescriptor<String> valueStateDescriptor =
                    new ValueStateDescriptor<>("restore-value-state", StringSerializer.INSTANCE);
            ListStateDescriptor<String> listStateDescriptor =
                    new ListStateDescriptor<>("restore-list-state", StringSerializer.INSTANCE);
            MapStateDescriptor<String, String> mapStateDescriptor =
                    new MapStateDescriptor<>(
                            "restore-map-state",
                            StringSerializer.INSTANCE,
                            StringSerializer.INSTANCE);

            ValueState<String> valueState =
                    backend.getPartitionedState(
                            "restore-ns", StringSerializer.INSTANCE, valueStateDescriptor);
            ListState<String> listState =
                    backend.getPartitionedState(
                            "restore-ns", StringSerializer.INSTANCE, listStateDescriptor);
            MapState<String, String> mapState =
                    backend.getPartitionedState(
                            "restore-ns", StringSerializer.INSTANCE, mapStateDescriptor);

            backend.setCurrentKey(1);
            assertEquals("value-1", valueState.value());
            assertEquals(
                    Arrays.asList("left", "right"),
                    toList(assertListStateValues(backend, listState, 2)));

            backend.setCurrentKey(3);
            assertEquals("L", mapState.get("left"));
            assertEquals("R", mapState.get("right"));
        }
    }

    @Test
    void restoreFromShardSnapshotKeepsTtlWiringActive(@TempDir Path tempDir) throws Exception {
        String checkpointDirectory = tempDir.resolve("checkpoints").toString();
        MockTtlTimeProvider ttlTimeProvider = new MockTtlTimeProvider();
        ttlTimeProvider.setCurrentTimestamp(10_000L);
        KeyedStateHandle snapshotHandle;

        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("restore-ttl-state", StringSerializer.INSTANCE);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());

        try (TestBackendContext context =
                createBackendContext(
                        tempDir.resolve("ttl-source"),
                        false,
                        checkpointDirectory,
                        null,
                        ttlTimeProvider,
                        true,
                        Collections.<KeyedStateHandle>emptyList())) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setTimeForTests(ttlTimeProvider.currentTimestamp());
            backend.setCurrentKey(4);
            backend.getPartitionedState("restore-ttl-ns", StringSerializer.INSTANCE, descriptor)
                    .update("ttl-value");

            snapshotHandle = runCheckpointSnapshot(backend, 52L);
        }

        try (TestBackendContext context =
                createBackendContext(
                        tempDir.resolve("ttl-restored"),
                        false,
                        checkpointDirectory,
                        null,
                        ttlTimeProvider,
                        true,
                        Collections.singletonList(snapshotHandle))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setTimeForTests(ttlTimeProvider.currentTimestamp());
            ValueState<String> restoredState =
                    backend.getPartitionedState(
                            "restore-ttl-ns", StringSerializer.INSTANCE, descriptor);

            backend.setCurrentKey(4);
            assertEquals("ttl-value", restoredState.value());

            ttlTimeProvider.setCurrentTimestamp(16_000L);
            backend.setTimeForTests(ttlTimeProvider.currentTimestamp());
            assertNull(restoredState.value());
        }
    }

    @Test
    void freshBackendCanReuseCheckpointDirectoryAfterPreviousJobCloses(@TempDir Path tempDir)
            throws Exception {
        String checkpointDirectory = tempDir.resolve("checkpoints").toString();

        try (TestBackendContext firstContext =
                createBackendContext(tempDir.resolve("first-job"), false, checkpointDirectory)) {
            assertNotNull(firstContext.cobbleBackend.getCobbleConfig().volumes.get(0).baseDir);
        }

        try (TestBackendContext secondContext =
                createBackendContext(tempDir.resolve("second-job"), false, checkpointDirectory)) {
            assertNotNull(secondContext.cobbleBackend.getCobbleConfig().volumes.get(0).baseDir);
            assertTrue(secondContext.cobbleBackend.getCobbleDb().getNativeHandle() != 0L);
        }
    }

    @Test
    void restoreFromSingleHandleShrinksToTargetKeyGroupRange(@TempDir Path tempDir)
            throws Exception {
        String checkpointDirectory = tempDir.resolve("checkpoints").toString();
        String restoredCheckpointDirectory = tempDir.resolve("restored-checkpoints").toString();
        KeyedStateHandle snapshotHandle;
        int retainedKey = findKeyForGroup(2);

        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("rescale-value-state", StringSerializer.INSTANCE);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir.resolve("scale-up-source"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.<KeyedStateHandle>emptyList(),
                        KeyGroupRange.of(0, 15))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(retainedKey);
            backend.getPartitionedState("rescale-ns", StringSerializer.INSTANCE, descriptor)
                    .update("retained-value");

            snapshotHandle = runCheckpointSnapshot(backend, 61L);
        }

        try (TestBackendContext context =
                createBackendContext(
                        tempDir.resolve("scale-up-target"),
                        false,
                        restoredCheckpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(snapshotHandle),
                        KeyGroupRange.of(0, 7))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            ValueState<String> valueState =
                    backend.getPartitionedState(
                            "rescale-ns", StringSerializer.INSTANCE, descriptor);

            backend.setCurrentKey(retainedKey);
            assertEquals("retained-value", valueState.value());

            ShardSnapshot rescaledSnapshot =
                    readSnapshotMetadata(runCheckpointSnapshot(backend, 62L)).shardSnapshot();
            assertEquals(1, rescaledSnapshot.ranges.size());
            assertEquals(0, rescaledSnapshot.ranges.get(0).start);
            assertEquals(7, rescaledSnapshot.ranges.get(0).end);
            assertEquals(3, backend.getCobbleConfig().volumes.size());
            assertVolumeKinds(
                    backend.getCobbleConfig().volumes.get(2), Config.VolumeUsageKind.READONLY);
        }
    }

    @Test
    void restoreFromMultipleHandlesExpandsIntoCombinedTargetRange(@TempDir Path tempDir)
            throws Exception {
        String checkpointDirectory = tempDir.resolve("checkpoints").toString();
        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("rescale-merge-state", StringSerializer.INSTANCE);
        int leftKey = findKeyForGroup(2);
        int rightKey = findKeyForGroup(10);
        KeyedStateHandle leftSnapshotHandle;
        KeyedStateHandle rightSnapshotHandle;

        try (TestBackendContext leftContext =
                createBackendContext(
                        tempDir.resolve("scale-down-left"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.<KeyedStateHandle>emptyList(),
                        KeyGroupRange.of(0, 7))) {
            CobbleKeyedStateBackend<Integer> backend = leftContext.cobbleBackend;
            backend.setCurrentKey(leftKey);
            backend.getPartitionedState("rescale-merge-ns", StringSerializer.INSTANCE, descriptor)
                    .update("left-value");
            leftSnapshotHandle = runCheckpointSnapshot(backend, 71L);
        }

        try (TestBackendContext rightContext =
                createBackendContext(
                        tempDir.resolve("scale-down-right"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.<KeyedStateHandle>emptyList(),
                        KeyGroupRange.of(8, 15))) {
            CobbleKeyedStateBackend<Integer> backend = rightContext.cobbleBackend;
            backend.setCurrentKey(rightKey);
            backend.getPartitionedState("rescale-merge-ns", StringSerializer.INSTANCE, descriptor)
                    .update("right-value");
            rightSnapshotHandle = runCheckpointSnapshot(backend, 72L);
        }

        try (TestBackendContext mergedContext =
                createBackendContext(
                        tempDir.resolve("scale-down-target"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Arrays.asList(leftSnapshotHandle, rightSnapshotHandle),
                        KeyGroupRange.of(0, 15))) {
            CobbleKeyedStateBackend<Integer> backend = mergedContext.cobbleBackend;
            ValueState<String> valueState =
                    backend.getPartitionedState(
                            "rescale-merge-ns", StringSerializer.INSTANCE, descriptor);

            backend.setCurrentKey(leftKey);
            assertEquals("left-value", valueState.value());
            backend.setCurrentKey(rightKey);
            assertEquals("right-value", valueState.value());

            ShardSnapshot mergedSnapshot =
                    readSnapshotMetadata(runCheckpointSnapshot(backend, 73L)).shardSnapshot();
            assertEquals(1, mergedSnapshot.ranges.size());
            assertEquals(0, mergedSnapshot.ranges.get(0).start);
            assertEquals(15, mergedSnapshot.ranges.get(0).end);
        }
    }

    @Test
    void valueStateReadWriteExceedsFourMemtables(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context =
                createBackendContext(tempDir, false, null, MemorySize.ofMebiBytes(1))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            ValueStateDescriptor<String> valueStateDescriptor =
                    new ValueStateDescriptor<>("value-state", StringSerializer.INSTANCE);
            ValueState<String> valueState =
                    backend.getPartitionedState(
                            "value-ns", StringSerializer.INSTANCE, valueStateDescriptor);

            int payloadBytes = 4096;
            long requiredBytes = backend.getCobbleConfig().memtableCapacity.longValue() * 4L;
            int entryCount = requiredEntryCount(requiredBytes, payloadBytes);

            for (int key = 0; key < entryCount; key++) {
                backend.setCurrentKey(key);
                valueState.update(payload("value", key, payloadBytes));
            }

            assertTrue((long) entryCount * payloadBytes > requiredBytes);
            assertValueStateEntry(backend, valueState, 0, payloadBytes);
            assertValueStateEntry(backend, valueState, entryCount / 2, payloadBytes);
            assertValueStateEntry(backend, valueState, entryCount - 1, payloadBytes);

            Schema schema = backend.getCobbleDb().currentSchema();
            assertBytesColumnFamily(schema, "value-state");
        }
    }

    @Test
    void valueStateTtlExpiresFromFlinkAndCobble(@TempDir Path tempDir) throws Exception {
        MockTtlTimeProvider ttlTimeProvider = new MockTtlTimeProvider();
        ttlTimeProvider.setCurrentTimestamp(0L);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir, false, null, MemorySize.ofMebiBytes(1), ttlTimeProvider, true)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>("ttl-value-state", StringSerializer.INSTANCE);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());

            backend.setCurrentKey(11);
            ValueState<String> state =
                    backend.getPartitionedState(
                            "ttl-value-ns", StringSerializer.INSTANCE, descriptor);
            state.update("ttl-value");

            setTtlTime(backend, ttlTimeProvider, 4_000L);
            assertEquals("ttl-value", state.value());

            setTtlTime(backend, ttlTimeProvider, 6_000L);
            assertNull(state.value());
        }
    }

    @Test
    void listStateTtlExpiresFromFlinkAndCobble(@TempDir Path tempDir) throws Exception {
        MockTtlTimeProvider ttlTimeProvider = new MockTtlTimeProvider();
        ttlTimeProvider.setCurrentTimestamp(0L);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir, false, null, MemorySize.ofMebiBytes(1), ttlTimeProvider, true)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            ListStateDescriptor<String> descriptor =
                    new ListStateDescriptor<>("ttl-list-state", StringSerializer.INSTANCE);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());

            backend.setCurrentKey(12);
            ListState<String> state =
                    backend.getPartitionedState(
                            "ttl-list-ns", StringSerializer.INSTANCE, descriptor);
            state.add("a");
            state.add("b");

            setTtlTime(backend, ttlTimeProvider, 4_000L);
            assertEquals(java.util.Arrays.asList("a", "b"), toList(state.get()));

            setTtlTime(backend, ttlTimeProvider, 6_000L);
            assertEquals(java.util.Collections.emptyList(), toList(state.get()));
        }
    }

    @Test
    void mapStateTtlExpiresFromFlinkAndCobble(@TempDir Path tempDir) throws Exception {
        MockTtlTimeProvider ttlTimeProvider = new MockTtlTimeProvider();
        ttlTimeProvider.setCurrentTimestamp(0L);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir, false, null, MemorySize.ofMebiBytes(1), ttlTimeProvider, true)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            MapStateDescriptor<String, String> descriptor =
                    new MapStateDescriptor<>(
                            "ttl-map-state", StringSerializer.INSTANCE, StringSerializer.INSTANCE);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());

            backend.setCurrentKey(13);
            MapState<String, String> state =
                    backend.getPartitionedState(
                            "ttl-map-ns", StringSerializer.INSTANCE, descriptor);
            state.put("left", "L");
            state.put("right", "R");

            setTtlTime(backend, ttlTimeProvider, 4_000L);
            assertEquals("L", state.get("left"));
            assertFalse(state.isEmpty());

            setTtlTime(backend, ttlTimeProvider, 6_000L);
            assertNull(state.get("left"));
            assertTrue(state.isEmpty());
        }
    }

    @Test
    void reducingStateTtlExpiresFromFlinkAndCobble(@TempDir Path tempDir) throws Exception {
        MockTtlTimeProvider ttlTimeProvider = new MockTtlTimeProvider();
        ttlTimeProvider.setCurrentTimestamp(0L);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir, false, null, MemorySize.ofMebiBytes(1), ttlTimeProvider, true)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            ReducingStateDescriptor<Integer> descriptor =
                    new ReducingStateDescriptor<>(
                            "ttl-reducing-state", sumReducer(), IntSerializer.INSTANCE);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());

            backend.setCurrentKey(14);
            ReducingState<Integer> state =
                    backend.getPartitionedState(
                            "ttl-reducing-ns", StringSerializer.INSTANCE, descriptor);
            state.add(3);
            state.add(4);

            setTtlTime(backend, ttlTimeProvider, 4_000L);
            assertEquals(7, state.get());

            setTtlTime(backend, ttlTimeProvider, 6_000L);
            assertNull(state.get());
        }
    }

    @Test
    void ttlDisabledStatesRemainAfterTimeAdvanceIncludingListElements(@TempDir Path tempDir)
            throws Exception {
        MockTtlTimeProvider ttlTimeProvider = new MockTtlTimeProvider();
        ttlTimeProvider.setCurrentTimestamp(0L);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir, false, null, MemorySize.ofMebiBytes(1), ttlTimeProvider, true)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(99);

            ValueStateDescriptor<String> valueDescriptor =
                    new ValueStateDescriptor<>("no-ttl-value-state", StringSerializer.INSTANCE);
            ValueState<String> valueState =
                    backend.getPartitionedState(
                            "no-ttl-value-ns", StringSerializer.INSTANCE, valueDescriptor);
            valueState.update("v");

            ListStateDescriptor<String> listDescriptor =
                    new ListStateDescriptor<>("no-ttl-list-state", StringSerializer.INSTANCE);
            ListState<String> listState =
                    backend.getPartitionedState(
                            "no-ttl-list-ns", StringSerializer.INSTANCE, listDescriptor);
            listState.add("a");
            listState.add("b");

            MapStateDescriptor<String, String> mapDescriptor =
                    new MapStateDescriptor<>(
                            "no-ttl-map-state",
                            StringSerializer.INSTANCE,
                            StringSerializer.INSTANCE);
            MapState<String, String> mapState =
                    backend.getPartitionedState(
                            "no-ttl-map-ns", StringSerializer.INSTANCE, mapDescriptor);
            mapState.put("k", "m");

            setTtlTime(backend, ttlTimeProvider, 120_000L);

            assertEquals("v", valueState.value());
            assertEquals(java.util.Arrays.asList("a", "b"), toList(listState.get()));
            assertEquals("m", mapState.get("k"));
        }
    }

    @Test
    void mapStateIsEmptyFiltersOtherNamespacesBeforeReturning(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context =
                createBackendContext(tempDir, false, null, MemorySize.ofMebiBytes(1))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            MapStateDescriptor<String, String> descriptor =
                    new MapStateDescriptor<>(
                            "namespace-map-state",
                            StringSerializer.INSTANCE,
                            StringSerializer.INSTANCE);

            backend.setCurrentKey(7);
            backend.getPartitionedState("ns-a", StringSerializer.INSTANCE, descriptor)
                    .put("shared-user-key", "value-a");
            backend.getPartitionedState("ns-b", StringSerializer.INSTANCE, descriptor)
                    .put("shared-user-key", "value-b");

            assertFalse(
                    backend.getPartitionedState("ns-a", StringSerializer.INSTANCE, descriptor)
                            .isEmpty());
            assertFalse(
                    backend.getPartitionedState("ns-b", StringSerializer.INSTANCE, descriptor)
                            .isEmpty());
            assertTrue(
                    backend.getPartitionedState("ns-c", StringSerializer.INSTANCE, descriptor)
                            .isEmpty());
        }
    }

    @Test
    void mapStateIterationAndClearStayWithinNamespace(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context =
                createBackendContext(tempDir, false, null, MemorySize.ofMebiBytes(1))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            MapStateDescriptor<String, String> descriptor =
                    new MapStateDescriptor<>(
                            "namespace-iter-map-state",
                            StringSerializer.INSTANCE,
                            StringSerializer.INSTANCE);

            backend.setCurrentKey(9);
            backend.getPartitionedState("ns-a", StringSerializer.INSTANCE, descriptor)
                    .put("shared-user-key", "value-a");
            backend.getPartitionedState("ns-b", StringSerializer.INSTANCE, descriptor)
                    .put("shared-user-key", "value-b");

            Map<String, String> nsAEntries = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry :
                    backend.getPartitionedState("ns-a", StringSerializer.INSTANCE, descriptor)
                            .entries()) {
                nsAEntries.put(entry.getKey(), entry.getValue());
            }
            assertEquals(Collections.singletonMap("shared-user-key", "value-a"), nsAEntries);

            MapState<String, String> nsAState =
                    backend.getPartitionedState("ns-a", StringSerializer.INSTANCE, descriptor);
            nsAState.clear();
            assertTrue(nsAState.isEmpty());

            MapState<String, String> nsBState =
                    backend.getPartitionedState("ns-b", StringSerializer.INSTANCE, descriptor);
            assertFalse(nsBState.isEmpty());
            assertEquals("value-b", nsBState.get("shared-user-key"));
        }
    }

    @Test
    void mapStatePresentNullDistinctFromAbsent(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context =
                createBackendContext(tempDir, false, null, MemorySize.ofMebiBytes(1))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            MapState<String, String> state =
                    backend.getPartitionedState(
                            "null-values-ns",
                            StringSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    "null-values-map",
                                    StringSerializer.INSTANCE,
                                    StringSerializer.INSTANCE));
            backend.setCurrentKey(1);

            state.put("present-null", null);
            state.put("present-non-null", "v");

            // get(...) cannot tell present-null from absent — both return null. Only contains(...)
            // distinguishes them, by going through physical row presence.
            assertTrue(state.contains("present-null"));
            assertNull(state.get("present-null"));
            assertTrue(state.contains("present-non-null"));
            assertEquals("v", state.get("present-non-null"));
            assertFalse(state.contains("absent"));
            assertNull(state.get("absent"));
            assertFalse(state.isEmpty());
        }
    }

    @Test
    void mapStateIteratorYieldsPresentNullEntries(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context =
                createBackendContext(tempDir, false, null, MemorySize.ofMebiBytes(1))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            MapState<String, String> state =
                    backend.getPartitionedState(
                            "iter-null-ns",
                            StringSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    "iter-null-map",
                                    StringSerializer.INSTANCE,
                                    StringSerializer.INSTANCE));
            backend.setCurrentKey(2);
            state.put("a", null);
            state.put("b", "vb");

            Map<String, String> observed = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : state.entries()) {
                observed.put(entry.getKey(), entry.getValue());
            }
            Map<String, String> expected = new LinkedHashMap<>();
            expected.put("a", null);
            expected.put("b", "vb");
            assertEquals(expected, observed);

            // keys() and values() must also see both entries; values() carries the null verbatim.
            Set<String> keys = new java.util.HashSet<>();
            state.keys().forEach(keys::add);
            assertEquals(new java.util.HashSet<>(Arrays.asList("a", "b")), keys);

            List<String> values = new ArrayList<>();
            state.values().forEach(values::add);
            assertEquals(2, values.size());
            assertTrue(values.contains(null));
            assertTrue(values.contains("vb"));
        }
    }

    @Test
    void mapStateIteratorSetValueThrowsUnsupported(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context =
                createBackendContext(tempDir, false, null, MemorySize.ofMebiBytes(1))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            MapState<String, String> state =
                    backend.getPartitionedState(
                            "iter-immutable-ns",
                            StringSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    "iter-immutable-map",
                                    StringSerializer.INSTANCE,
                                    StringSerializer.INSTANCE));
            backend.setCurrentKey(3);
            state.put("k", "v");

            Iterator<Map.Entry<String, String>> it = state.iterator();
            assertTrue(it.hasNext());
            Map.Entry<String, String> entry = it.next();
            assertThrows(UnsupportedOperationException.class, () -> entry.setValue("v2"));
            assertThrows(UnsupportedOperationException.class, () -> entry.setValue(null));
            // Drain to exhaustion so the iterator's underlying cursor closes; leaving it open
            // would deadlock the surrounding Db.close() in the test-context teardown.
            while (it.hasNext()) {
                it.next();
            }
        }
    }

    @Test
    void mapStatePutAllAcceptsNullValues(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context =
                createBackendContext(tempDir, false, null, MemorySize.ofMebiBytes(1))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            MapState<String, String> state =
                    backend.getPartitionedState(
                            "putall-null-ns",
                            StringSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    "putall-null-map",
                                    StringSerializer.INSTANCE,
                                    StringSerializer.INSTANCE));
            backend.setCurrentKey(4);

            Map<String, String> payload = new LinkedHashMap<>();
            payload.put("k1", null);
            payload.put("k2", "v2");
            state.putAll(payload);

            assertTrue(state.contains("k1"));
            assertNull(state.get("k1"));
            assertEquals("v2", state.get("k2"));
        }
    }

    @Test
    void mapStateRemoveAndClearWorkOnPresentNullRows(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context =
                createBackendContext(tempDir, false, null, MemorySize.ofMebiBytes(1))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            MapState<String, String> state =
                    backend.getPartitionedState(
                            "remove-null-ns",
                            StringSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    "remove-null-map",
                                    StringSerializer.INSTANCE,
                                    StringSerializer.INSTANCE));
            backend.setCurrentKey(5);
            state.put("a", null);
            state.put("b", null);
            assertTrue(state.contains("a"));
            assertTrue(state.contains("b"));

            state.remove("a");
            assertFalse(state.contains("a"));
            assertTrue(state.contains("b"));
            // isEmpty() must remain authoritative even when the fast-path scan returns
            // empty due to tombstones consuming its maxRows budget; CobbleMapState
            // falls through to the slow path in that case.
            assertFalse(state.isEmpty());
            int liveCount = 0;
            for (Map.Entry<String, String> ignored : state.entries()) {
                liveCount++;
            }
            assertEquals(1, liveCount);

            state.clear();
            assertFalse(state.contains("b"));
            assertTrue(state.isEmpty());
        }
    }

    @Test
    void mapStateGetSerializedValueIncludesPresentNullEntries(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context =
                createBackendContext(tempDir, false, null, MemorySize.ofMebiBytes(1))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            MapStateDescriptor<String, String> descriptor =
                    new MapStateDescriptor<>(
                            "qs-null-map", StringSerializer.INSTANCE, StringSerializer.INSTANCE);
            MapState<String, String> state =
                    backend.getPartitionedState(
                            "qs-null-ns", StringSerializer.INSTANCE, descriptor);
            backend.setCurrentKey(6);
            state.put("present-null", null);
            state.put("present-non-null", "v");

            @SuppressWarnings("unchecked")
            InternalKvState<Integer, String, Map<String, String>> internalState =
                    (InternalKvState<Integer, String, Map<String, String>>) state;
            byte[] keyAndNamespace =
                    KvStateSerializer.serializeKeyAndNamespace(
                            6, IntSerializer.INSTANCE, "qs-null-ns", StringSerializer.INSTANCE);
            byte[] serialized =
                    internalState.getSerializedValue(
                            keyAndNamespace,
                            internalState.getKeySerializer(),
                            internalState.getNamespaceSerializer(),
                            internalState.getValueSerializer());
            assertNotNull(serialized);
            Map<String, String> roundTripped =
                    KvStateSerializer.deserializeMap(
                            serialized, StringSerializer.INSTANCE, StringSerializer.INSTANCE);
            Map<String, String> expected = new LinkedHashMap<>();
            expected.put("present-null", null);
            expected.put("present-non-null", "v");
            assertEquals(expected, roundTripped);
        }
    }

    @Test
    void mapStatePresentNullRowSurvivesCobbleNativeCheckpoint(@TempDir Path tempDir)
            throws Exception {
        int key = 7;
        String checkpointDirectory = tempDir.resolve("checkpoints").toString();
        MapStateDescriptor<String, String> descriptor =
                new MapStateDescriptor<>(
                        "native-null-map", StringSerializer.INSTANCE, StringSerializer.INSTANCE);
        KeyedStateHandle snapshotHandle;
        try (TestBackendContext context =
                createBackendContext(tempDir.resolve("source"), false, checkpointDirectory)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            MapState<String, String> state =
                    backend.getPartitionedState(
                            "native-null-ns", StringSerializer.INSTANCE, descriptor);
            backend.setCurrentKey(key);
            state.put("present-null", null);
            state.put("present-non-null", "v");
            snapshotHandle = runCheckpointSnapshot(backend, 90L);
        }

        try (TestBackendContext context =
                createBackendContext(
                        tempDir.resolve("restore"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(snapshotHandle),
                        KeyGroupRange.of(0, 15))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);
            MapState<String, String> state =
                    backend.getPartitionedState(
                            "native-null-ns", StringSerializer.INSTANCE, descriptor);
            assertTrue(state.contains("present-null"));
            assertNull(state.get("present-null"));
            assertTrue(state.contains("present-non-null"));
            assertEquals("v", state.get("present-non-null"));
        }
    }

    @Test
    void canonicalRestoreAcceptsPresentNullMapValue(@TempDir Path tempDir) throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupsSavepointStateHandle savepoint =
                createCanonicalMapSavepointWithNullValue(key, keyGroup);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);
            MapState<String, String> mapState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    "map-null",
                                    StringSerializer.INSTANCE,
                                    StringSerializer.INSTANCE));

            assertTrue(mapState.contains("map-key-null"));
            assertNull(mapState.get("map-key-null"));
            assertTrue(mapState.contains("map-key-nonnull"));
            assertEquals("map-value", mapState.get("map-key-nonnull"));
        }
    }

    @Test
    void listStateReadWriteExceedsFourMemtables(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context =
                createBackendContext(tempDir, false, null, MemorySize.ofMebiBytes(1))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            ListStateDescriptor<String> listStateDescriptor =
                    new ListStateDescriptor<>("list-state", StringSerializer.INSTANCE);
            ListState<String> listState =
                    backend.getPartitionedState(
                            "list-ns", StringSerializer.INSTANCE, listStateDescriptor);

            int elementBytes = 1024;
            int elementsPerKey = 8;
            long requiredBytes = backend.getCobbleConfig().memtableCapacity.longValue() * 4L;
            int keyCount = requiredEntryCount(requiredBytes, (long) elementBytes * elementsPerKey);

            for (int key = 0; key < keyCount; key++) {
                backend.setCurrentKey(key);
                listState.addAll(expectedListValues(key, elementsPerKey, elementBytes));
            }

            assertTrue((long) keyCount * elementBytes * elementsPerKey > requiredBytes);
            assertEquals(
                    expectedListValues(0, elementsPerKey, elementBytes),
                    toList(assertListStateValues(backend, listState, 0)));
            assertEquals(
                    expectedListValues(keyCount / 2, elementsPerKey, elementBytes),
                    toList(assertListStateValues(backend, listState, keyCount / 2)));
            List<String> lastExpected =
                    expectedListValues(keyCount - 1, elementsPerKey, elementBytes);
            List<String> lastActual =
                    toList(assertListStateValues(backend, listState, keyCount - 1));
            assertEquals(lastExpected, lastActual);

            Schema schema = backend.getCobbleDb().currentSchema();
            assertBytesColumnFamily(schema, "list-state");
        }
    }

    @Test
    void mapStateReadWriteExceedsFourMemtables(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context =
                createBackendContext(tempDir, false, null, MemorySize.ofMebiBytes(1))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            MapStateDescriptor<String, String> mapStateDescriptor =
                    new MapStateDescriptor<>(
                            "map-state", StringSerializer.INSTANCE, StringSerializer.INSTANCE);
            MapState<String, String> mapState =
                    backend.getPartitionedState(
                            "map-ns", StringSerializer.INSTANCE, mapStateDescriptor);

            int entriesPerKey = 8;
            int valueBytes = 2048;
            long requiredBytes = backend.getCobbleConfig().memtableCapacity.longValue() * 4L;
            int keyCount = requiredEntryCount(requiredBytes, (long) entriesPerKey * valueBytes);

            for (int key = 0; key < keyCount; key++) {
                backend.setCurrentKey(key);
                for (Map.Entry<String, String> entry :
                        expectedMapValues(key, entriesPerKey, valueBytes).entrySet()) {
                    mapState.put(entry.getKey(), entry.getValue());
                }
            }

            assertTrue((long) keyCount * entriesPerKey * valueBytes > requiredBytes);
            assertMapStateValues(backend, mapState, 0, entriesPerKey, valueBytes);
            assertMapStateValues(backend, mapState, keyCount / 2, entriesPerKey, valueBytes);
            Map<String, String> lastExpected =
                    assertMapStateValues(
                            backend, mapState, keyCount - 1, entriesPerKey, valueBytes);

            Schema schema = backend.getCobbleDb().currentSchema();
            assertBytesColumnFamily(schema, "map-state");
            backend.setCurrentKey(keyCount - 1);
            assertEquals(
                    lastExpected.get("map-key-" + (keyCount - 1) + "-0"),
                    mapState.get("map-key-" + (keyCount - 1) + "-0"));
        }
    }

    @Test
    void keyedStateSchemaMatchesStateTypes(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;

            backend.setCurrentKey(7);

            ValueStateDescriptor<String> valueStateDescriptor =
                    new ValueStateDescriptor<>("value-state", StringSerializer.INSTANCE);
            ValueState<String> valueState =
                    backend.getPartitionedState(
                            "ns", StringSerializer.INSTANCE, valueStateDescriptor);
            valueState.update("hello");

            ListStateDescriptor<String> listStateDescriptor =
                    new ListStateDescriptor<>("list-state", StringSerializer.INSTANCE);
            backend.getPartitionedState("ns", StringSerializer.INSTANCE, listStateDescriptor)
                    .add("a");

            MapStateDescriptor<String, Integer> mapStateDescriptor =
                    new MapStateDescriptor<>(
                            "map-state", StringSerializer.INSTANCE, IntSerializer.INSTANCE);
            backend.getPartitionedState("ns", StringSerializer.INSTANCE, mapStateDescriptor)
                    .put("left", 1);

            Schema schema = backend.getCobbleDb().currentSchema();
            assertBytesColumnFamily(schema, "value-state");
            assertBytesColumnFamily(schema, "list-state");
            assertBytesColumnFamily(schema, "map-state");
            assertEquals("hello", valueState.value());
        }
    }

    @Test
    void normalizeCheckpointDirectorySupportsFileAndLocalPaths() {
        String normalizedFile =
                CobbleKeyedStateBackendBuilder.normalizeCheckpointDirectory("file:///tmp/cp");
        assertEquals("file:///tmp/cp", normalizedFile);
        assertEquals(
                "file:///tmp/cp",
                CobbleKeyedStateBackendBuilder.normalizeCheckpointDirectory("file:/tmp/cp"));

        assertTrue(
                CobbleKeyedStateBackendBuilder.normalizeCheckpointDirectory("/tmp/cp")
                        .startsWith("file:///"));
    }

    @Test
    void cobbleOperatorSnapshotDirectoryNormalizesSingleSlashFilePointers() {
        assertEquals(
                "file:///tmp/completed-checkpoints/cobble/op-test",
                CobblePathUtils.cobbleOperatorSnapshotDirectory(
                        "file:/tmp/completed-checkpoints/chk-101", "op-test"));
    }

    @Test
    void normalizeCheckpointDirectoryKeepsCustomSchemes() {
        assertEquals(
                "http://tmp/cp",
                CobbleKeyedStateBackendBuilder.normalizeCheckpointDirectory("http://tmp/cp"));
    }

    @Test
    void schemaRegisteredForValueListAndMapStates(@TempDir Path tempDir) throws Exception {
        TestBackendContext ctx =
                createBackendContext(
                        tempDir,
                        true,
                        "file:///tmp/checkpoints/chk-test",
                        null,
                        TtlTimeProvider.DEFAULT,
                        false);
        CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
        try {
            backend.getPartitionedState(
                    "val-ns",
                    StringSerializer.INSTANCE,
                    new ValueStateDescriptor<>("val-state", IntSerializer.INSTANCE));
            backend.getPartitionedState(
                    "list-ns",
                    StringSerializer.INSTANCE,
                    new ListStateDescriptor<>("list-state", IntSerializer.INSTANCE));
            backend.getPartitionedState(
                    "map-ns",
                    StringSerializer.INSTANCE,
                    new MapStateDescriptor<>(
                            "map-state", IntSerializer.INSTANCE, StringSerializer.INSTANCE));

            LinkedHashMap<String, StateInspectSchema> schemas = backend.getStateInspectSchemas();
            assertEquals(3, schemas.size());
            LinkedHashMap<String, StateInspectSemanticSchema> semanticSchemas =
                    backend.getStateInspectSemanticSchemas();
            assertEquals(3, semanticSchemas.size());
            assertEquals("INT", semanticSchemas.get("val-state").value().logicalType());
            assertEquals("INT", semanticSchemas.get("list-state").listElement().logicalType());
            assertEquals("INT", semanticSchemas.get("map-state").mapUserKey().logicalType());
            assertEquals("VARCHAR", semanticSchemas.get("map-state").mapUserValue().logicalType());

            StateInspectSchema valSchema = schemas.get("val-state");
            assertEquals(StateKind.VALUE, valSchema.stateKind());
            assertEquals("val-state", valSchema.columnFamily());
            assertFalse(valSchema.ttlEnabled());

            StateInspectSchema listSchema = schemas.get("list-state");
            assertEquals(StateKind.LIST, listSchema.stateKind());

            StateInspectSchema mapSchema = schemas.get("map-state");
            assertEquals(StateKind.MAP, mapSchema.stateKind());
        } finally {
            ctx.close();
        }
    }

    @Test
    void reRegisteringSameStateIsIdempotent(@TempDir Path tempDir) throws Exception {
        TestBackendContext ctx =
                createBackendContext(
                        tempDir,
                        true,
                        "file:///tmp/checkpoints/chk-test",
                        null,
                        TtlTimeProvider.DEFAULT,
                        false);
        CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
        try {
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new ValueStateDescriptor<>("only-state", IntSerializer.INSTANCE));
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new ValueStateDescriptor<>("only-state", IntSerializer.INSTANCE));

            assertEquals(1, backend.getStateInspectSchemas().size());
        } finally {
            ctx.close();
        }
    }

    @Test
    void schemaStoreEmbeddedInSnapshotMetadata(@TempDir Path tempDir) throws Exception {
        TestBackendContext ctx =
                createBackendContext(
                        tempDir,
                        true,
                        "file:///tmp/checkpoints/chk-test",
                        null,
                        TtlTimeProvider.DEFAULT,
                        false);
        CobbleKeyedStateBackend<Integer> backend = ctx.cobbleBackend;
        try {
            backend.setCurrentKey(1);
            ValueStateDescriptor<Integer> snapDescriptor =
                    new ValueStateDescriptor<>("snap-state", IntSerializer.INSTANCE);
            ValueState<Integer> valueState =
                    backend.getPartitionedState(
                            "void-state", StringSerializer.INSTANCE, snapDescriptor);
            valueState.update(100);

            MemCheckpointStreamFactory streamFactory =
                    new MemCheckpointStreamFactory(4 * 1024 * 1024);
            RunnableFuture<SnapshotResult<KeyedStateHandle>> future =
                    backend.snapshot(
                            1L,
                            0L,
                            streamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());
            future.run();
            SnapshotResult<KeyedStateHandle> result = future.get();
            KeyedStateHandle handle = result.getJobManagerOwnedSnapshot();
            assertInstanceOf(IncrementalRemoteKeyedStateHandle.class, handle);
            IncrementalRemoteKeyedStateHandle incrementalHandle =
                    (IncrementalRemoteKeyedStateHandle) handle;
            assertNotNull(incrementalHandle.getMetaStateHandle());

            CobbleSnapshotMetadata metadata;
            try (org.apache.flink.core.fs.FSDataInputStream stream =
                    incrementalHandle.getMetaStateHandle().openInputStream()) {
                metadata = CobbleSnapshotMetadata.read(new DataInputViewStreamWrapper(stream));
            }
            assertNotNull(metadata);
            assertNotNull(metadata.schemaStore());
            assertFalse(metadata.schemaStore().isEmpty());
            assertEquals(1, metadata.schemaStore().schemas().size());
            StateInspectSchema schema = metadata.schemaStore().schemas().get(0);
            assertEquals("snap-state", schema.stateName());
            assertEquals(StateKind.VALUE, schema.stateKind());
        } finally {
            ctx.close();
        }
    }

    @Test
    void inspectSchemaRegistryHashStableForSameSchema() throws Exception {
        StateInspectSchemaStore store = StateInspectSchemaStore.fromBytes(new byte[] {});
        // Empty store doesn't have any state, but we can compare empty stores.
        // The key property: same bytes → same hash.
        byte[] bytes1 = new byte[] {1, 2, 3, 4};
        byte[] bytes2 = new byte[] {1, 2, 3, 4};
        String hash1 = InspectSchemaRegistryLayout.sha256(bytes1);
        String hash2 = InspectSchemaRegistryLayout.sha256(bytes2);
        assertEquals(hash1, hash2);
        // Different bytes → different hash (with overwhelming probability).
        String hash3 = InspectSchemaRegistryLayout.sha256(new byte[] {5, 6, 7, 8});
        assertFalse(hash1.equals(hash3));
    }

    @Test
    void mergingSubtaskSchemasKeepsSemanticMetadata() {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "join-state",
                        "join-state",
                        false,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"),
                        StateInspectType.scalar("VARCHAR"),
                        StateInspectType.scalar("VARCHAR"));
        Map<String, StateInspectSemanticSchema> semanticSchemas = new LinkedHashMap<>();
        semanticSchemas.put("join-state", semanticSchema);

        StateInspectSchemaStore merged =
                CobbleCompletedCheckpointStore.mergeSchemaStores(
                        Collections.singletonList(
                                new StateInspectSchemaStore(
                                        Collections.singletonList(schema), semanticSchemas)));

        assertEquals(1, merged.schemas().size());
        assertEquals(semanticSchema, merged.semanticSchema("join-state"));
    }

    @Test
    void parseEventFileNameParsesValidFormat() {
        String fileName =
                "SCHEMA-00000000000000000100-abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789.ref";
        InspectSchemaRegistryLayout.SchemaEvent event =
                InspectSchemaRegistryLayout.parseEventFileName(fileName);
        assertNotNull(event);
        assertEquals(100L, event.checkpointId());
        assertEquals(
                "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", event.hash());
    }

    @Test
    void parseEventFileNameRejectsMalformedNames() {
        assertNull(InspectSchemaRegistryLayout.parseEventFileName(null));
        assertNull(InspectSchemaRegistryLayout.parseEventFileName(""));
        assertNull(InspectSchemaRegistryLayout.parseEventFileName("scenario.txt"));
        // Missing required .ref suffix.
        assertNull(
                InspectSchemaRegistryLayout.parseEventFileName(
                        "SCHEMA-00000000000000000100-abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"));
        // Non-hex hash after SCHEMA- prefix.
        assertNull(
                InspectSchemaRegistryLayout.parseEventFileName(
                        "SCHEMA-00000000000000000100-gggg0123456789abcdef0123456789abcdef0123456789abcdef0123456789.ref"));
        // Missing hash (only separator and ref suffix).
        assertNull(
                InspectSchemaRegistryLayout.parseEventFileName("SCHEMA-00000000000000000100-.ref"));
        // Not enough digits.
        assertNull(InspectSchemaRegistryLayout.parseEventFileName("SCHEMA-100-abcdef.ref"));
        // Non-numeric checkpoint id.
        assertNull(
                InspectSchemaRegistryLayout.parseEventFileName(
                        "SCHEMA-xxxxxxxxxxxxxxxxxxxx-abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789.ref"));
        // Hash too short (63 chars instead of 64).
        assertNull(
                InspectSchemaRegistryLayout.parseEventFileName(
                        "SCHEMA-00000000000000000100-0abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789.ref"));
        // Hash has invalid chars.
        assertNull(
                InspectSchemaRegistryLayout.parseEventFileName(
                        "SCHEMA-00000000000000000100-ZZZZf0123456789abcdef0123456789abcdef0123456789abcdef0123456789.ref"));
    }

    @Test
    void sameSchemaAcrossTwoCheckpointsWritesOneBlobAndOneEvent(@TempDir Path tempDir)
            throws Exception {
        Path rootDir = tempDir.resolve("checkpoint-root");
        Files.createDirectories(rootDir);
        // Pass a path that includes a chk-N component, matching how production code
        // receives checkpoint.getExternalPointer().
        String checkpointPointer = rootDir.resolve("chk-100").toUri().toString();

        // Build a non-empty schema store so the hash is meaningful.
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "value-state",
                        "value-state",
                        false,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE);
        StateInspectSchemaStore store =
                new StateInspectSchemaStore(Collections.singletonList(schema));

        CobbleInspectSchemaRegistry registry =
                new CobbleInspectSchemaRegistry(checkpointPointer, "abc123");

        // First checkpoint → writes blob + event.
        registry.writeIfChanged(100L, store);
        assertTrue(
                Files.exists(rootDir.resolve("cobble/abc123/inspect-schema/blobs")),
                "Blob directory should exist");
        assertTrue(
                Files.exists(rootDir.resolve("cobble/abc123/inspect-schema/events")),
                "Events directory should exist");

        // Second checkpoint with same schema → skips write.
        int fileCountBefore = countFiles(rootDir);
        registry.writeIfChanged(101L, store);
        int fileCountAfter = countFiles(rootDir);
        assertEquals(
                fileCountBefore,
                fileCountAfter,
                "Same schema should not create new files on second checkpoint");
    }

    @Test
    void changedSchemaWritesSecondBlobAndSecondEvent(@TempDir Path tempDir) throws Exception {
        Path rootDir = tempDir.resolve("checkpoint-root");
        Files.createDirectories(rootDir);
        String checkpointPointer = rootDir.resolve("chk-100").toUri().toString();

        // Schema A: IntSerializer value.
        StateInspectSchema schemaA =
                StateInspectSchema.forValue(
                        "val-a",
                        "val-a",
                        false,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE);
        StateInspectSchemaStore storeA =
                new StateInspectSchemaStore(Collections.singletonList(schemaA));

        // Schema B: different state name → different serialized bytes → different hash.
        StateInspectSchema schemaB =
                StateInspectSchema.forValue(
                        "val-b",
                        "val-b",
                        false,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE);
        StateInspectSchemaStore storeB =
                new StateInspectSchemaStore(Collections.singletonList(schemaB));

        CobbleInspectSchemaRegistry registry =
                new CobbleInspectSchemaRegistry(checkpointPointer, "abc123");

        registry.writeIfChanged(100L, storeA);
        int fileCountAfterA = countFiles(rootDir);

        registry.writeIfChanged(200L, storeB);
        int fileCountAfterB = countFiles(rootDir);

        assertTrue(
                fileCountAfterB > fileCountAfterA,
                "Changed schema should create additional blob or event files");
    }

    @Test
    void restartedRegistryRecoversLatestEvent(@TempDir Path tempDir) throws Exception {
        Path rootDir = tempDir.resolve("checkpoint-root");
        Files.createDirectories(rootDir);
        String checkpointPointer = rootDir.resolve("chk-100").toUri().toString();

        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "v1",
                        "v1",
                        false,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE);
        StateInspectSchemaStore store =
                new StateInspectSchemaStore(Collections.singletonList(schema));

        // First registry writes checkpoint 100.
        CobbleInspectSchemaRegistry registry1 =
                new CobbleInspectSchemaRegistry(checkpointPointer, "abc123");
        registry1.writeIfChanged(100L, store);

        // "Restart": a fresh registry instance (using same checkpoint pointer) should recover
        // the latest event and skip rewrite.
        CobbleInspectSchemaRegistry registry2 =
                new CobbleInspectSchemaRegistry(checkpointPointer, "abc123");
        int fileCountBeforeRestart = countFiles(rootDir);
        registry2.writeIfChanged(101L, store);
        int fileCountAfterRestart = countFiles(rootDir);
        assertEquals(
                fileCountBeforeRestart,
                fileCountAfterRestart,
                "After restart, same schema should be recognized and not rewritten");
    }

    @Test
    void resolveSchemaForCheckpointSelectsCorrectSchema(@TempDir Path tempDir) throws Exception {
        Path rootDir = tempDir.resolve("checkpoint-root");
        Files.createDirectories(rootDir);
        String checkpointPointer = rootDir.resolve("chk-100").toUri().toString();

        // Schema at checkpoint 100.
        StateInspectSchema schema100 =
                StateInspectSchema.forValue(
                        "s100",
                        "s100",
                        false,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE);
        StateInspectSchemaStore store100 =
                new StateInspectSchemaStore(Collections.singletonList(schema100));

        // Schema at checkpoint 200 (different state name).
        StateInspectSchema schema200 =
                StateInspectSchema.forValue(
                        "s200",
                        "s200",
                        false,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE);
        StateInspectSchemaStore store200 =
                new StateInspectSchemaStore(Collections.singletonList(schema200));

        CobbleInspectSchemaRegistry registry =
                new CobbleInspectSchemaRegistry(checkpointPointer, "abc123");
        registry.writeIfChanged(100L, store100);
        registry.writeIfChanged(200L, store200);

        // Checkpoint 150 → should resolve to schema from event 100.
        StateInspectSchemaStore resolved150 = registry.resolveSchemaForCheckpoint(150L);
        assertFalse(resolved150.isEmpty());
        assertEquals("s100", resolved150.schemas().get(0).stateName());

        // Checkpoint 100 → should resolve to same schema (event 100).
        StateInspectSchemaStore resolved100 = registry.resolveSchemaForCheckpoint(100L);
        assertEquals("s100", resolved100.schemas().get(0).stateName());

        // Checkpoint 200 → should resolve to updated schema (event 200).
        StateInspectSchemaStore resolved200 = registry.resolveSchemaForCheckpoint(200L);
        assertEquals("s200", resolved200.schemas().get(0).stateName());

        // Checkpoint 300 → should resolve to latest (event 200).
        StateInspectSchemaStore resolved300 = registry.resolveSchemaForCheckpoint(300L);
        assertEquals("s200", resolved300.schemas().get(0).stateName());
    }

    @Test
    void resolveSchemaReturnsEmptyWhenNoEventExists(@TempDir Path tempDir) throws Exception {
        Path rootDir = tempDir.resolve("checkpoint-root");
        Files.createDirectories(rootDir);
        String checkpointPointer = rootDir.resolve("chk-100").toUri().toString();
        CobbleInspectSchemaRegistry registry =
                new CobbleInspectSchemaRegistry(checkpointPointer, "abc123");
        StateInspectSchemaStore result = registry.resolveSchemaForCheckpoint(100L);
        assertTrue(result.isEmpty(), "Empty registry should return empty store");
    }

    @Test
    void chkDirectoryDoesNotReceiveSchemaSidecarFiles(@TempDir Path tempDir) throws Exception {
        // Verify that the registry writes into
        // <root>/cobble/<opId>/inspect-schema/..., not into <root>/chk-N/COBBLE-SCHEMA-*.
        Path rootDir = tempDir.resolve("checkpoint-root");
        Files.createDirectories(rootDir);
        String checkpointPointer = rootDir.resolve("chk-100").toUri().toString();

        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "v1",
                        "v1",
                        false,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE);
        StateInspectSchemaStore store =
                new StateInspectSchemaStore(Collections.singletonList(schema));

        CobbleInspectSchemaRegistry registry =
                new CobbleInspectSchemaRegistry(checkpointPointer, "abc123");
        registry.writeIfChanged(100L, store);

        // Registry files must live under cobble/<opId>/inspect-schema/.
        assertTrue(
                Files.exists(rootDir.resolve("cobble/abc123/inspect-schema/blobs")),
                "Schema blobs directory not found under cobble/inspect-schema/");
        assertTrue(
                Files.exists(rootDir.resolve("cobble/abc123/inspect-schema/events")),
                "Schema events directory not found under cobble/inspect-schema/");

        // No files should land directly inside a chk-* directory.
        Path chkDir = rootDir.resolve("chk-100");
        Files.createDirectories(chkDir);
        java.io.File[] chkFiles =
                chkDir.toFile().listFiles(f -> f.getName().startsWith("COBBLE-SCHEMA"));
        assertTrue(
                chkFiles == null || chkFiles.length == 0,
                "chk-N directory should not contain COBBLE-SCHEMA-* files");
    }

    @Test
    void registryEventFileNameIncludesSchemaHashIntegration(@TempDir Path tempDir)
            throws Exception {
        Path rootDir = tempDir.resolve("checkpoint-root");
        Files.createDirectories(rootDir);
        String checkpointPointer = rootDir.resolve("chk-42").toUri().toString();

        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "sv",
                        "sv",
                        false,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                        org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE);
        StateInspectSchemaStore store =
                new StateInspectSchemaStore(Collections.singletonList(schema));
        byte[] expectedBytes = store.toBytes();
        String expectedHash = InspectSchemaRegistryLayout.sha256(expectedBytes);

        CobbleInspectSchemaRegistry registry =
                new CobbleInspectSchemaRegistry(checkpointPointer, "abc123");
        registry.writeIfChanged(42L, store);

        // Verify blob file exists at the expected path.
        Path blobPath =
                rootDir.resolve("cobble/abc123/inspect-schema/blobs/" + expectedHash + ".csch");
        assertTrue(Files.exists(blobPath), "Blob file should exist at " + blobPath);

        // Verify event file references the correct hash.
        Path eventsDir = rootDir.resolve("cobble/abc123/inspect-schema/events");
        java.io.File[] eventFiles = eventsDir.toFile().listFiles();
        assertNotNull(eventFiles);
        assertTrue(eventFiles.length >= 1);
        boolean found = false;
        for (java.io.File f : eventFiles) {
            if (f.getName().contains(expectedHash)) {
                found = true;
                break;
            }
        }
        assertTrue(found, "One event file should reference the schema hash " + expectedHash);
    }

    // -------- ReducingState tests (Task 3B) --------

    @Test
    void reducingStateGetReturnsNullWhenAbsent(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingState<Integer> state =
                    backend.getPartitionedState(
                            "ns",
                            StringSerializer.INSTANCE,
                            new ReducingStateDescriptor<>(
                                    "absent-reducing", sumReducer(), IntSerializer.INSTANCE));
            assertNull(state.get());
        }
    }

    @Test
    void reducingStateAddOnEmptyWritesValue(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingState<Integer> state =
                    backend.getPartitionedState(
                            "ns",
                            StringSerializer.INSTANCE,
                            new ReducingStateDescriptor<>(
                                    "rs-empty-add", sumReducer(), IntSerializer.INSTANCE));
            state.add(7);
            assertEquals(7, state.get());
        }
    }

    @Test
    void reducingStateAddAccumulatesViaReduceFunction(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingState<Integer> state =
                    backend.getPartitionedState(
                            "ns",
                            StringSerializer.INSTANCE,
                            new ReducingStateDescriptor<>(
                                    "rs-accum", sumReducer(), IntSerializer.INSTANCE));
            state.add(2);
            state.add(3);
            state.add(5);
            assertEquals(10, state.get());
        }
    }

    @Test
    void reducingStateAddNullClearsExistingValue(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingState<Integer> state =
                    backend.getPartitionedState(
                            "ns",
                            StringSerializer.INSTANCE,
                            new ReducingStateDescriptor<>(
                                    "rs-add-null", sumReducer(), IntSerializer.INSTANCE));
            state.add(4);
            // Matches Flink HeapReducingState.add(null): clears the existing entry.
            state.add(null);
            assertNull(state.get());
        }
    }

    @Test
    void reducingStateClearRemovesRow(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingState<Integer> state =
                    backend.getPartitionedState(
                            "ns",
                            StringSerializer.INSTANCE,
                            new ReducingStateDescriptor<>(
                                    "rs-clear", sumReducer(), IntSerializer.INSTANCE));
            state.add(11);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    void reducingStateNamespaceIsolation(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingStateDescriptor<Integer> descriptor =
                    new ReducingStateDescriptor<>(
                            "rs-ns-isolation", sumReducer(), IntSerializer.INSTANCE);
            InternalReducingState<Integer, String, Integer> state =
                    asInternalReducingState(
                            backend.getPartitionedState(
                                    "a", StringSerializer.INSTANCE, descriptor));

            state.setCurrentNamespace("a");
            state.add(10);
            state.setCurrentNamespace("b");
            state.add(20);

            state.setCurrentNamespace("a");
            assertEquals(10, state.get());
            state.setCurrentNamespace("b");
            assertEquals(20, state.get());
        }
    }

    @Test
    void reducingStateKeyIsolation(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            ReducingStateDescriptor<Integer> descriptor =
                    new ReducingStateDescriptor<>(
                            "rs-key-isolation", sumReducer(), IntSerializer.INSTANCE);

            backend.setCurrentKey(1);
            ReducingState<Integer> s1 =
                    backend.getPartitionedState("ns", StringSerializer.INSTANCE, descriptor);
            s1.add(100);

            backend.setCurrentKey(2);
            ReducingState<Integer> s2 =
                    backend.getPartitionedState("ns", StringSerializer.INSTANCE, descriptor);
            s2.add(200);

            backend.setCurrentKey(1);
            assertEquals(
                    100,
                    backend.getPartitionedState("ns", StringSerializer.INSTANCE, descriptor).get());
            backend.setCurrentKey(2);
            assertEquals(
                    200,
                    backend.getPartitionedState("ns", StringSerializer.INSTANCE, descriptor).get());
        }
    }

    @Test
    void reducingStateRoundTripsCustomPojoSerializer(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(7);
            ReducingState<String> state =
                    backend.getPartitionedState(
                            "ns",
                            StringSerializer.INSTANCE,
                            new ReducingStateDescriptor<>(
                                    "rs-pojo", stringConcatReducer(), StringSerializer.INSTANCE));
            state.add("a");
            state.add("b");
            state.add("c");
            // Left-fold via the reducer: ((a+b)+c) = "abc".
            assertEquals("abc", state.get());
        }
    }

    @Test
    void reducingStateMergeNamespacesEmptySourcesIsNoOp(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingStateDescriptor<Integer> descriptor =
                    new ReducingStateDescriptor<>(
                            "rs-merge-empty", sumReducer(), IntSerializer.INSTANCE);
            InternalReducingState<Integer, String, Integer> state =
                    asInternalReducingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.add(5);
            state.mergeNamespaces("target", Collections.<String>emptyList());
            state.setCurrentNamespace("target");
            assertEquals(5, state.get());
        }
    }

    @Test
    void reducingStateMergeNamespacesAllSourcesAbsentTargetUnchanged(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingStateDescriptor<Integer> descriptor =
                    new ReducingStateDescriptor<>(
                            "rs-merge-no-src", sumReducer(), IntSerializer.INSTANCE);
            InternalReducingState<Integer, String, Integer> state =
                    asInternalReducingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.add(9);
            state.mergeNamespaces("target", Arrays.asList("missing-1", "missing-2"));
            state.setCurrentNamespace("target");
            assertEquals(9, state.get());
        }
    }

    @Test
    void reducingStateMergeNamespacesAllSourcesAbsentTargetAbsentLeavesTargetAbsent(
            @TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingStateDescriptor<Integer> descriptor =
                    new ReducingStateDescriptor<>(
                            "rs-merge-allabsent", sumReducer(), IntSerializer.INSTANCE);
            InternalReducingState<Integer, String, Integer> state =
                    asInternalReducingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.mergeNamespaces("target", Arrays.asList("missing-1", "missing-2"));
            state.setCurrentNamespace("target");
            assertNull(state.get());
        }
    }

    @Test
    void reducingStateMergeNamespacesFoldsSourcesIntoExistingTarget(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingStateDescriptor<Integer> descriptor =
                    new ReducingStateDescriptor<>(
                            "rs-merge-into-target", sumReducer(), IntSerializer.INSTANCE);
            InternalReducingState<Integer, String, Integer> state =
                    asInternalReducingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.add(10);
            state.setCurrentNamespace("s1");
            state.add(3);
            state.setCurrentNamespace("s2");
            state.add(4);

            state.mergeNamespaces("target", Arrays.asList("s1", "s2"));

            state.setCurrentNamespace("target");
            // foldedSources = reduce(3,4)=7; newTarget = reduce(10,7)=17.
            assertEquals(17, state.get());
            state.setCurrentNamespace("s1");
            assertNull(state.get());
            state.setCurrentNamespace("s2");
            assertNull(state.get());
        }
    }

    @Test
    void reducingStateMergeNamespacesFoldsSourcesIntoAbsentTarget(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingStateDescriptor<Integer> descriptor =
                    new ReducingStateDescriptor<>(
                            "rs-merge-absent-target", sumReducer(), IntSerializer.INSTANCE);
            InternalReducingState<Integer, String, Integer> state =
                    asInternalReducingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("s1");
            state.add(11);
            state.setCurrentNamespace("s2");
            state.add(22);

            state.mergeNamespaces("target", Arrays.asList("s1", "s2"));

            state.setCurrentNamespace("target");
            assertEquals(33, state.get());
            state.setCurrentNamespace("s1");
            assertNull(state.get());
            state.setCurrentNamespace("s2");
            assertNull(state.get());
        }
    }

    @Test
    void reducingStateMergeNamespacesNonCommutativeReducer(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingStateDescriptor<String> descriptor =
                    new ReducingStateDescriptor<>(
                            "rs-merge-concat", stringConcatReducer(), StringSerializer.INSTANCE);
            InternalReducingState<Integer, String, String> state =
                    asInternalReducingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("ns-a");
            state.add("a");
            state.setCurrentNamespace("ns-b");
            state.add("b");
            state.setCurrentNamespace("ns-c");
            state.add("c");

            // target absent; sources in insertion order [a,b,c] -> left-fold = "abc".
            state.mergeNamespaces("target", Arrays.asList("ns-a", "ns-b", "ns-c"));

            state.setCurrentNamespace("target");
            assertEquals("abc", state.get());
        }
    }

    @Test
    void reducingStateMergeNamespacesDuplicateSourceCountedOnce(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            CountingReducer<Integer> counting = new CountingReducer<>(sumReducer());
            ReducingStateDescriptor<Integer> descriptor =
                    new ReducingStateDescriptor<>("rs-merge-dup", counting, IntSerializer.INSTANCE);
            InternalReducingState<Integer, String, Integer> state =
                    asInternalReducingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("s1");
            state.add(50);

            counting.calls = 0;
            state.mergeNamespaces("target", Arrays.asList("s1", "s1"));

            state.setCurrentNamespace("target");
            assertEquals(50, state.get());
            // First "s1" contributes once; second "s1" must be dedup'd: zero reducer invocations
            // are expected since there is only one captured source and target is absent.
            assertEquals(0, counting.calls);
            state.setCurrentNamespace("s1");
            assertNull(state.get());
        }
    }

    @Test
    void reducingStateMergeNamespacesTargetInSourcesWithOthers(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingStateDescriptor<Integer> descriptor =
                    new ReducingStateDescriptor<>(
                            "rs-merge-target-in-src", sumReducer(), IntSerializer.INSTANCE);
            InternalReducingState<Integer, String, Integer> state =
                    asInternalReducingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.add(8);
            state.setCurrentNamespace("s1");
            state.add(13);

            state.mergeNamespaces("target", Arrays.asList("target", "s1"));

            state.setCurrentNamespace("target");
            // Vt=8 and Vs1=13 are folded together; target's contribution enters via the
            // captured-source path exactly once: foldedSources = reduce(8,13)=21. targetValue is
            // null (target was in sources). newTarget = 21.
            assertEquals(21, state.get());
            state.setCurrentNamespace("s1");
            assertNull(state.get());
        }
    }

    @Test
    void reducingStateMergeNamespacesSourcesEqualsTargetOnlyIsNoSelfReduce(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            CountingReducer<Integer> counting = new CountingReducer<>(sumReducer());
            ReducingStateDescriptor<Integer> descriptor =
                    new ReducingStateDescriptor<>(
                            "rs-merge-self", counting, IntSerializer.INSTANCE);
            InternalReducingState<Integer, String, Integer> state =
                    asInternalReducingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.add(42);

            counting.calls = 0;
            state.mergeNamespaces("target", Collections.singletonList("target"));

            state.setCurrentNamespace("target");
            assertEquals(42, state.get());
            // Vt was captured as a source value; foldedSources = Vt with no further reduce, and
            // targetValue is null because target was in sources. Reducer must NOT be invoked.
            assertEquals(0, counting.calls);
        }
    }

    @Test
    void reducingStateMergeNamespacesReducerMutatesFirstArgStillWritesBack(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            // A reducer that mutates its first argument and returns the same reference.
            ReduceFunction<int[]> inPlace =
                    (a, b) -> {
                        a[0] = a[0] + b[0];
                        return a;
                    };
            ReducingStateDescriptor<int[]> descriptor =
                    new ReducingStateDescriptor<>(
                            "rs-merge-mutate", inPlace, IntArraySerializer.INSTANCE);
            InternalReducingState<Integer, String, int[]> state =
                    asInternalReducingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.add(new int[] {5});
            state.setCurrentNamespace("s1");
            state.add(new int[] {7});

            state.mergeNamespaces("target", Collections.singletonList("s1"));

            state.setCurrentNamespace("target");
            int[] result = state.get();
            assertNotNull(result);
            assertEquals(12, result[0]);
        }
    }

    @Test
    void reducingStateMergeNamespacesVariableLengthNamespaceBytes(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingStateDescriptor<Integer> descriptor =
                    new ReducingStateDescriptor<>(
                            "rs-merge-varlen", sumReducer(), IntSerializer.INSTANCE);
            InternalReducingState<Integer, String, Integer> state =
                    asInternalReducingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            // Two source namespaces with deliberately different serialized lengths.
            state.setCurrentNamespace("a");
            state.add(1);
            state.setCurrentNamespace("a-longer-namespace");
            state.add(2);

            state.mergeNamespaces("target", Arrays.asList("a", "a-longer-namespace"));

            state.setCurrentNamespace("target");
            assertEquals(3, state.get());
            state.setCurrentNamespace("a");
            assertNull(state.get());
            state.setCurrentNamespace("a-longer-namespace");
            assertNull(state.get());
        }
    }

    @Test
    void reducingStateMergeNamespacesPropagatesReducerException(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReduceFunction<Integer> boom =
                    (a, b) -> {
                        throw new IllegalStateException("boom");
                    };
            ReducingStateDescriptor<Integer> descriptor =
                    new ReducingStateDescriptor<>("rs-merge-throws", boom, IntSerializer.INSTANCE);
            InternalReducingState<Integer, String, Integer> state =
                    asInternalReducingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            // Use ReducingState directly to avoid going through add(reduce). We seed each
            // namespace via updateInternal, which is a plain put.
            state.updateInternal(10);
            state.setCurrentNamespace("s1");
            state.updateInternal(20);
            state.setCurrentNamespace("s2");
            state.updateInternal(30);

            IllegalStateException error =
                    assertThrows(
                            IllegalStateException.class,
                            () -> state.mergeNamespaces("target", Arrays.asList("s1", "s2")));
            assertEquals("boom", error.getMessage());

            // Pre-merge values must be intact: no source row deleted, target unchanged.
            state.setCurrentNamespace("target");
            assertEquals(10, state.get());
            state.setCurrentNamespace("s1");
            assertEquals(20, state.get());
            state.setCurrentNamespace("s2");
            assertEquals(30, state.get());
        }
    }

    @Test
    void reducingStateSchemaRoundTripsThroughWriteAndRead(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            ReducingStateDescriptor<Integer> descriptor =
                    new ReducingStateDescriptor<>(
                            "rs-schema", sumReducer(), IntSerializer.INSTANCE);
            backend.getPartitionedState("ns", StringSerializer.INSTANCE, descriptor).add(1);

            StateInspectSchema schema = backend.getStateInspectSchemas().get("rs-schema");
            assertNotNull(schema);
            assertEquals(StateKind.REDUCING, schema.stateKind());

            // Round-trip via the StateInspectSchemaStore wire format.
            StateInspectSchemaStore store =
                    new StateInspectSchemaStore(Collections.singletonList(schema));
            byte[] bytes = store.toBytes();
            StateInspectSchemaStore roundTripped = StateInspectSchemaStore.fromBytes(bytes);
            StateInspectSchema decoded = roundTripped.schemas().get(0);
            assertEquals(StateKind.REDUCING, decoded.stateKind());
            assertEquals(schema.serializerClassNames(), decoded.serializerClassNames());
        }
    }

    @Test
    void restoresCanonicalSavepointReducingState(@TempDir Path tempDir) throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        // Single REDUCING entry with a serialized "sum-so-far" value, void namespace.
        KeyGroupsSavepointStateHandle savepoint =
                createCanonicalReducingSavepoint(key, keyGroup, 100);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);

            ReducingState<Integer> state =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ReducingStateDescriptor<>(
                                    "reducing", sumReducer(), IntSerializer.INSTANCE));
            assertEquals(100, state.get());

            // Imported value continues to participate in further add()/reduce() calls.
            state.add(23);
            assertEquals(123, state.get());
        }
    }

    @Test
    void canonicalReducingImportSurvivesCobbleCheckpointAndRestore(@TempDir Path tempDir)
            throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        // Stage 0: build a RocksDB canonical savepoint that includes a ReducingState entry,
        // matching the exact wire format Flink would emit for a real job.
        KeyedStateHandle rocksSavepoint =
                createRocksDbCanonicalReducingSavepoint(tempDir, key, keyGroup, 7);

        String checkpointDirectory = tempDir.resolve("checkpoints").toUri().toString();
        KeyedStateHandle cobbleCheckpoint;

        // Stage 1: import the canonical savepoint into Cobble, mutate via add(), then snapshot.
        try (TestBackendContext imported =
                createBackendContext(
                        tempDir.resolve("imported"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(rocksSavepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = imported.cobbleBackend;
            backend.setCurrentKey(key);

            ReducingState<Integer> state =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ReducingStateDescriptor<>(
                                    "reducing", sumReducer(), IntSerializer.INSTANCE));
            // The RocksDB savepoint helper seeds the reducing state to `seed`.
            assertEquals(7, state.get());
            state.add(35);
            assertEquals(42, state.get());

            cobbleCheckpoint = runCheckpointSnapshot(backend, 1L);
            backend.notifyCheckpointComplete(1L);
        }

        // Stage 2: brand-new Cobble backend, restored from the *Cobble* checkpoint (not from the
        // canonical savepoint), observes the post-mutation value.
        try (TestBackendContext restored =
                createBackendContext(
                        tempDir.resolve("restored"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(cobbleCheckpoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = restored.cobbleBackend;
            backend.setCurrentKey(key);

            ReducingState<Integer> state =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ReducingStateDescriptor<>(
                                    "reducing", sumReducer(), IntSerializer.INSTANCE));
            assertEquals(42, state.get());
        }
    }

    @Test
    void canonicalRestoreRejectsIncompatibleReducingValueSerializerBeforeDataRead(
            @TempDir Path tempDir) throws Exception {
        // Savepoint declares "reducing" with a String value serializer. The job re-registers it
        // with IntSerializer — the backend must reject the descriptor before any column family is
        // created or any row is read.
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupsSavepointStateHandle savepoint =
                createCanonicalReducingSavepointWithSerializer(
                        key, keyGroup, "stored-string", StringSerializer.INSTANCE);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);

            Set<String> columnFamiliesBefore =
                    backend.getCobbleDb().currentSchema().columnFamilies().keySet();

            Exception error =
                    assertThrows(
                            Exception.class,
                            () ->
                                    backend.getPartitionedState(
                                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            new ReducingStateDescriptor<>(
                                                    "reducing",
                                                    sumReducer(),
                                                    IntSerializer.INSTANCE)));
            assertTrue(
                    hasCause(error, org.apache.flink.util.StateMigrationException.class),
                    "expected StateMigrationException cause, got: " + error);
            String message = collectMessages(error);
            assertTrue(
                    message.contains("value serializer"),
                    "error must name the offending role, got: " + message);
            assertTrue(
                    message.contains(StringSerializer.class.getName()),
                    "error must name canonical serializer class, got: " + message);
            assertTrue(
                    message.contains(IntSerializer.class.getName()),
                    "error must name the rejected runtime serializer class, got: " + message);
            assertTrue(
                    message.contains("'reducing'"), "error must name the state, got: " + message);

            // Schema must be unchanged. The canonical restore created the family for "reducing",
            // but the failed descriptor registration must not add or mutate anything else.
            Set<String> columnFamiliesAfter =
                    backend.getCobbleDb().currentSchema().columnFamilies().keySet();
            assertEquals(columnFamiliesBefore, columnFamiliesAfter);

            // Re-registering with the canonical-compatible serializer must still succeed.
            ReducingState<String> recovered =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ReducingStateDescriptor<>(
                                    "reducing", stringConcatReducer(), StringSerializer.INSTANCE));
            assertEquals("stored-string", recovered.get());
        }
    }

    /**
     * Builds a hand-authored canonical savepoint stream with a single REDUCING entry under void
     * namespace, value serializer = IntSerializer, single Int payload {@code value}.
     */
    private static KeyGroupsSavepointStateHandle createCanonicalReducingSavepoint(
            int key, int keyGroup, int value) throws Exception {
        return createCanonicalReducingSavepointWithSerializer(
                key, keyGroup, value, IntSerializer.INSTANCE);
    }

    private static <V> KeyGroupsSavepointStateHandle createCanonicalReducingSavepointWithSerializer(
            int key, int keyGroup, V value, TypeSerializer<V> valueSerializer) throws Exception {
        List<StateMetaInfoSnapshot> stateMetaInfos =
                Collections.singletonList(
                        new RegisteredKeyValueStateBackendMetaInfo<>(
                                        org.apache.flink.api.common.state.StateDescriptor.Type
                                                .REDUCING,
                                        "reducing",
                                        VoidNamespaceSerializer.INSTANCE,
                                        valueSerializer)
                                .snapshot());
        DataOutputSerializer output = new DataOutputSerializer(256);
        new KeyedBackendSerializationProxy<>(IntSerializer.INSTANCE, stateMetaInfos, false)
                .write(output);

        DataOutputSerializer body = new DataOutputSerializer(64);
        body.writeShort(0);
        writeCanonicalEntry(
                body,
                canonicalKey(keyGroup, key, null),
                CobbleStateKeySerializer.serialize(valueSerializer, value),
                END_OF_KEY_GROUP_MARK);

        long keyGroupOffset = output.length();
        writeKeyGroupBody(output, body.getCopyOfBuffer(), false);

        KeyGroupRange range = KeyGroupRange.of(keyGroup, keyGroup);
        return new KeyGroupsSavepointStateHandle(
                new KeyGroupRangeOffsets(range, new long[] {keyGroupOffset}),
                new ByteStreamStateHandle(
                        "canonical-reducing-savepoint", output.getCopyOfBuffer()));
    }

    /**
     * Spin up a real RocksDB state backend, register a ReducingState, seed it via {@code
     * add(seed)}, and produce a canonical savepoint handle. This exercises the byte-identical
     * compatibility of Cobble's REDUCING import path with Flink's actual canonical writer.
     */
    private KeyedStateHandle createRocksDbCanonicalReducingSavepoint(
            Path tempDir, int key, int keyGroup, int seed) throws Exception {
        Path rocksDbPath = tempDir.resolve("rocksdb-local-reducing");
        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setTaskName("rocksdb-canonical-reducing-savepoint")
                        .setJobVertexID(TEST_JOB_VERTEX_ID)
                        .setManagedMemorySize(MemorySize.ofMebiBytes(128).getBytes())
                        .setTaskManagerRuntimeInfo(
                                new TestingTaskManagerRuntimeInfo(
                                        new Configuration(),
                                        tempDir.resolve("rocksdb-reducing-tm").toFile()))
                        .setTaskStateManager(new TestTaskStateManagerBuilder().build())
                        .build();
        AbstractKeyedStateBackend<Integer> backend = null;
        try {
            RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(rocksDbPath.toUri());
            backend =
                    rocksDbBackend.createKeyedStateBackend(
                            environment,
                            environment.getJobID(),
                            "rocksdb-canonical-reducing-savepoint",
                            IntSerializer.INSTANCE,
                            16,
                            KeyGroupRange.of(keyGroup, keyGroup),
                            environment.getTaskKvStateRegistry(),
                            TtlTimeProvider.DEFAULT,
                            environment.getMetricGroup(),
                            Collections.emptyList(),
                            new CloseableRegistry(),
                            0.5d);
            backend.setCurrentKey(key);
            ReducingState<Integer> state =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ReducingStateDescriptor<>(
                                    "reducing", sumReducer(), IntSerializer.INSTANCE));
            state.add(seed);

            SavepointResources<Integer> savepointResources =
                    ((CheckpointableKeyedStateBackend<Integer>) backend).savepoint();
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                    new SnapshotStrategyRunner<>(
                                    "RocksDB canonical reducing savepoint",
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
        } finally {
            if (backend != null) {
                backend.dispose();
            }
            environment.close();
        }
    }

    // -------- ReducingState test helpers --------

    private static ReduceFunction<Integer> sumReducer() {
        return (a, b) -> a + b;
    }

    private static ReduceFunction<String> stringConcatReducer() {
        return (a, b) -> a + b;
    }

    @SuppressWarnings("unchecked")
    private static <K, N, V> InternalReducingState<K, N, V> asInternalReducingState(Object state) {
        return (InternalReducingState<K, N, V>) state;
    }

    /** Wraps a reducer and counts how many times {@link #reduce} is invoked. */
    private static final class CountingReducer<T> implements ReduceFunction<T> {
        private final ReduceFunction<T> delegate;
        int calls;

        CountingReducer(ReduceFunction<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public T reduce(T a, T b) throws Exception {
            calls++;
            return delegate.reduce(a, b);
        }
    }

    // -------- AggregatingState tests (Task 3C) --------

    @Test
    void aggregatingStateGetReturnsNullWhenAbsent(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingState<Integer, String> state =
                    backend.getPartitionedState(
                            "ns",
                            StringSerializer.INSTANCE,
                            new AggregatingStateDescriptor<>(
                                    "absent-agg", new SumAggregator(), LongSerializer.INSTANCE));
            assertNull(state.get());
        }
    }

    @Test
    void aggregatingStateAddOnEmptyCreatesAccumulator(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingState<Integer, String> state =
                    backend.getPartitionedState(
                            "ns",
                            StringSerializer.INSTANCE,
                            new AggregatingStateDescriptor<>(
                                    "ag-empty-add", new SumAggregator(), LongSerializer.INSTANCE));
            state.add(7);
            // createAccumulator()=0; add(7,0)=7; getResult(7)="sum=7".
            assertEquals("sum=7", state.get());
        }
    }

    @Test
    void aggregatingStateAddAccumulatesViaAggregateFunction(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingState<Integer, String> state =
                    backend.getPartitionedState(
                            "ns",
                            StringSerializer.INSTANCE,
                            new AggregatingStateDescriptor<>(
                                    "ag-accum", new SumAggregator(), LongSerializer.INSTANCE));
            state.add(2);
            state.add(3);
            state.add(5);
            assertEquals("sum=10", state.get());
        }
    }

    @Test
    void aggregatingStateGetUsesGetResultForOutput(@TempDir Path tempDir) throws Exception {
        // Sanity: OUT (String) is computed via AggregateFunction.getResult, not by deserializing
        // the stored accumulator as the output type.
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingState<Integer, String> state =
                    backend.getPartitionedState(
                            "ns",
                            StringSerializer.INSTANCE,
                            new AggregatingStateDescriptor<>(
                                    "ag-output", new SumAggregator(), LongSerializer.INSTANCE));
            state.add(42);
            String out = state.get();
            assertNotNull(out);
            assertTrue(out.startsWith("sum="), "OUT must come from getResult, got: " + out);
        }
    }

    @Test
    void aggregatingStateAddNullClearsExistingValue(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingState<Integer, String> state =
                    backend.getPartitionedState(
                            "ns",
                            StringSerializer.INSTANCE,
                            new AggregatingStateDescriptor<>(
                                    "ag-add-null", new SumAggregator(), LongSerializer.INSTANCE));
            state.add(4);
            // Matches Flink HeapAggregatingState.add(null): clears the existing entry.
            state.add(null);
            assertNull(state.get());
        }
    }

    @Test
    void aggregatingStateClearRemovesRow(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingState<Integer, String> state =
                    backend.getPartitionedState(
                            "ns",
                            StringSerializer.INSTANCE,
                            new AggregatingStateDescriptor<>(
                                    "ag-clear", new SumAggregator(), LongSerializer.INSTANCE));
            state.add(11);
            state.clear();
            assertNull(state.get());
        }
    }

    @Test
    void aggregatingStateNamespaceIsolation(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-ns-isolation", new SumAggregator(), LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "a", StringSerializer.INSTANCE, descriptor));

            state.setCurrentNamespace("a");
            state.add(10);
            state.setCurrentNamespace("b");
            state.add(20);

            state.setCurrentNamespace("a");
            assertEquals("sum=10", state.get());
            state.setCurrentNamespace("b");
            assertEquals("sum=20", state.get());
        }
    }

    @Test
    void aggregatingStateKeyIsolation(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-key-isolation", new SumAggregator(), LongSerializer.INSTANCE);

            backend.setCurrentKey(1);
            AggregatingState<Integer, String> s1 =
                    backend.getPartitionedState("ns", StringSerializer.INSTANCE, descriptor);
            s1.add(100);

            backend.setCurrentKey(2);
            AggregatingState<Integer, String> s2 =
                    backend.getPartitionedState("ns", StringSerializer.INSTANCE, descriptor);
            s2.add(200);

            backend.setCurrentKey(1);
            assertEquals(
                    "sum=100",
                    ((AggregatingState<Integer, String>)
                                    backend.getPartitionedState(
                                            "ns", StringSerializer.INSTANCE, descriptor))
                            .get());
            backend.setCurrentKey(2);
            assertEquals(
                    "sum=200",
                    ((AggregatingState<Integer, String>)
                                    backend.getPartitionedState(
                                            "ns", StringSerializer.INSTANCE, descriptor))
                            .get());
        }
    }

    @Test
    void aggregatingStateMergeNamespacesEmptySourcesIsNoOp(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-empty", new SumAggregator(), LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.add(5);
            state.mergeNamespaces("target", Collections.<String>emptyList());
            state.setCurrentNamespace("target");
            assertEquals("sum=5", state.get());
        }
    }

    @Test
    void aggregatingStateMergeNamespacesAllSourcesAbsentTargetUnchanged(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-no-src", new SumAggregator(), LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.add(9);
            state.mergeNamespaces("target", Arrays.asList("missing-1", "missing-2"));
            state.setCurrentNamespace("target");
            assertEquals("sum=9", state.get());
        }
    }

    @Test
    void aggregatingStateMergeNamespacesAllSourcesAbsentTargetAbsentLeavesTargetAbsent(
            @TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-allabsent", new SumAggregator(), LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.mergeNamespaces("target", Arrays.asList("missing-1", "missing-2"));
            state.setCurrentNamespace("target");
            assertNull(state.get());
        }
    }

    @Test
    void aggregatingStateMergeNamespacesFoldsSourcesIntoExistingTarget(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-into-target", new SumAggregator(), LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.add(10);
            state.setCurrentNamespace("s1");
            state.add(3);
            state.setCurrentNamespace("s2");
            state.add(4);

            state.mergeNamespaces("target", Arrays.asList("s1", "s2"));

            state.setCurrentNamespace("target");
            // foldedSources = merge(3,4)=7; newTarget = merge(10,7)=17; getResult=>"sum=17".
            assertEquals("sum=17", state.get());
            state.setCurrentNamespace("s1");
            assertNull(state.get());
            state.setCurrentNamespace("s2");
            assertNull(state.get());
        }
    }

    @Test
    void aggregatingStateMergeNamespacesFoldsSourcesIntoAbsentTarget(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-absent-target", new SumAggregator(), LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("s1");
            state.add(11);
            state.setCurrentNamespace("s2");
            state.add(22);

            state.mergeNamespaces("target", Arrays.asList("s1", "s2"));

            state.setCurrentNamespace("target");
            assertEquals("sum=33", state.get());
            state.setCurrentNamespace("s1");
            assertNull(state.get());
            state.setCurrentNamespace("s2");
            assertNull(state.get());
        }
    }

    @Test
    void aggregatingStateMergeNamespacesNonCommutativeAggregator(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingStateDescriptor<String, String, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-concat",
                            new StringConcatAggregator(),
                            StringSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, String, String, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("ns-a");
            state.add("a");
            state.setCurrentNamespace("ns-b");
            state.add("b");
            state.setCurrentNamespace("ns-c");
            state.add("c");

            // target absent; sources in insertion order [a,b,c] -> left-fold via merge = "abc".
            state.mergeNamespaces("target", Arrays.asList("ns-a", "ns-b", "ns-c"));

            state.setCurrentNamespace("target");
            assertEquals("abc", state.get());
        }
    }

    @Test
    void aggregatingStateMergeNamespacesDuplicateSourceCountedOnce(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            CountingAggregator counting = new CountingAggregator(new SumAggregator());
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-dup", counting, LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("s1");
            state.add(50);

            counting.mergeCalls = 0;
            state.mergeNamespaces("target", Arrays.asList("s1", "s1"));

            state.setCurrentNamespace("target");
            assertEquals("sum=50", state.get());
            // First "s1" contributes once; second "s1" must be dedup'd: zero merge invocations
            // are expected since there is only one captured source and target is absent.
            assertEquals(0, counting.mergeCalls);
            state.setCurrentNamespace("s1");
            assertNull(state.get());
        }
    }

    @Test
    void aggregatingStateMergeNamespacesTargetInSourcesWithOthers(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-target-in-src", new SumAggregator(), LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.add(8);
            state.setCurrentNamespace("s1");
            state.add(13);

            state.mergeNamespaces("target", Arrays.asList("target", "s1"));

            state.setCurrentNamespace("target");
            // Vt=8 and Vs1=13 are folded together; target's contribution enters via the
            // captured-source path exactly once: foldedSources = merge(8,13)=21. targetValue is
            // null (target was in sources). newTarget = 21; getResult => "sum=21".
            assertEquals("sum=21", state.get());
            state.setCurrentNamespace("s1");
            assertNull(state.get());
        }
    }

    @Test
    void aggregatingStateMergeNamespacesSourcesEqualsTargetOnlyIsNoSelfMerge(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            CountingAggregator counting = new CountingAggregator(new SumAggregator());
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-self", counting, LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.add(42);

            counting.mergeCalls = 0;
            state.mergeNamespaces("target", Collections.singletonList("target"));

            state.setCurrentNamespace("target");
            assertEquals("sum=42", state.get());
            // Vt was captured as a source; foldedSources = Vt with no further merge, and
            // targetValue is null because target was in sources. merge() must NOT be invoked.
            assertEquals(0, counting.mergeCalls);
        }
    }

    @Test
    void aggregatingStateMergeNamespacesVariableLengthNamespaceBytes(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-varlen", new SumAggregator(), LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            // Two source namespaces with deliberately different serialized lengths.
            state.setCurrentNamespace("a");
            state.add(1);
            state.setCurrentNamespace("a-longer-namespace");
            state.add(2);

            state.mergeNamespaces("target", Arrays.asList("a", "a-longer-namespace"));

            state.setCurrentNamespace("target");
            assertEquals("sum=3", state.get());
            state.setCurrentNamespace("a");
            assertNull(state.get());
            state.setCurrentNamespace("a-longer-namespace");
            assertNull(state.get());
        }
    }

    @Test
    void aggregatingStateMergeNamespacesPropagatesMergerException(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregateFunction<Integer, Long, String> boom =
                    new AggregateFunction<Integer, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Integer in, Long acc) {
                            return acc + in;
                        }

                        @Override
                        public String getResult(Long acc) {
                            return "sum=" + acc;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            throw new IllegalStateException("boom");
                        }
                    };
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-throws", boom, LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            // Use updateInternal to plant accumulators directly without going through add(merge).
            state.updateInternal(10L);
            state.setCurrentNamespace("s1");
            state.updateInternal(20L);
            state.setCurrentNamespace("s2");
            state.updateInternal(30L);

            IllegalStateException error =
                    assertThrows(
                            IllegalStateException.class,
                            () -> state.mergeNamespaces("target", Arrays.asList("s1", "s2")));
            assertEquals("boom", error.getMessage());

            // Pre-merge accumulators must be intact (compute-then-write boundary): no source row
            // deleted, target unchanged.
            state.setCurrentNamespace("target");
            assertEquals(Long.valueOf(10L), state.getInternal());
            state.setCurrentNamespace("s1");
            assertEquals(Long.valueOf(20L), state.getInternal());
            state.setCurrentNamespace("s2");
            assertEquals(Long.valueOf(30L), state.getInternal());
        }
    }

    @Test
    void aggregateAddReturningNullClearsState(@TempDir Path tempDir) throws Exception {
        // P2: AggregateFunction.add returning null must clear the row via updateInternal.
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregateFunction<Integer, Long, String> addReturnsNull =
                    new AggregateFunction<Integer, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Integer in, Long acc) {
                            // Signal "clear" via null accumulator.
                            return in < 0 ? null : acc + in;
                        }

                        @Override
                        public String getResult(Long acc) {
                            return "sum=" + acc;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            return a + b;
                        }
                    };
            AggregatingState<Integer, String> state =
                    backend.getPartitionedState(
                            "ns",
                            StringSerializer.INSTANCE,
                            new AggregatingStateDescriptor<>(
                                    "ag-add-null-result", addReturnsNull, LongSerializer.INSTANCE));
            state.add(5);
            state.add(7);
            assertEquals("sum=12", state.get());
            // Negative triggers null accumulator -> updateInternal(null) -> clear.
            state.add(-1);
            assertNull(state.get());
        }
    }

    @Test
    void aggregateMergeReturningNullDeletesTarget(@TempDir Path tempDir) throws Exception {
        // P2: AggregateFunction.merge returning null while combining target+folded must delete
        // the target row instead of writing a null accumulator.
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregateFunction<Integer, Long, String> mergeReturnsNull =
                    new AggregateFunction<Integer, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Integer in, Long acc) {
                            return acc + in;
                        }

                        @Override
                        public String getResult(Long acc) {
                            return "sum=" + acc;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            return null;
                        }
                    };
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-null-result", mergeReturnsNull, LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.updateInternal(10L);
            state.setCurrentNamespace("s1");
            state.updateInternal(20L);

            // merge(10,20) returns null -> target row must be deleted.
            state.mergeNamespaces("target", Collections.singletonList("s1"));

            state.setCurrentNamespace("target");
            assertNull(state.getInternal(), "target row must be deleted when merge returns null");
            assertNull(state.get());
            state.setCurrentNamespace("s1");
            assertNull(state.getInternal(), "source row must always be deleted post-merge");
        }
    }

    @Test
    void aggregateAddExceptionPreservesPersistedValue(@TempDir Path tempDir) throws Exception {
        // P2: An AggregateFunction.add that mutates its accumulator and then throws must not
        // leak the mutated value to persisted state. add() is compute-then-write via
        // updateInternal, so a throw before the write keeps the prior bytes intact.
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregateFunction<Integer, long[], String> mutateThenThrow =
                    new AggregateFunction<Integer, long[], String>() {
                        @Override
                        public long[] createAccumulator() {
                            return new long[] {0L};
                        }

                        @Override
                        public long[] add(Integer in, long[] acc) {
                            acc[0] = acc[0] + in;
                            if (in == 99) {
                                throw new IllegalStateException("boom");
                            }
                            return acc;
                        }

                        @Override
                        public String getResult(long[] acc) {
                            return "sum=" + acc[0];
                        }

                        @Override
                        public long[] merge(long[] a, long[] b) {
                            return new long[] {a[0] + b[0]};
                        }
                    };
            AggregatingStateDescriptor<Integer, long[], String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-add-throws-preserved",
                            mutateThenThrow,
                            LongArraySerializer.INSTANCE);
            AggregatingState<Integer, String> state =
                    backend.getPartitionedState("ns", StringSerializer.INSTANCE, descriptor);
            state.add(10);
            state.add(20);
            assertEquals("sum=30", state.get());

            // Mutator throws after mutating its in-memory accumulator; persisted state must be
            // unaffected because updateInternal never runs.
            assertThrows(IllegalStateException.class, () -> state.add(99));

            assertEquals("sum=30", state.get());
        }
    }

    @Test
    void aggregateMergeExceptionPreservesPersistedSourcesAndTarget(@TempDir Path tempDir)
            throws Exception {
        // P2: Steps 1-6 of mergeNamespaces are compute-only. A throw before step 7 must leave
        // both source and target rows untouched. This is the exception-preserves-persisted-state
        // boundary that protects against partial writes.
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregateFunction<Integer, long[], String> mutateThenThrow =
                    new AggregateFunction<Integer, long[], String>() {
                        @Override
                        public long[] createAccumulator() {
                            return new long[] {0L};
                        }

                        @Override
                        public long[] add(Integer in, long[] acc) {
                            return new long[] {acc[0] + in};
                        }

                        @Override
                        public String getResult(long[] acc) {
                            return "sum=" + acc[0];
                        }

                        @Override
                        public long[] merge(long[] a, long[] b) {
                            // Mutate first arg in place, then throw before returning.
                            a[0] = a[0] + b[0];
                            throw new IllegalStateException("boom-merge");
                        }
                    };
            AggregatingStateDescriptor<Integer, long[], String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-throws-preserved",
                            mutateThenThrow,
                            LongArraySerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, long[], String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.updateInternal(new long[] {10L});
            state.setCurrentNamespace("s1");
            state.updateInternal(new long[] {20L});
            state.setCurrentNamespace("s2");
            state.updateInternal(new long[] {30L});

            IllegalStateException error =
                    assertThrows(
                            IllegalStateException.class,
                            () -> state.mergeNamespaces("target", Arrays.asList("s1", "s2")));
            assertEquals("boom-merge", error.getMessage());

            // Persisted state must be intact post-throw (compute-then-write boundary).
            state.setCurrentNamespace("target");
            assertEquals(10L, state.getInternal()[0]);
            state.setCurrentNamespace("s1");
            assertEquals(20L, state.getInternal()[0]);
            state.setCurrentNamespace("s2");
            assertEquals(30L, state.getInternal()[0]);
        }
    }

    @Test
    void aggregatingStateMergeNamespacesAggregatorMutatesFirstArgStillWritesBack(
            @TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregateFunction<Integer, long[], String> inPlaceMerge =
                    new AggregateFunction<Integer, long[], String>() {
                        @Override
                        public long[] createAccumulator() {
                            return new long[] {0L};
                        }

                        @Override
                        public long[] add(Integer in, long[] acc) {
                            return new long[] {acc[0] + in};
                        }

                        @Override
                        public String getResult(long[] acc) {
                            return "sum=" + acc[0];
                        }

                        @Override
                        public long[] merge(long[] a, long[] b) {
                            a[0] = a[0] + b[0];
                            return a;
                        }
                    };
            AggregatingStateDescriptor<Integer, long[], String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-mutate", inPlaceMerge, LongArraySerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, long[], String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.updateInternal(new long[] {5L});
            state.setCurrentNamespace("s1");
            state.updateInternal(new long[] {7L});

            state.mergeNamespaces("target", Collections.singletonList("s1"));

            state.setCurrentNamespace("target");
            assertEquals(12L, state.getInternal()[0]);
        }
    }

    @Test
    void aggregatingMergeNullThenLaterSourceReseedsMergedValue(@TempDir Path tempDir)
            throws Exception {
        // Heap semantics: merge(a,b) returning null does not terminate source folding. A later
        // non-null source should become the new merged value and still be written into target.
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregateFunction<Integer, Long, String> cancelingMerge =
                    new AggregateFunction<Integer, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Integer in, Long acc) {
                            return acc + in;
                        }

                        @Override
                        public String getResult(Long acc) {
                            return "sum=" + acc;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            // First two source accumulators cancel to null; a later non-null source
                            // must re-seed foldedSources instead of being skipped.
                            if (a == 1L && b == 2L) {
                                return null;
                            }
                            return a + b;
                        }
                    };
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-null-reseed", cancelingMerge, LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("s1");
            state.updateInternal(1L);
            state.setCurrentNamespace("s2");
            state.updateInternal(2L);
            state.setCurrentNamespace("s3");
            state.updateInternal(30L);

            state.mergeNamespaces("target", Arrays.asList("s1", "s2", "s3"));

            state.setCurrentNamespace("target");
            assertEquals(Long.valueOf(30L), state.getInternal());
            assertEquals("sum=30", state.get());
            state.setCurrentNamespace("s1");
            assertNull(state.getInternal());
            state.setCurrentNamespace("s2");
            assertNull(state.getInternal());
            state.setCurrentNamespace("s3");
            assertNull(state.getInternal());
        }
    }

    @Test
    void aggregatingMergeFinalNullLeavesIndependentTargetUntouched(@TempDir Path tempDir)
            throws Exception {
        // Heap semantics: if folding sources produces no merged value, transform(target, ...) is
        // not
        // called. An independent target must not be rewritten/deleted (and TTL must not be
        // refreshed).
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregateFunction<Integer, Long, String> mergeAlwaysNull =
                    new AggregateFunction<Integer, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Integer in, Long acc) {
                            return acc + in;
                        }

                        @Override
                        public String getResult(Long acc) {
                            return "sum=" + acc;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            return null;
                        }
                    };
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-final-null-independent-target",
                            mergeAlwaysNull,
                            LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.updateInternal(10L);
            state.setCurrentNamespace("s1");
            state.updateInternal(1L);
            state.setCurrentNamespace("s2");
            state.updateInternal(2L);

            state.mergeNamespaces("target", Arrays.asList("s1", "s2"));

            state.setCurrentNamespace("target");
            assertEquals(Long.valueOf(10L), state.getInternal());
            assertEquals("sum=10", state.get());
            state.setCurrentNamespace("s1");
            assertNull(state.getInternal());
            state.setCurrentNamespace("s2");
            assertNull(state.getInternal());
        }
    }

    @Test
    void aggregatingMergeFinalNullDeletesTargetWhenTargetWasASource(@TempDir Path tempDir)
            throws Exception {
        // Heap semantics: target listed in sources is removed during source folding. If final
        // merged
        // remains null, there is no transform write-back, so target stays deleted.
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregateFunction<Integer, Long, String> mergeAlwaysNull =
                    new AggregateFunction<Integer, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Integer in, Long acc) {
                            return acc + in;
                        }

                        @Override
                        public String getResult(Long acc) {
                            return "sum=" + acc;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            return null;
                        }
                    };
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-merge-final-null-target-source",
                            mergeAlwaysNull,
                            LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "target", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("target");
            state.updateInternal(10L);
            state.setCurrentNamespace("s1");
            state.updateInternal(1L);

            state.mergeNamespaces("target", Arrays.asList("target", "s1"));

            state.setCurrentNamespace("target");
            assertNull(state.getInternal());
            assertNull(state.get());
            state.setCurrentNamespace("s1");
            assertNull(state.getInternal());
        }
    }

    @Test
    void aggregatingStateUpdateInternalNullClears(@TempDir Path tempDir) throws Exception {
        // Direct contract test for updateInternal(null) -> clear.
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-update-null", new SumAggregator(), LongSerializer.INSTANCE);
            InternalAggregatingState<Integer, String, Integer, Long, String> state =
                    asInternalAggregatingState(
                            backend.getPartitionedState(
                                    "ns", StringSerializer.INSTANCE, descriptor));
            state.setCurrentNamespace("ns");
            state.updateInternal(123L);
            assertEquals(Long.valueOf(123L), state.getInternal());
            state.updateInternal(null);
            assertNull(state.getInternal());
        }
    }

    // -------- AggregatingState canonical-restore + FOLDING-rejection tests (Task 3C) --------

    @Test
    void restoresCanonicalSavepointAggregatingState(@TempDir Path tempDir) throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        // Single AGGREGATING entry with a serialized Long accumulator under void namespace.
        KeyGroupsSavepointStateHandle savepoint =
                createCanonicalAggregatingSavepoint(key, keyGroup, 100L);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);

            AggregatingState<Integer, String> state =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new AggregatingStateDescriptor<>(
                                    "aggregating", new SumAggregator(), LongSerializer.INSTANCE));
            // Accumulator was 100 -> getResult formats it as "sum=100".
            assertEquals("sum=100", state.get());

            // Imported accumulator continues to participate in add().
            state.add(23);
            assertEquals("sum=123", state.get());
        }
    }

    @Test
    void canonicalAggregatingImportSurvivesCobbleCheckpointAndRestore(@TempDir Path tempDir)
            throws Exception {
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        // Stage 0: build a real RocksDB canonical savepoint with an AggregatingState entry.
        KeyedStateHandle rocksSavepoint =
                createRocksDbCanonicalAggregatingSavepoint(tempDir, key, keyGroup, 7);

        String checkpointDirectory = tempDir.resolve("checkpoints").toUri().toString();
        KeyedStateHandle cobbleCheckpoint;

        // Stage 1: import the canonical savepoint, mutate via add(), then snapshot.
        try (TestBackendContext imported =
                createBackendContext(
                        tempDir.resolve("imported"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(rocksSavepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = imported.cobbleBackend;
            backend.setCurrentKey(key);

            AggregatingState<Integer, String> state =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new AggregatingStateDescriptor<>(
                                    "aggregating", new SumAggregator(), LongSerializer.INSTANCE));
            // The RocksDB savepoint helper seeded the accumulator to `seed`.
            assertEquals("sum=7", state.get());
            state.add(35);
            assertEquals("sum=42", state.get());

            cobbleCheckpoint = runCheckpointSnapshot(backend, 1L);
            backend.notifyCheckpointComplete(1L);
        }

        // Stage 2: brand-new backend, restored from the Cobble checkpoint, observes the
        // post-mutation accumulator.
        try (TestBackendContext restored =
                createBackendContext(
                        tempDir.resolve("restored"),
                        false,
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(cobbleCheckpoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = restored.cobbleBackend;
            backend.setCurrentKey(key);

            AggregatingState<Integer, String> state =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new AggregatingStateDescriptor<>(
                                    "aggregating", new SumAggregator(), LongSerializer.INSTANCE));
            assertEquals("sum=42", state.get());
        }
    }

    @Test
    void canonicalRestoreRejectsIncompatibleAggregatingValueSerializerBeforeDataRead(
            @TempDir Path tempDir) throws Exception {
        // Savepoint declares "aggregating" with a String accumulator serializer. The runtime job
        // re-registers it with a Long accumulator — the backend must reject before mutating any
        // column family or reading any row.
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupsSavepointStateHandle savepoint =
                createCanonicalAggregatingSavepointWithSerializer(
                        key, keyGroup, "stored-string", StringSerializer.INSTANCE);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);

            Set<String> columnFamiliesBefore =
                    backend.getCobbleDb().currentSchema().columnFamilies().keySet();

            Exception error =
                    assertThrows(
                            Exception.class,
                            () ->
                                    backend.getPartitionedState(
                                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            new AggregatingStateDescriptor<>(
                                                    "aggregating",
                                                    new SumAggregator(),
                                                    LongSerializer.INSTANCE)));
            assertTrue(
                    hasCause(error, org.apache.flink.util.StateMigrationException.class),
                    "expected StateMigrationException cause, got: " + error);
            String message = collectMessages(error);
            assertTrue(
                    message.contains("value serializer"),
                    "error must name the offending role, got: " + message);
            assertTrue(
                    message.contains(StringSerializer.class.getName()),
                    "error must name canonical serializer class, got: " + message);
            assertTrue(
                    message.contains(LongSerializer.class.getName()),
                    "error must name the rejected runtime serializer class, got: " + message);
            assertTrue(
                    message.contains("'aggregating'"),
                    "error must name the state, got: " + message);

            // Schema must be unchanged. The canonical restore already created the family for
            // "aggregating", but the failed descriptor registration must not add or mutate anything
            // else.
            Set<String> columnFamiliesAfter =
                    backend.getCobbleDb().currentSchema().columnFamilies().keySet();
            assertEquals(columnFamiliesBefore, columnFamiliesAfter);

            // Re-registering with the canonical-compatible serializer must still succeed.
            AggregatingState<String, String> recovered =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new AggregatingStateDescriptor<>(
                                    "aggregating",
                                    new StringConcatAggregator(),
                                    StringSerializer.INSTANCE));
            assertEquals("stored-string", recovered.get());
        }
    }

    @Test
    void canonicalRestoreRejectsKindMismatchBetweenAggregatingSavepointAndReducingDescriptor(
            @TempDir Path tempDir) throws Exception {
        // Canonical savepoint registers "agg" as AGGREGATING; runtime re-registers it as REDUCING.
        int key = 42;
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 16);
        KeyGroupsSavepointStateHandle savepoint =
                createCanonicalAggregatingSavepointNamed(key, keyGroup, 5L, "agg");

        try (TestBackendContext context =
                createBackendContext(
                        tempDir,
                        false,
                        null,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(savepoint),
                        KeyGroupRange.of(keyGroup, keyGroup))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(key);

            Exception error =
                    assertThrows(
                            Exception.class,
                            () ->
                                    backend.getPartitionedState(
                                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            new ReducingStateDescriptor<>(
                                                    "agg",
                                                    (a, b) -> a + b,
                                                    LongSerializer.INSTANCE)));
            assertTrue(
                    hasCause(error, org.apache.flink.util.StateMigrationException.class),
                    "expected StateMigrationException cause, got: " + error);
            String message = collectMessages(error);
            assertTrue(
                    message.contains("kind mismatch") || message.contains("registers"),
                    "error should report state-kind mismatch, got: " + message);
            assertTrue(
                    message.contains("AGGREGATING"),
                    "error must mention canonical AGGREGATING kind: " + message);
            assertTrue(
                    message.contains("REDUCING"),
                    "error must mention requested REDUCING kind: " + message);
        }
    }

    @Test
    void canonicalRestoreRejectsFoldingStateBeforeDataRead(@TempDir Path tempDir) throws Exception {
        // Build a canonical savepoint whose metadata advertises a FOLDING state. The Cobble
        // canonical-restore path must reject FOLDING outright (it never made it to the column
        // family / data-read stage).
        int keyGroup = 7;
        KeyGroupsSavepointStateHandle savepoint = createCanonicalFoldingSavepoint(keyGroup);

        Exception error =
                assertThrows(
                        Exception.class,
                        () -> {
                            try (TestBackendContext context =
                                    createBackendContext(
                                            tempDir,
                                            false,
                                            null,
                                            null,
                                            TtlTimeProvider.DEFAULT,
                                            false,
                                            Collections.singletonList(savepoint),
                                            KeyGroupRange.of(keyGroup, keyGroup))) {
                                // Pass-through: the failure happens during backend construction's
                                // restore pass before any user-visible state operations.
                                context.cobbleBackend.setCurrentKey(0);
                            }
                        });
        assertTrue(
                hasCause(error, UnsupportedOperationException.class),
                "expected UnsupportedOperationException cause, got: " + error);
        String message = collectMessages(error);
        assertTrue(
                message.contains("FOLDING"),
                "error must mention the rejected FOLDING kind, got: " + message);
    }

    @Test
    void freshBackendRejectsFoldingStateDescriptorWithoutMutatingSchema(@TempDir Path tempDir)
            throws Exception {
        // No savepoint — exercise the pre-flight rejection inside createOrUpdateInternalState. The
        // descriptor must be refused BEFORE ensureStateColumnFamily, so the backend's column-family
        // map must remain unchanged.
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);

            Set<String> columnFamiliesBefore =
                    backend.getCobbleDb().currentSchema().columnFamilies().keySet();
            Map<String, StateInspectSchema> schemasBefore =
                    new LinkedHashMap<>(backend.getStateInspectSchemas());

            @SuppressWarnings({"serial", "unchecked", "rawtypes"})
            org.apache.flink.api.common.state.StateDescriptor foldingDescriptor =
                    new org.apache.flink.api.common.state.StateDescriptor<
                            org.apache.flink.api.common.state.State, Integer>(
                            "folding-rejected", IntSerializer.INSTANCE, 0) {
                        @Override
                        public org.apache.flink.api.common.state.StateDescriptor.Type getType() {
                            return org.apache.flink.api.common.state.StateDescriptor.Type.FOLDING;
                        }
                    };

            IllegalArgumentException error =
                    assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    backend.getPartitionedState(
                                            "ns",
                                            StringSerializer.INSTANCE,
                                            (org.apache.flink.api.common.state.StateDescriptor)
                                                    foldingDescriptor));
            String message = collectMessages(error);
            assertTrue(
                    message.contains("not supported"),
                    "error should say the descriptor is unsupported, got: " + message);

            // Pre-flight rejection must NOT mutate the column-family set or the schema map.
            Set<String> columnFamiliesAfter =
                    backend.getCobbleDb().currentSchema().columnFamilies().keySet();
            assertEquals(columnFamiliesBefore, columnFamiliesAfter);
            assertEquals(schemasBefore.keySet(), backend.getStateInspectSchemas().keySet());
        }
    }

    /**
     * Builds a hand-authored canonical savepoint with a single AGGREGATING entry under void
     * namespace, value serializer = LongSerializer, single Long accumulator payload.
     */
    private static KeyGroupsSavepointStateHandle createCanonicalAggregatingSavepoint(
            int key, int keyGroup, long accumulator) throws Exception {
        return createCanonicalAggregatingSavepointWithSerializer(
                key, keyGroup, accumulator, LongSerializer.INSTANCE);
    }

    private static <V>
            KeyGroupsSavepointStateHandle createCanonicalAggregatingSavepointWithSerializer(
                    int key, int keyGroup, V accumulator, TypeSerializer<V> accumulatorSerializer)
                    throws Exception {
        return createCanonicalAggregatingSavepointNamedWithSerializer(
                key, keyGroup, accumulator, accumulatorSerializer, "aggregating");
    }

    private static KeyGroupsSavepointStateHandle createCanonicalAggregatingSavepointNamed(
            int key, int keyGroup, long accumulator, String stateName) throws Exception {
        return createCanonicalAggregatingSavepointNamedWithSerializer(
                key, keyGroup, accumulator, LongSerializer.INSTANCE, stateName);
    }

    private static <V>
            KeyGroupsSavepointStateHandle createCanonicalAggregatingSavepointNamedWithSerializer(
                    int key,
                    int keyGroup,
                    V accumulator,
                    TypeSerializer<V> accumulatorSerializer,
                    String stateName)
                    throws Exception {
        List<StateMetaInfoSnapshot> stateMetaInfos =
                Collections.singletonList(
                        new RegisteredKeyValueStateBackendMetaInfo<>(
                                        org.apache.flink.api.common.state.StateDescriptor.Type
                                                .AGGREGATING,
                                        stateName,
                                        VoidNamespaceSerializer.INSTANCE,
                                        accumulatorSerializer)
                                .snapshot());
        DataOutputSerializer output = new DataOutputSerializer(256);
        new KeyedBackendSerializationProxy<>(IntSerializer.INSTANCE, stateMetaInfos, false)
                .write(output);

        DataOutputSerializer body = new DataOutputSerializer(64);
        body.writeShort(0);
        writeCanonicalEntry(
                body,
                canonicalKey(keyGroup, key, null),
                CobbleStateKeySerializer.serialize(accumulatorSerializer, accumulator),
                END_OF_KEY_GROUP_MARK);

        long keyGroupOffset = output.length();
        writeKeyGroupBody(output, body.getCopyOfBuffer(), false);

        KeyGroupRange range = KeyGroupRange.of(keyGroup, keyGroup);
        return new KeyGroupsSavepointStateHandle(
                new KeyGroupRangeOffsets(range, new long[] {keyGroupOffset}),
                new ByteStreamStateHandle(
                        "canonical-aggregating-savepoint", output.getCopyOfBuffer()));
    }

    /**
     * Builds a canonical savepoint whose metadata advertises a FOLDING state (with a void namespace
     * and Long value serializer). The savepoint body is intentionally empty — the failure must
     * surface at metadata-recording time, before any data row is read.
     */
    private static KeyGroupsSavepointStateHandle createCanonicalFoldingSavepoint(int keyGroup)
            throws Exception {
        List<StateMetaInfoSnapshot> stateMetaInfos =
                Collections.singletonList(
                        new RegisteredKeyValueStateBackendMetaInfo<>(
                                        org.apache.flink.api.common.state.StateDescriptor.Type
                                                .FOLDING,
                                        "folding",
                                        VoidNamespaceSerializer.INSTANCE,
                                        LongSerializer.INSTANCE)
                                .snapshot());
        DataOutputSerializer output = new DataOutputSerializer(256);
        new KeyedBackendSerializationProxy<>(IntSerializer.INSTANCE, stateMetaInfos, false)
                .write(output);

        DataOutputSerializer body = new DataOutputSerializer(8);
        body.writeShort(0);
        body.writeByte(END_OF_KEY_GROUP_MARK);

        long keyGroupOffset = output.length();
        writeKeyGroupBody(output, body.getCopyOfBuffer(), false);

        KeyGroupRange range = KeyGroupRange.of(keyGroup, keyGroup);
        return new KeyGroupsSavepointStateHandle(
                new KeyGroupRangeOffsets(range, new long[] {keyGroupOffset}),
                new ByteStreamStateHandle("canonical-folding-savepoint", output.getCopyOfBuffer()));
    }

    /**
     * Spins up a real RocksDB state backend, registers an AggregatingState seeded via {@code
     * add(seed)}, and produces a canonical savepoint handle. Exercises byte-identical compatibility
     * of Cobble's AGGREGATING import path with Flink's actual canonical writer.
     */
    private KeyedStateHandle createRocksDbCanonicalAggregatingSavepoint(
            Path tempDir, int key, int keyGroup, int seed) throws Exception {
        Path rocksDbPath = tempDir.resolve("rocksdb-local-aggregating");
        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setTaskName("rocksdb-canonical-aggregating-savepoint")
                        .setJobVertexID(TEST_JOB_VERTEX_ID)
                        .setManagedMemorySize(MemorySize.ofMebiBytes(128).getBytes())
                        .setTaskManagerRuntimeInfo(
                                new TestingTaskManagerRuntimeInfo(
                                        new Configuration(),
                                        tempDir.resolve("rocksdb-aggregating-tm").toFile()))
                        .setTaskStateManager(new TestTaskStateManagerBuilder().build())
                        .build();
        AbstractKeyedStateBackend<Integer> backend = null;
        try {
            RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(rocksDbPath.toUri());
            backend =
                    rocksDbBackend.createKeyedStateBackend(
                            environment,
                            environment.getJobID(),
                            "rocksdb-canonical-aggregating-savepoint",
                            IntSerializer.INSTANCE,
                            16,
                            KeyGroupRange.of(keyGroup, keyGroup),
                            environment.getTaskKvStateRegistry(),
                            TtlTimeProvider.DEFAULT,
                            environment.getMetricGroup(),
                            Collections.emptyList(),
                            new CloseableRegistry(),
                            0.5d);
            backend.setCurrentKey(key);
            AggregatingState<Integer, String> state =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new AggregatingStateDescriptor<>(
                                    "aggregating", new SumAggregator(), LongSerializer.INSTANCE));
            state.add(seed);

            SavepointResources<Integer> savepointResources =
                    ((CheckpointableKeyedStateBackend<Integer>) backend).savepoint();
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                    new SnapshotStrategyRunner<>(
                                    "RocksDB canonical aggregating savepoint",
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
        } finally {
            if (backend != null) {
                backend.dispose();
            }
            environment.close();
        }
    }

    @Test
    void aggregatingStateTtlExpiresFromFlinkAndCobble(@TempDir Path tempDir) throws Exception {
        MockTtlTimeProvider ttlTimeProvider = new MockTtlTimeProvider();
        ttlTimeProvider.setCurrentTimestamp(0L);

        try (TestBackendContext context =
                createBackendContext(
                        tempDir, false, null, MemorySize.ofMebiBytes(1), ttlTimeProvider, true)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ttl-aggregating-state", new SumAggregator(), LongSerializer.INSTANCE);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());

            backend.setCurrentKey(14);
            AggregatingState<Integer, String> state =
                    backend.getPartitionedState(
                            "ttl-aggregating-ns", StringSerializer.INSTANCE, descriptor);
            state.add(3);
            state.add(4);

            setTtlTime(backend, ttlTimeProvider, 4_000L);
            assertEquals("sum=7", state.get());

            setTtlTime(backend, ttlTimeProvider, 6_000L);
            assertNull(state.get());
        }
    }

    @Test
    void aggregatingStateSchemaRoundTripsThroughWriteAndRead(@TempDir Path tempDir)
            throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);
            AggregatingStateDescriptor<Integer, Long, String> descriptor =
                    new AggregatingStateDescriptor<>(
                            "ag-schema", new SumAggregator(), LongSerializer.INSTANCE);
            backend.getPartitionedState("ns", StringSerializer.INSTANCE, descriptor).add(1);

            StateInspectSchema schema = backend.getStateInspectSchemas().get("ag-schema");
            assertNotNull(schema);
            assertEquals(StateKind.AGGREGATING, schema.stateKind());

            // The captured value serializer must be the ACCUMULATOR type (Long), not the OUT type
            // (String) — accumulators are what live on disk for AggregatingState.
            assertEquals(
                    LongSerializer.INSTANCE.getClass().getName(),
                    schema.valueSerializer().serializerClassName());

            // Round-trip via the StateInspectSchemaStore wire format.
            StateInspectSchemaStore store =
                    new StateInspectSchemaStore(Collections.singletonList(schema));
            byte[] bytes = store.toBytes();
            StateInspectSchemaStore roundTripped = StateInspectSchemaStore.fromBytes(bytes);
            StateInspectSchema decoded = roundTripped.schemas().get(0);
            assertEquals(StateKind.AGGREGATING, decoded.stateKind());
            assertEquals(schema.serializerClassNames(), decoded.serializerClassNames());
        }
    }

    // -------- AggregatingState test helpers --------

    /** IN=Integer, ACC=Long, OUT=String. ACC and OUT are deliberately different types. */
    private static final class SumAggregator implements AggregateFunction<Integer, Long, String> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Integer in, Long acc) {
            return acc + in;
        }

        @Override
        public String getResult(Long acc) {
            return "sum=" + acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /** Non-commutative aggregator: IN=ACC=OUT=String, accumulator is the concatenation so far. */
    private static final class StringConcatAggregator
            implements AggregateFunction<String, String, String> {
        @Override
        public String createAccumulator() {
            return "";
        }

        @Override
        public String add(String in, String acc) {
            return acc + in;
        }

        @Override
        public String getResult(String acc) {
            return acc;
        }

        @Override
        public String merge(String a, String b) {
            return a + b;
        }
    }

    /** Wraps an aggregator and counts merge() invocations (parallel to {@link CountingReducer}). */
    private static final class CountingAggregator
            implements AggregateFunction<Integer, Long, String> {
        private final AggregateFunction<Integer, Long, String> delegate;
        int mergeCalls;

        CountingAggregator(AggregateFunction<Integer, Long, String> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Long createAccumulator() {
            return delegate.createAccumulator();
        }

        @Override
        public Long add(Integer in, Long acc) {
            return delegate.add(in, acc);
        }

        @Override
        public String getResult(Long acc) {
            return delegate.getResult(acc);
        }

        @Override
        public Long merge(Long a, Long b) {
            mergeCalls++;
            return delegate.merge(a, b);
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, N, IN, ACC, OUT>
            InternalAggregatingState<K, N, IN, ACC, OUT> asInternalAggregatingState(Object state) {
        return (InternalAggregatingState<K, N, IN, ACC, OUT>) state;
    }

    private static ValueState<String> valueState(
            AbstractKeyedStateBackend<Integer> backend, String name, String namespace)
            throws Exception {
        return backend.getPartitionedState(
                namespace,
                StringSerializer.INSTANCE,
                new ValueStateDescriptor<>(name, StringSerializer.INSTANCE));
    }

    private static ListState<String> listState(
            AbstractKeyedStateBackend<Integer> backend, String name, String namespace)
            throws Exception {
        return backend.getPartitionedState(
                namespace,
                StringSerializer.INSTANCE,
                new ListStateDescriptor<>(name, StringSerializer.INSTANCE));
    }

    private static MapState<String, String> mapState(
            AbstractKeyedStateBackend<Integer> backend, String name, String namespace)
            throws Exception {
        return backend.getPartitionedState(
                namespace,
                StringSerializer.INSTANCE,
                new MapStateDescriptor<>(
                        name, StringSerializer.INSTANCE, StringSerializer.INSTANCE));
    }

    private static ReducingState<Integer> reducingState(
            AbstractKeyedStateBackend<Integer> backend, String name, String namespace)
            throws Exception {
        return backend.getPartitionedState(
                namespace,
                StringSerializer.INSTANCE,
                new ReducingStateDescriptor<>(name, sumReducer(), IntSerializer.INSTANCE));
    }

    private static AggregatingState<Integer, String> aggregatingState(
            AbstractKeyedStateBackend<Integer> backend, String name, String namespace)
            throws Exception {
        return backend.getPartitionedState(
                namespace,
                StringSerializer.INSTANCE,
                new AggregatingStateDescriptor<>(
                        name, new SumAggregator(), LongSerializer.INSTANCE));
    }

    /** Variable-length TypeSerializer<long[]> used by exception-preserves-state tests. */
    private static final class LongArraySerializer
            extends org.apache.flink.api.common.typeutils.TypeSerializer<long[]> {

        static final LongArraySerializer INSTANCE = new LongArraySerializer();

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializer<long[]> duplicate() {
            return this;
        }

        @Override
        public long[] createInstance() {
            return new long[0];
        }

        @Override
        public long[] copy(long[] from) {
            return from == null ? null : Arrays.copyOf(from, from.length);
        }

        @Override
        public long[] copy(long[] from, long[] reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(long[] record, DataOutputView target) throws IOException {
            target.writeInt(record.length);
            for (long v : record) {
                target.writeLong(v);
            }
        }

        @Override
        public long[] deserialize(DataInputView source) throws IOException {
            int length = source.readInt();
            long[] out = new long[length];
            for (int i = 0; i < length; i++) {
                out[i] = source.readLong();
            }
            return out;
        }

        @Override
        public long[] deserialize(long[] reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            int length = source.readInt();
            target.writeInt(length);
            for (int i = 0; i < length; i++) {
                target.writeLong(source.readLong());
            }
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof LongArraySerializer;
        }

        @Override
        public int hashCode() {
            return LongArraySerializer.class.hashCode();
        }

        @Override
        public TypeSerializerSnapshot<long[]> snapshotConfiguration() {
            return new LongArraySerializerSnapshot();
        }
    }

    /** Stateless serializer snapshot for {@link LongArraySerializer}. */
    public static final class LongArraySerializerSnapshot
            extends SimpleTypeSerializerSnapshot<long[]> {
        public LongArraySerializerSnapshot() {
            super(() -> LongArraySerializer.INSTANCE);
        }
    }

    /** Variable-length TypeSerializer<int[]> used by the mutate-in-place test. */
    private static final class IntArraySerializer
            extends org.apache.flink.api.common.typeutils.TypeSerializer<int[]> {

        static final IntArraySerializer INSTANCE = new IntArraySerializer();

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializer<int[]> duplicate() {
            return this;
        }

        @Override
        public int[] createInstance() {
            return new int[0];
        }

        @Override
        public int[] copy(int[] from) {
            return from == null ? null : Arrays.copyOf(from, from.length);
        }

        @Override
        public int[] copy(int[] from, int[] reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(int[] record, DataOutputView target) throws IOException {
            target.writeInt(record.length);
            for (int v : record) {
                target.writeInt(v);
            }
        }

        @Override
        public int[] deserialize(DataInputView source) throws IOException {
            int length = source.readInt();
            int[] out = new int[length];
            for (int i = 0; i < length; i++) {
                out[i] = source.readInt();
            }
            return out;
        }

        @Override
        public int[] deserialize(int[] reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            int length = source.readInt();
            target.writeInt(length);
            for (int i = 0; i < length; i++) {
                target.writeInt(source.readInt());
            }
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof IntArraySerializer;
        }

        @Override
        public int hashCode() {
            return IntArraySerializer.class.hashCode();
        }

        @Override
        public TypeSerializerSnapshot<int[]> snapshotConfiguration() {
            return new IntArraySerializerSnapshot();
        }
    }

    /** Stateless serializer snapshot for {@link IntArraySerializer}. */
    public static final class IntArraySerializerSnapshot
            extends SimpleTypeSerializerSnapshot<int[]> {
        public IntArraySerializerSnapshot() {
            super(() -> IntArraySerializer.INSTANCE);
        }
    }

    private static int countFiles(Path root) throws IOException {
        int count = 0;
        java.io.File rootFile = root.toFile();
        if (!rootFile.exists()) {
            return 0;
        }
        java.util.concurrent.atomic.AtomicInteger counter =
                new java.util.concurrent.atomic.AtomicInteger(0);
        countRecursive(rootFile, counter);
        return counter.get();
    }

    private static void countRecursive(
            java.io.File dir, java.util.concurrent.atomic.AtomicInteger counter) {
        java.io.File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (java.io.File f : files) {
            if (f.isFile()) {
                counter.incrementAndGet();
            } else if (f.isDirectory()) {
                countRecursive(f, counter);
            }
        }
    }

    private TestBackendContext createBackendContext(
            Path tempDir, boolean localDirPrimaryHighPriority, String checkpointDirectory)
            throws Exception {
        return createBackendContext(
                tempDir,
                localDirPrimaryHighPriority,
                checkpointDirectory,
                null,
                TtlTimeProvider.DEFAULT,
                false);
    }

    private TestBackendContext createBackendContext(
            Path tempDir,
            boolean localDirPrimaryHighPriority,
            String checkpointDirectory,
            MemorySize fixedMemoryPerSlot)
            throws Exception {
        return createBackendContext(
                tempDir,
                localDirPrimaryHighPriority,
                checkpointDirectory,
                fixedMemoryPerSlot,
                TtlTimeProvider.DEFAULT,
                false);
    }

    private TestBackendContext createBackendContext(
            Path tempDir,
            boolean localDirPrimaryHighPriority,
            String checkpointDirectory,
            MemorySize fixedMemoryPerSlot,
            TtlTimeProvider ttlTimeProvider,
            boolean manualCobbleTtlTimeProviderForTests)
            throws Exception {
        return createBackendContext(
                tempDir,
                localDirPrimaryHighPriority,
                checkpointDirectory,
                fixedMemoryPerSlot,
                ttlTimeProvider,
                manualCobbleTtlTimeProviderForTests,
                Collections.<KeyedStateHandle>emptyList());
    }

    private TestBackendContext createBackendContext(
            Path tempDir,
            boolean localDirPrimaryHighPriority,
            String checkpointDirectory,
            MemorySize fixedMemoryPerSlot,
            TtlTimeProvider ttlTimeProvider,
            boolean manualCobbleTtlTimeProviderForTests,
            Collection<KeyedStateHandle> stateHandles)
            throws Exception {
        return createBackendContext(
                tempDir,
                localDirPrimaryHighPriority,
                checkpointDirectory,
                fixedMemoryPerSlot,
                ttlTimeProvider,
                manualCobbleTtlTimeProviderForTests,
                stateHandles,
                KeyGroupRange.of(0, 15),
                new Configuration());
    }

    private TestBackendContext createBackendContext(
            Path tempDir,
            boolean localDirPrimaryHighPriority,
            String checkpointDirectory,
            MemorySize fixedMemoryPerSlot,
            TtlTimeProvider ttlTimeProvider,
            boolean manualCobbleTtlTimeProviderForTests,
            Collection<KeyedStateHandle> stateHandles,
            KeyGroupRange keyGroupRange)
            throws Exception {
        return createBackendContext(
                tempDir,
                localDirPrimaryHighPriority,
                checkpointDirectory,
                fixedMemoryPerSlot,
                ttlTimeProvider,
                manualCobbleTtlTimeProviderForTests,
                stateHandles,
                keyGroupRange,
                new Configuration());
    }

    private TestBackendContext createBackendContext(
            Path tempDir,
            boolean localDirPrimaryHighPriority,
            String checkpointDirectory,
            MemorySize fixedMemoryPerSlot,
            TtlTimeProvider ttlTimeProvider,
            boolean manualCobbleTtlTimeProviderForTests,
            Collection<KeyedStateHandle> stateHandles,
            KeyGroupRange keyGroupRange,
            Configuration extraConfiguration)
            throws Exception {
        Path configuredLocalDir = tempDir.resolve("configured-local-dir");
        Path taskManagerWorkingDir = tempDir.resolve("tm-working-dir");

        Configuration configuration = new Configuration();
        configuration.set(CobbleOptions.LOCAL_DIRECTORIES, configuredLocalDir.toString());
        configuration.set(CobbleOptions.MEMTABLE_BUFFER_RATIO, 0.25d);
        configuration.set(CobbleOptions.MEMTABLE_BUFFER_COUNT, 4);
        configuration.set(CobbleOptions.DIRECT_IO_BUFFER_SIZE, MemorySize.parse("8kb"));
        configuration.set(CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE, 128);
        configuration.set(
                CobbleOptions.LOCAL_DIR_PRIMARY_HIGH_PRIORITY, localDirPrimaryHighPriority);
        if (fixedMemoryPerSlot != null) {
            configuration.set(CobbleOptions.FIX_PER_SLOT_MEMORY_SIZE, fixedMemoryPerSlot);
        }
        if (checkpointDirectory != null) {
            configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDirectory);
        }
        configuration.addAll(extraConfiguration);

        CobbleStateBackend backend =
                new CobbleStateBackend(manualCobbleTtlTimeProviderForTests)
                        .configure(configuration, getClass().getClassLoader());

        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setTaskName("cobble-test-task")
                        .setJobVertexID(TEST_JOB_VERTEX_ID)
                        .setManagedMemorySize(MemorySize.ofMebiBytes(128).getBytes())
                        .setTaskManagerRuntimeInfo(
                                new TestingTaskManagerRuntimeInfo(
                                        new Configuration(), taskManagerWorkingDir.toFile()))
                        .setTaskStateManager(new TestTaskStateManagerBuilder().build())
                        .build();

        AbstractKeyedStateBackend<Integer> keyedStateBackend = null;
        try {
            TaskKvStateRegistry kvStateRegistry = environment.getTaskKvStateRegistry();
            keyedStateBackend =
                    backend.createKeyedStateBackend(
                            environment,
                            environment.getJobID(),
                            "test-operator",
                            IntSerializer.INSTANCE,
                            16,
                            keyGroupRange,
                            kvStateRegistry,
                            ttlTimeProvider,
                            environment.getMetricGroup(),
                            stateHandles,
                            new CloseableRegistry(),
                            0.5d);

            assertInstanceOf(CobbleKeyedStateBackend.class, keyedStateBackend);
            return new TestBackendContext(
                    environment,
                    (CobbleKeyedStateBackend<Integer>) keyedStateBackend,
                    configuredLocalDir);
        } catch (Exception e) {
            if (keyedStateBackend != null) {
                keyedStateBackend.close();
            }
            environment.close();
            throw e;
        }
    }

    /**
     * Variant of {@link #createBackendContext(Path, boolean, String, MemorySize, TtlTimeProvider,
     * boolean, Collection, KeyGroupRange, Configuration)} that lets the test inject a custom key
     * serializer instead of the default {@code IntSerializer.INSTANCE}. Used by the
     * reconfigured-key-serializer test to drive the canonical key snapshot through Flink's
     * StateSerializerProvider machinery on the import phase.
     */
    private TestBackendContext createBackendContextWithKeySerializer(
            Path tempDir,
            Collection<KeyedStateHandle> stateHandles,
            KeyGroupRange keyGroupRange,
            org.apache.flink.api.common.typeutils.TypeSerializer<Integer> keySerializer)
            throws Exception {
        Path configuredLocalDir = tempDir.resolve("configured-local-dir");
        Path taskManagerWorkingDir = tempDir.resolve("tm-working-dir");

        Configuration configuration = new Configuration();
        configuration.set(CobbleOptions.LOCAL_DIRECTORIES, configuredLocalDir.toString());
        configuration.set(CobbleOptions.MEMTABLE_BUFFER_RATIO, 0.25d);
        configuration.set(CobbleOptions.MEMTABLE_BUFFER_COUNT, 4);
        configuration.set(CobbleOptions.DIRECT_IO_BUFFER_SIZE, MemorySize.parse("8kb"));
        configuration.set(CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE, 128);

        CobbleStateBackend backend =
                new CobbleStateBackend(false).configure(configuration, getClass().getClassLoader());

        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setTaskName("cobble-test-task")
                        .setJobVertexID(TEST_JOB_VERTEX_ID)
                        .setManagedMemorySize(MemorySize.ofMebiBytes(128).getBytes())
                        .setTaskManagerRuntimeInfo(
                                new TestingTaskManagerRuntimeInfo(
                                        new Configuration(), taskManagerWorkingDir.toFile()))
                        .setTaskStateManager(new TestTaskStateManagerBuilder().build())
                        .build();

        AbstractKeyedStateBackend<Integer> keyedStateBackend = null;
        try {
            TaskKvStateRegistry kvStateRegistry = environment.getTaskKvStateRegistry();
            keyedStateBackend =
                    backend.createKeyedStateBackend(
                            environment,
                            environment.getJobID(),
                            "test-operator",
                            keySerializer,
                            16,
                            keyGroupRange,
                            kvStateRegistry,
                            TtlTimeProvider.DEFAULT,
                            environment.getMetricGroup(),
                            stateHandles,
                            new CloseableRegistry(),
                            0.5d);

            assertInstanceOf(CobbleKeyedStateBackend.class, keyedStateBackend);
            return new TestBackendContext(
                    environment,
                    (CobbleKeyedStateBackend<Integer>) keyedStateBackend,
                    configuredLocalDir);
        } catch (Exception e) {
            if (keyedStateBackend != null) {
                keyedStateBackend.close();
            }
            environment.close();
            throw e;
        }
    }

    private static void assertValueStateEntry(
            CobbleKeyedStateBackend<Integer> backend,
            ValueState<String> valueState,
            int key,
            int payloadBytes)
            throws Exception {
        String expected = payload("value", key, payloadBytes);
        backend.setCurrentKey(key);
        assertEquals(expected, valueState.value());
    }

    private static KeyedStateHandle runCheckpointSnapshot(
            CobbleKeyedStateBackend<Integer> backend, long checkpointId) throws Exception {
        return runCheckpointSnapshot(
                backend, checkpointId, new MemCheckpointStreamFactory(1024 * 1024));
    }

    private static KeyedStateHandle runCheckpointSnapshot(
            CobbleKeyedStateBackend<Integer> backend,
            long checkpointId,
            org.apache.flink.runtime.state.CheckpointStreamFactory streamFactory)
            throws Exception {
        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                backend.snapshot(
                        checkpointId,
                        System.currentTimeMillis(),
                        streamFactory,
                        CheckpointOptions.forCheckpointWithDefaultLocation());
        snapshotFuture.run();
        SnapshotResult<KeyedStateHandle> snapshotResult = snapshotFuture.get();
        assertNotNull(snapshotResult.getJobManagerOwnedSnapshot());
        return snapshotResult.getJobManagerOwnedSnapshot();
    }

    private static boolean hasCause(Throwable error, Class<? extends Throwable> expectedType) {
        Throwable current = error;
        while (current != null) {
            if (expectedType.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static CobbleSnapshotMetadata readSnapshotMetadata(KeyedStateHandle keyedStateHandle)
            throws Exception {
        IncrementalRemoteKeyedStateHandle incrementalHandle =
                assertInstanceOf(IncrementalRemoteKeyedStateHandle.class, keyedStateHandle);
        try (org.apache.flink.core.fs.FSDataInputStream inputStream =
                incrementalHandle.getMetaStateHandle().openInputStream()) {
            return CobbleSnapshotMetadata.read(new DataInputViewStreamWrapper(inputStream));
        }
    }

    private KeyedStateHandle createRocksDbCanonicalAllStateKindsSavepoint(
            Path tempDir, int key1, int key2, KeyGroupRange keyGroupRange) throws Exception {
        Path rocksDbPath = tempDir.resolve("rocksdb-local-all-kinds");
        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setTaskName("rocksdb-canonical-all-kinds-savepoint")
                        .setJobVertexID(TEST_JOB_VERTEX_ID)
                        .setManagedMemorySize(MemorySize.ofMebiBytes(128).getBytes())
                        .setTaskManagerRuntimeInfo(
                                new TestingTaskManagerRuntimeInfo(
                                        new Configuration(),
                                        tempDir.resolve("rocksdb-all-kinds-tm").toFile()))
                        .setTaskStateManager(new TestTaskStateManagerBuilder().build())
                        .build();
        AbstractKeyedStateBackend<Integer> backend = null;
        try {
            RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(rocksDbPath.toUri());
            backend =
                    rocksDbBackend.createKeyedStateBackend(
                            environment,
                            environment.getJobID(),
                            "rocksdb-canonical-all-kinds-savepoint",
                            IntSerializer.INSTANCE,
                            16,
                            keyGroupRange,
                            environment.getTaskKvStateRegistry(),
                            TtlTimeProvider.DEFAULT,
                            environment.getMetricGroup(),
                            Collections.emptyList(),
                            new CloseableRegistry(),
                            0.5d);

            ValueState<String> valueNsA = valueState(backend, "value-all", "ns-a");
            ListState<String> listNsA = listState(backend, "list-all", "ns-a");
            MapState<String, String> mapNsA = mapState(backend, "map-all", "ns-a");
            ReducingState<Integer> reducingNsA = reducingState(backend, "reducing-all", "ns-a");
            AggregatingState<Integer, String> aggregatingNsA =
                    aggregatingState(backend, "aggregating-all", "ns-a");

            backend.setCurrentKey(key1);
            valueNsA.update("value-k1-ns-a");
            listNsA.addAll(Arrays.asList("k1-a-left", "k1-a-right"));
            mapNsA.put("k1-null", null);
            mapNsA.put("k1-a", "map-k1-a");
            reducingNsA.add(1);
            reducingNsA.add(2);
            aggregatingNsA.add(3);
            aggregatingNsA.add(4);

            backend.setCurrentKey(key2);
            valueNsA.update("value-k2-ns-a");
            listNsA.addAll(Arrays.asList("k2-a-left", "k2-a-right"));
            mapNsA.put("k2-null", null);
            mapNsA.put("k2-a", "map-k2-a");
            reducingNsA.add(10);
            reducingNsA.add(20);
            aggregatingNsA.add(30);
            aggregatingNsA.add(40);

            ValueState<String> valueNsB = valueState(backend, "value-all", "ns-b");
            ListState<String> listNsB = listState(backend, "list-all", "ns-b");
            MapState<String, String> mapNsB = mapState(backend, "map-all", "ns-b");
            ReducingState<Integer> reducingNsB = reducingState(backend, "reducing-all", "ns-b");
            AggregatingState<Integer, String> aggregatingNsB =
                    aggregatingState(backend, "aggregating-all", "ns-b");

            backend.setCurrentKey(key1);
            valueNsB.update("value-k1-ns-b");
            listNsB.add("k1-b-only");
            mapNsB.put("k1-b", "map-k1-b");
            reducingNsB.add(5);
            aggregatingNsB.add(11);

            SavepointResources<Integer> savepointResources =
                    ((CheckpointableKeyedStateBackend<Integer>) backend).savepoint();
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                    new SnapshotStrategyRunner<>(
                                    "RocksDB canonical all-kinds savepoint",
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
        } finally {
            if (backend != null) {
                backend.dispose();
            }
            environment.close();
        }
    }

    private KeyedStateHandle createRocksDbCanonicalSavepoint(Path tempDir, int key, int keyGroup)
            throws Exception {
        Path rocksDbPath = tempDir.resolve("rocksdb-local");
        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setTaskName("rocksdb-canonical-savepoint")
                        .setJobVertexID(TEST_JOB_VERTEX_ID)
                        .setManagedMemorySize(MemorySize.ofMebiBytes(128).getBytes())
                        .setTaskManagerRuntimeInfo(
                                new TestingTaskManagerRuntimeInfo(
                                        new Configuration(),
                                        tempDir.resolve("rocksdb-tm").toFile()))
                        .setTaskStateManager(new TestTaskStateManagerBuilder().build())
                        .build();
        AbstractKeyedStateBackend<Integer> backend = null;
        try {
            RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(rocksDbPath.toUri());
            backend =
                    rocksDbBackend.createKeyedStateBackend(
                            environment,
                            environment.getJobID(),
                            "rocksdb-canonical-savepoint",
                            IntSerializer.INSTANCE,
                            16,
                            KeyGroupRange.of(keyGroup, keyGroup),
                            environment.getTaskKvStateRegistry(),
                            TtlTimeProvider.DEFAULT,
                            environment.getMetricGroup(),
                            Collections.emptyList(),
                            new CloseableRegistry(),
                            0.5d);
            backend.setCurrentKey(key);
            ValueState<String> valueState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("value", StringSerializer.INSTANCE));
            ListState<String> listState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ListStateDescriptor<>("list", StringSerializer.INSTANCE));
            MapState<String, String> mapState =
                    backend.getPartitionedState(
                            org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new MapStateDescriptor<>(
                                    "map", StringSerializer.INSTANCE, StringSerializer.INSTANCE));
            valueState.update("value-42");
            listState.addAll(Arrays.asList("left", "right"));
            mapState.put("map-key", "map-value");

            SavepointResources<Integer> savepointResources =
                    ((CheckpointableKeyedStateBackend<Integer>) backend).savepoint();
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                    new SnapshotStrategyRunner<>(
                                    "RocksDB canonical savepoint",
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
        } finally {
            if (backend != null) {
                backend.dispose();
            }
            environment.close();
        }
    }

    private static KeyGroupsSavepointStateHandle createCanonicalSavepoint(int key, int keyGroup)
            throws Exception {
        return createCanonicalSavepoint(key, keyGroup, false);
    }

    private static KeyGroupsSavepointStateHandle createCanonicalSavepoint(
            int key, int keyGroup, boolean useCompression) throws Exception {
        List<StateMetaInfoSnapshot> stateMetaInfos =
                Arrays.asList(
                        new RegisteredKeyValueStateBackendMetaInfo<>(
                                        org.apache.flink.api.common.state.StateDescriptor.Type
                                                .VALUE,
                                        "value",
                                        VoidNamespaceSerializer.INSTANCE,
                                        StringSerializer.INSTANCE)
                                .snapshot(),
                        new RegisteredKeyValueStateBackendMetaInfo<>(
                                        org.apache.flink.api.common.state.StateDescriptor.Type.LIST,
                                        "list",
                                        VoidNamespaceSerializer.INSTANCE,
                                        new ListSerializer<>(StringSerializer.INSTANCE))
                                .snapshot(),
                        new RegisteredKeyValueStateBackendMetaInfo<>(
                                        org.apache.flink.api.common.state.StateDescriptor.Type.MAP,
                                        "map",
                                        VoidNamespaceSerializer.INSTANCE,
                                        new MapSerializer<>(
                                                StringSerializer.INSTANCE,
                                                StringSerializer.INSTANCE))
                                .snapshot());
        DataOutputSerializer output = new DataOutputSerializer(512);
        new KeyedBackendSerializationProxy<>(IntSerializer.INSTANCE, stateMetaInfos, useCompression)
                .write(output);

        // Build the key-group body in a separate buffer so we can optionally wrap it with the same
        // snappy decorator that Flink's FullSnapshotAsyncWriter uses. The compressed/uncompressed
        // boundary starts at the key-group offset and ends at the END_OF_KEY_GROUP_MARK.
        DataOutputSerializer body = new DataOutputSerializer(256);
        body.writeShort(0);
        writeCanonicalEntry(
                body,
                canonicalKey(keyGroup, key, null),
                CobbleStateKeySerializer.serialize(StringSerializer.INSTANCE, "value-42"),
                1);
        byte[] listValue =
                new org.apache.flink.runtime.state.ListDelimitedSerializer()
                        .serializeList(Arrays.asList("left", "right"), StringSerializer.INSTANCE);
        writeCanonicalEntry(body, canonicalKey(keyGroup, key, null), listValue, 2);
        DataOutputSerializer mapValue = new DataOutputSerializer(64);
        mapValue.writeBoolean(false);
        StringSerializer.INSTANCE.serialize("map-value", mapValue);
        writeCanonicalEntry(
                body,
                canonicalKey(keyGroup, key, "map-key"),
                mapValue.getCopyOfBuffer(),
                END_OF_KEY_GROUP_MARK);

        long keyGroupOffset = output.length();
        writeKeyGroupBody(output, body.getCopyOfBuffer(), useCompression);

        KeyGroupRange range = KeyGroupRange.of(keyGroup, keyGroup);
        return new KeyGroupsSavepointStateHandle(
                new KeyGroupRangeOffsets(range, new long[] {keyGroupOffset}),
                new ByteStreamStateHandle(
                        useCompression ? "canonical-savepoint-compressed" : "canonical-savepoint",
                        output.getCopyOfBuffer()));
    }

    /**
     * Appends a key-group body to {@code output}, optionally wrapped by the same snappy decorator
     * Flink uses for compressed canonical streams. The decorator is closed before any further bytes
     * are written, matching {@link FullSnapshotAsyncWriter}'s behavior.
     */
    private static void writeKeyGroupBody(
            DataOutputSerializer output, byte[] body, boolean useCompression) throws IOException {
        if (!useCompression) {
            output.write(body);
            return;
        }
        java.io.ByteArrayOutputStream sink = new java.io.ByteArrayOutputStream(body.length);
        try (java.io.OutputStream compressed =
                org.apache.flink.runtime.state.SnappyStreamCompressionDecorator.INSTANCE
                        .decorateWithCompression((java.io.OutputStream) sink)) {
            compressed.write(body);
        }
        output.write(sink.toByteArray());
    }

    /**
     * Builds a canonical savepoint with a single MAP state ("map-null") that contains both a
     * present-null entry (canonical leading {@code writeBoolean(true)}) and a present non-null
     * entry (canonical {@code writeBoolean(false)} + payload). Used to prove the canonical import
     * path threads null-valued map entries into Cobble unchanged.
     */
    private static KeyGroupsSavepointStateHandle createCanonicalMapSavepointWithNullValue(
            int key, int keyGroup) throws Exception {
        List<StateMetaInfoSnapshot> stateMetaInfos =
                Collections.singletonList(
                        new RegisteredKeyValueStateBackendMetaInfo<>(
                                        org.apache.flink.api.common.state.StateDescriptor.Type.MAP,
                                        "map-null",
                                        VoidNamespaceSerializer.INSTANCE,
                                        new MapSerializer<>(
                                                StringSerializer.INSTANCE,
                                                StringSerializer.INSTANCE))
                                .snapshot());
        DataOutputSerializer output = new DataOutputSerializer(256);
        new KeyedBackendSerializationProxy<>(IntSerializer.INSTANCE, stateMetaInfos, false)
                .write(output);

        DataOutputSerializer body = new DataOutputSerializer(128);
        body.writeShort(0);

        // Present-null map entry: isNull = true, no payload.
        DataOutputSerializer presentNullValue = new DataOutputSerializer(4);
        presentNullValue.writeBoolean(true);
        writeCanonicalEntry(
                body,
                canonicalKey(keyGroup, key, "map-key-null"),
                presentNullValue.getCopyOfBuffer(),
                0);

        // Present non-null map entry: isNull = false, then serialized user value.
        DataOutputSerializer presentNonNullValue = new DataOutputSerializer(32);
        presentNonNullValue.writeBoolean(false);
        StringSerializer.INSTANCE.serialize("map-value", presentNonNullValue);
        writeCanonicalEntry(
                body,
                canonicalKey(keyGroup, key, "map-key-nonnull"),
                presentNonNullValue.getCopyOfBuffer(),
                END_OF_KEY_GROUP_MARK);

        long keyGroupOffset = output.length();
        writeKeyGroupBody(output, body.getCopyOfBuffer(), false);

        KeyGroupRange range = KeyGroupRange.of(keyGroup, keyGroup);
        return new KeyGroupsSavepointStateHandle(
                new KeyGroupRangeOffsets(range, new long[] {keyGroupOffset}),
                new ByteStreamStateHandle(
                        "canonical-savepoint-map-null", output.getCopyOfBuffer()));
    }

    private static KeyGroupsSavepointStateHandle createCanonicalTimerSavepoint(
            int keyGroup,
            TimerHeapInternalTimer<Integer, org.apache.flink.runtime.state.VoidNamespace> timer)
            throws Exception {
        TimerSerializer<Integer, org.apache.flink.runtime.state.VoidNamespace> serializer =
                new TimerSerializer<>(IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE);
        DataOutputSerializer output = new DataOutputSerializer(256);
        new KeyedBackendSerializationProxy<>(
                        IntSerializer.INSTANCE,
                        Collections.singletonList(
                                new RegisteredPriorityQueueStateBackendMetaInfo<>(
                                                "timer", serializer)
                                        .snapshot()),
                        false)
                .write(output);

        long keyGroupOffset = output.length();
        output.writeShort(0);
        DataOutputSerializer timerKey = new DataOutputSerializer(32);
        CompositeKeySerializationUtils.writeKeyGroup(
                keyGroup,
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(16),
                timerKey);
        serializer.serialize(timer, timerKey);
        writeCanonicalEntry(output, timerKey.getCopyOfBuffer(), new byte[0], END_OF_KEY_GROUP_MARK);

        KeyGroupRange range = KeyGroupRange.of(keyGroup, keyGroup);
        return new KeyGroupsSavepointStateHandle(
                new KeyGroupRangeOffsets(range, new long[] {keyGroupOffset}),
                new ByteStreamStateHandle("canonical-timer-savepoint", output.getCopyOfBuffer()));
    }

    private static byte[] canonicalKey(int keyGroup, int key, String mapKey) throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(64);
        int keyGroupPrefixBytes =
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(16);
        CompositeKeySerializationUtils.writeKeyGroup(keyGroup, keyGroupPrefixBytes, output);
        CompositeKeySerializationUtils.writeKey(key, IntSerializer.INSTANCE, output, false);
        CompositeKeySerializationUtils.writeNameSpace(
                org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                output,
                false);
        if (mapKey != null) {
            StringSerializer.INSTANCE.serialize(mapKey, output);
        }
        return output.getCopyOfBuffer();
    }

    private static void writeCanonicalEntry(
            DataOutputSerializer output, byte[] key, byte[] value, int followingStateId)
            throws IOException {
        setMetaDataFollowsFlagInKey(key);
        BytePrimitiveArraySerializer.INSTANCE.serialize(key, output);
        BytePrimitiveArraySerializer.INSTANCE.serialize(value, output);
        output.writeShort(followingStateId);
    }

    private static Iterable<String> assertListStateValues(
            CobbleKeyedStateBackend<Integer> backend, ListState<String> listState, int key)
            throws Exception {
        backend.setCurrentKey(key);
        return listState.get();
    }

    private static Map<String, String> assertMapStateValues(
            CobbleKeyedStateBackend<Integer> backend,
            MapState<String, String> mapState,
            int key,
            int entriesPerKey,
            int valueBytes)
            throws Exception {
        Map<String, String> expected = expectedMapValues(key, entriesPerKey, valueBytes);
        backend.setCurrentKey(key);
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), mapState.get(entry.getKey()));
        }
        return expected;
    }

    private static int findKeyForGroup(int keyGroup) {
        for (int candidate = 0; candidate < 100_000; candidate++) {
            if (KeyGroupRangeAssignment.assignToKeyGroup(candidate, 16) == keyGroup) {
                return candidate;
            }
        }
        throw new IllegalStateException("Failed to find key for key-group " + keyGroup + '.');
    }

    private static void assertBytesColumnFamily(Schema schema, String columnFamilyName) {
        Map<Integer, Schema.ColumnType> family = schema.columnFamilies().get(columnFamilyName);
        assertNotNull(family, "Missing column family: " + columnFamilyName);
        if (family.isEmpty()) {
            return;
        }
        assertEquals(1, family.size());
        assertTrue(family.containsKey(0));
        assertTrue(family.get(0) instanceof Schema.ColumnType.Bytes);
    }

    private static void assertVolumeKinds(
            Config.VolumeDescriptor volume, Config.VolumeUsageKind... expectedKinds) {
        assertNotNull(volume.kinds);
        List<Config.VolumeUsageKind> actualKinds = volume.kinds;
        assertEquals(expectedKinds.length, actualKinds.size());
        for (Config.VolumeUsageKind expectedKind : expectedKinds) {
            assertTrue(actualKinds.contains(expectedKind));
        }
    }

    private static <T> List<T> toList(Iterable<T> values) {
        if (values == null) {
            return null;
        }
        java.util.ArrayList<T> collected = new java.util.ArrayList<>();
        for (T value : values) {
            collected.add(value);
        }
        return collected;
    }

    private static int requiredEntryCount(long requiredBytes, long bytesPerEntry) {
        return Math.max(1, (int) (requiredBytes / bytesPerEntry) + 1);
    }

    private static void setTtlTime(
            CobbleKeyedStateBackend<Integer> backend,
            MockTtlTimeProvider ttlTimeProvider,
            long timestampMillis) {
        ttlTimeProvider.setCurrentTimestamp(timestampMillis);
        backend.setTimeForTests(timestampMillis);
    }

    private static void assertByteSegmentEquals(byte[] actual, int offset, byte[] expected) {
        for (int index = 0; index < expected.length; index++) {
            assertEquals(expected[index], actual[offset + index]);
        }
    }

    private static int readTrailingInt(byte[] bytes, int fromEnd) {
        int start = bytes.length - fromEnd;
        return ((bytes[start] & 0xFF) << 24)
                | ((bytes[start + 1] & 0xFF) << 16)
                | ((bytes[start + 2] & 0xFF) << 8)
                | (bytes[start + 3] & 0xFF);
    }

    private static String payload(String prefix, int seed, int targetBytes) {
        String base = prefix + "-" + seed + "-";
        StringBuilder builder = new StringBuilder(targetBytes);
        builder.append(base);
        while (builder.length() < targetBytes) {
            builder.append((char) ('a' + (seed % 26)));
        }
        if (builder.length() > targetBytes) {
            builder.setLength(targetBytes);
        }
        return builder.toString();
    }

    private static List<String> expectedListValues(int key, int elementsPerKey, int elementBytes) {
        java.util.ArrayList<String> values = new java.util.ArrayList<>(elementsPerKey);
        for (int index = 0; index < elementsPerKey; index++) {
            values.add(payload("list-" + key, index, elementBytes));
        }
        return values;
    }

    private static Map<String, String> expectedMapValues(
            int key, int entriesPerKey, int valueBytes) {
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < entriesPerKey; index++) {
            values.put(
                    "map-key-" + key + '-' + index, payload("map-value-" + key, index, valueBytes));
        }
        return values;
    }

    private static final class TestTimerElement
            implements HeapPriorityQueueElement,
                    PriorityComparable<TestTimerElement>,
                    Keyed<Integer> {
        private final long timestamp;
        private final int key;
        private int internalIndex = HeapPriorityQueueElement.NOT_CONTAINED;

        private TestTimerElement(long timestamp, int key) {
            this.timestamp = timestamp;
            this.key = key;
        }

        @Override
        public int getInternalIndex() {
            return internalIndex;
        }

        @Override
        public void setInternalIndex(int newIndex) {
            this.internalIndex = newIndex;
        }

        @Override
        public int comparePriorityTo(TestTimerElement other) {
            return Long.compare(timestamp, other.timestamp);
        }

        @Override
        public Integer getKey() {
            return key;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof TestTimerElement)) {
                return false;
            }
            TestTimerElement that = (TestTimerElement) other;
            return timestamp == that.timestamp && key == that.key;
        }

        @Override
        public int hashCode() {
            return (int) (31 * timestamp + key);
        }
    }

    private static final class TestTimerElementSerializer
            extends TypeSerializerSingleton<TestTimerElement> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TestTimerElement createInstance() {
            return new TestTimerElement(0L, 0);
        }

        @Override
        public TestTimerElement copy(TestTimerElement from) {
            return new TestTimerElement(from.timestamp, from.key);
        }

        @Override
        public TestTimerElement copy(TestTimerElement from, TestTimerElement reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return Long.BYTES + Integer.BYTES;
        }

        @Override
        public void serialize(TestTimerElement record, DataOutputView target) throws IOException {
            target.writeLong(record.timestamp);
            target.writeInt(record.key);
        }

        @Override
        public TestTimerElement deserialize(DataInputView source) throws IOException {
            return new TestTimerElement(source.readLong(), source.readInt());
        }

        @Override
        public TestTimerElement deserialize(TestTimerElement reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
            target.writeInt(source.readInt());
        }

        @Override
        public TypeSerializerSnapshot<TestTimerElement> snapshotConfiguration() {
            return new SimpleTypeSerializerSnapshot<TestTimerElement>(
                    TestTimerElementSerializer::new) {};
        }
    }

    private static final class FailingCheckpointStreamFactory extends MemCheckpointStreamFactory {
        private FailingCheckpointStreamFactory() {
            super(1024);
        }

        @Override
        public org.apache.flink.runtime.state.CheckpointStateOutputStream
                createCheckpointStateOutputStream(
                        org.apache.flink.runtime.state.CheckpointedStateScope scope)
                        throws IOException {
            throw new IOException("expected failing checkpoint stream");
        }
    }

    /**
     * Test-only {@link Integer} key serializer that delegates wire format to {@link IntSerializer}
     * but exposes a snapshot whose {@link
     * org.apache.flink.api.common.typeutils.TypeSerializerSnapshot#resolveSchemaCompatibility}
     * always returns {@link
     * org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility#compatibleWithReconfiguredSerializer
     * compatibleWithReconfiguredSerializer} carrying a fresh instance with version {@link
     * #RECONFIGURED_VERSION}. This drives Flink's {@code
     * StateSerializerProvider.setPreviousSerializerSnapshotForRestoredState} into its reconfigured
     * branch so the test can assert which provider the import phase ran on.
     */
    private static final class ReconfigurableIntKeySerializer
            extends org.apache.flink.api.common.typeutils.TypeSerializer<Integer> {
        private static final long serialVersionUID = 1L;
        static final String CANONICAL_VERSION = "canonical";
        static final String RUNTIME_VERSION = "runtime";
        static final String RECONFIGURED_VERSION = "reconfigured";

        final String version;

        ReconfigurableIntKeySerializer(String version) {
            this.version = version;
        }

        @Override
        public boolean isImmutableType() {
            return IntSerializer.INSTANCE.isImmutableType();
        }

        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializer<Integer> duplicate() {
            return this;
        }

        @Override
        public Integer createInstance() {
            return IntSerializer.INSTANCE.createInstance();
        }

        @Override
        public Integer copy(Integer from) {
            return IntSerializer.INSTANCE.copy(from);
        }

        @Override
        public Integer copy(Integer from, Integer reuse) {
            return IntSerializer.INSTANCE.copy(from, reuse);
        }

        @Override
        public int getLength() {
            return IntSerializer.INSTANCE.getLength();
        }

        @Override
        public void serialize(Integer record, DataOutputView target) throws IOException {
            IntSerializer.INSTANCE.serialize(record, target);
        }

        @Override
        public Integer deserialize(DataInputView source) throws IOException {
            return IntSerializer.INSTANCE.deserialize(source);
        }

        @Override
        public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
            return IntSerializer.INSTANCE.deserialize(reuse, source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            IntSerializer.INSTANCE.copy(source, target);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ReconfigurableIntKeySerializer
                    && ((ReconfigurableIntKeySerializer) obj).version.equals(version);
        }

        @Override
        public int hashCode() {
            return version.hashCode();
        }

        @Override
        public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
            return new ReconfigurableIntKeySerializerSnapshot(version);
        }
    }

    /**
     * Companion snapshot for {@link ReconfigurableIntKeySerializer}. The {@code
     * resolveSchemaCompatibility} contract is: regardless of the version the runtime supplied,
     * report {@code compatibleWithReconfiguredSerializer} carrying a brand-new instance whose
     * version is {@link ReconfigurableIntKeySerializer#RECONFIGURED_VERSION}. Used to verify that
     * the canonical import path actually feeds the builder's StateSerializerProvider, so the
     * backend ends up holding the reconfigured serializer.
     */
    public static final class ReconfigurableIntKeySerializerSnapshot
            implements TypeSerializerSnapshot<Integer> {
        private String snapshotVersion;

        // Public no-arg constructor required for snapshot deserialization, even though this test
        // path serializes and deserializes through the in-memory ByteStreamStateHandle without
        // recreating the snapshot class by name.
        public ReconfigurableIntKeySerializerSnapshot() {
            this.snapshotVersion = ReconfigurableIntKeySerializer.CANONICAL_VERSION;
        }

        ReconfigurableIntKeySerializerSnapshot(String snapshotVersion) {
            this.snapshotVersion = snapshotVersion;
        }

        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(org.apache.flink.core.memory.DataOutputView out)
                throws IOException {
            out.writeUTF(snapshotVersion);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            this.snapshotVersion = in.readUTF();
        }

        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializer<Integer> restoreSerializer() {
            return new ReconfigurableIntKeySerializer(snapshotVersion);
        }

        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility<Integer>
                resolveSchemaCompatibility(
                        org.apache.flink.api.common.typeutils.TypeSerializer<Integer>
                                newSerializer) {
            // Always force Flink down the "reconfigured" branch so the test can observe whether
            // the backend ends up using the reconfigured serializer (proves the import phase ran
            // on the builder's own StateSerializerProvider).
            return org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility
                    .compatibleWithReconfiguredSerializer(
                            new ReconfigurableIntKeySerializer(
                                    ReconfigurableIntKeySerializer.RECONFIGURED_VERSION));
        }
    }

    private static final class TestBackendContext implements AutoCloseable {
        private final MockEnvironment environment;
        private final CobbleKeyedStateBackend<Integer> cobbleBackend;
        private final Path configuredLocalDir;

        private TestBackendContext(
                MockEnvironment environment,
                CobbleKeyedStateBackend<Integer> cobbleBackend,
                Path configuredLocalDir) {
            this.environment = environment;
            this.cobbleBackend = cobbleBackend;
            this.configuredLocalDir = configuredLocalDir;
        }

        @Override
        public void close() throws Exception {
            try {
                cobbleBackend.close();
            } finally {
                environment.close();
            }
        }
    }
}
