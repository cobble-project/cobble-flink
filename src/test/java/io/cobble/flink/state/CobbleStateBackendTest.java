package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.ShardSnapshot;
import io.cobble.structured.Schema;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.TestTaskStateManagerBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.ttl.MockTtlTimeProvider;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;

/** Tests for {@link CobbleStateBackend}. */
class CobbleStateBackendTest {

    private static final String CHECKPOINT_SCOPE = "op_test-operator";

    @Test
    void createsBackendAndLoadsNativeLibrary() {
        assertDoesNotThrow(() -> new CobbleStateBackend());
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
        CobbleStateKeySerializer.ReusableSerializedKeyBuilder<String> builder =
                new CobbleStateKeySerializer.ReusableSerializedKeyBuilder<>(
                        StringSerializer.INSTANCE, 64);

        byte[] encoded = builder.buildKeyAndNamespace("key", StringSerializer.INSTANCE, "ns");

        assertEquals(keyBytes.length + namespaceBytes.length + Integer.BYTES, encoded.length);
        assertByteSegmentEquals(encoded, 0, keyBytes);
        assertByteSegmentEquals(encoded, keyBytes.length, namespaceBytes);
        assertEquals(keyBytes.length, readTrailingInt(encoded, Integer.BYTES));
    }

    @Test
    void serializeMapKeyUserKeyAndNamespaceUsesKeyAndUserKeyLengthSuffixes() throws Exception {
        byte[] keyBytes = CobbleStateKeySerializer.serialize(StringSerializer.INSTANCE, "key");
        byte[] userKeyBytes = CobbleStateKeySerializer.serialize(StringSerializer.INSTANCE, "uk");
        byte[] namespaceBytes = CobbleStateKeySerializer.serialize(StringSerializer.INSTANCE, "ns");
        CobbleStateKeySerializer.ReusableSerializedKeyBuilder<String> builder =
                new CobbleStateKeySerializer.ReusableSerializedKeyBuilder<>(
                        StringSerializer.INSTANCE, 64);

        byte[] encoded =
                builder.buildMapKeyUserKeyAndNamespace(
                        "key", StringSerializer.INSTANCE, "uk", StringSerializer.INSTANCE, "ns");

        assertEquals(
                keyBytes.length
                        + 1
                        + userKeyBytes.length
                        + namespaceBytes.length
                        + (Integer.BYTES * 2),
                encoded.length);
        assertByteSegmentEquals(encoded, 0, keyBytes);
        assertEquals(0, encoded[keyBytes.length]);
        assertByteSegmentEquals(encoded, keyBytes.length + 1, userKeyBytes);
        assertByteSegmentEquals(encoded, keyBytes.length + 1 + userKeyBytes.length, namespaceBytes);
        assertEquals(userKeyBytes.length, readTrailingInt(encoded, Integer.BYTES));
        assertEquals(keyBytes.length, readTrailingInt(encoded, Integer.BYTES * 2));
    }

    @Test
    void reusableSerializedKeyBuilderReusesSharedBufferUntilKeyChanges() throws Exception {
        CobbleStateKeySerializer.ReusableSerializedKeyBuilder<String> builder =
                new CobbleStateKeySerializer.ReusableSerializedKeyBuilder<>(
                        StringSerializer.INSTANCE, 64);

        builder.buildKeyAndNamespace("key", StringSerializer.INSTANCE, "ns-1");
        byte[] sharedBuffer = builder.sharedBuffer();

        builder.buildKeyAndNamespace("key", StringSerializer.INSTANCE, "ns-2");
        assertSame(sharedBuffer, builder.sharedBuffer());

        builder.buildKeyAndNamespace("other-key", StringSerializer.INSTANCE, "ns-3");
        assertSame(sharedBuffer, builder.sharedBuffer());
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
        assertEquals(
                "s3://bucket/checkpoints/shared/" + CHECKPOINT_SCOPE, volumes.get(0).baseDir);
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
    void timerStateUsesHeapPriorityQueueImplementation(@TempDir Path tempDir) throws Exception {
        try (TestBackendContext context = createBackendContext(tempDir, false, null)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            TestTimerElementSerializer timerSerializer = new TestTimerElementSerializer();

            KeyGroupedInternalPriorityQueue<TestTimerElement> queue =
                    backend.create("timer-state", timerSerializer);

            TestTimerElement later = new TestTimerElement(20L, 1);
            TestTimerElement earlier = new TestTimerElement(10L, 2);

            queue.add(later);
            queue.add(earlier);

            assertEquals(earlier, queue.peek());

            backend.setCurrentKey(2);
            assertEquals(earlier, queue.poll());

            backend.setCurrentKey(1);
            assertEquals(later, queue.poll());
            assertTrue(queue.isEmpty());
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
                        checkpointDirectory,
                        null,
                        TtlTimeProvider.DEFAULT,
                        false,
                        Collections.singletonList(snapshotHandle))) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;

            Schema schema = backend.getCobbleDb().currentSchema();
            assertTrue(schema.columnFamilies().containsKey("restore-value-state"));
            assertTrue(schema.columnFamilies().containsKey("restore-list-state"));
            assertTrue(schema.columnFamilies().containsKey("restore-map-state"));

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
    void restoreFromSingleHandleShrinksToTargetKeyGroupRange(@TempDir Path tempDir)
            throws Exception {
        String checkpointDirectory = tempDir.resolve("checkpoints").toString();
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
                        checkpointDirectory,
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
            assertTrue(schema.columnFamilies().containsKey("value-state"));
            assertTrue(schema.columnFamilies().get("value-state").isEmpty());
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
            assertTrue(
                    schema.columnFamilies().get("list-state").get(0)
                            instanceof Schema.ColumnType.List);
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
            assertTrue(schema.columnFamilies().containsKey("map-state"));
            assertTrue(schema.columnFamilies().get("map-state").isEmpty());
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
            assertTrue(schema.columnFamilies().containsKey("value-state"));
            assertTrue(schema.columnFamilies().get("value-state").isEmpty());
            assertTrue(
                    schema.columnFamilies().get("list-state").get(0)
                            instanceof Schema.ColumnType.List);
            assertTrue(schema.columnFamilies().containsKey("map-state"));
            assertTrue(schema.columnFamilies().get("map-state").isEmpty());
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
    void normalizeCheckpointDirectoryRejectsUnsupportedScheme() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CobbleKeyedStateBackendBuilder.normalizeCheckpointDirectory("http://tmp/cp"));
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
                KeyGroupRange.of(0, 15));
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
        Path configuredLocalDir = tempDir.resolve("configured-local-dir");
        Path taskManagerWorkingDir = tempDir.resolve("tm-working-dir");

        Configuration configuration = new Configuration();
        configuration.set(CobbleOptions.LOCAL_DIRECTORIES, configuredLocalDir.toString());
        configuration.set(CobbleOptions.WRITE_BUFFER_RATIO, 0.25d);
        configuration.set(CobbleOptions.MEMTABLE_BUFFER_COUNT, 4);
        configuration.set(
                CobbleOptions.LOCAL_DIR_PRIMARY_HIGH_PRIORITY, localDirPrimaryHighPriority);
        if (fixedMemoryPerSlot != null) {
            configuration.set(CobbleOptions.FIX_PER_SLOT_MEMORY_SIZE, fixedMemoryPerSlot);
        }
        if (checkpointDirectory != null) {
            configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDirectory);
        }

        CobbleStateBackend backend =
                new CobbleStateBackend(manualCobbleTtlTimeProviderForTests)
                        .configure(configuration, getClass().getClassLoader());

        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setTaskName("cobble-test-task")
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

    private static CobbleSnapshotMetadata readSnapshotMetadata(KeyedStateHandle keyedStateHandle)
            throws Exception {
        IncrementalRemoteKeyedStateHandle incrementalHandle =
                assertInstanceOf(IncrementalRemoteKeyedStateHandle.class, keyedStateHandle);
        try (org.apache.flink.core.fs.FSDataInputStream inputStream =
                incrementalHandle.getMetaStateHandle().openInputStream()) {
            return CobbleSnapshotMetadata.read(new DataInputViewStreamWrapper(inputStream));
        }
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
