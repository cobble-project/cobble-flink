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
import org.apache.flink.runtime.jobgraph.JobVertexID;
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
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.ttl.MockTtlTimeProvider;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
