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
import io.cobble.structured.Schema;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.TestTaskStateManagerBuilder;
import org.apache.flink.runtime.state.ttl.MockTtlTimeProvider;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Tests for {@link CobbleStateBackend}. */
class CobbleStateBackendTest {

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
                        instanceBasePath, "s3a://bucket/checkpoints", false);

        assertEquals(2, volumes.size());

        Config.VolumeDescriptor checkpointVolume = volumes.get(0);
        assertEquals("s3://bucket/checkpoints/shared", checkpointVolume.baseDir);
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
                        instanceBasePath, "s3p://bucket/checkpoints", true);

        assertEquals(2, volumes.size());
        assertEquals("s3://bucket/checkpoints/shared", volumes.get(0).baseDir);
        assertVolumeKinds(volumes.get(1), Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH);
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

        assertTrue(
                CobbleKeyedStateBackendBuilder.normalizeCheckpointDirectory("/tmp/cp")
                        .startsWith("file:///"));
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
                            KeyGroupRange.of(0, 15),
                            kvStateRegistry,
                            ttlTimeProvider,
                            environment.getMetricGroup(),
                            Collections.<KeyedStateHandle>emptyList(),
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
