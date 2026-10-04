package io.cobble.flink.inspect.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleEmbeddedCheckpoint;
import io.cobble.flink.common.CobbleSnapshotMetadataCodec;
import io.cobble.flink.common.CobbleSnapshotMetadataPayload;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.inspect.CobbleInspectClient;
import io.cobble.flink.inspect.InspectPage;
import io.cobble.flink.inspect.InspectSelection;
import io.cobble.flink.inspect.InspectSession;
import io.cobble.flink.inspect.LookupKey;
import io.cobble.flink.inspect.LookupRequest;
import io.cobble.flink.inspect.PageToken;
import io.cobble.flink.inspect.RawBytes;
import io.cobble.flink.inspect.ScanRequest;
import io.cobble.structured.Db;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.PostVersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.streaming.api.operators.InternalTimersSnapshot;
import org.apache.flink.streaming.api.operators.InternalTimersSnapshotReaderWriters;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.UUID;

class CobbleEmbeddedCheckpointInspectTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void timerScanMergesLegacyPrefixAndNativeTailWithStablePages() throws Exception {
        OperatorID operatorId = new OperatorID(51L, 53L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId, true);
                CobbleInspectClient client = CobbleInspectClient.builder().totalBuckets(4).build();
                InspectSession session =
                        client.open(
                                InspectSelection.checkpoint(
                                        fixture.directory.toUri().toString(),
                                        3L,
                                        operatorId.toHexString()))) {
            String target = "timer:_timer_state/event_service";
            java.util.List<Long> times = new java.util.ArrayList<>();
            PageToken token = null;
            do {
                InspectPage page = session.scan(new ScanRequest(target, 1, token));
                assertEquals(1, page.rows().size());
                times.add(
                        java.nio.ByteBuffer.wrap(page.rows().get(0).key().value()).getLong()
                                ^ Long.MIN_VALUE);
                token = page.nextPageToken();
            } while (token != null);
            assertEquals(Arrays.asList(10L, 20L, 30L, 40L, 50L), times);
            assertEquals(
                    1,
                    session.scan(
                                    new ScanRequest(
                                            target, 10, null, 0, new RawBytes(timerKey(20)), null))
                            .rows()
                            .size());
            assertTrue(
                    session.lookup(
                                    new LookupRequest(
                                            target,
                                            Collections.singletonList(
                                                    new LookupKey(0, new RawBytes(timerKey(10))))))
                            .rows()
                            .get(0)
                            .found());
            String legacyOnly = "timer:_timer_state/processing_service";
            InspectPage page = session.scan(new ScanRequest(legacyOnly, 1, null));
            assertEquals(1, page.rows().size());
            assertEquals(1, page.rows().get(0).bucket());
            assertNull(page.nextPageToken());
            assertFalse(
                    session.targets().stream()
                            .filter(item -> item.id().equals(target))
                            .findFirst()
                            .get()
                            .exactLookupSupported());
        }
    }

    @Test
    void discoversAndReadsEmbeddedCheckpointWithoutSidecars() throws Exception {
        OperatorID operatorId = new OperatorID(11L, 13L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            InspectCatalogDiscovery.Result catalog =
                    InspectCatalogDiscovery.discover(
                            fixture.directory.toUri().toString(),
                            CobbleConnectorStorageOptions.empty());
            CheckpointEntry checkpoint = catalog.checkpoints.get(0);
            OperatorEntry operator = checkpoint.findOperator(operatorId.toHexString());
            assertTrue(
                    operator.readerVolumeDirectories.stream()
                            .anyMatch(root -> root.endsWith("/old-volume")));

            SchemaResolveResult schema = MonitorInspectSchemaResolver.resolve(checkpoint, operator);
            assertEquals("checkpoint", catalog.sourceKind);
            assertEquals(3L, checkpoint.id);
            assertEquals(SchemaResolveResult.STATUS_AVAILABLE, schema.status);
            assertEquals(1, schema.store.schemas().size());

            java.util.List<java.io.File> temporaryDirectories;
            try (MonitorReaderSession reader =
                    MonitorReaderSession.open(
                            4,
                            CobbleConnectorStorageOptions.empty(),
                            catalog.sourceKind,
                            checkpoint,
                            operator)) {
                temporaryDirectories = reader.temporaryDirectories();
                assertTrue(
                        temporaryDirectories.get(0).getName().startsWith("cobble-embedded-read-"));
                assertEquals("value", utf8(reader.reader().get(0, utf8("key"), 0)));
            }
            for (java.io.File directory : temporaryDirectories) {
                assertFalse(directory.exists());
            }
        }
    }

    @Test
    void globalEntryFallsBackToEmbeddedSchemaWhenRegistryIsAbsent() throws Exception {
        OperatorID operatorId = new OperatorID(21L, 23L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, false);

            SchemaResolveResult result =
                    MonitorInspectSchemaResolver.resolve(
                            merged, merged.findOperator(operatorId.toHexString()));

            assertEquals(SchemaResolveResult.STATUS_AVAILABLE, result.status);
            assertEquals("embedded Flink checkpoint metadata", result.eventPath);
            assertEquals(1, result.store.schemas().size());
            assertTrue(result.warning.contains("Schema registry missing"));
        }
    }

    @Test
    void matchingRegistryRemainsPreferredOverEmbeddedSchema() throws Exception {
        OperatorID operatorId = new OperatorID(25L, 27L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, false);
            StateInspectSchemaStore embedded =
                    CobbleEmbeddedCheckpoint.select(new Path(fixture.directory.toUri()), "3")
                            .checkpoint()
                            .operator(operatorId.toHexString())
                            .schemaStore();
            writeRegistry(operatorId.toHexString(), 3L, embedded);

            SchemaResolveResult result =
                    MonitorInspectSchemaResolver.resolve(
                            merged, merged.findOperator(operatorId.toHexString()));

            assertEquals(SchemaResolveResult.STATUS_AVAILABLE, result.status);
            assertTrue(result.eventPath.contains("inspect-schema/events"));
            assertEquals(3L, result.schemaCheckpointId);
            assertNull(result.warning);
        }
    }

    @Test
    void staleRegistryIsReplacedByExactEmbeddedSchema() throws Exception {
        OperatorID operatorId = new OperatorID(26L, 28L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, false);
            StateInspectSchemaStore stale =
                    new StateInspectSchemaStore(
                            Collections.singletonList(
                                    StateInspectSchema.forValue(
                                            "stale",
                                            "default",
                                            false,
                                            IntSerializer.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            IntSerializer.INSTANCE)));
            writeRegistry(operatorId.toHexString(), 2L, stale);

            SchemaResolveResult result =
                    MonitorInspectSchemaResolver.resolve(
                            merged, merged.findOperator(operatorId.toHexString()));

            assertEquals(SchemaResolveResult.STATUS_AVAILABLE, result.status);
            assertEquals("embedded Flink checkpoint metadata", result.eventPath);
            assertEquals("state", result.store.schemas().get(0).stateName());
            assertEquals(3L, result.schemaCheckpointId);
            assertTrue(result.warning.contains("stale or mismatched"));
        }
    }

    @Test
    void globalReaderRemainsPreferredWhenEmbeddedMetadataIsAttached() throws Exception {
        OperatorID operatorId = new OperatorID(29L, 31L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, true);
            java.util.List<java.io.File> temporaryDirectories;
            try (MonitorReaderSession reader = openReader(merged, operatorId)) {
                temporaryDirectories = reader.temporaryDirectories();
                assertTrue(
                        temporaryDirectories.get(0).getName().startsWith("cobble-flink-monitor-"));
                assertEquals("value", utf8(reader.reader().get(0, utf8("key"), 0)));
            }
            for (java.io.File directory : temporaryDirectories) {
                assertFalse(directory.exists());
            }
        }
    }

    @Test
    void embeddedReaderIsFallbackWhenGlobalReaderCannotOpen() throws Exception {
        OperatorID operatorId = new OperatorID(33L, 35L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, false);
            java.util.List<java.io.File> temporaryDirectories;
            try (MonitorReaderSession reader = openReader(merged, operatorId)) {
                temporaryDirectories = reader.temporaryDirectories();
                assertTrue(
                        temporaryDirectories.get(0).getName().startsWith("cobble-embedded-read-"));
                assertEquals("value", utf8(reader.reader().get(0, utf8("key"), 0)));
            }
            for (java.io.File directory : temporaryDirectories) {
                assertFalse(directory.exists());
            }
        }
    }

    @Test
    void directMetadataDiscoversGlobalSidecarAndEmbeddedFallback() throws Exception {
        OperatorID operatorId = new OperatorID(37L, 39L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            mergedGlobalEntry(fixture, operatorId, true);

            InspectCatalogDiscovery.Result catalog =
                    InspectCatalogDiscovery.discover(
                            fixture.directory.resolve("_metadata").toUri().toString(),
                            CobbleConnectorStorageOptions.empty());
            OperatorEntry operator =
                    catalog.checkpoints.get(0).findOperator(operatorId.toHexString());

            assertTrue(operator.globalSnapshotLayout);
            assertTrue(operator.embeddedCheckpoint != null);
            assertTrue(operator.manifestCopyPath != null);
        }
    }

    @Test
    void directMetadataWithoutSidecarRemainsEmbeddedOnly() throws Exception {
        OperatorID operatorId = new OperatorID(41L, 43L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            InspectCatalogDiscovery.Result catalog =
                    InspectCatalogDiscovery.discover(
                            fixture.directory.resolve("_metadata").toUri().toString(),
                            CobbleConnectorStorageOptions.empty());
            OperatorEntry operator =
                    catalog.checkpoints.get(0).findOperator(operatorId.toHexString());

            assertFalse(operator.globalSnapshotLayout);
            assertTrue(operator.embeddedCheckpoint != null);
        }
    }

    private CheckpointEntry mergedGlobalEntry(
            ReadableCheckpoint fixture, OperatorID operatorId, boolean materializeGlobalSnapshot)
            throws Exception {
        CobbleEmbeddedCheckpoint.Location location =
                CobbleEmbeddedCheckpoint.select(new Path(fixture.directory.toUri()), "3");
        java.nio.file.Path operatorPath =
                tempDir.resolve("cobble").resolve(operatorId.toHexString());
        String operatorDirectory = operatorPath.toUri().toString();
        if (materializeGlobalSnapshot) {
            Config coordinatorConfig = nativeConfig(operatorPath, 4);
            try (DbCoordinator coordinator = DbCoordinator.open(coordinatorConfig)) {
                coordinator.materializeGlobalSnapshot(
                        4, 3L, Collections.singletonList(fixture.db.snapshot()));
            }
            Files.copy(
                    operatorPath.resolve("snapshot/SNAPSHOT-3"),
                    fixture.directory.resolve(
                            "COBBLE-SNAPSHOT-" + operatorId.toHexString() + "-MANIFEST"));
        }
        CheckpointEntry sidecar =
                new CheckpointEntry(
                        3L,
                        fixture.directory.toUri().toString(),
                        Collections.singletonList(
                                new OperatorEntry(
                                        operatorId.toHexString(),
                                        null,
                                        operatorDirectory,
                                        Collections.singletonList(operatorDirectory),
                                        true)));
        return InspectCatalogDiscovery.mergeEmbeddedCheckpoint(sidecar, location);
    }

    private MonitorReaderSession openReader(CheckpointEntry checkpoint, OperatorID operatorId) {
        return MonitorReaderSession.open(
                4,
                CobbleConnectorStorageOptions.empty(),
                "checkpoint",
                checkpoint,
                checkpoint.findOperator(operatorId.toHexString()));
    }

    private void writeRegistry(String operatorId, long checkpointId, StateInspectSchemaStore store)
            throws Exception {
        byte[] bytes = store.toBytes();
        String hash = InspectSchemaRegistryLayout.sha256(bytes);
        java.nio.file.Path schema =
                tempDir.resolve("cobble").resolve(operatorId).resolve("inspect-schema");
        Files.createDirectories(schema.resolve("events"));
        Files.createDirectories(schema.resolve("blobs"));
        Files.write(
                schema.resolve("blobs").resolve(InspectSchemaRegistryLayout.blobFileName(hash)),
                bytes);
        Files.write(
                schema.resolve("events")
                        .resolve(InspectSchemaRegistryLayout.eventFileName(checkpointId, hash)),
                Arrays.asList("checkpoint_id=" + checkpointId),
                StandardCharsets.UTF_8);
    }

    private ReadableCheckpoint writeCheckpoint(OperatorID operatorId) throws Exception {
        return writeCheckpoint(operatorId, false);
    }

    private ReadableCheckpoint writeCheckpoint(OperatorID operatorId, boolean timers)
            throws Exception {
        java.nio.file.Path directory = Files.createDirectory(tempDir.resolve("chk-3"));
        java.nio.file.Path volume = Files.createDirectory(directory.resolve("volume"));
        Config config = nativeConfig(volume, 4);
        Db db = Db.open(config, 0, 3);
        db.put(0, utf8("key"), 0, utf8("value"));
        if (timers) {
            try (io.cobble.structured.PriorityQueue queue =
                    db.newPriorityQueue("__cobble_timer___timer_state/event_service")) {
                queue.offer(0, timerKey(20), new byte[0]);
                queue.offer(0, timerKey(30), new byte[0]);
                queue.offer(0, timerKey(40), new byte[0]);
                queue.offer(0, timerKey(50), new byte[0]);
            }
        }
        ShardSnapshot shard = db.asyncSnapshot().get();
        db.retainSnapshot(shard.snapshotId);

        StateInspectSchema stateSchema =
                StateInspectSchema.forValue(
                        "state",
                        "default",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        LinkedHashMap<String, StateInspectSemanticSchema> semantic = new LinkedHashMap<>();
        semantic.put(
                "state",
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("INT")));
        java.util.List<StateInspectSchema> schemas =
                new java.util.ArrayList<>(Collections.singletonList(stateSchema));
        if (timers) {
            for (String domain : Arrays.asList("event", "processing")) {
                String name = "_timer_state/" + domain + "_service";
                schemas.add(
                        StateInspectSchema.forTimer(
                                name,
                                "__cobble_timer__" + name,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE));
                semantic.put(
                        name,
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                StateInspectType.unknown()));
            }
        }
        java.nio.file.Path state = directory.resolve("task-state");
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(state))) {
            CobbleSnapshotMetadataCodec.write(
                    new CobbleSnapshotMetadataPayload(
                            Collections.singletonList(
                                    Files.createDirectories(tempDir.resolve("old-volume"))
                                            .toUri()
                                            .toString()),
                            shard,
                            false,
                            Collections.emptyList(),
                            new StateInspectSchemaStore(schemas, semantic)),
                    new DataOutputViewStreamWrapper(output));
        }
        IncrementalRemoteKeyedStateHandle handle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        new KeyGroupRange(0, 3),
                        3L,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new FileStateHandle(new Path(state.toUri()), Files.size(state)));
        OperatorState operator = new OperatorState(operatorId, 1, 4);
        OperatorSubtaskState.Builder subtask =
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(
                                StateObjectCollection.<KeyedStateHandle>singleton(handle));
        if (timers) {
            subtask.setRawKeyedState(
                    StateObjectCollection.<KeyedStateHandle>singleton(legacyTimers()));
        }
        operator.putState(0, subtask.build());
        try (DataOutputStream output =
                new DataOutputStream(Files.newOutputStream(directory.resolve("_metadata")))) {
            Checkpoints.storeCheckpointMetadata(
                    new CheckpointMetadata(
                            3L, Collections.singletonList(operator), Collections.emptyList()),
                    output);
        }
        return new ReadableCheckpoint(directory, db);
    }

    private static byte[] timerKey(long timestamp) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(32);
        new TimerSerializer<>(IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE)
                .serialize(
                        new TimerHeapInternalTimer<>(timestamp, 7, VoidNamespace.INSTANCE), output);
        return output.getCopyOfBuffer();
    }

    private static KeyGroupsStateHandle legacyTimers() throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(256);
        long[] offsets = new long[2];
        for (int group = 0; group < 2; group++) {
            offsets[group] = output.length();
            new PostVersionedIOReadableWritable() {
                @Override
                public int getVersion() {
                    return 2;
                }

                @Override
                protected void read(DataInputView input, boolean wasVersioned) {
                    throw new UnsupportedOperationException();
                }
            }.write(output);
            output.writeInt(1);
            output.writeUTF("service");
            java.util.Set<TimerHeapInternalTimer<Integer, VoidNamespace>> event =
                    new java.util.HashSet<>();
            java.util.Set<TimerHeapInternalTimer<Integer, VoidNamespace>> processing =
                    new java.util.HashSet<>();
            if (group == 0) {
                event.add(new TimerHeapInternalTimer<>(10L, 7, VoidNamespace.INSTANCE));
                event.add(new TimerHeapInternalTimer<>(20L, 7, VoidNamespace.INSTANCE));
            } else {
                processing.add(new TimerHeapInternalTimer<>(60L, 7, VoidNamespace.INSTANCE));
            }
            InternalTimersSnapshot<Integer, VoidNamespace> snapshot =
                    new InternalTimersSnapshot<>(
                            IntSerializer.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            event,
                            processing);
            InternalTimersSnapshotReaderWriters.getWriterForVersion(
                            2, snapshot, IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE)
                    .writeTimersSnapshot(output);
        }
        return new KeyGroupsStateHandle(
                new KeyGroupRangeOffsets(0, 1, offsets),
                new ByteStreamStateHandle("legacy-timers", output.getCopyOfBuffer()));
    }

    private Config nativeConfig(java.nio.file.Path volume, int totalBuckets) {
        Config config = new Config().numColumns(1).totalBuckets(totalBuckets);
        config.governanceMode = Config.GovernanceMode.NOOP;
        config.logConsole = false;
        config.addVolume(volume.toString());
        return config;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String utf8(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private static final class ReadableCheckpoint implements AutoCloseable {
        private final java.nio.file.Path directory;
        private final Db db;

        private ReadableCheckpoint(java.nio.file.Path directory, Db db) {
            this.directory = directory;
            this.db = db;
        }

        @Override
        public void close() {
            db.close();
        }
    }
}
