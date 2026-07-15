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
import io.cobble.structured.Db;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
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
    void discoversAndReadsEmbeddedCheckpointWithoutSidecars() throws Exception {
        OperatorID operatorId = new OperatorID(11L, 13L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            InspectCatalogDiscovery.Result catalog =
                    InspectCatalogDiscovery.discover(
                            fixture.directory.toUri().toString(),
                            CobbleConnectorStorageOptions.empty());
            CheckpointEntry checkpoint = catalog.checkpoints.get(0);
            OperatorEntry operator = checkpoint.findOperator(operatorId.toHexString());

            SchemaResolveResult schema = MonitorInspectSchemaResolver.resolve(checkpoint, operator);
            assertEquals("checkpoint", catalog.sourceKind);
            assertEquals(3L, checkpoint.id);
            assertEquals(SchemaResolveResult.STATUS_AVAILABLE, schema.status);
            assertEquals(1, schema.store.schemas().size());

            MonitorReaderSession reader =
                    MonitorReaderSession.open(
                            4,
                            CobbleConnectorStorageOptions.empty(),
                            catalog.sourceKind,
                            checkpoint,
                            operator);
            java.util.List<java.io.File> temporaryDirectories = reader.temporaryDirectories();
            assertTrue(
                    temporaryDirectories
                            .get(0)
                            .getName()
                            .startsWith("cobble-flink-embedded-checkpoint-"));
            assertEquals("value", utf8(reader.reader().get(0, utf8("key"), 0)));
            reader.close();
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
            MonitorReaderSession reader = openReader(merged, operatorId);
            java.util.List<java.io.File> temporaryDirectories = reader.temporaryDirectories();
            assertTrue(temporaryDirectories.get(0).getName().startsWith("cobble-flink-monitor-"));
            assertEquals("value", utf8(reader.reader().get(0, utf8("key"), 0)));
            reader.close();
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
            MonitorReaderSession reader = openReader(merged, operatorId);
            java.util.List<java.io.File> temporaryDirectories = reader.temporaryDirectories();
            assertTrue(
                    temporaryDirectories
                            .get(0)
                            .getName()
                            .startsWith("cobble-flink-embedded-checkpoint-"));
            assertEquals("value", utf8(reader.reader().get(0, utf8("key"), 0)));
            reader.close();
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
        java.nio.file.Path directory = Files.createDirectory(tempDir.resolve("chk-3"));
        java.nio.file.Path volume = Files.createDirectory(directory.resolve("volume"));
        Config config = nativeConfig(volume, 4);
        Db db = Db.open(config, 0, 3);
        db.put(0, utf8("key"), 0, utf8("value"));
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
        java.nio.file.Path state = directory.resolve("task-state");
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(state))) {
            CobbleSnapshotMetadataCodec.write(
                    new CobbleSnapshotMetadataPayload(
                            shard,
                            false,
                            new StateInspectSchemaStore(
                                    Collections.singletonList(stateSchema), semantic)),
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
        operator.putState(
                0,
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(
                                StateObjectCollection.<KeyedStateHandle>singleton(handle))
                        .build());
        try (DataOutputStream output =
                new DataOutputStream(Files.newOutputStream(directory.resolve("_metadata")))) {
            Checkpoints.storeCheckpointMetadata(
                    new CheckpointMetadata(
                            3L, Collections.singletonList(operator), Collections.emptyList()),
                    output);
        }
        return new ReadableCheckpoint(directory, db);
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
