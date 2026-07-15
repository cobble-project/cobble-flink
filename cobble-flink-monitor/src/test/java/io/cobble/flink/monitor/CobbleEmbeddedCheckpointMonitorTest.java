package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import io.cobble.flink.inspect.internal.*;
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
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.UUID;

/** Catalog and embedded-schema coverage for a direct Flink metadata monitor target. */
class CobbleEmbeddedCheckpointMonitorTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void discoversEmbeddedCheckpointCatalogAndUsesEmbeddedSchema() throws Exception {
        OperatorID operatorId = new OperatorID(11L, 13L);
        ReadableSavepoint fixture = writeSavepoint("chk-3", operatorId);
        java.nio.file.Path savepoint = fixture.directory;
        CobbleFlinkMonitorServer.CheckpointCatalog catalog =
                CobbleFlinkMonitorServer.CheckpointCatalog.discover(
                        savepoint.toUri().toString(), CobbleConnectorStorageOptions.empty());

        CheckpointEntry checkpoint = catalog.checkpoints.get(0);
        OperatorEntry operator = checkpoint.findOperator(operatorId.toHexString());
        SchemaResolveResult schema = MonitorInspectSchemaResolver.resolve(checkpoint, operator);
        assertEquals("checkpoint", catalog.sourceKind);
        assertEquals(3L, checkpoint.id);
        assertEquals(SchemaResolveResult.STATUS_AVAILABLE, schema.status);
        assertEquals(1, schema.store.schemas().size());
        try {
            ServerConfig config =
                    ServerConfig.parse(
                            new String[] {
                                "--checkpoint", savepoint.toUri().toString(), "--total-buckets", "4"
                            });
            Class<?> state =
                    Class.forName("io.cobble.flink.monitor.CobbleFlinkMonitorServer$MonitorState");
            Method open =
                    state.getDeclaredMethod(
                            "openEmbeddedCheckpointReader",
                            ServerConfig.class,
                            CheckpointEntry.class,
                            OperatorEntry.class);
            open.setAccessible(true);
            ReaderHandle handle = (ReaderHandle) open.invoke(null, config, checkpoint, operator);
            try {
                assertEquals(
                        "value",
                        new String(handle.reader.get(0, "key".getBytes("UTF-8"), 0), "UTF-8"));
            } finally {
                handle.reader.close();
                Method cleanup =
                        CobbleFlinkMonitorServer.class.getDeclaredMethod(
                                "deleteTemporaryDirectories", java.util.List.class);
                cleanup.setAccessible(true);
                cleanup.invoke(null, handle.temporaryDirectories);
                for (java.io.File directory : handle.temporaryDirectories) {
                    assertFalse(directory.exists());
                }
            }
        } finally {
            fixture.close();
        }
    }

    @Test
    void globalEntryFallsBackToEmbeddedSchemaWhenRegistryIsAbsent() throws Exception {
        OperatorID operatorId = new OperatorID(21L, 23L);
        ReadableSavepoint fixture = writeSavepoint("chk-3", operatorId);
        try {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, false);
            OperatorEntry operator = merged.findOperator(operatorId.toHexString());

            SchemaResolveResult result = MonitorInspectSchemaResolver.resolve(merged, operator);

            assertEquals(SchemaResolveResult.STATUS_AVAILABLE, result.status);
            assertEquals("embedded Flink checkpoint metadata", result.eventPath);
            assertEquals(1, result.store.schemas().size());
            assertTrue(result.warning.contains("Schema registry missing"));
        } finally {
            fixture.close();
        }
    }

    @Test
    void globalRegistryRemainsPreferredOverEmbeddedSchema() throws Exception {
        OperatorID operatorId = new OperatorID(25L, 27L);
        ReadableSavepoint fixture = writeSavepoint("chk-3", operatorId);
        try {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, false);
            StateInspectSchemaStore registryStore =
                    CobbleEmbeddedCheckpoint.select(new Path(fixture.directory.toUri()), "3")
                            .checkpoint()
                            .operator(operatorId.toHexString())
                            .schemaStore();
            writeRegistry(tempDir, operatorId.toHexString(), 3L, registryStore);

            SchemaResolveResult result =
                    MonitorInspectSchemaResolver.resolve(
                            merged, merged.findOperator(operatorId.toHexString()));

            assertEquals(SchemaResolveResult.STATUS_AVAILABLE, result.status);
            assertTrue(result.eventPath.contains("inspect-schema/events"));
            assertEquals(3L, result.schemaCheckpointId);
            assertEquals(null, result.warning);
        } finally {
            fixture.close();
        }
    }

    @Test
    void staleRegistrySchemaIsReplacedByExactEmbeddedSchema() throws Exception {
        OperatorID operatorId = new OperatorID(26L, 28L);
        ReadableSavepoint fixture = writeSavepoint("chk-3", operatorId);
        try {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, false);
            StateInspectSchemaStore staleStore =
                    new StateInspectSchemaStore(
                            Collections.singletonList(
                                    StateInspectSchema.forValue(
                                            "stale",
                                            "default",
                                            false,
                                            IntSerializer.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            IntSerializer.INSTANCE)));
            writeRegistry(tempDir, operatorId.toHexString(), 2L, staleStore);

            SchemaResolveResult result =
                    MonitorInspectSchemaResolver.resolve(
                            merged, merged.findOperator(operatorId.toHexString()));

            assertEquals(SchemaResolveResult.STATUS_AVAILABLE, result.status);
            assertEquals("embedded Flink checkpoint metadata", result.eventPath);
            assertEquals("state", result.store.schemas().get(0).stateName());
            assertEquals(3L, result.schemaCheckpointId);
            assertTrue(result.warning.contains("stale or mismatched"));
        } finally {
            fixture.close();
        }
    }

    @Test
    void globalReaderRemainsPreferredWhenEmbeddedMetadataIsAlsoPresent() throws Exception {
        OperatorID operatorId = new OperatorID(29L, 31L);
        ReadableSavepoint fixture = writeSavepoint("chk-3", operatorId);
        try {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, true);
            CobbleFlinkMonitorServer.CheckpointCatalog catalog =
                    CobbleFlinkMonitorServer.CheckpointCatalog.discover(
                            fixture.directory.toUri().toString(),
                            CobbleConnectorStorageOptions.empty());
            ReaderHandle handle = openReader(catalog, merged, operatorId.toHexString());
            try {
                assertTrue(
                        handle.temporaryDirectories
                                .get(0)
                                .getName()
                                .startsWith("cobble-flink-monitor-"));
                assertEquals(
                        "value",
                        new String(handle.reader.get(0, "key".getBytes("UTF-8"), 0), "UTF-8"));
            } finally {
                closeReader(handle);
            }
        } finally {
            fixture.close();
        }
    }

    @Test
    void embeddedReaderIsUsedWhenTheGlobalReaderCannotOpen() throws Exception {
        OperatorID operatorId = new OperatorID(33L, 35L);
        ReadableSavepoint fixture = writeSavepoint("chk-3", operatorId);
        try {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, false);
            CobbleFlinkMonitorServer.CheckpointCatalog catalog =
                    CobbleFlinkMonitorServer.CheckpointCatalog.discover(
                            fixture.directory.toUri().toString(),
                            CobbleConnectorStorageOptions.empty());
            ReaderHandle handle = openReader(catalog, merged, operatorId.toHexString());
            try {
                assertTrue(
                        handle.temporaryDirectories
                                .get(0)
                                .getName()
                                .startsWith("cobble-flink-embedded-checkpoint-"));
                assertEquals(
                        "value",
                        new String(handle.reader.get(0, "key".getBytes("UTF-8"), 0), "UTF-8"));
            } finally {
                closeReader(handle);
            }
        } finally {
            fixture.close();
        }
    }

    @Test
    void directMetadataEntryKeepsGlobalSidecarAndEmbeddedFallback() throws Exception {
        OperatorID operatorId = new OperatorID(37L, 39L);
        ReadableSavepoint fixture = writeSavepoint("chk-3", operatorId);
        try {
            mergedGlobalEntry(fixture, operatorId, true);
            CobbleFlinkMonitorServer.CheckpointCatalog catalog =
                    CobbleFlinkMonitorServer.CheckpointCatalog.discover(
                            fixture.directory.resolve("_metadata").toUri().toString(),
                            CobbleConnectorStorageOptions.empty());

            OperatorEntry operator =
                    catalog.checkpoints.get(0).findOperator(operatorId.toHexString());
            assertTrue(operator.globalSnapshotLayout);
            assertTrue(operator.embeddedCheckpoint != null);
            assertTrue(operator.manifestCopyPath != null);
        } finally {
            fixture.close();
        }
    }

    @Test
    void directMetadataEntryWithoutSidecarRemainsEmbeddedOnly() throws Exception {
        OperatorID operatorId = new OperatorID(41L, 43L);
        ReadableSavepoint fixture = writeSavepoint("chk-3", operatorId);
        try {
            CobbleFlinkMonitorServer.CheckpointCatalog catalog =
                    CobbleFlinkMonitorServer.CheckpointCatalog.discover(
                            fixture.directory.resolve("_metadata").toUri().toString(),
                            CobbleConnectorStorageOptions.empty());

            OperatorEntry operator =
                    catalog.checkpoints.get(0).findOperator(operatorId.toHexString());
            assertFalse(operator.globalSnapshotLayout);
            assertTrue(operator.embeddedCheckpoint != null);
        } finally {
            fixture.close();
        }
    }

    private CheckpointEntry mergedGlobalEntry(
            ReadableSavepoint fixture, OperatorID operatorId, boolean materializeGlobalSnapshot)
            throws Exception {
        CobbleEmbeddedCheckpoint.Location location =
                CobbleEmbeddedCheckpoint.select(new Path(fixture.directory.toUri()), "3");
        String operatorDirectory =
                tempDir.resolve("cobble").resolve(operatorId.toHexString()).toUri().toString();
        if (materializeGlobalSnapshot) {
            Config coordinatorConfig = new Config().totalBuckets(4);
            coordinatorConfig.governanceMode = Config.GovernanceMode.NOOP;
            coordinatorConfig.logConsole = false;
            coordinatorConfig.addVolume(new Path(operatorDirectory).getPath());
            try (DbCoordinator coordinator = DbCoordinator.open(coordinatorConfig)) {
                coordinator.materializeGlobalSnapshot(
                        4, 3L, Collections.singletonList(fixture.db.snapshot()));
            }
            Files.copy(
                    tempDir.resolve("cobble")
                            .resolve(operatorId.toHexString())
                            .resolve("snapshot")
                            .resolve("SNAPSHOT-3"),
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
        return CobbleFlinkMonitorServer.CheckpointCatalog.mergeEmbeddedCheckpoint(
                sidecar, location);
    }

    private ReaderHandle openReader(
            CobbleFlinkMonitorServer.CheckpointCatalog catalog,
            CheckpointEntry checkpoint,
            String operatorId)
            throws Exception {
        ServerConfig config =
                ServerConfig.parse(
                        new String[] {
                            "--checkpoint", tempDir.toUri().toString(), "--total-buckets", "4"
                        });
        Class<?> state =
                Class.forName("io.cobble.flink.monitor.CobbleFlinkMonitorServer$MonitorState");
        Method open =
                state.getDeclaredMethod(
                        "openReader",
                        ServerConfig.class,
                        CobbleFlinkMonitorServer.CheckpointCatalog.class,
                        CheckpointEntry.class,
                        OperatorEntry.class);
        open.setAccessible(true);
        return (ReaderHandle)
                open.invoke(null, config, catalog, checkpoint, checkpoint.findOperator(operatorId));
    }

    private void closeReader(ReaderHandle handle) throws Exception {
        handle.reader.close();
        Method cleanup =
                CobbleFlinkMonitorServer.class.getDeclaredMethod(
                        "deleteTemporaryDirectories", java.util.List.class);
        cleanup.setAccessible(true);
        cleanup.invoke(null, handle.temporaryDirectories);
        for (java.io.File directory : handle.temporaryDirectories) {
            assertFalse(directory.exists());
        }
    }

    private void writeRegistry(
            java.nio.file.Path root,
            String operatorId,
            long checkpointId,
            StateInspectSchemaStore store)
            throws Exception {
        byte[] bytes = store.toBytes();
        String hash = InspectSchemaRegistryLayout.sha256(bytes);
        java.nio.file.Path schema =
                root.resolve("cobble").resolve(operatorId).resolve("inspect-schema");
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

    private ReadableSavepoint writeSavepoint(String directoryName, OperatorID operatorId)
            throws Exception {
        java.nio.file.Path directory = Files.createDirectory(tempDir.resolve(directoryName));
        java.nio.file.Path volume = Files.createDirectory(directory.resolve("volume"));
        Config config = new Config().numColumns(1).totalBuckets(4);
        config.governanceMode = Config.GovernanceMode.NOOP;
        config.logConsole = false;
        config.addVolume(volume.toString());
        Db db = Db.open(config, 0, 3);
        db.put(0, "key".getBytes("UTF-8"), 0, "value".getBytes("UTF-8"));
        ShardSnapshot shard = db.asyncSnapshot().get();
        db.retainSnapshot(shard.snapshotId);
        java.nio.file.Path state = directory.resolve("task-state");
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
        return new ReadableSavepoint(directory, db);
    }

    private static final class ReadableSavepoint implements AutoCloseable {
        private final java.nio.file.Path directory;
        private final Db db;

        private ReadableSavepoint(java.nio.file.Path directory, Db db) {
            this.directory = directory;
            this.db = db;
        }

        @Override
        public void close() {
            db.close();
        }
    }
}
