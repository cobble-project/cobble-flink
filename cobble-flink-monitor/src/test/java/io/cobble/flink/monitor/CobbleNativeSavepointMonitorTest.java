package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.cobble.Config;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleSnapshotMetadataCodec;
import io.cobble.flink.common.CobbleSnapshotMetadataPayload;
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
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.UUID;

/** Catalog and embedded-schema coverage for a direct NATIVE savepoint monitor target. */
class CobbleNativeSavepointMonitorTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void discoversNativeCatalogAndUsesEmbeddedSchema() throws Exception {
        OperatorID operatorId = new OperatorID(11L, 13L);
        ReadableSavepoint fixture = writeSavepoint("chk-3", operatorId);
        java.nio.file.Path savepoint = fixture.directory;
        CobbleFlinkMonitorServer.CheckpointCatalog catalog =
                CobbleFlinkMonitorServer.CheckpointCatalog.discover(
                        savepoint.toUri().toString(), CobbleConnectorStorageOptions.empty());

        CheckpointEntry checkpoint = catalog.checkpoints.get(0);
        OperatorEntry operator = checkpoint.findOperator(operatorId.toHexString());
        SchemaResolveResult schema = MonitorInspectSchemaResolver.resolve(checkpoint, operator);
        assertEquals("native_savepoint", catalog.sourceKind);
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
                            "openNativeSavepointReader",
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
