package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cobble.Config;
import io.cobble.ShardSnapshot;
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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.UUID;

/** Native savepoint planning coverage using Flink's actual metadata serialization. */
class CobbleNativeSavepointSourceTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void detectsRenamedNativeSavepointAndResolvesEmbeddedSchema() throws Exception {
        OperatorID operatorId = new OperatorID(7L, 9L);
        java.nio.file.Path savepoint = writeNativeSavepoint("renamed-backup", operatorId);
        String uri = savepoint.toUri().toString();

        CobbleResolvedSource detected =
                CobbleSourceKindDetector.detect(uri, CobbleSourceKind.AUTO, false);
        assertEquals(StateSourceConfig.Layout.NATIVE_SAVEPOINT, detected.stateConfig().layout());

        org.apache.flink.configuration.Configuration options =
                new org.apache.flink.configuration.Configuration();
        options.set(CobbleSourceTableOptions.STATE_NAME, "state");
        options.set(CobbleSourceTableOptions.STATE_OPERATOR_ID, operatorId.toHexString());
        StateSourceResolvedSchema resolved =
                StateSourceSchemaResolver.resolve(
                        uri,
                        StateSourceConfig.Layout.NATIVE_SAVEPOINT,
                        StateSourceOptions.parseForState(options),
                        "3",
                        new ResolvedSchema(
                                Arrays.asList(
                                        Column.physical("key", DataTypes.INT()),
                                        Column.physical("value", DataTypes.INT())),
                                Collections.emptyList(),
                                null));
        assertEquals(operatorId.toHexString(), resolved.operatorId());
        assertEquals(3L, resolved.schemaCheckpointId());
    }

    @Test
    void doesNotTreatCanonicalOrNonCobbleMetadataAsNative() throws Exception {
        java.nio.file.Path directory = Files.createDirectory(tempDir.resolve("canonical-copy"));
        try (DataOutputStream output =
                new DataOutputStream(Files.newOutputStream(directory.resolve("_metadata")))) {
            Checkpoints.storeCheckpointMetadata(
                    new CheckpointMetadata(4L, Collections.emptyList(), Collections.emptyList()),
                    output);
        }
        org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(directory.toUri());
        assertEquals(
                CobbleSourceKindDetector.Probe.UNKNOWN,
                CobbleSourceKindDetector.probeStatePath(
                        path.getFileSystem(), path, directory.toUri().toString()));
    }

    @Test
    void opensReaderAgainstMaterializedNativeShard() throws Exception {
        OperatorID operatorId = new OperatorID(17L, 19L);
        ReadableSavepoint fixture = writeReadableNativeSavepoint(operatorId);
        java.nio.file.Path savepoint = fixture.directory;
        StateSourceConfig config =
                new StateSourceConfig(
                        savepoint.toUri().toString(),
                        StateSourceConfig.Layout.NATIVE_SAVEPOINT,
                        operatorId.toHexString(),
                        "state",
                        "value",
                        "latest",
                        "batch",
                        3L,
                        4,
                        0L,
                        Collections.emptyList());
        try {
            assertEquals(3L, CobbleStateSourceRuntime.resolveCheckpointId(config));
            try (CobbleStateSourceRuntime.ReaderHandle handle =
                    CobbleStateSourceRuntime.openReader(config, 3L)) {
                assertEquals(
                        "value",
                        new String(handle.reader.get(0, "key".getBytes("UTF-8"), 0), "UTF-8"));
            }
        } finally {
            fixture.close();
        }
    }

    private java.nio.file.Path writeNativeSavepoint(String directoryName, OperatorID operatorId)
            throws Exception {
        java.nio.file.Path directory = Files.createDirectory(tempDir.resolve(directoryName));
        java.nio.file.Path state = directory.resolve("task-state");
        ShardSnapshot shard = new ShardSnapshot();
        shard.dbId = "db";
        shard.snapshotId = 3L;
        shard.manifestPath = directory.resolve("volume/snapshot/SNAPSHOT-3").toUri().toString();
        ShardSnapshot.Range range = new ShardSnapshot.Range();
        range.start = 0;
        range.end = 3;
        shard.ranges.add(range);
        StateInspectSchema schema =
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
                                    Collections.singletonList(schema), semantic)),
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
        return directory;
    }

    private ReadableSavepoint writeReadableNativeSavepoint(OperatorID operatorId) throws Exception {
        java.nio.file.Path directory = Files.createDirectory(tempDir.resolve("readable-native"));
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
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(state))) {
            CobbleSnapshotMetadataCodec.write(
                    new CobbleSnapshotMetadataPayload(
                            shard, false, StateInspectSchemaStore.empty()),
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
