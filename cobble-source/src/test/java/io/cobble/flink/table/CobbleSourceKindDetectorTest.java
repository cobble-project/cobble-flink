package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleSnapshotMetadataCodec;
import io.cobble.flink.common.CobbleSnapshotMetadataPayload;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;

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
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;

/** Tests for {@link CobbleSourceKindDetector} layout probing and explicit-kind handling. */
class CobbleSourceKindDetectorTest {

    private static final boolean SINK_SHAPED = true;
    private static final boolean NOT_SINK_SHAPED = false;

    @TempDir private Path tempDir;

    @Test
    void autoDetectsSinkRootFromSinkInspectSchema() throws Exception {
        Path root = sinkRoot("sink-root");

        CobbleResolvedSource resolved =
                CobbleSourceKindDetector.detect(uri(root), CobbleSourceKind.AUTO, NOT_SINK_SHAPED);

        assertEquals(CobbleSourceKind.SINK, resolved.kind());
    }

    @Test
    void autoDetectsSidecarCheckpointRootWhenEmbeddedMetadataIsUnreadable() throws Exception {
        Path root = checkpointRoot("checkpoint-root");

        CobbleResolvedSource resolved =
                CobbleSourceKindDetector.detect(uri(root), CobbleSourceKind.AUTO, NOT_SINK_SHAPED);

        assertEquals(CobbleSourceKind.STATE, resolved.kind());
        assertEquals(StateSourceConfig.Layout.CHECKPOINT_ROOT, resolved.stateConfig().layout());
    }

    @Test
    void autoCheckpointRootIsStateEvenWithSinkShapedDdl() throws Exception {
        Path root = checkpointRoot("checkpoint-root-sink-shaped");

        CobbleResolvedSource resolved =
                CobbleSourceKindDetector.detect(uri(root), CobbleSourceKind.AUTO, SINK_SHAPED);

        assertEquals(CobbleSourceKind.STATE, resolved.kind());
        assertEquals(StateSourceConfig.Layout.CHECKPOINT_ROOT, resolved.stateConfig().layout());
    }

    @Test
    void embeddedCheckpointTakesPriorityOverManifestSidecar() throws Exception {
        Path root = embeddedCheckpointRoot("embedded-checkpoint-root");

        CobbleResolvedSource resolved =
                CobbleSourceKindDetector.detect(uri(root), CobbleSourceKind.AUTO, NOT_SINK_SHAPED);

        assertEquals(CobbleSourceKind.STATE, resolved.kind());
        assertEquals(StateSourceConfig.Layout.EMBEDDED_CHECKPOINT, resolved.stateConfig().layout());
    }

    @Test
    void autoDetectsStateRootFromStateInspectSchema() throws Exception {
        Path root = stateOperatorRoot("state-operator-root");

        CobbleResolvedSource resolved =
                CobbleSourceKindDetector.detect(uri(root), CobbleSourceKind.AUTO, NOT_SINK_SHAPED);

        assertEquals(CobbleSourceKind.STATE, resolved.kind());
        assertEquals(StateSourceConfig.Layout.OPERATOR_ROOT, resolved.stateConfig().layout());
    }

    @Test
    void autoDoesNotTreatSnapshotCurrentAloneAsSinkWithoutSinkShapedDdl() throws Exception {
        Path root = ambiguousRoot("ambiguous-root");

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                CobbleSourceKindDetector.detect(
                                        uri(root), CobbleSourceKind.AUTO, NOT_SINK_SHAPED));
        assertTrue(
                error.getMessage().contains("Unable to auto-detect"),
                "expected ambiguous message but got: " + error.getMessage());
    }

    @Test
    void autoFallsBackToSinkForSinkShapedDdlOnAmbiguousRoot() throws Exception {
        Path root = ambiguousRoot("ambiguous-sink-shaped");

        CobbleResolvedSource resolved =
                CobbleSourceKindDetector.detect(uri(root), CobbleSourceKind.AUTO, SINK_SHAPED);

        assertEquals(CobbleSourceKind.SINK, resolved.kind());
    }

    @Test
    void autoUnknownPathFailsEvenWithSinkShapedDdl() throws Exception {
        Path root = tempDir.resolve("empty-unknown");
        Files.createDirectories(root);

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                CobbleSourceKindDetector.detect(
                                        uri(root), CobbleSourceKind.AUTO, SINK_SHAPED));
        assertTrue(
                error.getMessage().contains("Unable to auto-detect"),
                "expected ambiguous message but got: " + error.getMessage());
    }

    @Test
    void explicitSinkRejectsStateRoot() throws Exception {
        Path root = checkpointRoot("explicit-sink-on-state");

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                CobbleSourceKindDetector.detect(
                                        uri(root), CobbleSourceKind.SINK, SINK_SHAPED));
        assertTrue(
                error.getMessage().contains("source.kind='sink' was requested"),
                "expected sink mismatch message but got: " + error.getMessage());
    }

    @Test
    void explicitStateRejectsSinkRoot() throws Exception {
        Path root = sinkRoot("explicit-state-on-sink");

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                CobbleSourceKindDetector.detect(
                                        uri(root), CobbleSourceKind.STATE, NOT_SINK_SHAPED));
        assertTrue(
                error.getMessage().contains("source.kind='state' was requested"),
                "expected state mismatch message but got: " + error.getMessage());
    }

    @Test
    void missingPathFailsClearly() {
        Path root = tempDir.resolve("does-not-exist");

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                CobbleSourceKindDetector.detect(
                                        uri(root), CobbleSourceKind.AUTO, SINK_SHAPED));
        assertTrue(
                error.getMessage().contains("does not exist"),
                "expected missing path message but got: " + error.getMessage());
    }

    @Test
    void stateOperatorRootExplainsCheckpointContextRequirement() throws Exception {
        Path root = stateOperatorRoot("operator-root-diagnostics");

        CobbleResolvedSource resolved =
                CobbleSourceKindDetector.detect(uri(root), CobbleSourceKind.AUTO, NOT_SINK_SHAPED);

        assertEquals(StateSourceConfig.Layout.OPERATOR_ROOT, resolved.stateConfig().layout());
        assertTrue(
                resolved.diagnostics().contains("checkpoint root"),
                "expected checkpoint-root guidance but got: " + resolved.diagnostics());
    }

    @Test
    void explicitStateAcceptsCheckpointRoot() throws Exception {
        Path root = checkpointRoot("explicit-state-on-checkpoint");

        CobbleResolvedSource resolved =
                CobbleSourceKindDetector.detect(uri(root), CobbleSourceKind.STATE, NOT_SINK_SHAPED);

        assertEquals(CobbleSourceKind.STATE, resolved.kind());
        assertEquals(StateSourceConfig.Layout.CHECKPOINT_ROOT, resolved.stateConfig().layout());
    }

    @Test
    void explicitRemoteStateOperatorUsesFlinkFilesystemProbe() throws Exception {
        Path physicalRoot = stateOperatorRoot("remote-state-operator-probe");
        org.apache.flink.core.fs.Path flinkRoot =
                new org.apache.flink.core.fs.Path(physicalRoot.toUri());
        CobbleSourceKindDetector.Probe probe =
                CobbleSourceKindDetector.probeStatePath(
                        flinkRoot.getFileSystem(),
                        flinkRoot,
                        "hdfs://namenode:8020/checkpoints/operator");

        CobbleResolvedSource resolved =
                CobbleSourceKindDetector.resolveExplicitState(
                        "hdfs://namenode:8020/checkpoints/operator", probe);

        assertEquals(CobbleSourceKindDetector.Probe.STATE_OPERATOR, probe);
        assertEquals(CobbleSourceKind.STATE, resolved.kind());
        assertEquals(StateSourceConfig.Layout.OPERATOR_ROOT, resolved.stateConfig().layout());
        assertEquals("hdfs://namenode:8020/checkpoints/operator", resolved.stateConfig().pathUri());
    }

    @Test
    void explicitSinkAcceptsSinkRoot() throws Exception {
        Path root = sinkRoot("explicit-sink-on-sink");

        CobbleResolvedSource resolved =
                CobbleSourceKindDetector.detect(uri(root), CobbleSourceKind.SINK, SINK_SHAPED);

        assertEquals(CobbleSourceKind.SINK, resolved.kind());
    }

    // ------------------------------------------------------------------------------------------
    //  Raw source detector tests
    // ------------------------------------------------------------------------------------------

    @Test
    void explicitRawAcceptsTableRootFromSinkSignal() throws Exception {
        Path root = sinkRoot("raw-on-sink-root");

        CobbleResolvedSource resolved =
                CobbleSourceKindDetector.detect(uri(root), CobbleSourceKind.RAW, NOT_SINK_SHAPED);

        assertEquals(CobbleSourceKind.RAW, resolved.kind());
    }

    @Test
    void explicitRawAcceptsAmbiguousRoot() throws Exception {
        Path root = ambiguousRoot("raw-on-ambiguous-root");

        CobbleResolvedSource resolved =
                CobbleSourceKindDetector.detect(uri(root), CobbleSourceKind.RAW, NOT_SINK_SHAPED);

        assertEquals(CobbleSourceKind.RAW, resolved.kind());
    }

    @Test
    void explicitRawRejectsCheckpointRoot() throws Exception {
        Path root = checkpointRoot("raw-on-checkpoint-root");

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                CobbleSourceKindDetector.detect(
                                        uri(root), CobbleSourceKind.RAW, NOT_SINK_SHAPED));
        assertTrue(
                error.getMessage().contains("source.kind='raw' expects a Cobble table root"),
                "expected raw-on-checkpoint rejection but got: " + error.getMessage());
    }

    @Test
    void explicitRawRejectsStateOperatorRoot() throws Exception {
        Path root = stateOperatorRoot("raw-on-state-operator-root");

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                CobbleSourceKindDetector.detect(
                                        uri(root), CobbleSourceKind.RAW, NOT_SINK_SHAPED));
        assertTrue(
                error.getMessage().contains("source.kind='raw' expects a Cobble table root"),
                "expected raw-on-state-operator rejection but got: " + error.getMessage());
    }

    @Test
    void explicitRawRejectsUnknownPath() throws Exception {
        Path root = tempDir.resolve("raw-on-unknown");
        Files.createDirectories(root);

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                CobbleSourceKindDetector.detect(
                                        uri(root), CobbleSourceKind.RAW, NOT_SINK_SHAPED));
        assertTrue(
                error.getMessage().contains("does not appear to be a Cobble table root"),
                "expected unknown-path rejection but got: " + error.getMessage());
    }

    @Test
    void autoNeverSelectsRawEvenOnAmbiguousRoot() throws Exception {
        Path root = ambiguousRoot("auto-never-raw");

        // With a sink-shaped DDL, auto falls back to sink — never raw.
        CobbleResolvedSource resolvedSinkShaped =
                CobbleSourceKindDetector.detect(uri(root), CobbleSourceKind.AUTO, SINK_SHAPED);
        assertEquals(CobbleSourceKind.SINK, resolvedSinkShaped.kind());

        // Without a sink-shaped DDL, auto fails — never falls back to raw.
        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                CobbleSourceKindDetector.detect(
                                        uri(root), CobbleSourceKind.AUTO, NOT_SINK_SHAPED));
        assertTrue(
                error.getMessage().contains("Unable to auto-detect"),
                "expected ambiguous failure, not raw fallback: " + error.getMessage());
    }

    // ------------------------------------------------------------------------------------------
    //  Fixtures
    // ------------------------------------------------------------------------------------------

    private Path sinkRoot(String name) throws Exception {
        Path root = tempDir.resolve(name);
        byte[] blob = new SinkInspectSchemaStore(null).toBytes();
        writeInspectSchemaBlob(root, blob);
        // Secondary sink signal; on its own it must not be enough, but it should not break sink
        // detection either.
        write(root.resolve("snapshot").resolve("CURRENT"), new byte[] {1});
        return root;
    }

    private Path ambiguousRoot(String name) throws Exception {
        Path root = tempDir.resolve(name);
        // Only a weak sink signal: snapshot/CURRENT, no inspect-schema sidecar, no chk-*.
        write(root.resolve("snapshot").resolve("CURRENT"), new byte[] {1, 2, 3});
        return root;
    }

    private Path stateOperatorRoot(String name) throws Exception {
        Path root = tempDir.resolve(name);
        byte[] blob = StateInspectSchemaStore.empty().toBytes();
        writeInspectSchemaBlob(root, blob);
        return root;
    }

    private Path checkpointRoot(String name) throws Exception {
        Path root = tempDir.resolve(name);
        Path checkpoint = root.resolve("chk-5");
        write(checkpoint.resolve("_metadata"), new byte[] {0});
        write(checkpoint.resolve("COBBLE-SNAPSHOT-operator-1-MANIFEST"), new byte[] {0});
        return root;
    }

    private Path embeddedCheckpointRoot(String name) throws Exception {
        Path root = tempDir.resolve(name);
        Path chk = root.resolve("chk-5");
        Path state = chk.resolve("task-state");
        ShardSnapshot shard = new ShardSnapshot();
        shard.dbId = "db";
        shard.snapshotId = 5L;
        shard.manifestPath = chk.resolve("volume/snapshot/SNAPSHOT-5").toUri().toString();
        ShardSnapshot.Range range = new ShardSnapshot.Range();
        range.start = 0;
        range.end = 3;
        shard.ranges.add(range);
        Files.createDirectories(chk);
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(state))) {
            CobbleSnapshotMetadataCodec.write(
                    new CobbleSnapshotMetadataPayload(
                            shard, false, StateInspectSchemaStore.empty()),
                    new DataOutputViewStreamWrapper(output));
        }
        OperatorState operator = new OperatorState(new OperatorID(1L, 2L), 1, 4);
        operator.putState(
                0,
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(
                                StateObjectCollection.<KeyedStateHandle>singleton(
                                        new IncrementalRemoteKeyedStateHandle(
                                                UUID.randomUUID(),
                                                new KeyGroupRange(0, 3),
                                                5L,
                                                Collections.emptyList(),
                                                Collections.emptyList(),
                                                new FileStateHandle(
                                                        new org.apache.flink.core.fs.Path(
                                                                state.toUri()),
                                                        Files.size(state)))))
                        .build());
        try (DataOutputStream output =
                new DataOutputStream(Files.newOutputStream(chk.resolve("_metadata")))) {
            Checkpoints.storeCheckpointMetadata(
                    new CheckpointMetadata(
                            5L, Collections.singletonList(operator), Collections.emptyList()),
                    output);
        }
        write(chk.resolve("COBBLE-SNAPSHOT-operator-1-MANIFEST"), new byte[] {0});
        return root;
    }

    private void writeInspectSchemaBlob(Path root, byte[] blob) throws Exception {
        String hash = InspectSchemaRegistryLayout.sha256(blob);
        Path blobPath =
                root.resolve("inspect-schema")
                        .resolve("blobs")
                        .resolve(InspectSchemaRegistryLayout.blobFileName(hash));
        write(blobPath, blob);
    }

    private static void write(Path path, byte[] bytes) throws Exception {
        Files.createDirectories(path.getParent());
        Files.write(path, bytes);
    }

    private static String uri(Path path) {
        return path.toUri().toString();
    }
}
