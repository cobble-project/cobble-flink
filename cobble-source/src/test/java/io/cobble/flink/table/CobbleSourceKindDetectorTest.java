package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;

import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

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
    void autoDetectsCheckpointRootFromChkMetadataAndCobbleManifest() throws Exception {
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
    void explicitSinkAcceptsSinkRoot() throws Exception {
        Path root = sinkRoot("explicit-sink-on-sink");

        CobbleResolvedSource resolved =
                CobbleSourceKindDetector.detect(uri(root), CobbleSourceKind.SINK, SINK_SHAPED);

        assertEquals(CobbleSourceKind.SINK, resolved.kind());
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
        Path chk = root.resolve("chk-5");
        write(chk.resolve("_metadata"), new byte[] {0});
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
