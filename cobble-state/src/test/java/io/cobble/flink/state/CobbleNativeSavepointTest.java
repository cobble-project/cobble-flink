package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.*;

import io.cobble.Config;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleFlinkStorageConfig;
import io.cobble.structured.Db;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

class CobbleNativeSavepointTest {
    @Test
    void nativeDirectoryPreservesRemoteSchemesAndPathEscaping() throws Exception {
        assertEquals("/tmp/save point", nativeDirectory("file:/tmp/save%20point"));
        assertEquals("/tmp/save point", nativeDirectory("/tmp/save%20point"));
        assertEquals("custom:///tmp/save%20point", nativeDirectory("custom:/tmp/save%20point"));
        assertEquals(
                "custom://authority/tmp/save%20point",
                nativeDirectory("custom://authority/tmp/save%20point"));
        assertEquals(
                "custom:///tmp/literal%2520/escaped%20space",
                nativeDirectory("custom:/tmp/literal%2520/escaped%20space"));
        org.apache.flink.core.fs.Path encodedSlash =
                new org.apache.flink.core.fs.Path(URI.create("custom:/tmp/encoded%2Fslash"));
        assertEquals(
                encodedSlash.toUri().getPath(),
                URI.create(CobbleFlinkStorageConfig.nativePath(encodedSlash.toUri())).getPath());
    }

    private static String nativeDirectory(String uri) throws Exception {
        return CobbleFlinkStorageConfig.nativePath(
                new org.apache.flink.core.fs.Path(URI.create(uri)).toUri());
    }

    @Test
    void unpublishedExportIsRemovedWithoutDeletingItsSourceOrSibling(@TempDir Path root)
            throws Exception {
        verifyOwnership(root, false);
    }

    @Test
    void publishedExportIsNotRemovedByRegistryOrResourceClose(@TempDir Path root) throws Exception {
        verifyOwnership(root, true);
    }

    private static void verifyOwnership(Path root, boolean publish) throws Exception {
        Config config =
                new Config()
                        .addVolume(root.resolve("source").toString())
                        .numColumns(1)
                        .totalBuckets(1);
        config.jniDirectBufferSize = 8192;
        config.jniDirectBufferPoolSize = 128;
        try (Db db = Db.open(config)) {
            db.put(0, new byte[] {1}, 0, new byte[] {2});
            ShardSnapshot source = db.snapshot();
            Path savepoint = root.resolve("savepoint");
            Path sibling = savepoint.resolve("cobble/sibling");
            Files.createDirectories(sibling);
            CheckpointOptions options =
                    CheckpointOptions.alignedNoTimeout(
                            SavepointType.savepoint(SavepointFormatType.NATIVE),
                            AbstractFsCheckpointStorageAccess.encodePathAsReference(
                                    new org.apache.flink.core.fs.Path(savepoint.toUri())));
            CobbleNativeSavepoint export =
                    new CobbleNativeSavepoint(
                            db,
                            source.snapshotId,
                            options,
                            CobbleFlinkStorageConfig.empty().withRoutes(config));
            ShardSnapshot exported = export.await(30000);
            Path directory = savepoint.resolve("cobble").resolve(exported.dbId);
            assertTrue(Files.isDirectory(directory));
            if (publish) export.markPublished();
            export.close();
            export.close();
            assertEquals(publish, Files.exists(directory));
            assertTrue(Files.isDirectory(sibling));
            assertTrue(db.expireSnapshot(source.snapshotId));
            assertArrayEquals(new byte[] {2}, db.get(0, new byte[] {1}).getBytes(0));
            assertTrue(db.expireSnapshot(db.snapshot().snapshotId));
        }
    }
}
