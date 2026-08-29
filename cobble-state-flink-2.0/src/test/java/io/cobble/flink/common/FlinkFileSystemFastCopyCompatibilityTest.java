package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.CustomFileSystem;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.ICloseableRegistry;
import org.apache.flink.core.fs.PathsCopyingFileSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

class FlinkFileSystemFastCopyCompatibilityTest {

    @TempDir Path tempDir;

    @BeforeAll
    static void registerResolver() {
        FlinkPathsCopyingFileSystemFastCopyResolver.register();
    }

    @Test
    void delegatesToFlinkTwoPathsCopyingFilesystem() throws Exception {
        Path sourceRoot = Files.createDirectory(tempDir.resolve("source"));
        Path destinationRoot = Files.createDirectory(tempDir.resolve("destination"));
        Files.write(sourceRoot.resolve("source.bin"), new byte[] {9, 8, 7});

        CustomFileSystem source =
                new CobbleFlinkFileSystems.FlinkCustomFileSystem(
                        FileSystem.getLocalFileSystem(),
                        new org.apache.flink.core.fs.Path(sourceRoot.toUri()));
        CustomFileSystem destination =
                new CobbleFlinkFileSystems.FlinkCustomFileSystem(
                        new FastCopyLocalFileSystem(),
                        new org.apache.flink.core.fs.Path(destinationRoot.toUri()));

        assertTrue(source.canFastCopyTo("source.bin", destination, "copied.bin"));
        source.fastCopyTo("source.bin", destination, "copied.bin");
        assertArrayEquals(
                new byte[] {9, 8, 7}, Files.readAllBytes(destinationRoot.resolve("copied.bin")));
    }

    private static final class FastCopyLocalFileSystem
            extends org.apache.flink.core.fs.local.LocalFileSystem
            implements PathsCopyingFileSystem {

        @Override
        public boolean canCopyPaths(
                org.apache.flink.core.fs.Path source, org.apache.flink.core.fs.Path destination) {
            return true;
        }

        @Override
        public void copyFiles(List<CopyRequest> requests, ICloseableRegistry closeableRegistry)
                throws IOException {
            for (CopyRequest request : requests) {
                Files.copy(
                        java.nio.file.Paths.get(request.getSource().toUri()),
                        java.nio.file.Paths.get(request.getDestination().toUri()),
                        StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }
}
