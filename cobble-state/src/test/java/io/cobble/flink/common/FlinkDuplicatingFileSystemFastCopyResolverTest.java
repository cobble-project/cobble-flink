package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.CustomFileSystem;

import org.apache.flink.core.fs.DuplicatingFileSystem;
import org.apache.flink.core.fs.FileSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

class FlinkDuplicatingFileSystemFastCopyResolverTest {

    @TempDir Path tempDir;

    @BeforeAll
    static void registerResolver() {
        FlinkDuplicatingFileSystemFastCopyResolver.register();
    }

    @Test
    void delegatesToDuplicatingFilesystem() throws Exception {
        Path sourceRoot = Files.createDirectory(tempDir.resolve("source"));
        Path destinationRoot = Files.createDirectory(tempDir.resolve("destination"));
        Files.write(sourceRoot.resolve("source.bin"), new byte[] {1, 2, 3, 4});

        CustomFileSystem source = fileSystem(FileSystem.getLocalFileSystem(), sourceRoot);
        CustomFileSystem destination =
                fileSystem(new FastCopyLocalFileSystem(false), destinationRoot);

        assertTrue(source.canFastCopyTo("source.bin", destination, "copied.bin"));
        source.fastCopyTo("source.bin", destination, "copied.bin");
        assertArrayEquals(
                new byte[] {1, 2, 3, 4}, Files.readAllBytes(destinationRoot.resolve("copied.bin")));
    }

    @Test
    void removesPartialDestinationWhenCopyFails() throws Exception {
        Path sourceRoot = Files.createDirectory(tempDir.resolve("failing-source"));
        Path destinationRoot = Files.createDirectory(tempDir.resolve("failing-destination"));
        Files.write(sourceRoot.resolve("source.bin"), new byte[] {5, 6, 7});

        CustomFileSystem source = fileSystem(FileSystem.getLocalFileSystem(), sourceRoot);
        CustomFileSystem destination =
                fileSystem(new FastCopyLocalFileSystem(true), destinationRoot);

        assertThrows(
                IllegalStateException.class,
                () -> source.fastCopyTo("source.bin", destination, "partial.bin"));
        assertFalse(Files.exists(destinationRoot.resolve("partial.bin")));
    }

    private static CustomFileSystem fileSystem(FileSystem fileSystem, Path root) {
        return new CobbleFlinkFileSystems.FlinkCustomFileSystem(
                fileSystem, new org.apache.flink.core.fs.Path(root.toUri()));
    }

    private static final class FastCopyLocalFileSystem
            extends org.apache.flink.core.fs.local.LocalFileSystem
            implements DuplicatingFileSystem {
        private final boolean failAfterCopy;

        private FastCopyLocalFileSystem(boolean failAfterCopy) {
            this.failAfterCopy = failAfterCopy;
        }

        @Override
        public boolean canFastDuplicate(
                org.apache.flink.core.fs.Path source, org.apache.flink.core.fs.Path destination) {
            return true;
        }

        @Override
        public void duplicate(List<CopyRequest> requests) throws IOException {
            for (CopyRequest request : requests) {
                Files.copy(
                        java.nio.file.Paths.get(request.getSource().toUri()),
                        java.nio.file.Paths.get(request.getDestination().toUri()),
                        StandardCopyOption.REPLACE_EXISTING);
                if (failAfterCopy) {
                    throw new IOException("injected fast-copy failure");
                }
            }
        }
    }
}
