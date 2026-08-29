package io.cobble.flink.common;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.PathsCopyingFileSystem;

import java.io.IOException;
import java.util.Collections;

/** Fast-copy bridge for Flink versions that expose {@link PathsCopyingFileSystem}. */
public final class FlinkPathsCopyingFileSystemFastCopyResolver
        implements FlinkFileSystemFastCopyResolver {

    private static final FlinkPathsCopyingFileSystemFastCopyResolver INSTANCE =
            new FlinkPathsCopyingFileSystemFastCopyResolver();

    private FlinkPathsCopyingFileSystemFastCopyResolver() {}

    public static void register() {
        CobbleFlinkFileSystems.registerFastCopyResolver(INSTANCE);
    }

    @Override
    public boolean canCopy(
            FileSystem sourceFileSystem,
            Path source,
            FileSystem destinationFileSystem,
            Path destination) {
        PathsCopyingFileSystem copyingFileSystem =
                copyingFileSystem(sourceFileSystem, destinationFileSystem);
        if (copyingFileSystem == null) {
            return false;
        }
        try {
            return copyingFileSystem.canCopyPaths(source, destination);
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void copy(
            FileSystem sourceFileSystem,
            Path source,
            FileSystem destinationFileSystem,
            Path destination)
            throws IOException {
        PathsCopyingFileSystem copyingFileSystem =
                copyingFileSystem(sourceFileSystem, destinationFileSystem);
        if (copyingFileSystem == null) {
            throw new IOException("Neither Flink filesystem supports optimized file copy");
        }
        long sourceSize = sourceFileSystem.getFileStatus(source).getLen();
        try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            copyingFileSystem.copyFiles(
                    Collections.singletonList(
                            PathsCopyingFileSystem.CopyRequest.of(source, destination, sourceSize)),
                    closeableRegistry);
        }
    }

    private static PathsCopyingFileSystem copyingFileSystem(
            FileSystem sourceFileSystem, FileSystem destinationFileSystem) {
        if (destinationFileSystem instanceof PathsCopyingFileSystem) {
            return (PathsCopyingFileSystem) destinationFileSystem;
        }
        return sourceFileSystem instanceof PathsCopyingFileSystem
                ? (PathsCopyingFileSystem) sourceFileSystem
                : null;
    }
}
