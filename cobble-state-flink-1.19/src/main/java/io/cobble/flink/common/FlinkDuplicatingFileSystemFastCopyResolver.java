package io.cobble.flink.common;

import org.apache.flink.core.fs.DuplicatingFileSystem;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.Collections;

/** Fast-copy bridge for Flink versions that expose {@link DuplicatingFileSystem}. */
public final class FlinkDuplicatingFileSystemFastCopyResolver
        implements FlinkFileSystemFastCopyResolver {

    private static final FlinkDuplicatingFileSystemFastCopyResolver INSTANCE =
            new FlinkDuplicatingFileSystemFastCopyResolver();

    private FlinkDuplicatingFileSystemFastCopyResolver() {}

    public static void register() {
        CobbleFlinkFileSystems.registerFastCopyResolver(INSTANCE);
    }

    @Override
    public boolean canCopy(
            FileSystem sourceFileSystem,
            Path source,
            FileSystem destinationFileSystem,
            Path destination) {
        DuplicatingFileSystem copyingFileSystem =
                copyingFileSystem(sourceFileSystem, destinationFileSystem);
        if (copyingFileSystem == null) {
            return false;
        }
        try {
            return copyingFileSystem.canFastDuplicate(source, destination);
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
        DuplicatingFileSystem copyingFileSystem =
                copyingFileSystem(sourceFileSystem, destinationFileSystem);
        if (copyingFileSystem == null) {
            throw new IOException("Neither Flink filesystem supports optimized file copy");
        }
        copyingFileSystem.duplicate(
                Collections.singletonList(
                        DuplicatingFileSystem.CopyRequest.of(source, destination)));
    }

    private static DuplicatingFileSystem copyingFileSystem(
            FileSystem sourceFileSystem, FileSystem destinationFileSystem) {
        if (destinationFileSystem instanceof DuplicatingFileSystem) {
            return (DuplicatingFileSystem) destinationFileSystem;
        }
        return sourceFileSystem instanceof DuplicatingFileSystem
                ? (DuplicatingFileSystem) sourceFileSystem
                : null;
    }
}
