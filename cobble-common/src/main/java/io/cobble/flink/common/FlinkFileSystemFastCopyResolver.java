package io.cobble.flink.common;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/** Version-specific bridge to Flink's optimized filesystem copy API. */
public interface FlinkFileSystemFastCopyResolver {

    /** Returns whether the source can be copied through Flink's optimized copy path. */
    boolean canCopy(
            FileSystem sourceFileSystem,
            Path source,
            FileSystem destinationFileSystem,
            Path destination);

    /** Copies one file through Flink's optimized copy path. */
    void copy(
            FileSystem sourceFileSystem,
            Path source,
            FileSystem destinationFileSystem,
            Path destination)
            throws IOException;
}
