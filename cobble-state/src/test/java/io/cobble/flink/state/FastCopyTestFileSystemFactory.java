package io.cobble.flink.state;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;

import java.net.URI;

/** Factory for the test-only fast-copy filesystem. */
public final class FastCopyTestFileSystemFactory implements FileSystemFactory {
    @Override
    public String getScheme() {
        return FastCopyTestFileSystem.SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri) {
        return new FastCopyTestFileSystem();
    }
}
