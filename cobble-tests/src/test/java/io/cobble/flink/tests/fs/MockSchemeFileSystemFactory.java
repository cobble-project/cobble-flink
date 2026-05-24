package io.cobble.flink.tests.fs;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;

import java.io.IOException;
import java.net.URI;

/** Test-only Flink filesystem factory for {@code mockfs://} URIs. */
public final class MockSchemeFileSystemFactory implements FileSystemFactory {
    @Override
    public String getScheme() {
        return "mockfs";
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        return new MockSchemeFileSystem(fsUri);
    }
}
