package io.cobble.flink.tests.fs;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;

import java.io.IOException;
import java.net.URI;

/** Test-only filesystem that maps {@code mockfs://} paths onto local filesystem paths. */
final class MockSchemeFileSystem extends FileSystem {
    private static final URI DEFAULT_URI = URI.create("mockfs:///");

    private final URI uri;
    private final LocalFileSystem delegate;

    MockSchemeFileSystem(URI uri) {
        this.uri = uri == null ? DEFAULT_URI : uri;
        this.delegate = LocalFileSystem.getSharedInstance();
    }

    @Override
    public Path getWorkingDirectory() {
        return delegate.getWorkingDirectory();
    }

    @Override
    public Path getHomeDirectory() {
        return delegate.getHomeDirectory();
    }

    @Override
    public URI getUri() {
        return DEFAULT_URI;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return delegate.getFileStatus(toLocalPath(f));
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException {
        return delegate.getFileBlockLocations(file, start, len);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return delegate.open(toLocalPath(f), bufferSize);
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        return delegate.open(toLocalPath(f));
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        return delegate.listStatus(toLocalPath(f));
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return delegate.delete(toLocalPath(f), recursive);
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        return delegate.mkdirs(toLocalPath(f));
    }

    @Override
    public FSDataOutputStream create(Path f, WriteMode overwriteMode) throws IOException {
        return delegate.create(toLocalPath(f), overwriteMode);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return delegate.rename(toLocalPath(src), toLocalPath(dst));
    }

    @Override
    public boolean isDistributedFS() {
        return false;
    }

    @Override
    public FileSystemKind getKind() {
        return delegate.getKind();
    }

    private Path toLocalPath(Path path) {
        URI raw = path.toUri();
        String rawPath = raw.getPath();
        if (rawPath == null || rawPath.isEmpty()) {
            return new Path(uri.getPath());
        }
        return new Path(rawPath);
    }
}
