package io.cobble.flink.state;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.DuplicatingFileSystem;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Test-only Flink filesystem that maps custom-scheme paths to local files. */
final class FastCopyTestFileSystem extends FileSystem implements DuplicatingFileSystem {
    static final String SCHEME = "cobble-fastcopy-test";
    private static final URI URI_ROOT = URI.create(SCHEME + ":///");
    private static final AtomicInteger FAST_COPY_COUNT = new AtomicInteger();

    private final LocalFileSystem delegate = LocalFileSystem.getSharedInstance();

    static void resetFastCopyCount() {
        FAST_COPY_COUNT.set(0);
    }

    static int fastCopyCount() {
        return FAST_COPY_COUNT.get();
    }

    @Override
    public boolean canFastDuplicate(Path source, Path destination) {
        return SCHEME.equals(source.toUri().getScheme())
                && SCHEME.equals(destination.toUri().getScheme());
    }

    @Override
    public void duplicate(List<CopyRequest> requests) throws IOException {
        for (CopyRequest request : requests) {
            java.nio.file.Path destination = toNioPath(request.getDestination());
            Files.createDirectories(destination.getParent());
            Files.copy(
                    toNioPath(request.getSource()),
                    destination,
                    StandardCopyOption.REPLACE_EXISTING);
            FAST_COPY_COUNT.incrementAndGet();
        }
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
        return URI_ROOT;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return delegate.getFileStatus(toLocalPath(path));
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException {
        return delegate.getFileBlockLocations(file, start, len);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        return delegate.open(toLocalPath(path), bufferSize);
    }

    @Override
    public FSDataInputStream open(Path path) throws IOException {
        return delegate.open(toLocalPath(path));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return delegate.listStatus(toLocalPath(path));
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return delegate.delete(toLocalPath(path), recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return delegate.mkdirs(toLocalPath(path));
    }

    @Override
    public FSDataOutputStream create(Path path, WriteMode overwriteMode) throws IOException {
        return delegate.create(toLocalPath(path), overwriteMode);
    }

    @Override
    public boolean rename(Path source, Path destination) throws IOException {
        return delegate.rename(toLocalPath(source), toLocalPath(destination));
    }

    @Override
    public boolean isDistributedFS() {
        return false;
    }

    @Override
    public FileSystemKind getKind() {
        return delegate.getKind();
    }

    private static Path toLocalPath(Path path) {
        return new Path(path.toUri().getPath());
    }

    private static java.nio.file.Path toNioPath(Path path) {
        return java.nio.file.Paths.get(path.toUri().getPath());
    }
}
