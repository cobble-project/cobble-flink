package io.cobble.flink.common;

import io.cobble.CustomFileSystem;
import io.cobble.CustomFileSystemRegistry;
import io.cobble.CustomRandomAccessFile;
import io.cobble.CustomSequentialWriteFile;
import io.cobble.ProcessFileSystemRequest;
import io.cobble.ProcessFileSystems;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Registers a process-level Cobble filesystem fallback that delegates unknown/inaccessible schemes
 * to Flink's filesystem registry.
 */
public final class CobbleFlinkFileSystems {
    private static final AtomicBoolean REGISTERED = new AtomicBoolean(false);
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean(false);
    private static final ExecutorService DELETE_EXECUTOR =
            Executors.newSingleThreadExecutor(
                    runnable -> {
                        Thread thread = new Thread(runnable, "cobble-flink-fs-delete");
                        thread.setDaemon(true);
                        return thread;
                    });

    private CobbleFlinkFileSystems() {}

    public static void ensureRegistered() {
        registerShutdownHook();
        if (REGISTERED.compareAndSet(false, true)) {
            ProcessFileSystems.registerCustomRegistry(new FlinkRegistry());
        }
    }

    private static void registerShutdownHook() {
        if (SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(
                                    () -> {
                                        // JVM is exiting: best-effort cleanup only, never block
                                        // exit.
                                        try {
                                            ProcessFileSystems.clearCustomRegistry();
                                        } catch (Throwable ignored) {
                                        }
                                        try {
                                            DELETE_EXECUTOR.shutdownNow();
                                        } catch (Throwable ignored) {
                                        }
                                    },
                                    "cobble-flink-fs-shutdown"));
        }
    }

    private static final class FlinkRegistry implements CustomFileSystemRegistry {
        @Override
        public CustomFileSystem tryResolve(ProcessFileSystemRequest request) {
            String baseDir =
                    request.normalizedBaseDir() != null
                            ? request.normalizedBaseDir()
                            : request.baseDir();
            if (baseDir == null || baseDir.trim().isEmpty()) {
                return null;
            }
            try {
                Path rootPath = new Path(baseDir);
                FileSystem fileSystem = rootPath.getFileSystem();
                // Probe once so auth/permission errors happen here and not deep in native read
                // path.
                fileSystem.exists(rootPath);
                return new FlinkCustomFileSystem(fileSystem, rootPath);
            } catch (Throwable ignored) {
                return null;
            }
        }
    }

    private static final class FlinkCustomFileSystem implements CustomFileSystem {
        private final FileSystem fileSystem;
        private final Path rootPath;

        private FlinkCustomFileSystem(FileSystem fileSystem, Path rootPath) {
            this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem");
            this.rootPath = Objects.requireNonNull(rootPath, "rootPath");
        }

        @Override
        public void createDir(String path) {
            try {
                fileSystem.mkdirs(resolve(path));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create dir " + path, e);
            }
        }

        @Override
        public boolean exists(String path) {
            try {
                return fileSystem.exists(resolve(path));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to check path " + path, e);
            }
        }

        @Override
        public void delete(String path) {
            try {
                fileSystem.delete(resolve(path), true);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to delete path " + path, e);
            }
        }

        @Override
        public void deleteAsync(String path) {
            Path resolved = resolve(path);
            DELETE_EXECUTOR.execute(
                    () -> {
                        try {
                            fileSystem.delete(resolved, true);
                        } catch (IOException ignored) {
                        }
                    });
        }

        @Override
        public void rename(String from, String to) {
            try {
                fileSystem.rename(resolve(from), resolve(to));
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to rename path from " + from + " to " + to, e);
            }
        }

        @Override
        public String[] list(String path) {
            try {
                FileStatus[] statuses = fileSystem.listStatus(resolve(path));
                if (statuses == null || statuses.length == 0) {
                    return new String[0];
                }
                return Arrays.stream(statuses)
                        .map(status -> status.getPath().getName())
                        .toArray(String[]::new);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to list path " + path, e);
            }
        }

        @Override
        public CustomRandomAccessFile openRead(String path) {
            try {
                Path resolved = resolve(path);
                FSDataInputStream stream = fileSystem.open(resolved);
                long size = fileSystem.getFileStatus(resolved).getLen();
                return new FlinkCustomRandomAccessFile(stream, size);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to open read path " + path, e);
            }
        }

        @Override
        public CustomSequentialWriteFile openWrite(String path) {
            try {
                FSDataOutputStream stream =
                        fileSystem.create(resolve(path), FileSystem.WriteMode.OVERWRITE);
                return new FlinkCustomSequentialWriteFile(stream);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to open write path " + path, e);
            }
        }

        @Override
        public Long lastModified(String path) {
            try {
                return fileSystem.getFileStatus(resolve(path)).getModificationTime() / 1000L;
            } catch (IOException e) {
                return null;
            }
        }

        @Override
        public void close() {}

        private Path resolve(String path) {
            if (path == null || path.isEmpty() || "/".equals(path)) {
                return rootPath;
            }
            Path candidate = new Path(path);
            if (candidate.isAbsolute() || candidate.toUri().getScheme() != null) {
                return candidate;
            }
            return new Path(rootPath, path);
        }
    }

    private static final class FlinkCustomRandomAccessFile implements CustomRandomAccessFile {
        private final FSDataInputStream stream;
        private final long size;
        private final Object lock = new Object();

        private FlinkCustomRandomAccessFile(FSDataInputStream stream, long size) {
            this.stream = stream;
            this.size = size;
        }

        @Override
        public byte[] readAt(long offset, int readSize) {
            if (readSize == 0) {
                return new byte[0];
            }
            if (offset < 0L || readSize < 0) {
                throw new IllegalArgumentException("offset and size must be non-negative");
            }
            byte[] output = new byte[readSize];
            synchronized (lock) {
                try {
                    stream.seek(offset);
                    int read = 0;
                    while (read < readSize) {
                        int n = stream.read(output, read, readSize - read);
                        if (n < 0) {
                            throw new IllegalStateException(
                                    "Unexpected EOF while reading "
                                            + readSize
                                            + " bytes at offset "
                                            + offset);
                        }
                        read += n;
                    }
                    return output;
                } catch (IOException e) {
                    throw new IllegalStateException(
                            "Failed to read at offset " + offset + " size " + readSize, e);
                }
            }
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public boolean supportDirect() {
            return false;
        }

        @Override
        public ByteBuffer readAtDirect(long offset, int size) {
            throw new UnsupportedOperationException("Direct read is not supported.");
        }

        @Override
        public void close() {
            try {
                stream.close();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to close read stream", e);
            }
        }
    }

    private static final class FlinkCustomSequentialWriteFile implements CustomSequentialWriteFile {
        private final FSDataOutputStream stream;
        private long size;
        private volatile boolean closed;

        private FlinkCustomSequentialWriteFile(FSDataOutputStream stream) {
            this.stream = stream;
            this.size = 0L;
            this.closed = false;
        }

        @Override
        public int write(byte[] data) {
            if (data == null) {
                throw new IllegalArgumentException("data must not be null");
            }
            if (closed) {
                throw new IllegalStateException("Output stream is already closed");
            }
            try {
                stream.write(data);
                size += data.length;
                return data.length;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to write output stream", e);
            }
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public boolean supportDirect() {
            return false;
        }

        @Override
        public int writeDirect(ByteBuffer data, int length) {
            throw new UnsupportedOperationException("Direct write is not supported.");
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                stream.flush();
            } catch (ClosedChannelException ignored) {
                return;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to flush output stream", e);
            }
            try {
                stream.sync();
            } catch (ClosedChannelException ignored) {
                return;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to sync output stream", e);
            }
            try {
                stream.close();
            } catch (ClosedChannelException ignored) {
                // Already closed by owner side; treat as idempotent close.
            } catch (IOException e) {
                throw new IllegalStateException("Failed to close output stream", e);
            }
        }
    }
}
