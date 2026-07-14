package io.cobble.flink.common;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Connector-scoped metadata access through Flink's filesystem registry. */
public final class CobbleMetadataFileIO implements AutoCloseable {

    private static final int MAX_METADATA_FILE_BYTES = 16 * 1024 * 1024;

    private final Path root;
    private final FileSystem fileSystem;

    private CobbleMetadataFileIO(Path root, FileSystem fileSystem) {
        this.root = root;
        this.fileSystem = fileSystem;
    }

    /** Opens metadata access rooted at a table path without changing process-global FS config. */
    public static CobbleMetadataFileIO open(String pathUri) throws IOException {
        return open(pathUri, CobbleConnectorStorageOptions.empty());
    }

    /** Opens metadata access rooted at a table path without changing process-global FS config. */
    public static CobbleMetadataFileIO open(
            String pathUri, CobbleConnectorStorageOptions storageOptions) throws IOException {
        Path root = new Path(pathUri);
        return new CobbleMetadataFileIO(
                root, CobbleFlinkFileSystemResolver.resolve(pathUri, storageOptions));
    }

    public boolean exists(String relativePath) throws IOException {
        return fileSystem.exists(resolve(relativePath));
    }

    /** Returns direct child names, or an empty list when the directory is absent. */
    public List<String> list(String relativePath) throws IOException {
        if (!exists(relativePath)) {
            return Collections.emptyList();
        }
        FileStatus[] statuses = fileSystem.listStatus(resolve(relativePath));
        List<String> names = new ArrayList<String>(statuses.length);
        for (FileStatus status : statuses) {
            names.add(status.getPath().getName());
        }
        return names;
    }

    public byte[] read(String relativePath) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (FSDataInputStream input = fileSystem.open(resolve(relativePath))) {
            byte[] chunk = new byte[8 * 1024];
            int read;
            while ((read = input.read(chunk)) >= 0) {
                if (buffer.size() > MAX_METADATA_FILE_BYTES - read) {
                    throw new IOException("Cobble metadata file exceeds the 16 MiB read limit.");
                }
                buffer.write(chunk, 0, read);
            }
        }
        return buffer.toByteArray();
    }

    public void mkdirs(String relativePath) throws IOException {
        fileSystem.mkdirs(resolve(relativePath));
    }

    public void write(String relativePath, byte[] bytes) throws IOException {
        try (FSDataOutputStream output =
                fileSystem.create(resolve(relativePath), FileSystem.WriteMode.OVERWRITE)) {
            output.write(bytes);
        }
    }

    public void writeIfAbsent(String relativePath, byte[] bytes) throws IOException {
        if (exists(relativePath)) {
            verifyIdentical(relativePath, bytes);
            return;
        }
        try {
            try (FSDataOutputStream output =
                    fileSystem.create(resolve(relativePath), FileSystem.WriteMode.NO_OVERWRITE)) {
                output.write(bytes);
            }
        } catch (IOException e) {
            if (!exists(relativePath)) {
                throw e;
            }
            verifyIdentical(relativePath, bytes);
        }
    }

    public void delete(String relativePath) throws IOException {
        fileSystem.delete(resolve(relativePath), true);
    }

    @Override
    public void close() {}

    private Path resolve(String relativePath) {
        return relativePath.isEmpty() ? root : new Path(root, relativePath);
    }

    private void verifyIdentical(String relativePath, byte[] expected) throws IOException {
        if (!Arrays.equals(expected, read(relativePath))) {
            throw new IOException(
                    "Cobble metadata path already exists with different content: " + relativePath);
        }
    }
}
