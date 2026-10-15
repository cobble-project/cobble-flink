package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.ProcessFileSystemRequest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;

class CobbleMetadataRequestTest {
    @TempDir Path tempDir;
    private CountingFileSystem fs;

    @AfterEach
    void resetDefaults() {
        CobbleFlinkFileSystemResolver.initialize(new Configuration());
    }

    @Test
    void metadataReadsAndListsUseOnlyTheirActualOperation() throws Exception {
        String root = configure();
        Files.write(tempDir.resolve("data"), new byte[] {1, 2});
        try (CobbleMetadataFileIO io = CobbleMetadataFileIO.open(root, options())) {
            assertEquals(0, fs.existsCalls);
            assertArrayEquals(new byte[] {1, 2}, io.read("data"));
            assertEquals(1, fs.openCalls);
            assertEquals(0, fs.existsCalls);
            assertEquals(Collections.singletonList("data"), io.list(""));
            assertEquals(1, fs.listCalls);
            assertEquals(0, fs.existsCalls);
        }
    }

    @Test
    void onlyMissingOrNullListingsBecomeEmptyAndOtherIoErrorsPropagate() throws Exception {
        String root = configure();
        try (CobbleMetadataFileIO io = CobbleMetadataFileIO.open(root, options())) {
            fs.listFailure = new FileNotFoundException("missing");
            assertTrue(io.list("absent").isEmpty());
            fs.listFailure = null;
            fs.nullListing = true;
            assertTrue(io.list("absent").isEmpty());
            fs.nullListing = false;
            for (IOException failure :
                    new IOException[] {
                        new IOException("permission denied"),
                        new SocketTimeoutException("timed out")
                    }) {
                fs.listFailure = failure;
                assertSame(failure, assertThrows(IOException.class, () -> io.list("denied")));
            }
            fs.openFailure = new IOException("read denied");
            assertSame(fs.openFailure, assertThrows(IOException.class, () -> io.read("denied")));
            assertEquals(0, fs.existsCalls);
        }
    }

    @Test
    void checkpointEntryUsesOneStatusAndPreservesMissingDiagnostic() throws Exception {
        String root = configure();
        Files.write(tempDir.resolve("_metadata"), new byte[] {0});
        assertEquals(
                1,
                CobbleEmbeddedCheckpoint.locateMetadataPaths(
                                new org.apache.flink.core.fs.Path(root + "/_metadata"))
                        .size());
        assertEquals(1, fs.statusCalls);
        assertEquals(0, fs.existsCalls);
        IOException missing =
                assertThrows(
                        IOException.class,
                        () ->
                                CobbleEmbeddedCheckpoint.locateMetadataPaths(
                                        new org.apache.flink.core.fs.Path(root + "/absent")));
        assertTrue(missing.getMessage().contains("path does not exist"));
        fs.statusFailure = new SocketTimeoutException("status timed out");
        assertSame(
                fs.statusFailure,
                assertThrows(
                        IOException.class,
                        () ->
                                CobbleEmbeddedCheckpoint.locateMetadataPaths(
                                        new org.apache.flink.core.fs.Path(root + "/_metadata"))));
    }

    @Test
    void bridgePreservesExplicitProbeWithoutAddingOneForDefaultProvider() throws Exception {
        String root = configure();
        fs.existsFailure = new IOException("denied");
        assertNotNull(CobbleFlinkFileSystems.tryResolve(request(root, false)));
        assertEquals(0, fs.existsCalls);
        assertNull(CobbleFlinkFileSystems.tryResolve(request(root, true)));
        assertEquals(1, fs.existsCalls);
        fs.existsFailure = null;
        fs.missingRoot = true;
        assertNotNull(CobbleFlinkFileSystems.tryResolve(request(root, true)));
        assertEquals(2, fs.existsCalls);
    }

    @Test
    void currentPointerIsReadFreshThroughTheSameFacade() throws Exception {
        String root = configure();
        Files.createDirectories(tempDir.resolve("snapshot"));
        Path current = tempDir.resolve("snapshot/CURRENT");
        Files.write(current, new byte[] {1});
        try (CobbleMetadataFileIO io = CobbleMetadataFileIO.open(root, options())) {
            assertArrayEquals(new byte[] {1}, io.read("snapshot/CURRENT"));
            Files.write(current, new byte[] {2});
            assertArrayEquals(new byte[] {2}, io.read("snapshot/CURRENT"));
            assertEquals(2, fs.openCalls);
            assertEquals(0, fs.existsCalls);
        }
    }

    private String configure() {
        fs = new CountingFileSystem();
        FileSystemFactory factory =
                new FileSystemFactory() {
                    public String getScheme() {
                        return "metadata-requests";
                    }

                    public void configure(Configuration configuration) {}

                    public FileSystem create(URI uri) {
                        return fs;
                    }
                };
        PluginManager plugins =
                new PluginManager() {
                    public <P> Iterator<P> load(Class<P> type) {
                        return type == FileSystemFactory.class
                                ? Collections.singletonList(type.cast(factory)).iterator()
                                : Collections.emptyIterator();
                    }
                };
        CobbleFlinkFileSystemResolver.initialize(new Configuration(), plugins);
        return "metadata-requests://bucket" + tempDir.toUri().getPath();
    }

    private static CobbleConnectorStorageOptions options() {
        return CobbleConnectorStorageOptions.fromStorageOptions(
                Collections.singletonMap("storage.option.probe", "true"));
    }

    private static ProcessFileSystemRequest request(String root, boolean explicit) {
        return new ProcessFileSystemRequest(
                root,
                root,
                null,
                null,
                explicit ? Collections.singletonMap("probe", "true") : Collections.emptyMap(),
                "test-only native failure");
    }

    private static final class CountingFileSystem extends LocalFileSystem {
        private final LocalFileSystem delegate = new LocalFileSystem();
        private int existsCalls;
        private int statusCalls;
        private int listCalls;
        private int openCalls;
        private IOException existsFailure;
        private IOException statusFailure;
        private IOException listFailure;
        private IOException openFailure;
        private boolean missingRoot;
        private boolean nullListing;

        private org.apache.flink.core.fs.Path local(org.apache.flink.core.fs.Path path) {
            return new org.apache.flink.core.fs.Path(Paths.get(path.toUri().getPath()).toUri());
        }

        @Override
        public boolean exists(org.apache.flink.core.fs.Path path) throws IOException {
            existsCalls++;
            if (existsFailure != null) throw existsFailure;
            return !missingRoot && delegate.exists(local(path));
        }

        @Override
        public FileStatus getFileStatus(org.apache.flink.core.fs.Path path) throws IOException {
            statusCalls++;
            if (statusFailure != null) throw statusFailure;
            return delegate.getFileStatus(local(path));
        }

        @Override
        public FileStatus[] listStatus(org.apache.flink.core.fs.Path path) throws IOException {
            listCalls++;
            if (listFailure != null) throw listFailure;
            return nullListing ? null : delegate.listStatus(local(path));
        }

        @Override
        public FSDataInputStream open(org.apache.flink.core.fs.Path path) throws IOException {
            openCalls++;
            if (openFailure != null) throw openFailure;
            return delegate.open(local(path));
        }
    }
}
