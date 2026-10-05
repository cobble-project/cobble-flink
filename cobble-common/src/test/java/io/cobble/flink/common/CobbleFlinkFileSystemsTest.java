package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.CustomFileSystem;
import io.cobble.CustomSequentialWriteFile;
import io.cobble.Db;
import io.cobble.ProcessFileSystemRequest;
import io.cobble.ShardSnapshot;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;

class CobbleFlinkFileSystemsTest {

    @TempDir Path tempDir;

    @Test
    void nativeRegistryAndMetadataReadsShareFullProcessDefaultsAndRequestOverrides()
            throws Exception {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Path service = tempDir.resolve("filesystem-service");
        Files.write(service, Collections.singletonList(ConfiguredNativeFactory.class.getName()));
        URL serviceUrl = service.toUri().toURL();
        ClassLoader scoped =
                new ClassLoader(original) {
                    @Override
                    public Enumeration<URL> getResources(String name) throws IOException {
                        return name.equals("META-INF/services/" + FileSystemFactory.class.getName())
                                ? Collections.enumeration(Collections.singletonList(serviceUrl))
                                : super.getResources(name);
                    }
                };
        PluginManager plugins =
                new PluginManager() {
                    @Override
                    public <P> Iterator<P> load(Class<P> service) {
                        return service == FileSystemFactory.class
                                ? Collections.singletonList(
                                                service.cast(new ConfiguredNativeFactory()))
                                        .iterator()
                                : Collections.emptyIterator();
                    }
                };
        Configuration defaults = new Configuration();
        defaults.setString("provider.option", "first");
        defaults.setString("hadoop.opaque.setting", "hadoop-value");
        defaults.setString("plugin.opaque.setting", "plugin-value");
        try {
            Thread.currentThread().setContextClassLoader(scoped);
            CobbleLoader.ensureCobbleLoaded();
            CobbleFlinkFileSystemResolver.initialize(defaults, plugins);
            String first = nativeRoundTrip("first", Collections.emptyMap());
            FileSystem opened =
                    CobbleFlinkFileSystemResolver.resolve(
                            first, CobbleConnectorStorageOptions.empty());
            defaults.setString("provider.option", "second");
            CobbleFlinkFileSystemResolver.initialize(defaults, plugins);
            nativeRoundTrip("second", Collections.emptyMap());
            assertTrue(opened.exists(new org.apache.flink.core.fs.Path(first)));
            assertThrows(
                    IOException.class,
                    () ->
                            CobbleFlinkFileSystemResolver.resolve(
                                    first, CobbleConnectorStorageOptions.empty()));
            String explicit =
                    nativeRoundTrip(
                            "explicit", Collections.singletonMap("provider.option", "explicit"));
            assertTrue(
                    CobbleFlinkFileSystemResolver.resolve(
                                    explicit,
                                    CobbleConnectorStorageOptions.fromStorageOptions(
                                            Collections.singletonMap(
                                                    "storage.option.provider.option", "explicit")))
                            .exists(new org.apache.flink.core.fs.Path(explicit)));
        } finally {
            CobbleFlinkFileSystemResolver.initialize(new Configuration());
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private String nativeRoundTrip(String identity, Map<String, String> requestOptions)
            throws Exception {
        Path local = tempDir.resolve(identity);
        String root = "configured-native://" + identity + local.toUri().getPath();
        Config config = new Config().numColumns(1).totalBuckets(1);
        config.governanceMode = Config.GovernanceMode.NOOP;
        config.logConsole = false;
        Config.VolumeDescriptor volume = Config.VolumeDescriptor.singleVolume(root);
        volume.customOptions = requestOptions;
        config.addVolume(volume);
        ShardSnapshot snapshot;
        try (Db db = Db.open(config)) {
            db.put(0, new byte[] {1}, 0, new byte[] {2});
            assertArrayEquals(new byte[] {2}, db.get(0, new byte[] {1}, 0));
            snapshot = db.snapshot();
            db.retainSnapshot(snapshot.snapshotId);
        }
        org.apache.flink.core.fs.Path manifest =
                new org.apache.flink.core.fs.Path(snapshot.manifestPath);
        assertTrue(Files.size(Paths.get(manifest.toUri().getPath())) > 0);
        try (FSDataInputStream metadata =
                CobbleFlinkFileSystemResolver.resolve(
                                snapshot.manifestPath,
                                CobbleConnectorStorageOptions.fromVolume(
                                        null, null, requestOptions))
                        .open(manifest)) {
            assertTrue(metadata.read() >= 0);
        }
        return root;
    }

    public static final class ConfiguredNativeFactory implements FileSystemFactory {
        private Configuration configuration;

        @Override
        public String getScheme() {
            return "configured-native";
        }

        @Override
        public void configure(Configuration configuration) {
            this.configuration = new Configuration(configuration);
        }

        @Override
        public FileSystem create(URI uri) throws IOException {
            if (!uri.getAuthority().equals(configuration.getString("provider.option", null))
                    || !"hadoop-value"
                            .equals(configuration.getString("hadoop.opaque.setting", null))
                    || !"plugin-value"
                            .equals(configuration.getString("plugin.opaque.setting", null))) {
                throw new IOException("Missing effective native/metadata filesystem settings");
            }
            return new LocalFileSystem() {
                @Override
                public File pathToFile(org.apache.flink.core.fs.Path path) {
                    return new File(path.toUri().getPath());
                }
            };
        }
    }

    @Test
    void resolvesNativeRequestThroughTheSharedFlinkResolver() {
        ProcessFileSystemRequest request =
                new ProcessFileSystemRequest(
                        "missing-provider://bucket/table",
                        tempDir.toUri().toString(),
                        "access",
                        "secret",
                        Collections.singletonMap("endpoint", "http://127.0.0.1:9000"),
                        null);

        CustomFileSystem fileSystem = CobbleFlinkFileSystems.tryResolve(request);
        assertNotNull(fileSystem);
        try {
            CustomSequentialWriteFile output = fileSystem.openWrite("metadata.bin");
            output.write(new byte[] {7, 8, 9});
            output.close();
            assertArrayEquals(
                    new byte[] {7, 8, 9}, fileSystem.openRead("metadata.bin").readAt(0, 3));
            assertEquals(Long.valueOf(3L), fileSystem.fileSize("metadata.bin"));
            fileSystem.createDir("nested");
            assertNull(fileSystem.fileSize("nested"));
            assertThrows(IllegalStateException.class, () -> fileSystem.fileSize("missing.bin"));
        } finally {
            fileSystem.close();
        }
    }

    @Test
    void usesBaseDirWhenNativeRequestHasNoNormalizedBaseDir() {
        ProcessFileSystemRequest request =
                new ProcessFileSystemRequest(
                        tempDir.toUri().toString(),
                        null,
                        null,
                        null,
                        Collections.<String, String>emptyMap(),
                        null);

        CustomFileSystem fileSystem = CobbleFlinkFileSystems.tryResolve(request);
        assertNotNull(fileSystem);
        fileSystem.close();
    }
}
