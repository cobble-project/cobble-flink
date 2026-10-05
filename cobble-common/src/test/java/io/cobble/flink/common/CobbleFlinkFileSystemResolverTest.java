package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

class CobbleFlinkFileSystemResolverTest {

    @TempDir Path tempDir;

    @Test
    void scopedHdfsUsesOptionalHadoopFactoryWithoutGlobalFallback() {
        IOException error =
                assertThrows(
                        IOException.class,
                        () ->
                                CobbleFlinkFileSystemResolver.resolve(
                                        "hdfs:///checkpoint",
                                        CobbleConnectorStorageOptions.fromStorageOptions(
                                                Collections.singletonMap(
                                                        "storage.option.fs.defaultFS",
                                                        "file:///"))));
        assertTrue(error.getMessage().contains("refusing to switch to process-global credentials"));
        assertTrue(
                error.getCause().toString().contains("Hadoop")
                        || error.getCause().toString().contains("hdfs"));
        assertFalse(error.getCause().getMessage().contains("No connector-scoped"));
    }

    @Test
    void passesGenericAndAliasCredentialsToScopedS3Provider() throws Exception {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(scopedS3ClassLoader(original));
            Map<String, String> generic = new HashMap<>();
            generic.put("storage.option.access_key_id", "access");
            generic.put("storage.option.secret_access_key", "secret");
            generic.put("storage.option.endpoint", "http://storage.example");
            Map<String, String> aliases = new HashMap<>();
            aliases.put("s3.access-key", "access");
            aliases.put("s3.secret-key", "secret");
            aliases.put("s3.endpoint", "http://storage.example");

            CobbleConnectorStorageOptions genericOptions =
                    CobbleConnectorStorageOptions.fromStorageOptions(generic);
            for (CobbleConnectorStorageOptions options :
                    Arrays.asList(
                            genericOptions,
                            CobbleConnectorStorageOptions.fromStorageOptions(aliases),
                            CobbleFlinkStorageConfig.empty()
                                    .resolve("s3://bucket/table", genericOptions))) {
                FileSystem fileSystem =
                        CobbleFlinkFileSystemResolver.resolve("s3://bucket/table", options);
                assertTrue(
                        fileSystem.exists(new org.apache.flink.core.fs.Path("s3://bucket/table")));
            }

            IOException error =
                    assertThrows(
                            IOException.class,
                            () ->
                                    CobbleFlinkFileSystemResolver.resolve(
                                            "s3://bucket/table",
                                            CobbleConnectorStorageOptions.fromStorageOptions(
                                                    Collections.singletonMap(
                                                            "storage.option.endpoint",
                                                            "http://storage.example"))));
            assertTrue(
                    error.getMessage()
                            .contains("refusing to switch to process-global credentials"));
            assertTrue(error.getCause().getMessage().contains("Missing scoped credentials"));
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private ClassLoader scopedS3ClassLoader(ClassLoader parent) throws IOException {
        Path service = tempDir.resolve("filesystem-service");
        Files.write(
                service,
                Collections.singletonList(ScopedS3Factory.class.getName()),
                StandardCharsets.UTF_8);
        URL serviceUrl = service.toUri().toURL();
        return new ClassLoader(parent) {
            @Override
            public Enumeration<URL> getResources(String name) throws IOException {
                if (name.equals("META-INF/services/" + FileSystemFactory.class.getName())) {
                    return Collections.enumeration(Collections.singletonList(serviceUrl));
                }
                return super.getResources(name);
            }
        };
    }

    public static final class ScopedS3Factory implements FileSystemFactory {
        private Configuration configuration;

        @Override
        public String getScheme() {
            return "s3";
        }

        @Override
        public void configure(Configuration configuration) {
            this.configuration = new Configuration(configuration);
        }

        @Override
        public FileSystem create(URI uri) {
            Configuration configured = new Configuration(configuration);
            return new LocalFileSystem() {
                @Override
                public boolean exists(org.apache.flink.core.fs.Path path) throws IOException {
                    if (!"access".equals(configured.getString("s3.access-key", null))
                            || !"secret".equals(configured.getString("s3.secret-key", null))) {
                        throw new IOException("Missing scoped credentials");
                    }
                    assertEquals(
                            "http://storage.example", configured.getString("s3.endpoint", null));
                    return true;
                }
            };
        }
    }

    @Test
    void refusesGlobalCredentialFallbackWhenConfiguredProviderFails() {
        CobbleConnectorStorageOptions storageOptions =
                CobbleConnectorStorageOptions.fromStorageOptions(
                        Collections.singletonMap(
                                "storage.option.endpoint", "https://storage.example"));

        IOException error =
                assertThrows(
                        IOException.class,
                        () ->
                                CobbleFlinkFileSystemResolver.resolve(
                                        "missing-provider://bucket/table", storageOptions));

        assertTrue(error.getMessage().contains("refusing to switch to process-global credentials"));
        assertTrue(error.getCause().getMessage().contains("connector-scoped"));
    }
}
