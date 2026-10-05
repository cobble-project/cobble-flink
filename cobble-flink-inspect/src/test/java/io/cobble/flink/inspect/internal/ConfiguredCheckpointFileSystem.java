package io.cobble.flink.inspect.internal;

import io.cobble.flink.common.CobbleConnectorStorageOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileStatus;
import org.apache.flink.core.fs.local.LocalFileSystem;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

/** A credential-sensitive remote metadata provider backed by local checkpoint test files. */
final class ConfiguredCheckpointFileSystem {
    private ConfiguredCheckpointFileSystem() {}

    static CobbleConnectorStorageOptions options() {
        Map<String, String> options = new HashMap<>();
        options.put("s3.access-key", "request-access");
        options.put("s3.secret-key", "request-secret");
        options.put("s3.endpoint", "http://request-endpoint");
        return CobbleConnectorStorageOptions.fromStorageOptions(options);
    }

    static Path remote(java.nio.file.Path file) {
        return new Path("s3://fixture" + file.toAbsolutePath().toUri().getPath());
    }

    static ClassLoader classLoader(java.nio.file.Path directory, ClassLoader parent)
            throws IOException {
        java.nio.file.Path service = directory.resolve("filesystem-service");
        Files.write(
                service,
                Collections.singletonList(Factory.class.getName()),
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

    public static final class Factory implements FileSystemFactory {
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
            return new Remote(configuration);
        }
    }

    private static final class Remote extends LocalFileSystem {
        private final Configuration configuration;

        private Remote(Configuration configuration) {
            this.configuration = new Configuration(configuration);
        }

        private void requireCredentials() throws IOException {
            if (!"request-access".equals(configuration.getString("s3.access-key", null))
                    || !"request-secret".equals(configuration.getString("s3.secret-key", null))
                    || !"http://request-endpoint"
                            .equals(configuration.getString("s3.endpoint", null))) {
                throw new IOException("Checkpoint provider did not receive request credentials");
            }
        }

        @Override
        public File pathToFile(Path path) {
            return new File(path.toUri().getPath());
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            requireCredentials();
            super.getFileStatus(path);
            return new LocalFileStatus(pathToFile(path), this) {
                @Override
                public Path getPath() {
                    return path;
                }
            };
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            requireCredentials();
            FileStatus[] files = super.listStatus(path);
            if (files == null) return null;
            for (int index = 0; index < files.length; index++) {
                files[index] = getFileStatus(new Path(path, files[index].getPath().getName()));
            }
            return files;
        }

        @Override
        public FSDataInputStream open(Path path) throws IOException {
            requireCredentials();
            return super.open(path);
        }
    }
}
