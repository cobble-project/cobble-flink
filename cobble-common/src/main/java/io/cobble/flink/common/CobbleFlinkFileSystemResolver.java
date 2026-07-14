package io.cobble.flink.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.PluginFileSystemFactory;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Creates connector-scoped Flink filesystems when SQL storage options override cluster defaults.
 */
public final class CobbleFlinkFileSystemResolver {

    private CobbleFlinkFileSystemResolver() {}

    public static FileSystem resolve(String pathUri, CobbleConnectorStorageOptions storageOptions)
            throws IOException {
        Path path = new Path(pathUri);
        if (storageOptions == null || !storageOptions.hasExplicitOptions()) {
            return path.getFileSystem();
        }

        URI uri = path.toUri();
        String scheme = uri.getScheme();
        if (scheme == null || "file".equalsIgnoreCase(scheme)) {
            return path.getFileSystem();
        }

        Throwable providerFailure;
        try {
            return resolveConnectorScoped(path, uri, storageOptions, pathUri);
        } catch (Exception | LinkageError | ServiceConfigurationError e) {
            providerFailure = e;
        }
        try {
            return path.getFileSystem();
        } catch (IOException e) {
            e.addSuppressed(providerFailure);
            throw e;
        } catch (RuntimeException e) {
            e.addSuppressed(providerFailure);
            throw e;
        } catch (LinkageError | ServiceConfigurationError e) {
            e.addSuppressed(providerFailure);
            throw e;
        }
    }

    private static FileSystem resolveConnectorScoped(
            Path path, URI uri, CobbleConnectorStorageOptions storageOptions, String pathUri)
            throws IOException {
        Configuration configuration = storageOptions.flinkConfiguration(pathUri);
        List<FileSystemFactory> matching = findFactories(uri.getScheme(), configuration);
        if (matching.isEmpty()) {
            throw new IOException(
                    "No connector-scoped Flink filesystem provider is available for scheme '"
                            + uri.getScheme()
                            + "'.");
        }
        if (matching.size() > 1) {
            throw new IOException(
                    "Multiple connector-scoped Flink filesystem providers are available for scheme '"
                            + uri.getScheme()
                            + "'.");
        }
        FileSystemFactory factory = PluginFileSystemFactory.of(matching.get(0));
        factory.configure(configuration);
        FileSystem fileSystem = factory.create(uri);
        // Missing sink roots are valid; provider configuration errors must surface before native
        // Cobble I/O starts.
        fileSystem.exists(path);
        return fileSystem;
    }

    private static List<FileSystemFactory> findFactories(
            String scheme, Configuration configuration) {
        List<FileSystemFactory> matches = new ArrayList<FileSystemFactory>();
        Set<String> classes = new HashSet<String>();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        collect(
                ServiceLoader.load(FileSystemFactory.class, contextClassLoader).iterator(),
                scheme,
                classes,
                matches);

        PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
        collect(pluginManager.load(FileSystemFactory.class), scheme, classes, matches);
        return matches;
    }

    private static void collect(
            Iterator<FileSystemFactory> factories,
            String scheme,
            Set<String> classes,
            List<FileSystemFactory> output) {
        while (factories.hasNext()) {
            FileSystemFactory factory = factories.next();
            if (scheme.equalsIgnoreCase(factory.getScheme())
                    && classes.add(factory.getClass().getName())) {
                output.add(factory);
            }
        }
    }
}
