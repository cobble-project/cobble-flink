package io.cobble.flink.common;

import io.cobble.Config;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.PluginFileSystemFactory;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.fs.hdfs.HadoopFsFactory;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;

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

    private static volatile Configuration defaults = new Configuration();

    private CobbleFlinkFileSystemResolver() {}

    /** Replaces process-wide Flink filesystem defaults. Callers own process isolation. */
    public static void initialize(Configuration configuration) {
        initialize(configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));
    }

    static synchronized void initialize(Configuration configuration, PluginManager pluginManager) {
        Configuration copy = new Configuration(configuration);
        FileSystem.initialize(copy, pluginManager);
        defaults = new Configuration(copy);
    }

    /** Applies explicit options before current Flink provider defaults to a native volume. */
    public static void applyTo(
            Config.VolumeDescriptor volume, CobbleConnectorStorageOptions options) {
        options.applyTo(volume);
        CobbleFlinkStorageConfig.from(defaults).fill(volume);
    }

    static Configuration effectiveConfiguration(
            String path, CobbleConnectorStorageOptions options) {
        Configuration base = new Configuration(defaults);
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = path;
        options.applyTo(volume);
        CobbleFlinkStorageConfig.from(base).fill(volume);
        Configuration effective = new Configuration(base);
        String scheme = new Path(path).toUri().getScheme();
        // Do not retain stale aliases or identities rejected by native provider resolution.
        if ("s3".equalsIgnoreCase(scheme)
                || "s3a".equalsIgnoreCase(scheme)
                || "s3p".equalsIgnoreCase(scheme)) {
            CobbleFlinkStorageConfig.clearS3ProviderOptions(effective);
        }
        effective.addAll(
                CobbleConnectorStorageOptions.fromVolume(
                                volume.accessId, volume.secretKey, volume.customOptions)
                        .flinkConfiguration(path));
        return effective;
    }

    /** Opens file-backed handles (including resolved relative handles) with scoped credentials. */
    public static FSDataInputStream open(
            StreamStateHandle handle,
            Path checkpointDirectory,
            CobbleConnectorStorageOptions storageOptions)
            throws IOException {
        if (handle instanceof FileStateHandle) {
            Path path =
                    handle instanceof RelativeFileStateHandle
                            ? new Path(
                                    checkpointDirectory,
                                    ((RelativeFileStateHandle) handle).getRelativePath())
                            : ((FileStateHandle) handle).getFilePath();
            return resolve(path.toString(), storageOptions).open(path);
        }
        return handle.openInputStream();
    }

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

        try {
            return resolveConnectorScoped(path, uri, storageOptions, pathUri);
        } catch (Exception | LinkageError | ServiceConfigurationError e) {
            throw new IOException(
                    "Unable to initialize the configured storage provider for scheme '"
                            + scheme
                            + "'; refusing to switch to process-global credentials.",
                    e);
        }
    }

    private static FileSystem resolveConnectorScoped(
            Path path, URI uri, CobbleConnectorStorageOptions storageOptions, String pathUri)
            throws IOException {
        Configuration configuration = effectiveConfiguration(pathUri, storageOptions);
        List<FileSystemFactory> matching = findFactories(uri.getScheme(), configuration);
        if (matching.isEmpty() && "hdfs".equalsIgnoreCase(uri.getScheme())) {
            try {
                matching.add(new HadoopFsFactory());
            } catch (LinkageError missingHadoop) {
                throw new IOException(
                        "Scoped HDFS access requires Flink Hadoop filesystem and Hadoop classes.",
                        missingHadoop);
            }
        }
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
