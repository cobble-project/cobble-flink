package io.cobble.flink.common;

import io.cobble.Config;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.PluginFileSystemFactory;
import org.apache.flink.core.fs.SafetyNetWrapperFileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.runtime.fs.hdfs.HadoopFsFactory;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.util.WrappingProxy;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Creates connector-scoped Flink filesystems when SQL storage options override cluster defaults.
 */
public final class CobbleFlinkFileSystemResolver {

    private static volatile Configuration defaults = new Configuration();
    private static final CobbleScopedFileSystemCache RESOURCES = new CobbleScopedFileSystemCache();
    private static PluginManager initializedPluginManager;

    private CobbleFlinkFileSystemResolver() {}

    /** Replaces process-wide Flink filesystem defaults. Callers own process isolation. */
    public static synchronized void initialize(Configuration configuration) {
        initialize(configuration, RESOURCES.pluginManager(configuration));
    }

    static synchronized void initialize(Configuration configuration, PluginManager pluginManager) {
        Configuration copy = new Configuration(configuration);
        if (copy.equals(defaults) && initializedPluginManager == pluginManager) {
            return;
        }
        FileSystem.initialize(copy, pluginManager);
        defaults = new Configuration(copy);
        initializedPluginManager = pluginManager;
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
        FileSystem fileSystem = getOrCreateScopedFileSystem(uri, storageOptions, pathUri);
        // Validate every path, including cache hits, without serializing remote I/O across callers.
        fileSystem.exists(path);
        return fileSystem;
    }

    private static synchronized FileSystem getOrCreateScopedFileSystem(
            URI uri, CobbleConnectorStorageOptions storageOptions, String pathUri)
            throws IOException {
        Configuration configuration = effectiveConfiguration(pathUri, storageOptions);
        PluginManager manager =
                initializedPluginManager != null
                                && CobbleScopedFileSystemCache.samePluginConfiguration(
                                        configuration, defaults)
                        ? initializedPluginManager
                        : RESOURCES.pluginManager(configuration);
        List<FactoryCandidate> matching = findFactories(uri.getScheme(), manager);
        if (matching.isEmpty() && "hdfs".equalsIgnoreCase(uri.getScheme())) {
            try {
                matching.add(new FactoryCandidate(new HadoopFsFactory(), true));
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
        FactoryCandidate candidate = matching.get(0);
        FileSystem fileSystem =
                candidate.processOwned
                        ? RESOURCES.getOrCheckCapacity(
                                candidate.factory.getClass(), manager, uri, configuration)
                        : null;
        if (fileSystem != null) {
            return fileSystem;
        }
        // Do not let a shared provider capture a task's context loader in client threads. Keep
        // configure/create together: injected managers may return the same mutable factory.
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(
                        candidate.processOwned
                                ? candidate.factory.getClass().getClassLoader()
                                : Thread.currentThread().getContextClassLoader())) {
            FileSystemFactory factory = PluginFileSystemFactory.of(candidate.factory);
            factory.configure(configuration);
            fileSystem = factory.create(uri);
            if (candidate.processOwned && !containsSafetyNet(fileSystem)) {
                // Retain clients even when a path/credential probe fails: future attempts must
                // revalidate, but must not abandon another client on every failed probe.
                RESOURCES.put(
                        candidate.factory.getClass(), manager, uri, configuration, fileSystem);
            }
        }
        return fileSystem;
    }

    private static List<FactoryCandidate> findFactories(
            String scheme, PluginManager pluginManager) {
        List<FactoryCandidate> matches = new ArrayList<>();
        Set<String> classes = new HashSet<String>();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        collect(
                ServiceLoader.load(FileSystemFactory.class, contextClassLoader).iterator(),
                scheme,
                classes,
                matches,
                false);
        collect(pluginManager.load(FileSystemFactory.class), scheme, classes, matches, true);
        return matches;
    }

    private static void collect(
            Iterator<FileSystemFactory> factories,
            String scheme,
            Set<String> classes,
            List<FactoryCandidate> output,
            boolean pluginOwned) {
        while (factories.hasNext()) {
            FileSystemFactory factory = factories.next();
            if (!scheme.equalsIgnoreCase(factory.getScheme())) {
                continue;
            }
            if (classes.add(factory.getClass().getName())) {
                output.add(
                        new FactoryCandidate(
                                factory,
                                pluginOwned || parentOwned(factory.getClass().getClassLoader())));
            } else if (pluginOwned) {
                for (FactoryCandidate candidate : output) {
                    if (candidate.factory.getClass() == factory.getClass()) {
                        candidate.processOwned = true;
                    }
                }
            }
        }
    }

    private static boolean parentOwned(ClassLoader loader) {
        for (ClassLoader parent = CobbleFlinkFileSystemResolver.class.getClassLoader();
                parent != null;
                parent = parent.getParent()) {
            if (parent == loader) return true;
        }
        return loader == null;
    }

    private static boolean containsSafetyNet(FileSystem fileSystem) {
        Map<FileSystem, Boolean> seen = new IdentityHashMap<>();
        while (seen.put(fileSystem, Boolean.TRUE) == null) {
            if (fileSystem instanceof SafetyNetWrapperFileSystem) return true;
            if (!(fileSystem instanceof WrappingProxy)) return false;
            Object delegate = ((WrappingProxy<?>) fileSystem).getWrappedDelegate();
            if (!(delegate instanceof FileSystem)) return false;
            fileSystem = (FileSystem) delegate;
        }
        return true;
    }

    private static final class FactoryCandidate {
        private final FileSystemFactory factory;
        // A task-defined provider is deliberately not retained by the process cache. A matching
        // class from the installed plugin manager establishes stable ownership even via TCCL.
        private boolean processOwned;

        private FactoryCandidate(FileSystemFactory factory, boolean processOwned) {
            this.factory = factory;
            this.processOwned = processOwned;
        }
    }
}
