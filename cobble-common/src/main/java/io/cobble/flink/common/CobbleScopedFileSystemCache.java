package io.cobble.flink.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginConfig;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Bounded process-owned resources for connector-scoped filesystem resolution. */
final class CobbleScopedFileSystemCache {
    private static final int MAX_PLUGIN_CONFIGURATIONS = 8;
    private static final int MAX_FILE_SYSTEMS = 64;

    // Flink FileSystem has no generic close contract. Do not evict instances still held by callers.
    private final Map<PluginKey, PluginManager> pluginManagers = new HashMap<>();
    private final Map<FileSystemKey, FileSystem> fileSystems = new HashMap<>();
    private final int maxPluginConfigurations;
    private final int maxFileSystems;

    CobbleScopedFileSystemCache() {
        this(MAX_PLUGIN_CONFIGURATIONS, MAX_FILE_SYSTEMS);
    }

    CobbleScopedFileSystemCache(int maxPluginConfigurations, int maxFileSystems) {
        this.maxPluginConfigurations = maxPluginConfigurations;
        this.maxFileSystems = maxFileSystems;
    }

    synchronized PluginManager pluginManager(Configuration configuration) {
        PluginKey key = new PluginKey(configuration);
        PluginManager manager = pluginManagers.get(key);
        if (manager == null) {
            if (pluginManagers.size() >= maxPluginConfigurations) {
                throw new IllegalStateException(
                        "Too many distinct connector plugin configurations (limit "
                                + maxPluginConfigurations
                                + "); reuse configurations or restart the process.");
            }
            manager = PluginUtils.createPluginManagerFromRootFolder(configuration);
            pluginManagers.put(key, manager);
        }
        return manager;
    }

    static boolean samePluginConfiguration(Configuration first, Configuration second) {
        return new PluginKey(first).equals(new PluginKey(second));
    }

    synchronized FileSystem getOrCheckCapacity(
            Class<?> provider, PluginManager manager, URI uri, Configuration configuration)
            throws IOException {
        FileSystem cached =
                fileSystems.get(new FileSystemKey(provider, manager, uri, configuration));
        if (cached == null && fileSystems.size() >= maxFileSystems) {
            throw new IOException(
                    "Too many distinct connector-scoped filesystem configurations (limit "
                            + maxFileSystems
                            + "); reuse configurations or restart the process.");
        }
        return cached;
    }

    synchronized void put(
            Class<?> provider,
            PluginManager manager,
            URI uri,
            Configuration configuration,
            FileSystem fileSystem)
            throws IOException {
        getOrCheckCapacity(provider, manager, uri, configuration);
        fileSystems.put(new FileSystemKey(provider, manager, uri, configuration), fileSystem);
    }

    private static final class PluginKey {
        private final String path;
        private final List<String> parentFirstPatterns;

        private PluginKey(Configuration configuration) {
            PluginConfig config = PluginConfig.fromConfiguration(configuration);
            path =
                    config.getPluginsPath()
                            .map(p -> p.toAbsolutePath().normalize().toString())
                            .orElse(null);
            parentFirstPatterns = Arrays.asList(config.getAlwaysParentFirstPatterns());
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PluginKey)) return false;
            PluginKey key = (PluginKey) other;
            return Objects.equals(path, key.path)
                    && parentFirstPatterns.equals(key.parentFirstPatterns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, parentFirstPatterns);
        }
    }

    private static final class FileSystemKey {
        private final Class<?> provider;
        private final PluginManager manager;
        private final String scheme;
        private final String authority;
        private final Map<String, String> configuration;

        private FileSystemKey(
                Class<?> provider, PluginManager manager, URI uri, Configuration configuration) {
            this.provider = provider;
            this.manager = manager;
            scheme = uri.getScheme().toLowerCase(Locale.ROOT);
            authority = uri.getRawAuthority();
            this.configuration = new HashMap<>(configuration.toMap());
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof FileSystemKey)) return false;
            FileSystemKey key = (FileSystemKey) other;
            return provider == key.provider
                    && manager == key.manager
                    && scheme.equals(key.scheme)
                    && Objects.equals(authority, key.authority)
                    && configuration.equals(key.configuration);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    provider, System.identityHashCode(manager), scheme, authority, configuration);
        }
    }
}
