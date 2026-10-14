package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;

class CobbleScopedFileSystemCacheTest {
    @Test
    void pluginIdentityIgnoresCredentialsButIncludesLoadingRulesAndIsBounded() {
        CobbleScopedFileSystemCache cache = new CobbleScopedFileSystemCache(2, 1);
        Configuration first = new Configuration();
        first.setString("s3.secret-key", "first-secret");
        PluginManager manager = cache.pluginManager(first);
        Configuration credentials = new Configuration(first);
        credentials.setString("s3.secret-key", "second-secret");
        assertSame(manager, cache.pluginManager(credentials));
        Configuration second = new Configuration(first);
        second.set(
                CoreOptions.PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("second.plugin."));
        assertNotSame(manager, cache.pluginManager(second));
        Configuration excess = new Configuration(first);
        excess.set(
                CoreOptions.PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("third.plugin."));
        IllegalStateException error =
                assertThrows(IllegalStateException.class, () -> cache.pluginManager(excess));
        assertTrue(
                error.getMessage().contains("Too many distinct connector plugin configurations"));
        assertFalse(error.getMessage().contains("secret"));
        assertSame(manager, cache.pluginManager(first));
    }

    @Test
    void fullIdentityIsIsolatedAndCapacityNeverEvictsExistingHandles() throws Exception {
        CobbleScopedFileSystemCache cache = new CobbleScopedFileSystemCache(1, 1);
        Configuration configuration = new Configuration();
        configuration.setString("s3.secret-key", "do-not-print-this");
        PluginManager manager = cache.pluginManager(configuration);
        URI uri = URI.create("s3://bucket/first");
        LocalFileSystem fileSystem = new LocalFileSystem();
        assertNull(cache.getOrCheckCapacity(LocalFileSystem.class, manager, uri, configuration));
        cache.put(LocalFileSystem.class, manager, uri, configuration, fileSystem);
        assertSame(
                fileSystem,
                cache.getOrCheckCapacity(
                        LocalFileSystem.class,
                        manager,
                        URI.create("s3://bucket/other-path"),
                        new Configuration(configuration)));
        assertThrows(
                IOException.class,
                () ->
                        cache.getOrCheckCapacity(
                                LocalFileSystem.class,
                                manager,
                                URI.create("s3://other-bucket/first"),
                                configuration));
        assertThrows(
                IOException.class,
                () -> cache.getOrCheckCapacity(String.class, manager, uri, configuration));
        assertThrows(
                IOException.class,
                () ->
                        cache.getOrCheckCapacity(
                                LocalFileSystem.class,
                                new CobbleScopedFileSystemCache().pluginManager(configuration),
                                uri,
                                configuration));
        Configuration changed = new Configuration(configuration);
        changed.setString("s3.secret-key", "different");
        IOException error =
                assertThrows(
                        IOException.class,
                        () ->
                                cache.getOrCheckCapacity(
                                        LocalFileSystem.class, manager, uri, changed));
        assertTrue(
                error.getMessage()
                        .contains("Too many distinct connector-scoped filesystem configurations"));
        assertFalse(error.getMessage().contains("do-not-print-this"));
        assertSame(
                fileSystem,
                cache.getOrCheckCapacity(LocalFileSystem.class, manager, uri, configuration));
    }
}
