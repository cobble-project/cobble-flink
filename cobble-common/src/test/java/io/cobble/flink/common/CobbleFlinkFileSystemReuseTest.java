package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CobbleSafetyNetTestAccess;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.SafetyNetCloseableRegistry;
import org.apache.flink.core.fs.SafetyNetWrapperFileSystem;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.plugin.DefaultPluginManager;
import org.apache.flink.core.plugin.PluginConfig;
import org.apache.flink.core.plugin.PluginDescriptor;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.util.WrappingProxy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

class CobbleFlinkFileSystemReuseTest {
    @TempDir Path tempDir;

    @AfterEach
    void resetDefaults() {
        CobbleFlinkFileSystemResolver.initialize(new Configuration());
    }

    @Test
    void stableProviderIsSharedAcrossTaskLoadersAndEveryPathIsValidated() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        CobbleFlinkFileSystemResolver.initialize(new Configuration(), manager(factory));
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            ClassLoader firstTask = new ClassLoader(original) {};
            Thread.currentThread().setContextClassLoader(firstTask);
            FileSystem first = resolve("bucket", "a");
            assertSame(firstTask, Thread.currentThread().getContextClassLoader());
            ClassLoader secondTask = new ClassLoader(original) {};
            Thread.currentThread().setContextClassLoader(secondTask);
            assertSame(first, resolve("bucket", "a"));
            assertSame(secondTask, Thread.currentThread().getContextClassLoader());
            assertEquals(1, factory.creates.get());
            assertEquals(2, factory.probes.get());
            assertSame(RecordingFactory.class.getClassLoader(), factory.creationContext);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void configurationAuthorityAndDefaultsRemainIsolated() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        PluginManager manager = manager(factory);
        Configuration defaults = new Configuration();
        defaults.setString("identity", "default");
        CobbleFlinkFileSystemResolver.initialize(defaults, manager);
        FileSystem first = resolve("bucket", "a");
        assertNotSame(first, resolve("other-bucket", "a"));
        assertNotSame(first, resolve("bucket", "b"));
        assertEquals(3, factory.creates.get());
        defaults.setString("identity", "changed-default");
        CobbleFlinkFileSystemResolver.initialize(defaults, manager);
        assertSame(first, resolve("bucket", "a"));
        assertTrue(first.exists(new org.apache.flink.core.fs.Path("reuse://bucket/a")));
    }

    @Test
    void identicalInitializeDoesNotClearFlinkGlobalCacheButChangedDefaultsDo() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        PluginManager manager = manager(factory);
        Configuration defaults = new Configuration();
        defaults.setString("identity", "first");
        CobbleFlinkFileSystemResolver.initialize(defaults, manager);
        FileSystem first = CobbleFlinkFileSystemResolver.resolve("reuse://bucket/first", null);
        CobbleFlinkFileSystemResolver.initialize(new Configuration(defaults), manager);
        assertSame(first, CobbleFlinkFileSystemResolver.resolve("reuse://bucket/first", null));
        defaults.setString("identity", "second");
        CobbleFlinkFileSystemResolver.initialize(defaults, manager);
        assertNotSame(first, CobbleFlinkFileSystemResolver.resolve("reuse://bucket/second", null));
        assertTrue(first.exists(new org.apache.flink.core.fs.Path("reuse://bucket/first")));
    }

    @Test
    void probeFailureRetainsClientAndRevalidatesButCreateFailureIsRetried() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        CobbleFlinkFileSystemResolver.initialize(new Configuration(), manager(factory));
        CobbleConnectorStorageOptions probeFailure = options("probe-fail", "true");
        for (int i = 0; i < 2; i++) {
            IOException error =
                    assertThrows(
                            IOException.class,
                            () ->
                                    CobbleFlinkFileSystemResolver.resolve(
                                            "reuse://bucket/path", probeFailure));
            assertTrue(
                    error.getMessage()
                            .contains("refusing to switch to process-global credentials"));
        }
        assertEquals(1, factory.creates.get());
        assertEquals(2, factory.probes.get());
        for (int i = 0; i < 2; i++) {
            assertThrows(
                    IOException.class,
                    () ->
                            CobbleFlinkFileSystemResolver.resolve(
                                    "reuse://bucket/path", options("create-fail", "true")));
        }
        assertEquals(3, factory.creates.get());
    }

    @Test
    void sharedMutableFactoryDoesNotMixConcurrentConfigurations() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        CobbleFlinkFileSystemResolver.initialize(new Configuration(), manager(factory));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = executor.submit(() -> repeatResolve("a"));
            Future<?> second = executor.submit(() -> repeatResolve("b"));
            first.get();
            second.get();
            assertEquals(2, factory.creates.get());
            assertEquals(40, factory.probes.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void cachedPathProbesCanProceedConcurrently() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        CobbleFlinkFileSystemResolver.initialize(new Configuration(), manager(factory));
        resolve("concurrent", "a");
        factory.probeEntries = new CountDownLatch(2);
        factory.probeRelease = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = executor.submit(() -> repeatResolve("a"));
            Future<?> second = executor.submit(() -> repeatResolve("a"));
            assertTrue(factory.probeEntries.await(3, TimeUnit.SECONDS));
            factory.probeRelease.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            assertEquals(1, factory.creates.get());
        } finally {
            factory.probeRelease.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void taskSafetyNetHandlesAreNeverShared() throws Exception {
        try (SafetyNetCloseableRegistry registry = CobbleSafetyNetTestAccess.newRegistry()) {
            RecordingFactory factory =
                    new RecordingFactory() {
                        @Override
                        public FileSystem create(URI uri) throws IOException {
                            return new SafetyNetWrapperFileSystem(super.create(uri), registry);
                        }
                    };
            CobbleFlinkFileSystemResolver.initialize(new Configuration(), manager(factory));
            assertNotSame(resolve("bucket", "a"), resolve("bucket", "a"));
        }
    }

    @Test
    void realPluginManagerReusesPluginLoaderAcrossScopedConfigurations() throws Exception {
        URL jar = pluginJar().toUri().toURL();
        PluginManager plugins =
                new DefaultPluginManager(
                        Collections.singletonList(
                                new PluginDescriptor("fixture", new URL[] {jar}, new String[0])),
                        PluginConfig.fromConfiguration(new Configuration())
                                .getAlwaysParentFirstPatterns());
        CobbleFlinkFileSystemResolver.initialize(new Configuration(), plugins);
        FileSystem first =
                CobbleFlinkFileSystemResolver.resolve(
                        "fixture://bucket/one", options("identity", "one"));
        assertSame(
                first,
                CobbleFlinkFileSystemResolver.resolve(
                        "fixture://bucket/two", options("identity", "one")));
        FileSystem second =
                CobbleFlinkFileSystemResolver.resolve(
                        "fixture://bucket/two", options("identity", "two"));
        assertNotSame(first, second);
        ClassLoader pluginLoader = unwrap(first).getClass().getClassLoader();
        assertTrue(pluginLoader.getClass().getName().contains("PluginClassLoader"));
        assertSame(pluginLoader, unwrap(second).getClass().getClassLoader());
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(pluginLoader);
            assertSame(
                    first,
                    CobbleFlinkFileSystemResolver.resolve(
                            "fixture://bucket/one", options("identity", "one")));
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void taskDefinedProviderIsNotRetainedByProcessCache() throws Exception {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader task =
                new URLClassLoader(new URL[] {pluginJar().toUri().toURL()}, original)) {
            Thread.currentThread().setContextClassLoader(task);
            FileSystem first =
                    CobbleFlinkFileSystemResolver.resolve(
                            "fixture://bucket/one", options("identity", "one"));
            FileSystem second =
                    CobbleFlinkFileSystemResolver.resolve(
                            "fixture://bucket/one", options("identity", "one"));
            assertNotSame(first, second);
            assertSame(task, unwrap(first).getClass().getClassLoader());
            assertSame(task, Thread.currentThread().getContextClassLoader());
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private static void repeatResolve(String identity) {
        try {
            for (int i = 0; i < 20; i++) resolve("concurrent", identity);
        } catch (IOException error) {
            throw new AssertionError(error);
        }
    }

    private static FileSystem resolve(String authority, String identity) throws IOException {
        return CobbleFlinkFileSystemResolver.resolve(
                "reuse://" + authority + "/" + identity, options("identity", identity));
    }

    private static CobbleConnectorStorageOptions options(String key, String value) {
        return CobbleConnectorStorageOptions.fromStorageOptions(
                Collections.singletonMap("storage.option." + key, value));
    }

    private static PluginManager manager(FileSystemFactory factory) {
        return new PluginManager() {
            @Override
            public <P> Iterator<P> load(Class<P> service) {
                return service == FileSystemFactory.class
                        ? Collections.singletonList(service.cast(factory)).iterator()
                        : Collections.emptyIterator();
            }
        };
    }

    private static FileSystem unwrap(FileSystem fileSystem) {
        while (fileSystem instanceof WrappingProxy)
            fileSystem = (FileSystem) ((WrappingProxy<?>) fileSystem).getWrappedDelegate();
        return fileSystem;
    }

    private Path pluginJar() throws IOException {
        Path source = tempDir.resolve("FixtureFactory.java");
        String factory =
                "package fixture; public class FixtureFactory implements org.apache.flink.core.fs.FileSystemFactory {"
                        + "public String getScheme(){return \"fixture\";} public void configure(org.apache.flink.configuration.Configuration c){}"
                        + "public org.apache.flink.core.fs.FileSystem create(java.net.URI u){return new FixtureFs();}"
                        + "public static class FixtureFs extends org.apache.flink.core.fs.local.LocalFileSystem {"
                        + "public boolean exists(org.apache.flink.core.fs.Path p){return true;}}}";
        Files.write(source, factory.getBytes(StandardCharsets.UTF_8));
        Path classes = Files.createDirectories(tempDir.resolve("classes"));
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler);
        assertEquals(
                0,
                compiler.run(
                        null,
                        null,
                        null,
                        "-classpath",
                        System.getProperty("java.class.path"),
                        "-d",
                        classes.toString(),
                        source.toString()));
        Path jar = tempDir.resolve("fixture.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar));
                Stream<Path> files = Files.walk(classes)) {
            for (Path file : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
                output.putNextEntry(new JarEntry(classes.relativize(file).toString()));
                Files.copy(file, output);
                output.closeEntry();
            }
            output.putNextEntry(
                    new JarEntry("META-INF/services/" + FileSystemFactory.class.getName()));
            output.write("fixture.FixtureFactory\n".getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
        return jar;
    }

    private static class RecordingFactory implements FileSystemFactory {
        private Configuration configuration;
        private final AtomicInteger creates = new AtomicInteger();
        private final AtomicInteger probes = new AtomicInteger();
        private volatile ClassLoader creationContext;
        private volatile CountDownLatch probeEntries;
        private volatile CountDownLatch probeRelease;

        @Override
        public String getScheme() {
            return "reuse";
        }

        @Override
        public void configure(Configuration configuration) {
            this.configuration = new Configuration(configuration);
        }

        @Override
        public FileSystem create(URI uri) throws IOException {
            creates.incrementAndGet();
            creationContext = Thread.currentThread().getContextClassLoader();
            if (configuration.getString("create-fail", "false").equals("true"))
                throw new IOException("create failure");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            Configuration captured = new Configuration(configuration);
            return new LocalFileSystem() {
                @Override
                public boolean exists(org.apache.flink.core.fs.Path path) throws IOException {
                    probes.incrementAndGet();
                    if (probeEntries != null) {
                        probeEntries.countDown();
                        try {
                            if (!probeRelease.await(5, TimeUnit.SECONDS))
                                throw new IOException("probe timed out");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                    }
                    if (captured.getString("probe-fail", "false").equals("true"))
                        throw new IOException("probe failure");
                    if (!path.getName().equals(captured.getString("identity", "default")))
                        throw new IOException("wrong scoped identity");
                    return true;
                }
            };
        }
    }
}
