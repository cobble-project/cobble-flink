package io.cobble.flink.monitor;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves user-supplied jar paths into a {@link URLClassLoader} used as the thread context
 * classloader so that {@code StateInspectDecoder} can restore user serializers (POJO, Avro, custom)
 * referenced by {@link io.cobble.flink.common.inspect.SerializerInspectSchema}.
 *
 * <p>Paths may be individual jar files or directories (directories expand to their {@code .jar}
 * children, sorted by filename for deterministic ordering). Duplicate canonical paths are
 * de-duplicated while preserving first-seen order.
 */
final class UserClasspath implements AutoCloseable {

    private final List<String> entries;
    private final URLClassLoader classLoader;

    private UserClasspath(List<String> entries, URLClassLoader classLoader) {
        this.entries = entries;
        this.classLoader = classLoader;
    }

    /** Empty classpath backed by the monitor's own classloader (no user jars). */
    static UserClasspath empty() {
        ClassLoader parent = CobbleFlinkMonitorServer.class.getClassLoader();
        URLClassLoader loader = new URLClassLoader(new URL[0], parent);
        return new UserClasspath(Collections.emptyList(), loader);
    }

    /**
     * Build a user classpath from the given paths. Each path is either a jar file or a directory
     * (whose {@code .jar} children are added, sorted by name).
     *
     * @throws InputException if any path does not exist.
     */
    static UserClasspath create(List<String> paths) {
        if (paths == null || paths.isEmpty()) {
            return empty();
        }
        Map<String, URL> resolved = new LinkedHashMap<>();
        for (String raw : paths) {
            String path = raw == null ? null : raw.trim();
            if (path == null || path.isEmpty()) {
                continue;
            }
            File file = new File(path);
            if (!file.exists()) {
                throw new InputException("--user-jar path does not exist: " + path);
            }
            if (file.isDirectory()) {
                File[] children = file.listFiles((dir, name) -> name.endsWith(".jar"));
                if (children == null) {
                    throw new InputException("cannot read directory: " + path);
                }
                List<File> sorted = new ArrayList<>(Arrays.asList(children));
                sorted.sort(Comparator.comparing(File::getName));
                for (File child : sorted) {
                    addResolved(resolved, child);
                }
            } else {
                addResolved(resolved, file);
            }
        }
        URL[] urls = resolved.values().toArray(new URL[0]);
        List<String> entryPaths = new ArrayList<>(resolved.keySet());
        ClassLoader parent = CobbleFlinkMonitorServer.class.getClassLoader();
        URLClassLoader loader = new URLClassLoader(urls, parent);
        return new UserClasspath(Collections.unmodifiableList(entryPaths), loader);
    }

    private static void addResolved(Map<String, URL> resolved, File file) {
        try {
            String canonical = file.getCanonicalPath();
            if (resolved.containsKey(canonical)) {
                return;
            }
            resolved.put(canonical, file.toURI().toURL());
        } catch (IOException e) {
            throw new InputException(
                    "cannot resolve --user-jar path: " + file + ": " + e.getMessage());
        }
    }

    List<String> entries() {
        return entries;
    }

    ClassLoader classLoader() {
        return classLoader;
    }

    @Override
    public void close() {
        try {
            classLoader.close();
        } catch (IOException ignored) {
            // Closing a URLClassLoader is best-effort during shutdown.
        }
    }
}
