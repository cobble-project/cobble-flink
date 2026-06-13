package io.cobble.flink.monitor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

final class StaticWebResources {
    private static final String[] RESOURCE_PATHS = {"/index.html", "/app.js", "/style.css"};
    private static final Map<String, byte[]> RESOURCES = loadResources();

    private StaticWebResources() {}

    static byte[] read(String path) {
        return RESOURCES.get(path);
    }

    private static Map<String, byte[]> loadResources() {
        Map<String, byte[]> resources = new HashMap<>();
        for (String path : RESOURCE_PATHS) {
            resources.put(path, loadResource("/web" + path));
        }
        return Collections.unmodifiableMap(resources);
    }

    private static byte[] loadResource(String path) {
        try (InputStream input = StaticWebResources.class.getResourceAsStream(path)) {
            if (input == null) {
                throw new IllegalStateException("missing web resource: " + path);
            }
            return readAll(input);
        } catch (IOException e) {
            throw new IllegalStateException("failed to load web resource: " + path, e);
        }
    }

    private static byte[] readAll(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int read;
        while ((read = input.read(buffer)) >= 0) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }
}
