package io.cobble.flink.inspect.internal;

import org.apache.flink.core.fs.Path;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/**
 * Shared path helpers used by monitor resolver classes.
 *
 * <p>These are shared by the SDK's catalog and schema resolvers.
 */
public final class InspectPathUtils {

    private InspectPathUtils() {}

    public static String normalizeStorageDirectory(String directory) {
        if (directory == null || directory.trim().isEmpty()) {
            throw new InspectInputException("storage directory must not be blank");
        }
        URI uri = URI.create(directory);
        String scheme = uri.getScheme();
        if (scheme == null || scheme.trim().isEmpty()) {
            return normalizeLocalPath(new File(directory));
        }
        String normalizedScheme = normalizeCheckpointScheme(scheme);
        if ("file".equals(normalizedScheme)) {
            return normalizeLocalPath(new File(uri));
        }
        if (normalizedScheme.equals(scheme.toLowerCase(Locale.ROOT))) {
            return directory;
        }
        try {
            return new URI(
                            normalizedScheme,
                            uri.getAuthority(),
                            uri.getPath(),
                            uri.getQuery(),
                            uri.getFragment())
                    .toString();
        } catch (URISyntaxException e) {
            throw new InspectInputException("failed to normalize storage directory: " + directory);
        }
    }

    public static String normalizeLocalPath(File file) {
        try {
            String path = file.getAbsoluteFile().toPath().normalize().toString();
            path = path.replace(File.separatorChar, '/');
            if (!path.startsWith("/")) {
                path = "/" + path;
            }
            return new URI("file", "", path, null, null).toString();
        } catch (URISyntaxException e) {
            throw new InspectInputException("failed to normalize local path: " + file);
        }
    }

    public static String pathToStorageString(Path path) {
        // For file:// URIs, extract the local path portion.
        URI uri = path.toUri();
        String scheme = uri.getScheme();
        if (scheme == null || scheme.trim().isEmpty() || "file".equalsIgnoreCase(scheme)) {
            return new File(uri).getAbsoluteFile().toPath().normalize().toString();
        }
        return path.toString();
    }

    public static String normalizeCheckpointScheme(String scheme) {
        String normalized = scheme.toLowerCase(Locale.ROOT);
        if ("s3a".equals(normalized) || "s3p".equals(normalized)) {
            return "s3";
        }
        return normalized;
    }
}
