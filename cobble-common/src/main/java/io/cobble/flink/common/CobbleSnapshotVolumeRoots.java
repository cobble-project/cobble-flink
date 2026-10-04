package io.cobble.flink.common;

import io.cobble.Config;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/** Path-only storage routes retained by checkpoints while native adoption runs asynchronously. */
public final class CobbleSnapshotVolumeRoots {
    private CobbleSnapshotVolumeRoots() {}

    public static List<String> fromConfig(Config config) {
        if (config.volumes == null) {
            return Collections.emptyList();
        }
        List<String> roots = new ArrayList<>();
        for (Config.VolumeDescriptor volume : config.volumes) {
            if (volume.kinds.contains(Config.VolumeUsageKind.SNAPSHOT)
                    || volume.kinds.contains(Config.VolumeUsageKind.READONLY)) {
                roots.add(pathOnly(volume.baseDir));
            }
        }
        return unique(roots);
    }

    public static List<String> unique(List<String> roots) {
        Set<String> result = new LinkedHashSet<>();
        for (String root : roots) {
            result.add(pathOnly(root));
        }
        return Collections.unmodifiableList(new ArrayList<>(result));
    }

    public static void addReadonlyVolumes(
            List<Config.VolumeDescriptor> volumes,
            List<String> roots,
            Consumer<Config.VolumeDescriptor> configureCredentials) {
        Set<String> configured = new LinkedHashSet<>();
        for (Config.VolumeDescriptor volume : volumes) {
            configured.add(pathOnly(volume.baseDir));
        }
        for (String root : unique(roots)) {
            if (!configured.add(root)) {
                continue;
            }
            Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
            volume.baseDir = root;
            volume.kinds = Collections.singletonList(Config.VolumeUsageKind.READONLY);
            configureCredentials.accept(volume);
            volumes.add(volume);
        }
    }

    /**
     * Resolves credentials from the caller's current storage routes, never from a checkpoint.
     * Default Config volumes are initialized only when there are roots to add.
     */
    public static void addReadonlyVolumes(Config config, List<String> roots) {
        addReadonlyVolumes(config, roots, CobbleFlinkStorageConfig.empty());
    }

    public static void addReadonlyVolumes(
            Config config, List<String> roots, CobbleFlinkStorageConfig storage) {
        if (roots.isEmpty()) return;
        if (config.volumes == null) config.volumes = new ArrayList<>();
        CobbleFlinkStorageConfig current = storage.withRoutes(config);
        addReadonlyVolumes(config.volumes, roots, current::fill);
    }

    private static String pathOnly(String root) {
        if (root == null || root.trim().isEmpty()) {
            throw new IllegalArgumentException("Checkpoint volume root must not be empty");
        }
        URI uri = URI.create(root.trim());
        try {
            String authority = uri.getAuthority();
            if (authority != null && uri.getUserInfo() != null) {
                authority = authority.substring(authority.lastIndexOf('@') + 1);
            }
            uri = new URI(uri.getScheme(), authority, uri.getPath(), null, null).normalize();
            String normalized;
            if (uri.getScheme() == null) {
                normalized =
                        Paths.get(uri.getPath()).toAbsolutePath().normalize().toUri().toString();
            } else if ("file".equalsIgnoreCase(uri.getScheme())) {
                normalized = Paths.get(uri).toAbsolutePath().normalize().toUri().toString();
            } else {
                normalized = uri.toString();
            }
            while (normalized.endsWith("/") && URI.create(normalized).getPath().length() > 1) {
                normalized = normalized.substring(0, normalized.length() - 1);
            }
            return normalized;
        } catch (URISyntaxException error) {
            throw new IllegalArgumentException("Invalid checkpoint volume root", error);
        }
    }
}
