package io.cobble.flink.common;

import io.cobble.Config;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Immutable, job-scoped provider defaults and current volume routes for Flink storage access. */
public final class CobbleFlinkStorageConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_S3_REGION = "us-east-1";
    private static final String[] KEYS = {
        "s3.access-key",
        "s3.access.key",
        "fs.s3a.access.key",
        "fs.s3a.access-key",
        "presto.s3.access-key",
        "presto.s3.access.key",
        "s3.secret-key",
        "s3.secret.key",
        "fs.s3a.secret.key",
        "fs.s3a.secret-key",
        "presto.s3.secret-key",
        "presto.s3.secret.key",
        "s3.endpoint",
        "fs.s3a.endpoint",
        "presto.s3.endpoint",
        "s3.path.style.access",
        "fs.s3a.path.style.access",
        "s3.region",
        "fs.s3a.endpoint.region",
        "fs.s3a.region",
        "fs.oss.accessKeyId",
        "fs.oss.accessKeySecret",
        "fs.oss.endpoint"
    };
    private static final CobbleFlinkStorageConfig EMPTY =
            new CobbleFlinkStorageConfig(Collections.emptyMap(), Collections.emptyList());
    private final Map<String, String> options;
    private final List<Route> routes;

    private CobbleFlinkStorageConfig(Map<String, String> options, List<Route> routes) {
        this.options = Collections.unmodifiableMap(new HashMap<>(options));
        this.routes = Collections.unmodifiableList(new ArrayList<>(routes));
    }

    public static CobbleFlinkStorageConfig empty() {
        return EMPTY;
    }

    /** Capture current provider keys only; never read credentials from checkpoint metadata. */
    public static CobbleFlinkStorageConfig from(ReadableConfig configuration) {
        Map<String, String> options = new HashMap<>();
        for (String key : KEYS) {
            configuration
                    .getOptional(ConfigOptions.key(key).stringType().noDefaultValue())
                    .ifPresent(value -> options.put(key, value));
        }
        return options.isEmpty()
                ? EMPTY
                : new CobbleFlinkStorageConfig(options, Collections.emptyList());
    }

    public boolean isEmpty() {
        return options.isEmpty() && routes.isEmpty();
    }

    /**
     * Copy only current storage routes; neither checkpoint metadata nor Config tuning is retained.
     */
    public CobbleFlinkStorageConfig withRoutes(Config config) {
        List<Route> combined = new ArrayList<>(routes);
        if (config.volumes != null) {
            for (Config.VolumeDescriptor volume : config.volumes) {
                combined.add(new Route(volume));
            }
        }
        return new CobbleFlinkStorageConfig(options, combined);
    }

    /**
     * Resolve runtime filesystem options without changing the original, user-visible DDL options.
     */
    public CobbleConnectorStorageOptions resolve(
            String path, CobbleConnectorStorageOptions explicit) {
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = path;
        explicit.applyTo(volume);
        fill(volume);
        return CobbleConnectorStorageOptions.fromVolume(
                volume.accessId, volume.secretKey, volume.customOptions);
    }

    public void register(Config config, Config.VolumeDescriptor volume) {
        withRoutes(config).fill(volume);
        config.addVolume(volume);
    }

    /**
     * Fill missing settings only. A credential identity is inherited as a pair, never field by
     * field.
     */
    public void fill(Config.VolumeDescriptor volume) {
        volume.baseDir = nativePath(volume.baseDir);
        URI uri = storageUri(volume.baseDir);
        if (uri.getScheme() == null || "file".equalsIgnoreCase(uri.getScheme())) return;
        if (volume.customOptions != null)
            volume.customOptions = new HashMap<>(volume.customOptions);
        copyQueryOptions(volume);
        Config.VolumeDescriptor defaults = new Config.VolumeDescriptor();
        defaults.baseDir = volume.baseDir;
        applyProviderDefaults(defaults, volume.baseDir);
        Route route = matchingRoute(volume);
        if (route != null) {
            Config.VolumeDescriptor routed = route.volume();
            mergeMissing(routed, defaults);
            defaults = routed;
        }
        mergeMissing(volume, defaults);
    }

    private static void copyQueryOptions(Config.VolumeDescriptor volume) {
        String query = storageUri(volume.baseDir).getRawQuery();
        if (query == null) return;
        if (volume.customOptions == null) volume.customOptions = new HashMap<>();
        for (String part : query.split("&")) {
            String[] pair = part.split("=", 2);
            volume.customOptions.putIfAbsent(
                    URLDecoder.decode(pair[0], StandardCharsets.UTF_8),
                    pair.length == 1 ? "" : URLDecoder.decode(pair[1], StandardCharsets.UTF_8));
        }
    }

    private Route matchingRoute(Config.VolumeDescriptor volume) {
        URI target = storageUri(volume.baseDir).normalize();
        Route match = null;
        int longest = -1;
        boolean ambiguous = false;
        for (Route route : routes) {
            URI candidate = storageUri(route.baseDir).normalize();
            if (!sameProvider(target, candidate) || !compatibleEndpoint(volume, route.volume()))
                continue;
            String prefix = candidate.getPath();
            String path = target.getPath();
            boolean contains =
                    Objects.equals(target.getAuthority(), candidate.getAuthority())
                            && (Objects.equals(path, prefix)
                                    || path.startsWith(
                                            prefix.endsWith("/") ? prefix : prefix + "/"));
            if (contains && prefix.length() > longest) {
                match = route;
                longest = prefix.length();
                ambiguous = false;
            } else if (contains && prefix.length() == longest && !route.sameSettings(match)) {
                ambiguous = true;
            }
        }
        if (ambiguous)
            throw new IllegalArgumentException(
                    "Ambiguous current storage routes; configure a volume containing the requested path.");
        if (match != null) return match;
        for (Route route : routes) {
            URI candidate = storageUri(route.baseDir);
            if (!sameProvider(target, candidate)
                    || !Objects.equals(target.getAuthority(), candidate.getAuthority())
                    || !compatibleEndpoint(volume, route.volume())) continue;
            if (match != null && !route.sameSettings(match)) {
                throw new IllegalArgumentException(
                        "Ambiguous current storage routes; configure a volume containing the requested path.");
            }
            match = route;
        }
        return match;
    }

    public static boolean containsPath(String base, String path) {
        URI root = storageUri(nativePath(base)).normalize();
        URI target = storageUri(nativePath(path)).normalize();
        if (!sameProvider(root, target)
                || !Objects.equals(root.getAuthority(), target.getAuthority())) return false;
        String prefix = root.getPath();
        return target.getPath().equals(prefix)
                || target.getPath().startsWith(prefix.endsWith("/") ? prefix : prefix + "/");
    }

    private static boolean sameProvider(URI first, URI second) {
        return first.getScheme().equalsIgnoreCase(second.getScheme());
    }

    private static URI storageUri(String path) {
        URI uri = path.contains("://") ? URI.create(path) : new Path(path).toUri();
        if (uri.getScheme() == null)
            return java.nio.file.Paths.get(path).toAbsolutePath().normalize().toUri();
        if ("file".equalsIgnoreCase(uri.getScheme()))
            return java.nio.file.Paths.get(uri).toAbsolutePath().normalize().toUri();
        return uri;
    }

    private static String option(Config.VolumeDescriptor volume, String key) {
        return volume.customOptions == null ? null : volume.customOptions.get(key);
    }

    private static boolean compatibleEndpoint(
            Config.VolumeDescriptor target, Config.VolumeDescriptor source) {
        String endpoint = option(target, "endpoint");
        return endpoint == null || Objects.equals(endpoint, option(source, "endpoint"));
    }

    private static void mergeMissing(
            Config.VolumeDescriptor target, Config.VolumeDescriptor source) {
        String access = credential(target, true);
        String secret = credential(target, false);
        String scheme = storageUri(target.baseDir).getScheme();
        boolean pairedCredentials = "s3".equalsIgnoreCase(scheme) || "oss".equalsIgnoreCase(scheme);
        if (pairedCredentials && (access == null) != (secret == null)) {
            throw new IllegalArgumentException(
                    "Explicit storage credentials require both access and secret keys.");
        }
        if (access != null) {
            target.accessId = access;
            target.secretKey = secret;
        }
        boolean compatible = compatibleEndpoint(target, source);
        boolean inheritIdentity =
                access == null
                        && secret == null
                        && option(target, "session_token") == null
                        && compatible;
        if (inheritIdentity) {
            String inheritedAccess = credential(source, true);
            String inheritedSecret = credential(source, false);
            if (pairedCredentials && (inheritedAccess == null) != (inheritedSecret == null)) {
                throw new IllegalArgumentException(
                        "Inherited storage credentials require both access and secret keys.");
            }
            target.accessId = inheritedAccess;
            target.secretKey = inheritedSecret;
        }
        if ("oss".equalsIgnoreCase(scheme) && target.accessId != null && target.secretKey != null) {
            if (target.customOptions == null) target.customOptions = new HashMap<>();
            target.customOptions.putIfAbsent("access_key_id", target.accessId);
            target.customOptions.putIfAbsent("access_key_secret", target.secretKey);
        }
        if (source.customOptions != null && !source.customOptions.isEmpty()) {
            if (target.customOptions == null) target.customOptions = new HashMap<>();
            for (Map.Entry<String, String> entry : source.customOptions.entrySet()) {
                String key = entry.getKey();
                if (pairedCredentials
                        && ("access_key_id".equals(key)
                                || "secret_access_key".equals(key)
                                || "access_id".equals(key)
                                || "access_key".equals(key)
                                || "access_key_secret".equals(key))) continue;
                if ("s3".equalsIgnoreCase(scheme)
                        && "session_token".equals(key)
                        && !inheritIdentity) continue;
                if (!compatible
                        && ("region".equals(key)
                                || "enable_virtual_host_style".equals(key)
                                || "disable_config_load".equals(key)
                                || "disable_ec2_metadata".equals(key))) continue;
                target.customOptions.putIfAbsent(key, entry.getValue());
            }
        }
    }

    private static String credential(Config.VolumeDescriptor volume, boolean access) {
        String field = access ? volume.accessId : volume.secretKey;
        String scheme = storageUri(volume.baseDir).getScheme();
        if (!"s3".equalsIgnoreCase(scheme) && !"oss".equalsIgnoreCase(scheme)) return field;
        String canonical = option(volume, access ? "access_key_id" : "secret_access_key");
        if (!access && "oss".equalsIgnoreCase(scheme)) {
            canonical = option(volume, "access_key_secret");
        }
        return canonical != null
                ? canonical
                : field != null ? field : option(volume, access ? "access_id" : "access_key");
    }

    private static final class Route implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String baseDir;
        private final String accessId;
        private final String secretKey;
        private final Map<String, String> customOptions;

        private Route(Config.VolumeDescriptor volume) {
            baseDir = nativePath(volume.baseDir);
            accessId = volume.accessId;
            secretKey = volume.secretKey;
            Config.VolumeDescriptor snapshot = new Config.VolumeDescriptor();
            snapshot.baseDir = baseDir;
            snapshot.customOptions =
                    volume.customOptions == null
                            ? new HashMap<>()
                            : new HashMap<>(volume.customOptions);
            copyQueryOptions(snapshot);
            customOptions = Collections.unmodifiableMap(snapshot.customOptions);
        }

        private Config.VolumeDescriptor volume() {
            Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
            volume.baseDir = baseDir;
            volume.accessId = accessId;
            volume.secretKey = secretKey;
            volume.customOptions = new HashMap<>(customOptions);
            return volume;
        }

        private boolean sameSettings(Route other) {
            return Objects.equals(accessId, other.accessId)
                    && Objects.equals(secretKey, other.secretKey)
                    && Objects.equals(customOptions, other.customOptions);
        }
    }

    /** Match the native S3 spelling already used by the checkpoint writer. */
    public static String nativePath(String path) {
        String scheme = storageUri(path).getScheme();
        if ("s3".equalsIgnoreCase(scheme)
                || "s3a".equalsIgnoreCase(scheme)
                || "s3p".equalsIgnoreCase(scheme)) {
            return "s3" + path.substring(path.indexOf(':'));
        }
        return path;
    }

    private void applyProviderDefaults(Config.VolumeDescriptor volume, String directory) {
        if (volume == null || directory == null) {
            return;
        }

        directory = nativePath(directory);
        String scheme = storageUri(directory).getScheme();
        if (scheme == null) {
            return;
        }

        Map<String, String> raw = options;
        switch (scheme.toLowerCase(Locale.ROOT)) {
            case "s3":
                applyS3Defaults(volume, raw);
                break;
            case "oss":
                applyOssDefaults(volume, raw);
                break;
            default:
                break;
        }
    }

    private static void applyS3Defaults(Config.VolumeDescriptor volume, Map<String, String> raw) {
        String accessId =
                firstNonBlank(
                        raw,
                        "s3.access-key",
                        "s3.access.key",
                        "fs.s3a.access.key",
                        "fs.s3a.access-key",
                        "presto.s3.access-key",
                        "presto.s3.access.key");
        String secretKey =
                firstNonBlank(
                        raw,
                        "s3.secret-key",
                        "s3.secret.key",
                        "fs.s3a.secret.key",
                        "fs.s3a.secret-key",
                        "presto.s3.secret-key",
                        "presto.s3.secret.key");
        String endpoint =
                firstNonBlank(raw, "s3.endpoint", "fs.s3a.endpoint", "presto.s3.endpoint");
        String pathStyleAccess =
                firstNonBlank(raw, "s3.path.style.access", "fs.s3a.path.style.access");
        String region = firstNonBlank(raw, "s3.region", "fs.s3a.endpoint.region", "fs.s3a.region");

        if (accessId != null) {
            volume.accessId = accessId;
        }
        if (secretKey != null) {
            volume.secretKey = secretKey;
        }

        Map<String, String> customOptions = new HashMap<>();
        if (endpoint != null) {
            customOptions.put("endpoint", normalizeEndpoint(endpoint));
        }

        boolean explicitPathStyle = pathStyleAccess != null;
        if (explicitPathStyle) {
            customOptions.put(
                    "enable_virtual_host_style",
                    Boolean.parseBoolean(pathStyleAccess) ? "false" : "true");
        } else if (endpoint != null && isLikelyLocalEndpoint(endpoint)) {
            customOptions.put("enable_virtual_host_style", "false");
        }

        if (region != null) {
            customOptions.put("region", region);
        } else if (endpoint != null) {
            customOptions.put("region", DEFAULT_S3_REGION);
        }

        if (accessId != null || secretKey != null) {
            customOptions.put("disable_config_load", "true");
            customOptions.put("disable_ec2_metadata", "true");
        }

        mergeCustomOptions(volume, customOptions);
    }

    private static void applyOssDefaults(Config.VolumeDescriptor volume, Map<String, String> raw) {
        String accessId = firstNonBlank(raw, "fs.oss.accessKeyId");
        String secretKey = firstNonBlank(raw, "fs.oss.accessKeySecret");
        String endpoint = firstNonBlank(raw, "fs.oss.endpoint");

        if (accessId != null) {
            volume.accessId = accessId;
        }
        if (secretKey != null) {
            volume.secretKey = secretKey;
        }
        if (endpoint != null) {
            Map<String, String> customOptions = new HashMap<>();
            customOptions.put("endpoint", normalizeEndpoint(endpoint));
            mergeCustomOptions(volume, customOptions);
        }
    }

    private static void mergeCustomOptions(
            Config.VolumeDescriptor volume, Map<String, String> customOptions) {
        if (customOptions.isEmpty()) {
            return;
        }
        if (volume.customOptions == null) {
            volume.customOptions = new HashMap<>();
        }
        volume.customOptions.putAll(customOptions);
    }

    private static String firstNonBlank(Map<String, String> raw, String... keys) {
        for (String key : keys) {
            String value = raw.get(key);
            if (value != null && !value.trim().isEmpty()) {
                return value.trim();
            }
        }
        return null;
    }

    private static String normalizeEndpoint(String endpoint) {
        String trimmed = endpoint.trim();
        if (trimmed.contains("://")) {
            return trimmed;
        }
        if (isLikelyLocalEndpoint(trimmed)) {
            return "http://" + trimmed;
        }
        return "https://" + trimmed;
    }

    private static boolean isLikelyLocalEndpoint(String endpoint) {
        String normalized = endpoint;
        int schemeSeparator = normalized.indexOf("://");
        if (schemeSeparator >= 0) {
            normalized = normalized.substring(schemeSeparator + 3);
        }
        int slashIndex = normalized.indexOf('/');
        if (slashIndex >= 0) {
            normalized = normalized.substring(0, slashIndex);
        }
        String lower = normalized.toLowerCase(Locale.ROOT);
        return lower.startsWith("127.")
                || lower.startsWith("localhost")
                || lower.startsWith("0.0.0.0")
                || lower.startsWith("host.docker.internal");
    }
}
