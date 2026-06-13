package io.cobble.flink.monitor;

import io.cobble.Config;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

final class CobbleReaderConfigs {
    private static final String DEFAULT_S3_REGION = "us-east-1";

    private CobbleReaderConfigs() {}

    static Config base(int totalBuckets) {
        Config cobbleConfig = new Config().numColumns(1).totalBuckets(totalBuckets);
        cobbleConfig.snapshotRetention = null;
        return cobbleConfig;
    }

    static void addVolume(Config cobbleConfig, String volumeDirectory, Configuration flinkConfig) {
        Config.VolumeDescriptor volume =
                Config.VolumeDescriptor.singleVolume(normalizeStorageScheme(volumeDirectory));
        applyFlinkVolumeOptions(volume, volume.baseDir, flinkConfig);
        cobbleConfig.addVolume(volume);
    }

    private static void applyFlinkVolumeOptions(
            Config.VolumeDescriptor volume, String normalizedDirectory, Configuration flinkConfig) {
        if (volume == null || normalizedDirectory == null || flinkConfig == null) {
            return;
        }
        String scheme = new Path(normalizedDirectory).toUri().getScheme();
        if (scheme == null) {
            return;
        }

        Map<String, String> raw = flinkConfig.toMap();
        switch (scheme.toLowerCase(Locale.ROOT)) {
            case "s3":
                applyS3Options(volume, normalizedDirectory, raw);
                break;
            case "oss":
                applyOssOptions(volume, raw);
                break;
            default:
                break;
        }
    }

    private static void applyS3Options(
            Config.VolumeDescriptor volume, String normalizedDirectory, Map<String, String> raw) {
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
        URI uri = new Path(normalizedDirectory).toUri();
        if (endpoint != null && !hasQueryOption(uri, "endpoint")) {
            customOptions.put("endpoint", normalizeEndpoint(endpoint));
        }

        if (pathStyleAccess != null) {
            customOptions.put(
                    "enable_virtual_host_style",
                    Boolean.parseBoolean(pathStyleAccess) ? "false" : "true");
        } else if (endpoint != null && isLikelyLocalEndpoint(endpoint)) {
            customOptions.put("enable_virtual_host_style", "false");
        }

        if (region != null) {
            customOptions.put("region", region);
        } else if (endpoint != null && !hasQueryOption(uri, "region")) {
            customOptions.put("region", DEFAULT_S3_REGION);
        }

        if ((accessId != null || secretKey != null)
                && !hasQueryOption(uri, "disable_config_load")) {
            customOptions.put("disable_config_load", "true");
        }
        if ((accessId != null || secretKey != null)
                && !hasQueryOption(uri, "disable_ec2_metadata")) {
            customOptions.put("disable_ec2_metadata", "true");
        }

        mergeCustomOptions(volume, customOptions);
    }

    private static void applyOssOptions(Config.VolumeDescriptor volume, Map<String, String> raw) {
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

    private static boolean hasQueryOption(URI uri, String key) {
        if (uri.getQuery() == null || uri.getQuery().isEmpty()) {
            return false;
        }
        String prefix = key + '=';
        for (String queryPart : uri.getQuery().split("&")) {
            if (queryPart.equals(key) || queryPart.startsWith(prefix)) {
                return true;
            }
        }
        return false;
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

    private static String normalizeStorageScheme(String directory) {
        URI uri = URI.create(directory);
        String scheme = uri.getScheme();
        if (scheme == null) {
            return directory;
        }
        String normalizedScheme = scheme.toLowerCase(Locale.ROOT);
        if ("s3a".equals(normalizedScheme) || "s3p".equals(normalizedScheme)) {
            normalizedScheme = "s3";
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
            throw new InputException("failed to normalize storage path: " + directory);
        }
    }
}
