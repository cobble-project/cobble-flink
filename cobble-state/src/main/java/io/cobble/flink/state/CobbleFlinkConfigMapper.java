package io.cobble.flink.state;

import io.cobble.Config;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.Path;

import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Maps Flink-side configuration into the subset of Cobble config exposed by cobble-flink. */
final class CobbleFlinkConfigMapper {

    private static final String DEFAULT_S3_REGION = "us-east-1";

    private CobbleFlinkConfigMapper() {}

    static void applyExposedOptions(Config config, Configuration flinkConfig) {
        config.memtableType = parseMemtableType(flinkConfig.get(CobbleOptions.MEMTABLE_TYPE));
        config.compactionPolicy =
                parseCompactionPolicy(flinkConfig.get(CobbleOptions.COMPACTION_POLICY));
        config.sstBloomFilterEnabled = flinkConfig.get(CobbleOptions.SST_BLOOM_FILTER_ENABLED);
        config.sstBloomBitsPerKey =
                requirePositive(
                        CobbleOptions.SST_BLOOM_FILTER_BITS_PER_KEY.key(),
                        flinkConfig.get(CobbleOptions.SST_BLOOM_FILTER_BITS_PER_KEY));
        config.sstPartitionedIndex = flinkConfig.get(CobbleOptions.SST_PARTITIONED_INDEX_ENABLED);
        config.jniDirectBufferSize =
                toPositiveIntBytes(
                        CobbleOptions.DIRECT_IO_BUFFER_SIZE.key(),
                        flinkConfig.get(CobbleOptions.DIRECT_IO_BUFFER_SIZE));
        config.jniDirectBufferPoolSize =
                requirePositive(
                        CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE.key(),
                        flinkConfig.get(CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE));
        config.logMaxFileSize =
                toPositiveIntBytes(
                        CobbleOptions.LOG_MAX_FILE_SIZE.key(),
                        flinkConfig.get(CobbleOptions.LOG_MAX_FILE_SIZE));
        config.logKeepFiles =
                requirePositive(
                        CobbleOptions.LOG_KEEP_FILES.key(),
                        flinkConfig.get(CobbleOptions.LOG_KEEP_FILES));
        config.logLevel = normalizeLogLevel(flinkConfig.get(CobbleOptions.LOG_LEVEL));

        if (flinkConfig.contains(CobbleOptions.VALUE_SEPARATION_THRESHOLD)) {
            config.valueSeparationThreshold =
                    toPositiveIntBytes(
                            CobbleOptions.VALUE_SEPARATION_THRESHOLD.key(),
                            flinkConfig.get(CobbleOptions.VALUE_SEPARATION_THRESHOLD));
        }
        if (flinkConfig.contains(CobbleOptions.SNAPSHOT_RETENTION)) {
            config.snapshotRetention =
                    requirePositive(
                            CobbleOptions.SNAPSHOT_RETENTION.key(),
                            flinkConfig.get(CobbleOptions.SNAPSHOT_RETENTION));
        }
    }

    static void applyCheckpointVolumeOptions(
            Config.VolumeDescriptor checkpointVolume,
            String normalizedCheckpointDirectory,
            Configuration flinkConfig) {
        if (checkpointVolume == null || normalizedCheckpointDirectory == null) {
            return;
        }

        String scheme = new Path(normalizedCheckpointDirectory).toUri().getScheme();
        if (scheme == null) {
            return;
        }

        Map<String, String> raw = flinkConfig.toMap();
        switch (scheme.toLowerCase(Locale.ROOT)) {
            case "s3":
                applyS3CheckpointVolumeOptions(
                        checkpointVolume, normalizedCheckpointDirectory, raw);
                break;
            case "oss":
                applyOssCheckpointVolumeOptions(checkpointVolume, raw);
                break;
            default:
                break;
        }
    }

    private static void applyS3CheckpointVolumeOptions(
            Config.VolumeDescriptor checkpointVolume,
            String normalizedCheckpointDirectory,
            Map<String, String> raw) {
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
            checkpointVolume.accessId = accessId;
        }
        if (secretKey != null) {
            checkpointVolume.secretKey = secretKey;
        }

        Map<String, String> customOptions = new HashMap<>();
        URI checkpointUri = new Path(normalizedCheckpointDirectory).toUri();
        if (endpoint != null && !hasQueryOption(checkpointUri, "endpoint")) {
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
        } else if (endpoint != null && !hasQueryOption(checkpointUri, "region")) {
            customOptions.put("region", DEFAULT_S3_REGION);
        }

        if ((accessId != null || secretKey != null)
                && !hasQueryOption(checkpointUri, "disable_config_load")) {
            customOptions.put("disable_config_load", "true");
        }
        if ((accessId != null || secretKey != null)
                && !hasQueryOption(checkpointUri, "disable_ec2_metadata")) {
            customOptions.put("disable_ec2_metadata", "true");
        }

        mergeCustomOptions(checkpointVolume, customOptions);
    }

    private static void applyOssCheckpointVolumeOptions(
            Config.VolumeDescriptor checkpointVolume, Map<String, String> raw) {
        String accessId = firstNonBlank(raw, "fs.oss.accessKeyId");
        String secretKey = firstNonBlank(raw, "fs.oss.accessKeySecret");
        String endpoint = firstNonBlank(raw, "fs.oss.endpoint");

        if (accessId != null) {
            checkpointVolume.accessId = accessId;
        }
        if (secretKey != null) {
            checkpointVolume.secretKey = secretKey;
        }
        if (endpoint != null) {
            Map<String, String> customOptions = new HashMap<>();
            customOptions.put("endpoint", normalizeEndpoint(endpoint));
            mergeCustomOptions(checkpointVolume, customOptions);
        }
    }

    private static void mergeCustomOptions(
            Config.VolumeDescriptor checkpointVolume, Map<String, String> customOptions) {
        if (customOptions.isEmpty()) {
            return;
        }
        if (checkpointVolume.customOptions == null) {
            checkpointVolume.customOptions = new HashMap<>();
        }
        checkpointVolume.customOptions.putAll(customOptions);
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

    private static Config.MemtableType parseMemtableType(String memtableType) {
        if (memtableType == null) {
            return Config.MemtableType.HASH;
        }
        switch (memtableType.trim().toLowerCase(Locale.ROOT)) {
            case "hash":
                return Config.MemtableType.HASH;
            case "skiplist":
                return Config.MemtableType.SKIPLIST;
            case "vec":
                return Config.MemtableType.VEC;
            default:
                throw new IllegalConfigurationException(
                        "state.backend.cobble.memtable.type must be one of [hash, skiplist, vec], but was: "
                                + memtableType);
        }
    }

    private static Config.CompactionPolicyKind parseCompactionPolicy(String compactionPolicy) {
        if (compactionPolicy == null) {
            return Config.CompactionPolicyKind.ROUND_ROBIN;
        }
        switch (compactionPolicy.trim().toLowerCase(Locale.ROOT)) {
            case "round_robin":
                return Config.CompactionPolicyKind.ROUND_ROBIN;
            case "min_overlap":
                return Config.CompactionPolicyKind.MIN_OVERLAP;
            default:
                throw new IllegalConfigurationException(
                        "state.backend.cobble.compaction.policy must be one of [round_robin, min_overlap], but was: "
                                + compactionPolicy);
        }
    }

    private static String normalizeLogLevel(String logLevel) {
        if (logLevel == null) {
            return "INFO";
        }
        String normalized = logLevel.trim().toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "trace":
            case "debug":
            case "info":
            case "warn":
            case "error":
            case "off":
                return normalized.toUpperCase(Locale.ROOT);
            default:
                throw new IllegalConfigurationException(
                        "state.backend.cobble.log.level must be one of [trace, debug, info, warn, error, off], but was: "
                                + logLevel);
        }
    }

    private static int toPositiveIntBytes(String optionKey, MemorySize size) {
        long bytes = size.getBytes();
        if (bytes <= 0 || bytes > Integer.MAX_VALUE) {
            throw new IllegalConfigurationException(
                    optionKey + " must be in (0, " + Integer.MAX_VALUE + "] bytes");
        }
        return (int) bytes;
    }

    private static int requirePositive(String optionKey, int value) {
        if (value <= 0) {
            throw new IllegalConfigurationException(optionKey + " must be > 0");
        }
        return value;
    }
}
