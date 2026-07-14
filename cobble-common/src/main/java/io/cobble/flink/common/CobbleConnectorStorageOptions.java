package io.cobble.flink.common;

import io.cobble.Config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Serializable connector-scoped provider options for a single Cobble table root.
 *
 * <p>Every key below {@code storage.option.} is provider-defined. Cobble removes only that prefix
 * and forwards the remaining key unchanged; it does not maintain a filesystem or option-key
 * allowlist.
 */
public final class CobbleConnectorStorageOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String STORAGE_OPTION_PREFIX = "storage.option.";
    public static final String S3_ENDPOINT_KEY = "s3.endpoint";
    public static final String S3_ACCESS_KEY = "s3.access-key";
    public static final String S3_ACCESS_KEY_ALIAS = "s3.access.key";
    public static final String S3_SECRET_KEY = "s3.secret-key";
    public static final String S3_SECRET_KEY_ALIAS = "s3.secret.key";
    public static final String S3_PATH_STYLE_ACCESS_KEY = "s3.path.style.access";
    public static final String S3_REGION_KEY = "s3.region";

    private static final String STORAGE_OPTION_KEY = "storage.option";
    private static final String ENDPOINT_OPTION = "endpoint";
    private static final String REGION_OPTION = "region";
    private static final String VIRTUAL_HOST_STYLE_OPTION = "enable_virtual_host_style";
    private static final String ACCESS_KEY_OPTION = "access_key_id";
    private static final String SECRET_KEY_OPTION = "secret_access_key";

    private static final ConfigOption<Map<String, String>> STORAGE_OPTIONS =
            ConfigOptions.key(STORAGE_OPTION_KEY)
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Arbitrary provider options for connector table storage access. The"
                                    + " suffix after storage.option. is forwarded unchanged.");

    private static final ConfigOption<String> S3_ENDPOINT =
            ConfigOptions.key(S3_ENDPOINT_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3-compatible endpoint for connector table storage access.");

    private static final ConfigOption<String> S3_ACCESS =
            ConfigOptions.key(S3_ACCESS_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3 access key for connector table storage access.");

    private static final ConfigOption<String> S3_ACCESS_ALIAS =
            ConfigOptions.key(S3_ACCESS_KEY_ALIAS)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Paimon-compatible alias for connector table S3 access key.");

    private static final ConfigOption<String> S3_SECRET =
            ConfigOptions.key(S3_SECRET_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3 secret key for connector table storage access.");

    private static final ConfigOption<String> S3_SECRET_ALIAS =
            ConfigOptions.key(S3_SECRET_KEY_ALIAS)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Paimon-compatible alias for connector table S3 secret key.");

    private static final ConfigOption<Boolean> S3_PATH_STYLE_ACCESS =
            ConfigOptions.key(S3_PATH_STYLE_ACCESS_KEY)
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Use path-style rather than virtual-host-style connector table S3 access.");

    private static final ConfigOption<String> S3_REGION =
            ConfigOptions.key(S3_REGION_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3 region for connector table storage access.");

    private static final Set<String> ALIAS_KEYS = aliasKeys();
    private static final CobbleConnectorStorageOptions EMPTY =
            new CobbleConnectorStorageOptions(
                    null, null, Collections.emptyMap(), Collections.emptyMap());

    private final String s3AccessId;
    private final String s3SecretKey;
    private final Map<String, String> genericOptions;
    private final Map<String, String> s3Options;

    private CobbleConnectorStorageOptions(
            String s3AccessId,
            String s3SecretKey,
            Map<String, String> genericOptions,
            Map<String, String> s3Options) {
        this.s3AccessId = s3AccessId;
        this.s3SecretKey = s3SecretKey;
        this.genericOptions =
                Collections.unmodifiableMap(new HashMap<String, String>(genericOptions));
        this.s3Options = Collections.unmodifiableMap(new HashMap<String, String>(s3Options));
    }

    public static CobbleConnectorStorageOptions empty() {
        return EMPTY;
    }

    /** Adds all accepted connector table options to a source or sink factory option set. */
    public static void addFactoryOptions(Set<ConfigOption<?>> options) {
        options.add(STORAGE_OPTIONS);
        options.add(S3_ENDPOINT);
        options.add(S3_ACCESS);
        options.add(S3_ACCESS_ALIAS);
        options.add(S3_SECRET);
        options.add(S3_SECRET_ALIAS);
        options.add(S3_PATH_STYLE_ACCESS);
        options.add(S3_REGION);
    }

    /** Parses storage options from a table option map, ignoring unrelated connector options. */
    public static CobbleConnectorStorageOptions from(Map<String, String> tableOptions) {
        return parse(tableOptions, false);
    }

    /** Parses a map that is expected to contain storage options only. */
    public static CobbleConnectorStorageOptions fromStorageOptions(
            Map<String, String> storageOptions) {
        return parse(storageOptions, true);
    }

    private static CobbleConnectorStorageOptions parse(
            Map<String, String> options, boolean rejectUnrelatedKeys) {
        if (options == null) {
            throw new IllegalArgumentException("Storage options must not be null.");
        }
        if (options.containsKey(STORAGE_OPTION_KEY)) {
            throw new IllegalArgumentException(
                    "storage.option is not supported directly; use storage.option.<name>.");
        }
        if (rejectUnrelatedKeys) {
            for (String key : options.keySet()) {
                if (!ALIAS_KEYS.contains(key) && !key.startsWith(STORAGE_OPTION_PREFIX)) {
                    throw new IllegalArgumentException("Unknown connector storage option: " + key);
                }
            }
        }

        Map<String, String> generic = genericOptions(options);
        Map<String, String> s3 = new HashMap<String, String>();
        String s3Access =
                resolveAlias(options, S3_ACCESS_KEY, S3_ACCESS_KEY_ALIAS, "S3 access key");
        String s3Secret =
                resolveAlias(options, S3_SECRET_KEY, S3_SECRET_KEY_ALIAS, "S3 secret key");
        boolean hasS3CredentialAlias = s3Access != null || s3Secret != null;
        mergeAlias(generic, s3, ACCESS_KEY_OPTION, s3Access, S3_ACCESS_KEY);
        mergeAlias(generic, s3, SECRET_KEY_OPTION, s3Secret, S3_SECRET_KEY);
        if (hasS3CredentialAlias
                && (effectiveOption(generic, s3, ACCESS_KEY_OPTION) == null
                        || effectiveOption(generic, s3, SECRET_KEY_OPTION) == null)) {
            throw new IllegalArgumentException(
                    "Explicit S3 credentials require both access and secret keys.");
        }

        String endpoint = normalizedOptional(options.get(S3_ENDPOINT_KEY));
        String mappedEndpoint = endpoint == null ? null : normalizeEndpoint(endpoint);
        mergeAlias(generic, s3, ENDPOINT_OPTION, mappedEndpoint, S3_ENDPOINT_KEY);
        mergeAlias(
                generic,
                s3,
                REGION_OPTION,
                normalizedOptional(options.get(S3_REGION_KEY)),
                S3_REGION_KEY);

        String pathStyle = normalizedOptional(options.get(S3_PATH_STYLE_ACCESS_KEY));
        if (pathStyle != null
                && !"true".equalsIgnoreCase(pathStyle)
                && !"false".equalsIgnoreCase(pathStyle)) {
            throw new IllegalArgumentException(
                    S3_PATH_STYLE_ACCESS_KEY + " must be either true or false.");
        }
        mergeAlias(
                generic,
                s3,
                VIRTUAL_HOST_STYLE_OPTION,
                pathStyle == null ? null : Boolean.toString(!Boolean.parseBoolean(pathStyle)),
                S3_PATH_STYLE_ACCESS_KEY);
        if (mappedEndpoint != null && effectiveOption(generic, s3, REGION_OPTION) == null) {
            s3.put(REGION_OPTION, "us-east-1");
        }

        if (generic.isEmpty() && s3.isEmpty()) {
            return EMPTY;
        }
        return new CobbleConnectorStorageOptions(
                hasS3CredentialAlias ? effectiveOption(generic, s3, ACCESS_KEY_OPTION) : null,
                hasS3CredentialAlias ? effectiveOption(generic, s3, SECRET_KEY_OPTION) : null,
                generic,
                s3);
    }

    static CobbleConnectorStorageOptions fromVolume(
            String accessId, String secretKey, Map<String, String> customOptions) {
        Map<String, String> generic =
                customOptions == null
                        ? Collections.emptyMap()
                        : new HashMap<String, String>(customOptions);
        return new CobbleConnectorStorageOptions(
                accessId, secretKey, generic, Collections.<String, String>emptyMap());
    }

    Configuration flinkConfiguration(String pathUri) {
        Configuration configuration = new Configuration();
        for (Map.Entry<String, String> entry : genericOptions.entrySet()) {
            configuration.setString(entry.getKey(), entry.getValue());
        }
        if (isS3Path(pathUri)) {
            copyToFlink(configuration, S3_ENDPOINT_KEY, effectiveStorageOption(ENDPOINT_OPTION));
            copyToFlink(configuration, S3_REGION_KEY, effectiveStorageOption(REGION_OPTION));
            copyToFlink(configuration, S3_ACCESS_KEY, s3AccessId);
            copyToFlink(configuration, S3_SECRET_KEY, s3SecretKey);
            String virtualHostStyle = effectiveStorageOption(VIRTUAL_HOST_STYLE_OPTION);
            if (virtualHostStyle != null) {
                configuration.setString(
                        S3_PATH_STYLE_ACCESS_KEY,
                        Boolean.toString(!Boolean.parseBoolean(virtualHostStyle)));
            }
        }
        return configuration;
    }

    boolean hasExplicitOptions() {
        return s3AccessId != null
                || s3SecretKey != null
                || !genericOptions.isEmpty()
                || !s3Options.isEmpty();
    }

    private String effectiveStorageOption(String name) {
        String generic = genericOptions.get(name);
        return generic != null ? generic : s3Options.get(name);
    }

    private static void copyToFlink(Configuration configuration, String key, String value) {
        if (value != null) {
            configuration.setString(key, value);
        }
    }

    private static Map<String, String> genericOptions(Map<String, String> options) {
        Map<String, String> generic = new HashMap<String, String>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (!entry.getKey().startsWith(STORAGE_OPTION_PREFIX)) {
                continue;
            }
            String name = entry.getKey().substring(STORAGE_OPTION_PREFIX.length());
            if (name.isEmpty()) {
                throw new IllegalArgumentException("Storage option name must not be empty.");
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException(
                        "Storage option " + entry.getKey() + " must have a value.");
            }
            generic.put(name, entry.getValue());
        }
        return generic;
    }

    private static void mergeAlias(
            Map<String, String> generic,
            Map<String, String> s3,
            String optionName,
            String aliasValue,
            String aliasKey) {
        if (aliasValue == null) {
            return;
        }
        String existing = generic.get(optionName);
        if (existing != null && !existing.equals(aliasValue)) {
            throw new IllegalArgumentException(
                    "Conflicting connector storage options: storage.option."
                            + optionName
                            + " and "
                            + aliasKey);
        }
        if (existing == null) {
            s3.put(optionName, aliasValue);
        }
    }

    private static String effectiveOption(
            Map<String, String> generic, Map<String, String> s3, String optionName) {
        String value = generic.get(optionName);
        return value != null ? value : s3.get(optionName);
    }

    public boolean isEmpty() {
        return !hasExplicitOptions();
    }

    /** Applies connector-scoped options to any non-local table-root volume. */
    public void applyTo(Config.VolumeDescriptor volume) {
        if (volume == null
                || volume.baseDir == null
                || !isRemotePath(volume.baseDir)
                || isEmpty()) {
            return;
        }
        boolean s3Path = isS3Path(volume.baseDir);
        if (genericOptions.isEmpty() && !s3Path) {
            return;
        }
        if (s3Path && s3AccessId != null) {
            volume.accessId = s3AccessId;
            volume.secretKey = s3SecretKey;
        }
        Map<String, String> merged =
                volume.customOptions == null
                        ? new HashMap<String, String>()
                        : new HashMap<String, String>(volume.customOptions);
        merged.putAll(genericOptions);
        if (s3Path) {
            merged.putAll(s3Options);
        }
        volume.customOptions = merged;
    }

    private static boolean isRemotePath(String path) {
        if (path == null) {
            return false;
        }
        String scheme = URI.create(path).getScheme();
        return scheme != null && !"file".equalsIgnoreCase(scheme);
    }

    private static boolean isS3Path(String path) {
        String scheme = URI.create(path).getScheme();
        return "s3".equalsIgnoreCase(scheme)
                || "s3a".equalsIgnoreCase(scheme)
                || "s3p".equalsIgnoreCase(scheme);
    }

    private static String resolveAlias(
            Map<String, String> options, String canonical, String alias, String description) {
        String canonicalValue = normalizedOptional(options.get(canonical));
        String aliasValue = normalizedOptional(options.get(alias));
        if (canonicalValue != null && aliasValue != null && !canonicalValue.equals(aliasValue)) {
            throw new IllegalArgumentException(
                    "Conflicting " + description + " options: " + canonical + " and " + alias);
        }
        return canonicalValue != null ? canonicalValue : aliasValue;
    }

    private static String normalizedOptional(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static Set<String> aliasKeys() {
        Set<String> keys = new HashSet<String>();
        keys.add(S3_ENDPOINT_KEY);
        keys.add(S3_ACCESS_KEY);
        keys.add(S3_ACCESS_KEY_ALIAS);
        keys.add(S3_SECRET_KEY);
        keys.add(S3_SECRET_KEY_ALIAS);
        keys.add(S3_PATH_STYLE_ACCESS_KEY);
        keys.add(S3_REGION_KEY);
        return Collections.unmodifiableSet(keys);
    }

    private static String normalizeEndpoint(String endpoint) {
        if (endpoint.contains("://")) {
            return endpoint;
        }
        String authority = endpoint;
        int slash = authority.indexOf('/');
        if (slash >= 0) {
            authority = authority.substring(0, slash);
        }
        String host = authority;
        int portSeparator = host.indexOf(':');
        if (portSeparator >= 0) {
            host = host.substring(0, portSeparator);
        }
        String lowerHost = host.toLowerCase(Locale.ROOT);
        boolean local =
                "localhost".equals(lowerHost)
                        || lowerHost.startsWith("127.")
                        || "0.0.0.0".equals(lowerHost)
                        || "host.docker.internal".equals(lowerHost);
        return (local ? "http://" : "https://") + endpoint;
    }

    @Override
    public String toString() {
        return "CobbleConnectorStorageOptions{configured="
                + !isEmpty()
                + ", optionCount="
                + (genericOptions.size() + s3Options.size())
                + ", values=redacted}";
    }
}
