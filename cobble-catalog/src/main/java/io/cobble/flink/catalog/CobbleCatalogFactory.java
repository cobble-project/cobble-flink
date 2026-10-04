package io.cobble.flink.catalog;

import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleFlinkStorageConfig;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Creates a file-backed Cobble catalog for Flink SQL's {@code CREATE CATALOG} command. */
public final class CobbleCatalogFactory implements CatalogFactory {
    public static final String IDENTIFIER = "cobble";
    static final ConfigOption<String> PATH =
            ConfigOptions.key("path").stringType().noDefaultValue();
    static final ConfigOption<String> STORAGE_ID =
            ConfigOptions.key("storage-id").stringType().defaultValue("flink");
    static final ConfigOption<Integer> BUCKETS =
            ConfigOptions.key("buckets").intType().defaultValue(1);

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.<ConfigOption<?>>singleton(PATH);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        HashSet<ConfigOption<?>> options = new HashSet<ConfigOption<?>>();
        options.add(STORAGE_ID);
        options.add(BUCKETS);
        CobbleConnectorStorageOptions.addFactoryOptions(options);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        Map<String, String> options = context.getOptions();
        validateOptions(options);
        String path = options.get(PATH.key());
        if (path == null || path.trim().isEmpty())
            throw new ValidationException("Cobble catalog option 'path' must not be empty.");
        String storageId =
                options.containsKey(STORAGE_ID.key())
                        ? options.get(STORAGE_ID.key())
                        : STORAGE_ID.defaultValue();
        int buckets;
        try {
            buckets =
                    Integer.parseInt(
                            options.containsKey(BUCKETS.key())
                                    ? options.get(BUCKETS.key())
                                    : String.valueOf(BUCKETS.defaultValue()));
        } catch (NumberFormatException error) {
            throw new ValidationException(
                    "Cobble catalog option 'buckets' must be an integer.", error);
        }
        if (buckets < 1 || buckets > 65536)
            throw new ValidationException("Cobble catalog option 'buckets' must be in 1..=65536.");
        try {
            return new CobbleCatalog(
                    context.getName(),
                    path,
                    storageId,
                    buckets,
                    CobbleConnectorStorageOptions.from(options),
                    CobbleFlinkStorageConfig.from(context.getConfiguration()));
        } catch (IllegalArgumentException error) {
            throw new ValidationException(error.getMessage(), error);
        }
    }

    private static void validateOptions(Map<String, String> options) {
        for (String key : options.keySet()) {
            if (PATH.key().equals(key)
                    || STORAGE_ID.key().equals(key)
                    || BUCKETS.key().equals(key)
                    || key.startsWith(CobbleConnectorStorageOptions.STORAGE_OPTION_PREFIX)
                    || CobbleConnectorStorageOptions.S3_ENDPOINT_KEY.equals(key)
                    || CobbleConnectorStorageOptions.S3_ACCESS_KEY.equals(key)
                    || CobbleConnectorStorageOptions.S3_ACCESS_KEY_ALIAS.equals(key)
                    || CobbleConnectorStorageOptions.S3_SECRET_KEY.equals(key)
                    || CobbleConnectorStorageOptions.S3_SECRET_KEY_ALIAS.equals(key)
                    || CobbleConnectorStorageOptions.S3_PATH_STYLE_ACCESS_KEY.equals(key)
                    || CobbleConnectorStorageOptions.S3_REGION_KEY.equals(key)) {
                continue;
            }
            throw new ValidationException("Unsupported Cobble catalog option: " + key);
        }
    }
}
