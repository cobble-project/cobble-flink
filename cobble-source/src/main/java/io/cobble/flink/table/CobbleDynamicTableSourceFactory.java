package io.cobble.flink.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Flink SQL source factory that reads snapshot rows from a Cobble table path. */
public final class CobbleDynamicTableSourceFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "cobble";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CobbleSourceTableOptions.PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CobbleSourceTableOptions.BUCKET);
        options.add(CobbleSourceTableOptions.SCAN_CHECKPOINT_ID);
        options.add(CobbleSourceTableOptions.SCAN_MODE);
        options.add(CobbleSourceTableOptions.SCAN_POLL_INTERVAL_MS);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        String pathUri = normalizePathToUri(options.get(CobbleSourceTableOptions.PATH));
        int bucketCount =
                options.getOptional(CobbleSourceTableOptions.BUCKET).orElse(Integer.valueOf(-1));
        String checkpointId =
                normalizeCheckpointId(options.get(CobbleSourceTableOptions.SCAN_CHECKPOINT_ID));
        String scanMode = normalizeScanMode(options.get(CobbleSourceTableOptions.SCAN_MODE));
        long pollIntervalMillis =
                options.get(CobbleSourceTableOptions.SCAN_POLL_INTERVAL_MS).longValue();

        if (bucketCount == 0 || bucketCount < -1) {
            throw new ValidationException(CobbleSourceTableOptions.BUCKET.key() + " must be > 0");
        }
        if (pollIntervalMillis <= 0L) {
            throw new ValidationException(
                    CobbleSourceTableOptions.SCAN_POLL_INTERVAL_MS.key() + " must be > 0");
        }
        if ("streaming".equals(scanMode) && !"latest".equals(checkpointId)) {
            throw new ValidationException(
                    "Cobble source streaming mode currently supports only scan.checkpoint-id='latest'.");
        }

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        UniqueConstraint primaryKey =
                resolvedSchema
                        .getPrimaryKey()
                        .orElseThrow(
                                new ValidationExceptionSupplier(
                                        "The initial Cobble source requires a PRIMARY KEY."));

        DataType physicalDataType = resolvedSchema.toPhysicalRowDataType();
        RowType physicalRowType = (RowType) physicalDataType.getLogicalType();
        List<RowType.RowField> physicalFields = physicalRowType.getFields();
        Map<String, Integer> fieldIndexByName = new HashMap<>();
        for (int i = 0; i < physicalFields.size(); i++) {
            fieldIndexByName.put(physicalFields.get(i).getName(), Integer.valueOf(i));
        }

        List<CobbleDynamicTableSource.SerializableField> keyFields = new ArrayList<>();
        for (String keyColumn : primaryKey.getColumns()) {
            Integer fieldIndex = fieldIndexByName.get(keyColumn);
            if (fieldIndex == null) {
                throw new ValidationException(
                        "Primary key column "
                                + keyColumn
                                + " is not part of the physical row schema.");
            }
            RowType.RowField field = physicalFields.get(fieldIndex.intValue());
            validateTypeSupported(field);
            keyFields.add(
                    new CobbleDynamicTableSource.SerializableField(
                            field.getName(),
                            field.getType().asSerializableString(),
                            fieldIndex.intValue(),
                            -1));
        }

        Set<String> keyColumnNames = new HashSet<>(primaryKey.getColumns());
        List<CobbleDynamicTableSource.SerializableField> valueFields = new ArrayList<>();
        int structuredColumnIndex = 0;
        for (int i = 0; i < physicalFields.size(); i++) {
            RowType.RowField field = physicalFields.get(i);
            if (keyColumnNames.contains(field.getName())) {
                continue;
            }
            validateTypeSupported(field);
            valueFields.add(
                    new CobbleDynamicTableSource.SerializableField(
                            field.getName(),
                            field.getType().asSerializableString(),
                            i,
                            structuredColumnIndex));
            structuredColumnIndex++;
        }

        if (valueFields.isEmpty()) {
            throw new ValidationException(
                    "The initial Cobble source requires at least one non-primary-key column.");
        }

        CobbleDynamicTableSource.SerializableConfig config =
                new CobbleDynamicTableSource.SerializableConfig(
                        pathUri,
                        bucketCount,
                        checkpointId,
                        scanMode,
                        pollIntervalMillis,
                        keyFields,
                        valueFields);
        return new CobbleDynamicTableSource(
                config, context.getObjectIdentifier().asSummaryString());
    }

    private static void validateTypeSupported(RowType.RowField field) {
        try {
            InternalSerializers.create(field.getType());
        } catch (UnsupportedOperationException e) {
            throw new ValidationException(
                    "Cobble source does not support field "
                            + field.getName()
                            + " with logical type "
                            + field.getType(),
                    e);
        }
    }

    private static String normalizeCheckpointId(String checkpointId) {
        String trimmed = checkpointId == null ? "" : checkpointId.trim();
        if (trimmed.isEmpty()) {
            throw new ValidationException(
                    CobbleSourceTableOptions.SCAN_CHECKPOINT_ID.key() + " must not be empty.");
        }
        if ("latest".equalsIgnoreCase(trimmed)) {
            return "latest";
        }
        try {
            long parsed = Long.parseLong(trimmed);
            if (parsed <= 0L) {
                throw new ValidationException(
                        CobbleSourceTableOptions.SCAN_CHECKPOINT_ID.key()
                                + " must be a positive number or 'latest'.");
            }
            return Long.toString(parsed);
        } catch (NumberFormatException e) {
            throw new ValidationException(
                    CobbleSourceTableOptions.SCAN_CHECKPOINT_ID.key()
                            + " must be a positive number or 'latest'.",
                    e);
        }
    }

    private static String normalizeScanMode(String scanMode) {
        String normalized = scanMode == null ? "" : scanMode.trim().toLowerCase();
        if ("batch".equals(normalized) || "streaming".equals(normalized)) {
            return normalized;
        }
        throw new ValidationException(
                CobbleSourceTableOptions.SCAN_MODE.key()
                        + " must be one of ['batch', 'streaming'].");
    }

    private static final class ValidationExceptionSupplier
            implements java.util.function.Supplier<ValidationException> {
        private final String message;

        private ValidationExceptionSupplier(String message) {
            this.message = message;
        }

        @Override
        public ValidationException get() {
            return new ValidationException(message);
        }
    }

    private static String normalizePathToUri(String path) {
        String trimmed = path == null ? "" : path.trim();
        if (trimmed.isEmpty()) {
            throw new ValidationException(
                    CobbleSourceTableOptions.PATH.key() + " must not be empty.");
        }

        URI parsed = URI.create(trimmed);
        URI normalized =
                parsed.getScheme() == null ? Paths.get(trimmed).toAbsolutePath().toUri() : parsed;
        if (!"file".equalsIgnoreCase(normalized.getScheme())) {
            throw new ValidationException(
                    CobbleSourceTableOptions.PATH.key()
                            + " currently supports only local file paths, but got "
                            + normalized);
        }
        return normalized.toString();
    }
}
