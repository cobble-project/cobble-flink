package io.cobble.flink.table;

import io.cobble.flink.common.CobbleLoader;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
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
        options.add(CobbleSourceTableOptions.SOURCE_KIND);
        options.add(CobbleSourceTableOptions.BUCKET);
        options.add(CobbleSourceTableOptions.SCAN_CHECKPOINT_ID);
        options.add(CobbleSourceTableOptions.SCAN_MODE);
        options.add(CobbleSourceTableOptions.SCAN_POLL_INTERVAL_MS);
        options.add(CobbleSourceTableOptions.SOURCE_BLOCK_CACHE_MEMORY);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        CobbleLoader.ensureCobbleLoaded();
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        String pathUri = normalizePathToUri(options.get(CobbleSourceTableOptions.PATH));
        int bucketCount = options.getOptional(CobbleSourceTableOptions.BUCKET).orElse(-1);
        String checkpointId =
                normalizeCheckpointId(options.get(CobbleSourceTableOptions.SCAN_CHECKPOINT_ID));
        String scanMode = normalizeScanMode(options.get(CobbleSourceTableOptions.SCAN_MODE));
        long pollIntervalMillis = options.get(CobbleSourceTableOptions.SCAN_POLL_INTERVAL_MS);
        MemorySize sourceBlockCacheMemory =
                options.get(CobbleSourceTableOptions.SOURCE_BLOCK_CACHE_MEMORY);

        if (bucketCount == 0 || bucketCount < -1) {
            throw new ValidationException(CobbleSourceTableOptions.BUCKET.key() + " must be > 0");
        }
        if (pollIntervalMillis <= 0L) {
            throw new ValidationException(
                    CobbleSourceTableOptions.SCAN_POLL_INTERVAL_MS.key() + " must be > 0");
        }
        if (sourceBlockCacheMemory.getBytes() < 0L) {
            throw new ValidationException(
                    CobbleSourceTableOptions.SOURCE_BLOCK_CACHE_MEMORY.key() + " must be >= 0");
        }
        if ("streaming".equals(scanMode) && !"latest".equals(checkpointId)) {
            throw new ValidationException(
                    "Cobble source streaming mode currently supports only scan.checkpoint-id='latest'.");
        }

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        CobbleSourceKind requestedKind =
                CobbleSourceKind.fromUserOption(options.get(CobbleSourceTableOptions.SOURCE_KIND));
        CobbleResolvedSource resolvedSource =
                CobbleSourceKindDetector.detect(
                        pathUri, requestedKind, isSinkShaped(resolvedSchema));
        if (resolvedSource.kind() == CobbleSourceKind.STATE) {
            // Reviewer preference for Step 1: detect state paths but fail in the factory, because
            // there is no state source runtime yet. Sink key/value codecs are intentionally not
            // instantiated on this path.
            throw new ValidationException(
                    "Cobble state source was detected, but state source runtime is not implemented"
                            + " in this step. "
                            + resolvedSource.diagnostics());
        }

        // Resolved as a Cobble sink table: existing sink source behavior, unchanged.
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
            fieldIndexByName.put(physicalFields.get(i).getName(), i);
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
                        sourceBlockCacheMemory.getBytes(),
                        keyFields,
                        valueFields);
        return new CobbleDynamicTableSource(
                config, context.getObjectIdentifier().asSummaryString());
    }

    /**
     * Returns whether the table schema is sink-shaped: it declares a primary key and has at least
     * one non-primary-key column. This lets {@code auto} keep treating an ambiguous path as a sink
     * source, preserving existing behavior for sink tables without an inspect-schema sidecar.
     */
    private static boolean isSinkShaped(ResolvedSchema resolvedSchema) {
        if (!resolvedSchema.getPrimaryKey().isPresent()) {
            return false;
        }
        RowType physicalRowType = (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType();
        int primaryKeyColumnCount = resolvedSchema.getPrimaryKey().get().getColumns().size();
        return physicalRowType.getFieldCount() - primaryKeyColumnCount >= 1;
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
        if (normalized.getScheme() == null || normalized.getScheme().trim().isEmpty()) {
            throw new ValidationException(
                    CobbleSourceTableOptions.PATH.key()
                            + " must be a valid path or URI, but got "
                            + path);
        }
        return normalized.toString();
    }
}
