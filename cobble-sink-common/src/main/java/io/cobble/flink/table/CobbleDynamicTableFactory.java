package io.cobble.flink.table;

import io.cobble.flink.common.CobbleLoader;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
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

/** Flink SQL sink factory that writes insert-only rows into a shared-path Cobble structured DB. */
public final class CobbleDynamicTableFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "cobble";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CobbleTableOptions.PATH);
        options.add(CobbleTableOptions.BUCKET);
        options.add(FactoryUtil.SINK_PARALLELISM);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CobbleTableOptions.SNAPSHOT_RETENTION);
        options.add(CobbleTableOptions.SINK_USE_MANAGED_MEMORY_ALLOCATOR);
        options.add(CobbleTableOptions.SINK_WRITER_BUFFER_MEMORY);
        options.add(FactoryUtil.SINK_PARALLELISM);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        CobbleLoader.ensureCobbleLoaded();
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        String pathUri = normalizePathToUri(options.get(CobbleTableOptions.PATH));
        int bucketCount = options.get(CobbleTableOptions.BUCKET);
        int snapshotRetention = options.get(CobbleTableOptions.SNAPSHOT_RETENTION);
        boolean sinkUseManagedMemoryAllocator =
                options.get(CobbleTableOptions.SINK_USE_MANAGED_MEMORY_ALLOCATOR);
        MemorySize sinkWriterBufferMemory =
                options.get(CobbleTableOptions.SINK_WRITER_BUFFER_MEMORY);
        Integer sinkParallelism = options.get(FactoryUtil.SINK_PARALLELISM);

        if (bucketCount <= 0) {
            throw new ValidationException(CobbleTableOptions.BUCKET.key() + " must be > 0");
        }
        if (snapshotRetention <= 0) {
            throw new ValidationException(
                    CobbleTableOptions.SNAPSHOT_RETENTION.key() + " must be > 0");
        }
        if (sinkParallelism == null || sinkParallelism.intValue() <= 0) {
            throw new ValidationException(
                    FactoryUtil.SINK_PARALLELISM.key() + " must be configured with a value > 0.");
        }
        if (sinkWriterBufferMemory.getBytes() <= 0L) {
            throw new ValidationException(
                    CobbleTableOptions.SINK_WRITER_BUFFER_MEMORY.key() + " must be > 0.");
        }

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        UniqueConstraint primaryKey =
                resolvedSchema
                        .getPrimaryKey()
                        .orElseThrow(
                                new ValidationExceptionSupplier(
                                        "The initial Cobble sink requires a PRIMARY KEY."));

        DataType physicalDataType = resolvedSchema.toPhysicalRowDataType();
        RowType physicalRowType = (RowType) physicalDataType.getLogicalType();
        List<RowType.RowField> physicalFields = physicalRowType.getFields();
        Map<String, Integer> fieldIndexByName = new HashMap<>();
        for (int i = 0; i < physicalFields.size(); i++) {
            fieldIndexByName.put(physicalFields.get(i).getName(), i);
        }

        List<CobbleDynamicTableSink.SerializableField> keyFields = new ArrayList<>();
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
                    new CobbleDynamicTableSink.SerializableField(
                            field.getName(),
                            field.getType().asSerializableString(),
                            fieldIndex.intValue(),
                            -1));
        }

        Set<String> keyColumnNames = new HashSet<>(primaryKey.getColumns());
        List<CobbleDynamicTableSink.SerializableField> valueFields = new ArrayList<>();
        int structuredColumnIndex = 0;
        for (int i = 0; i < physicalFields.size(); i++) {
            RowType.RowField field = physicalFields.get(i);
            if (keyColumnNames.contains(field.getName())) {
                continue;
            }
            validateTypeSupported(field);
            valueFields.add(
                    new CobbleDynamicTableSink.SerializableField(
                            field.getName(),
                            field.getType().asSerializableString(),
                            i,
                            structuredColumnIndex));
            structuredColumnIndex++;
        }

        if (valueFields.isEmpty()) {
            throw new ValidationException(
                    "The initial Cobble sink requires at least one non-primary-key column.");
        }

        CobbleDynamicTableSink.SerializableConfig config =
                new CobbleDynamicTableSink.SerializableConfig(
                        pathUri,
                        bucketCount,
                        snapshotRetention,
                        sinkParallelism.intValue(),
                        sinkUseManagedMemoryAllocator,
                        sinkWriterBufferMemory.getBytes(),
                        keyFields,
                        valueFields);
        return new CobbleDynamicTableSink(config, context.getObjectIdentifier().asSummaryString());
    }

    private static void validateTypeSupported(RowType.RowField field) {
        try {
            InternalSerializers.create(field.getType());
        } catch (UnsupportedOperationException e) {
            throw new ValidationException(
                    "Cobble sink does not support field "
                            + field.getName()
                            + " with logical type "
                            + field.getType(),
                    e);
        }
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
            throw new ValidationException(CobbleTableOptions.PATH.key() + " must not be empty.");
        }

        URI parsed = URI.create(trimmed);
        URI normalized =
                parsed.getScheme() == null ? Paths.get(trimmed).toAbsolutePath().toUri() : parsed;
        if (normalized.getScheme() == null || normalized.getScheme().trim().isEmpty()) {
            throw new ValidationException(
                    CobbleTableOptions.PATH.key()
                            + " must be a valid path or URI, but got "
                            + path);
        }
        return normalized.toString();
    }
}
