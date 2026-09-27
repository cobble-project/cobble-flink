package io.cobble.flink.table;

import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.TableSchema;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** Flink SQL source that reads rows from committed Cobble Tables. */
final class CobbleDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    private SerializableConfig config;
    private final String summary;

    CobbleDynamicTableSource(SerializableConfig config, String summary) {
        this.config = config;
        this.summary = summary;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return SourceProvider.of(new CobbleSource(config));
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return LookupFunctionProvider.of(
                new CobbleLookupFunction(config, resolveLookupKeyPositions(context)));
    }

    @Override
    public DynamicTableSource copy() {
        return new CobbleDynamicTableSource(config.copy(), summary);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        config =
                config.withProjection(
                        SourceProjection.indexes(projectedFields, config.totalFieldCount()));
    }

    @Override
    public String asSummaryString() {
        return "CobbleTableSource{" + summary + "}";
    }

    private int[] resolveLookupKeyPositions(LookupContext context) {
        int[][] keys = context.getKeys();
        if (keys.length != config.keyFields.size()) {
            throw new ValidationException(
                    "Cobble lookup join requires equality conditions for all PRIMARY KEY columns.");
        }

        int[] positionsByPrimaryKey = new int[config.keyFields.size()];
        for (int keyFieldIndex = 0; keyFieldIndex < config.keyFields.size(); keyFieldIndex++) {
            SerializableField keyField = config.keyFields.get(keyFieldIndex);
            positionsByPrimaryKey[keyFieldIndex] = -1;
            for (int lookupPosition = 0; lookupPosition < keys.length; lookupPosition++) {
                int[] lookupKey = keys[lookupPosition];
                if (lookupKey.length != 1) {
                    throw new ValidationException(
                            "Cobble lookup join supports only top-level PRIMARY KEY columns.");
                }
                if (SourceProjection.originalIndex(config.outputProjection, lookupKey[0])
                        == keyField.rowIndex) {
                    positionsByPrimaryKey[keyFieldIndex] = lookupPosition;
                    break;
                }
            }
            if (positionsByPrimaryKey[keyFieldIndex] < 0) {
                throw new ValidationException(
                        "Cobble lookup join requires equality conditions for all PRIMARY KEY columns.");
            }
        }
        return positionsByPrimaryKey;
    }

    /** Serializable source runtime config. */
    static final class SerializableConfig implements CobbleTableScanConfig {
        private static final long serialVersionUID = 1L;

        final String pathUri;
        final CobbleConnectorStorageOptions storageOptions;
        final int bucketCount;
        final String scanCheckpointId;
        final String scanMode;
        final long pollIntervalMillis;
        final long sourceBlockCacheMemoryBytes;
        final List<SerializableField> keyFields;
        final List<SerializableField> valueFields;
        final int[] outputProjection;

        SerializableConfig(
                String pathUri,
                int bucketCount,
                String scanCheckpointId,
                String scanMode,
                long pollIntervalMillis,
                long sourceBlockCacheMemoryBytes,
                List<SerializableField> keyFields,
                List<SerializableField> valueFields) {
            this(
                    pathUri,
                    bucketCount,
                    scanCheckpointId,
                    scanMode,
                    pollIntervalMillis,
                    sourceBlockCacheMemoryBytes,
                    keyFields,
                    valueFields,
                    CobbleConnectorStorageOptions.empty());
        }

        SerializableConfig(
                String pathUri,
                int bucketCount,
                String scanCheckpointId,
                String scanMode,
                long pollIntervalMillis,
                long sourceBlockCacheMemoryBytes,
                List<SerializableField> keyFields,
                List<SerializableField> valueFields,
                CobbleConnectorStorageOptions storageOptions) {
            this(
                    pathUri,
                    bucketCount,
                    scanCheckpointId,
                    scanMode,
                    pollIntervalMillis,
                    sourceBlockCacheMemoryBytes,
                    keyFields,
                    valueFields,
                    storageOptions,
                    SourceProjection.all(keyFields.size() + valueFields.size()));
        }

        private SerializableConfig(
                String pathUri,
                int bucketCount,
                String scanCheckpointId,
                String scanMode,
                long pollIntervalMillis,
                long sourceBlockCacheMemoryBytes,
                List<SerializableField> keyFields,
                List<SerializableField> valueFields,
                CobbleConnectorStorageOptions storageOptions,
                int[] outputProjection) {
            this.pathUri = pathUri;
            this.storageOptions = storageOptions;
            this.bucketCount = bucketCount;
            this.scanCheckpointId = scanCheckpointId;
            this.scanMode = scanMode;
            this.pollIntervalMillis = pollIntervalMillis;
            this.sourceBlockCacheMemoryBytes = sourceBlockCacheMemoryBytes;
            this.keyFields = Collections.unmodifiableList(new ArrayList<>(keyFields));
            this.valueFields = Collections.unmodifiableList(new ArrayList<>(valueFields));
            this.outputProjection = outputProjection.clone();
        }

        SerializableConfig copy() {
            return new SerializableConfig(
                    pathUri,
                    bucketCount,
                    scanCheckpointId,
                    scanMode,
                    pollIntervalMillis,
                    sourceBlockCacheMemoryBytes,
                    keyFields,
                    valueFields,
                    storageOptions,
                    outputProjection);
        }

        SerializableConfig withProjection(int[] projection) {
            return new SerializableConfig(
                    pathUri,
                    bucketCount,
                    scanCheckpointId,
                    scanMode,
                    pollIntervalMillis,
                    sourceBlockCacheMemoryBytes,
                    keyFields,
                    valueFields,
                    storageOptions,
                    projection);
        }

        @Override
        public String pathUri() {
            return pathUri;
        }

        @Override
        public CobbleConnectorStorageOptions storageOptions() {
            return storageOptions;
        }

        @Override
        public int bucketCount() {
            return bucketCount;
        }

        @Override
        public String scanCheckpointId() {
            return scanCheckpointId;
        }

        @Override
        public String scanMode() {
            return scanMode;
        }

        @Override
        public long pollIntervalMillis() {
            return pollIntervalMillis;
        }

        @Override
        public boolean isStreamingLatest() {
            return "streaming".equals(scanMode) && "latest".equals(scanCheckpointId);
        }

        @Override
        public boolean hasConfiguredBucketCount() {
            return bucketCount > 0;
        }

        @Override
        public Boundedness boundedness() {
            return isStreamingLatest() ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
        }

        @Override
        public int scanColumnCount() {
            return Math.max(1, valueFields.size());
        }

        List<SerializableField> physicalFields() {
            List<SerializableField> fields = new ArrayList<SerializableField>();
            fields.addAll(keyFields);
            fields.addAll(valueFields);
            Collections.sort(
                    fields,
                    new Comparator<SerializableField>() {
                        @Override
                        public int compare(SerializableField left, SerializableField right) {
                            return Integer.compare(left.rowIndex, right.rowIndex);
                        }
                    });
            return fields;
        }

        org.apache.flink.table.types.logical.RowType rowType() {
            List<String> names = new ArrayList<String>();
            List<String> types = new ArrayList<String>();
            for (SerializableField field : physicalFields()) {
                names.add(field.name);
                types.add(field.logicalType);
            }
            return CobbleTableRowConverter.parseRowType(names, types);
        }

        org.apache.flink.table.types.logical.RowType projectedRowType() {
            return SourceProjection.rowType(rowType(), outputProjection);
        }

        List<SerializableField> projectedFields() {
            List<SerializableField> full = physicalFields();
            List<SerializableField> selected = new ArrayList<>(outputProjection.length);
            for (int index : outputProjection) selected.add(full.get(index));
            return selected;
        }

        TableSchema tableSchema() {
            List<String> primaryKey = new ArrayList<String>(keyFields.size());
            for (SerializableField field : keyFields) {
                primaryKey.add(field.name);
            }
            return CobbleTableRowConverter.toTableSchema(rowType(), primaryKey);
        }

        int totalFieldCount() {
            return keyFields.size() + valueFields.size();
        }
    }

    /** Serializable field mapping from Flink physical row to Cobble key/value bytes. */
    static final class SerializableField implements Serializable {
        private static final long serialVersionUID = 1L;

        final String name;
        final String logicalType;
        final int rowIndex;
        final int structuredColumnIndex;

        SerializableField(
                String name, String logicalType, int rowIndex, int structuredColumnIndex) {
            this.name = name;
            this.logicalType = logicalType;
            this.rowIndex = rowIndex;
            this.structuredColumnIndex = structuredColumnIndex;
        }
    }
}
