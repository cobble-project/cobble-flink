package io.cobble.flink.table;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Flink SQL source that reads rows from Cobble sink checkpoints. */
final class CobbleDynamicTableSource implements ScanTableSource, LookupTableSource {

    private final SerializableConfig config;
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
                if (lookupKey[0] == keyField.rowIndex) {
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
    static final class SerializableConfig implements Serializable {
        private static final long serialVersionUID = 1L;

        final String pathUri;
        final int bucketCount;
        final String scanCheckpointId;
        final String scanMode;
        final long pollIntervalMillis;
        final List<SerializableField> keyFields;
        final List<SerializableField> valueFields;

        SerializableConfig(
                String pathUri,
                int bucketCount,
                String scanCheckpointId,
                String scanMode,
                long pollIntervalMillis,
                List<SerializableField> keyFields,
                List<SerializableField> valueFields) {
            this.pathUri = pathUri;
            this.bucketCount = bucketCount;
            this.scanCheckpointId = scanCheckpointId;
            this.scanMode = scanMode;
            this.pollIntervalMillis = pollIntervalMillis;
            this.keyFields = Collections.unmodifiableList(new ArrayList<>(keyFields));
            this.valueFields = Collections.unmodifiableList(new ArrayList<>(valueFields));
        }

        SerializableConfig copy() {
            return new SerializableConfig(
                    pathUri,
                    bucketCount,
                    scanCheckpointId,
                    scanMode,
                    pollIntervalMillis,
                    keyFields,
                    valueFields);
        }

        boolean isStreamingLatest() {
            return "streaming".equals(scanMode) && "latest".equals(scanCheckpointId);
        }

        boolean hasConfiguredBucketCount() {
            return bucketCount > 0;
        }

        Boundedness boundedness() {
            return isStreamingLatest() ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
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
