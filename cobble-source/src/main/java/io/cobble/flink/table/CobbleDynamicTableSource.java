package io.cobble.flink.table;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Flink SQL source that reads rows from Cobble sink checkpoints. */
final class CobbleDynamicTableSource implements ScanTableSource {

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
    public DynamicTableSource copy() {
        return new CobbleDynamicTableSource(config.copy(), summary);
    }

    @Override
    public String asSummaryString() {
        return "CobbleTableSource{" + summary + "}";
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
