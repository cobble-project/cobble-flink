package io.cobble.flink.table;

import org.apache.flink.api.connector.source.Boundedness;

import java.util.Arrays;

/**
 * Scan-source config for {@code source.kind='raw'}.
 *
 * <p>This is a deliberately small, schema-less config: it carries no Flink {@code TypeSerializer},
 * no typed key/value field mappings, and no sink sidecar schema. It only knows the table root path,
 * the scan mode, and the explicit column indexes the user requested.
 *
 * <p>{@link #scanColumnCount()} returns {@code max(selectedColumns) + 1} — the DB-open
 * column-family width that the Cobble reader needs so its read view matches the writer's column
 * family. This is distinct from {@link #projectedColumnIndexes()}, which is the per-scan projection
 * and may be a non-contiguous subset (e.g. {@code [0, 2]} with {@code scanColumnCount() == 3}).
 */
final class RawSourceConfig implements CobbleTableScanConfig {

    private static final long serialVersionUID = 1L;

    private final String pathUri;
    private final int bucketCount;
    private final String scanCheckpointId;
    private final String scanMode;
    private final long pollIntervalMillis;
    private final int[] selectedColumns;

    RawSourceConfig(
            String pathUri,
            int bucketCount,
            String scanCheckpointId,
            String scanMode,
            long pollIntervalMillis,
            int[] selectedColumns) {
        this.pathUri = pathUri;
        this.bucketCount = bucketCount;
        this.scanCheckpointId = scanCheckpointId;
        this.scanMode = scanMode;
        this.pollIntervalMillis = pollIntervalMillis;
        this.selectedColumns = Arrays.copyOf(selectedColumns, selectedColumns.length);
    }

    @Override
    public String pathUri() {
        return pathUri;
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
        int max = -1;
        for (int col : selectedColumns) {
            if (col > max) {
                max = col;
            }
        }
        return max + 1;
    }

    @Override
    public int[] projectedColumnIndexes() {
        return Arrays.copyOf(selectedColumns, selectedColumns.length);
    }

    @Override
    public ScannedRowDecoder createDecoder() {
        return new CobbleRawRowDecoder(selectedColumns.length);
    }
}
