package io.cobble.flink.table;

import io.cobble.flink.common.CobbleConnectorStorageOptions;

import org.apache.flink.api.connector.source.Boundedness;

import java.io.Serializable;

/**
 * Shared scan-source configuration contract for Cobble table-root scans.
 *
 * <p>Both the typed sink source ({@link CobbleDynamicTableSource.SerializableConfig}) and the raw
 * source ({@link RawSourceConfig}) implement this interface so that {@link CobbleSource}, {@link
 * CobbleSourceEnumerator}, and {@link CobbleSourceReader} can depend on a single type rather than
 * each concrete config.
 *
 * <p>Two column-related concerns are deliberately separated:
 *
 * <ul>
 *   <li>{@link #scanColumnCount()} — the DB-open column-family width (the writer's structured value
 *       column count). This is passed to {@code Config.numColumns(...)} so the read view matches
 *       the writer. It is <em>not</em> a per-scan projection.
 *   <li>{@link #projectedColumnIndexes()} — the per-scan projection, passed to {@code
 *       ScanOptions.forColumns(...)}. For the raw source this may be a non-contiguous subset (e.g.
 *       {@code [0, 2]}), while {@link #scanColumnCount()} is {@code max(indexes)+1} (e.g. 3).
 * </ul>
 */
interface CobbleTableScanConfig extends Serializable {

    String pathUri();

    CobbleConnectorStorageOptions storageOptions();

    int bucketCount();

    String scanCheckpointId();

    String scanMode();

    long pollIntervalMillis();

    boolean isStreamingLatest();

    boolean hasConfiguredBucketCount();

    Boundedness boundedness();

    /** DB-open column-family width; passed to {@code Config.numColumns(...)}. */
    int scanColumnCount();

    /** Per-scan column projection indexes; passed to {@code ScanOptions.forColumns(...)}. */
    int[] projectedColumnIndexes();

    /**
     * Creates the reader-side row decoder. Called on the TaskManager side in {@link
     * CobbleSource#createReader}, never on the JobManager side, so decoders that hold
     * non-serializable Flink {@code TypeSerializer} objects are safe.
     */
    ScannedRowDecoder createDecoder();
}
