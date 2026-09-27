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
 * <p>{@link #scanColumnCount()} describes the stored column-family width, not a read projection.
 * Typed Table scans use TableScanPlan projection; raw scans select physical columns separately.
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
}
