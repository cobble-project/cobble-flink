package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.MetricSample;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleNativeMetrics;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.Table;
import io.cobble.table.TableWriterBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** One native table handler for one deterministic physical bucket. */
final class CobbleSingleBucketWriter
        implements AutoCloseable,
                CobbleConcurrentSnapshots.Trigger,
                CobbleNativeMetrics.MetricSnapshotProvider {
    final int bucketId;
    final String writerPath;
    final Table table;
    private CompletableFuture<ShardSnapshot> activeSnapshot;
    private boolean closeRequested;
    private boolean closed;

    private CobbleSingleBucketWriter(int bucketId, String writerPath, Table table) {
        this.bucketId = bucketId;
        this.writerPath = writerPath;
        this.table = table;
    }

    static CobbleSingleBucketWriter open(
            CobbleDynamicTableSink.SerializableConfig config,
            int subtaskId,
            int bucketId,
            int ownedBucketCount,
            CobbleBucketWriterState restoredState)
            throws IOException {
        Config runtime;
        TableWriterBuilder builder;
        String writerPath =
                CobbleSinkPaths.bucketWriterLocalDirectory(config, subtaskId, bucketId)
                        .getAbsolutePath();
        if (config.isCatalogTable()) {
            runtime = CobbleSinkPaths.createCatalogWriterRuntime(config, ownedBucketCount);
            builder = config.catalogWritePlan.writerBuilder(runtime).bucket(bucketId);
        } else {
            runtime =
                    CobbleSinkPaths.createTableWriterRuntime(
                            config, subtaskId, bucketId, ownedBucketCount);
            builder =
                    Table.writerBuilder(runtime)
                            .tableName(CobbleTableRowConverter.TABLE_NAME)
                            .bucket(bucketId);
        }
        Table table =
                restoredState == null
                        ? openFresh(config, builder)
                        : builder.resumeFromSnapshot(restoredState.snapshotId);
        try {
            if (config.isCatalogTable()) {
                try (io.cobble.flink.catalog.CobbleCatalogTableReference.Opened opened =
                        config.catalogTable.openValidated()) {
                    opened.table().refreshWriter(table);
                }
            }
            return new CobbleSingleBucketWriter(bucketId, writerPath, table);
        } catch (RuntimeException error) {
            table.close();
            throw error;
        }
    }

    private static Table openFresh(
            CobbleDynamicTableSink.SerializableConfig config, TableWriterBuilder builder) {
        return config.isCatalogTable() ? builder.open() : builder.create(config.tableSchema());
    }

    @Override
    public int bucketId() {
        return bucketId;
    }

    @Override
    public synchronized CompletableFuture<ShardSnapshot> start() {
        if (closeRequested) {
            throw new IllegalStateException("Cobble bucket writer is closing.");
        }
        activeSnapshot = table.startAsyncSnapshot().future();
        return activeSnapshot;
    }

    @Override
    public synchronized List<MetricSample> metrics() {
        if (closeRequested || closed) {
            throw new IllegalStateException("Cobble bucket writer is closing.");
        }
        return table.metrics();
    }

    @Override
    public void close() {
        Table tableToClose;
        synchronized (this) {
            if (closed) {
                return;
            }
            closeRequested = true;
            if (activeSnapshot != null && !activeSnapshot.isDone()) {
                // The failed attempt's completion callback invokes close again after native work.
                return;
            }
            closed = true;
            tableToClose = table;
        }
        tableToClose.close();
    }
}
