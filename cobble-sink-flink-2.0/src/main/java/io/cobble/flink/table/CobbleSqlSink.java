package io.cobble.flink.table;

import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.SnapshotTools;
import io.cobble.flink.catalog.CobbleCatalogTableReference;
import io.cobble.flink.common.CobbleConnectorMetrics;
import io.cobble.flink.common.CobbleLoader;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.Table;
import io.cobble.table.TableKey;
import io.cobble.table.TableKeyBuilder;
import io.cobble.table.TableSchema;
import io.cobble.table.TableSnapshotCommitter;
import io.cobble.table.Value;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Sink with synchronous snapshots and checkpointed global snapshot commits. */
final class CobbleSqlSink
        implements Sink<RowData>,
                SupportsCommitter<CobbleShardCommittable>,
                SupportsPreCommitTopology<CobbleShardCommittable, CobbleShardCommittable>,
                SupportsWriterState<RowData, CobbleBucketWriterState> {

    private static final long serialVersionUID = 1L;
    // Flink's default checkpoint timeout; SQL sinks receive the environment's actual value.
    private static final long DEFAULT_SNAPSHOT_TIMEOUT_MILLIS = 600_000L;

    private final CobbleDynamicTableSink.SerializableConfig config;
    private final long snapshotTimeoutMillis;

    CobbleSqlSink(CobbleDynamicTableSink.SerializableConfig config) {
        this(config, DEFAULT_SNAPSHOT_TIMEOUT_MILLIS);
    }

    CobbleSqlSink(CobbleDynamicTableSink.SerializableConfig config, long snapshotTimeoutMillis) {
        this.config = config;
        this.snapshotTimeoutMillis =
                snapshotTimeoutMillis > 0L
                        ? snapshotTimeoutMillis
                        : DEFAULT_SNAPSHOT_TIMEOUT_MILLIS;
    }

    @Override
    public CommittingSinkWriter<RowData, CobbleShardCommittable> createWriter(
            WriterInitContext context) throws IOException {
        return new Writer(config, snapshotTimeoutMillis, context, null);
    }

    @Override
    public StatefulSinkWriter<RowData, CobbleBucketWriterState> restoreWriter(
            WriterInitContext context, Collection<CobbleBucketWriterState> recoveredState)
            throws IOException {
        return new Writer(config, snapshotTimeoutMillis, context, recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<CobbleBucketWriterState> getWriterStateSerializer() {
        return new CobbleBucketWriterState.Serializer();
    }

    @Override
    public Committer<CobbleShardCommittable> createCommitter(
            org.apache.flink.api.connector.sink2.CommitterInitContext context) {
        return new PassthroughCommitter();
    }

    @Override
    public SimpleVersionedSerializer<CobbleShardCommittable> getCommittableSerializer() {
        return new CobbleShardCommittable.Serializer();
    }

    @Override
    public DataStream<CommittableMessage<CobbleShardCommittable>> addPreCommitTopology(
            DataStream<CommittableMessage<CobbleShardCommittable>> committables) {
        return committables
                .global()
                .transform(
                        "Cobble Global Commit Operator",
                        committables.getType(),
                        new GlobalCommitOperatorFactory(config))
                .setParallelism(1)
                // Flink's committer retains the writer parallelism. Keep each writer's
                // summary and lineage together when returning from the global operator.
                .partitionCustom(
                        (Integer subtaskId, int partitions) -> subtaskId % partitions,
                        CommittableMessage::getSubtaskId);
    }

    @Override
    public SimpleVersionedSerializer<CobbleShardCommittable> getWriteResultSerializer() {
        return getCommittableSerializer();
    }

    private static final class Writer
            implements CommittingSinkWriter<RowData, CobbleShardCommittable>,
                    StatefulSinkWriter<RowData, CobbleBucketWriterState> {
        private final CobbleDynamicTableSink.SerializableConfig config;
        private final long snapshotTimeoutMillis;
        private final int subtaskId;
        private final int totalBuckets;
        private final List<Integer> ownedBuckets;
        private final Map<Integer, CobbleSingleBucketWriter> writers;
        private final CobbleRowDataCodecs.RuntimeKeyEncoder keyEncoder;
        private final CobbleTableRowConverter rowConverter;
        private final TableSchema tableSchema;
        private final CobbleConnectorMetrics.SinkMetrics metrics;
        private List<CobbleShardCommittable> endOfInputCommittables;
        private final Map<Integer, CobbleBucketWriterState> lastPreparedByBucket;
        private boolean snapshotFailed;

        private Writer(
                CobbleDynamicTableSink.SerializableConfig config,
                long snapshotTimeoutMillis,
                WriterInitContext context,
                Collection<CobbleBucketWriterState> recoveredState)
                throws IOException {
            CobbleLoader.ensureCobbleLoaded();
            this.config = config;
            this.snapshotTimeoutMillis = snapshotTimeoutMillis;
            this.subtaskId = context.getTaskInfo().getIndexOfThisSubtask();
            this.totalBuckets = config.bucketCount;
            this.ownedBuckets = CobbleBucketWriterStates.ownedBuckets(config, subtaskId);
            Map<Integer, CobbleBucketWriterState> restored =
                    CobbleBucketWriterStates.restoredByBucket(recoveredState, ownedBuckets);
            if (recoveredState == null) {
                restored =
                        CobbleBucketWriterStates.currentByBucket(
                                loadCurrentTableSnapshot(config), ownedBuckets);
            }
            this.keyEncoder = new CobbleRowDataCodecs.RuntimeKeyEncoder(config.keyFields);
            this.rowConverter = new CobbleTableRowConverter(config.rowType());
            this.tableSchema = config.tableSchema();
            this.writers = new LinkedHashMap<Integer, CobbleSingleBucketWriter>();
            this.lastPreparedByBucket = new LinkedHashMap<Integer, CobbleBucketWriterState>();
            try {
                for (Integer bucket : ownedBuckets) {
                    CobbleSingleBucketWriter writer =
                            CobbleSingleBucketWriter.open(
                                    config,
                                    subtaskId,
                                    bucket.intValue(),
                                    ownedBuckets.size(),
                                    restored.get(bucket));
                    writers.put(bucket, writer);
                }
                this.metrics = CobbleConnectorMetrics.sink(context.metricGroup());
            } catch (IOException | RuntimeException | LinkageError e) {
                RuntimeException closeFailure = closeAll(writers.values());
                if (closeFailure != null) {
                    e.addSuppressed(closeFailure);
                }
                throw e;
            }
        }

        @Override
        public void write(RowData element, Context context) throws IOException {
            ensureSnapshotAvailable();
            try {
                byte[] encodedKey = keyEncoder.encode(element);
                List<Value> values = rowConverter.toValues(element);
                int bucket = CobbleTableRowConverter.bucket(tableSchema, values, totalBuckets);
                CobbleSingleBucketWriter writer = writers.get(Integer.valueOf(bucket));
                if (writer == null) {
                    throw new IOException(
                            "Record bucket "
                                    + bucket
                                    + " is not assigned to sink subtask "
                                    + subtaskId
                                    + ".");
                }
                MutationStats stats =
                        applyRowChange(writer.table, config, encodedKey, element, values);
                if (stats.mutated) {
                    metrics.sent(stats.bytes);
                }
            } catch (IOException e) {
                metrics.error();
                throw e;
            } catch (RuntimeException e) {
                metrics.error();
                throw e;
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            ensureSnapshotAvailable();
            if (!endOfInput || endOfInputCommittables != null) {
                return;
            }
            endOfInputCommittables = snapshotCommittables();
            if (!config.isCatalogTable()) {
                for (CobbleShardCommittable committable : endOfInputCommittables) {
                    CobbleSinkPaths.markEndOfInputSnapshot(config, committable);
                }
            }
        }

        @Override
        public Collection<CobbleShardCommittable> prepareCommit()
                throws IOException, InterruptedException {
            ensureSnapshotAvailable();
            if (endOfInputCommittables != null) {
                List<CobbleShardCommittable> committables = endOfInputCommittables;
                endOfInputCommittables = null;
                return committables;
            }
            return snapshotCommittables();
        }

        private List<CobbleShardCommittable> snapshotCommittables()
                throws IOException, InterruptedException {
            List<CobbleConcurrentSnapshots.Completed> completed;
            try {
                completed =
                        CobbleConcurrentSnapshots.snapshotAll(
                                writers.values(), snapshotTimeoutMillis);
            } catch (IOException | InterruptedException | RuntimeException | LinkageError error) {
                snapshotFailed = true;
                throw error;
            }
            List<CobbleShardCommittable> committables =
                    new ArrayList<CobbleShardCommittable>(completed.size());
            Map<Integer, CobbleBucketWriterState> prepared =
                    new LinkedHashMap<Integer, CobbleBucketWriterState>();
            for (CobbleConcurrentSnapshots.Completed completion : completed) {
                CobbleSingleBucketWriter writer = writers.get(Integer.valueOf(completion.bucketId));
                ShardSnapshot snapshot = completion.snapshot;
                prepared.put(
                        Integer.valueOf(writer.bucketId),
                        CobbleBucketWriterState.bucket(
                                snapshot.dbId, snapshot.snapshotId, writer.bucketId));
                committables.add(
                        new CobbleShardCommittable(
                                totalBuckets, writer.bucketId, writer.writerPath, snapshot));
            }
            lastPreparedByBucket.clear();
            lastPreparedByBucket.putAll(prepared);
            return committables;
        }

        private void ensureSnapshotAvailable() throws IOException {
            if (snapshotFailed) {
                throw new IOException("Cobble writer cannot continue after a failed snapshot.");
            }
        }

        @Override
        public void close() throws Exception {
            RuntimeException closeFailure = closeAll(writers.values());
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public List<CobbleBucketWriterState> snapshotState(long checkpointId) {
            return new ArrayList<CobbleBucketWriterState>(lastPreparedByBucket.values());
        }

        private static RuntimeException closeAll(Collection<CobbleSingleBucketWriter> writers) {
            RuntimeException failure = null;
            for (CobbleSingleBucketWriter writer : writers) {
                try {
                    writer.close();
                } catch (RuntimeException error) {
                    if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                }
            }
            return failure;
        }
    }

    private static final class PassthroughCommitter implements Committer<CobbleShardCommittable> {
        @Override
        public void commit(Collection<CommitRequest<CobbleShardCommittable>> committables) {
            for (CommitRequest<CobbleShardCommittable> request : committables) {
                request.signalAlreadyCommitted();
            }
        }

        @Override
        public void close() {}
    }

    static final class Global implements Committer<CobbleShardCommittable> {
        private final CobbleDynamicTableSink.SerializableConfig config;
        private final DbCoordinator coordinator;
        private final TableSnapshotCommitter tableCommitter;
        private final CobbleCatalogTableReference.Opened catalogOpened;
        private long greatestCommitId = -1L;

        Global(CobbleDynamicTableSink.SerializableConfig config) throws IOException {
            CobbleLoader.ensureCobbleLoaded();
            this.config = config;
            if (config.isCatalogTable()) {
                CobbleCatalogTableReference.Opened opened = config.catalogTable.openValidated();
                try {
                    io.cobble.Config catalogRuntime =
                            config.catalogTable.runtimeConfig().totalBuckets(config.bucketCount);
                    this.coordinator = opened.table().coordinator(catalogRuntime);
                    this.tableCommitter =
                            opened.table()
                                    .snapshotCommitter(
                                            catalogRuntime,
                                            Math.max(4, config.snapshotRetention + 2));
                    this.catalogOpened = opened;
                } catch (RuntimeException error) {
                    opened.close();
                    throw error;
                }
            } else {
                io.cobble.Config coordinatorConfig =
                        CobbleSinkPaths.createCoordinatorConfig(config);
                this.coordinator = DbCoordinator.open(coordinatorConfig);
                this.tableCommitter =
                        TableSnapshotCommitter.open(
                                coordinatorConfig,
                                config.bucketCount,
                                Math.max(4, config.snapshotRetention + 2));
                this.catalogOpened = null;
            }
        }

        @Override
        public void commit(Collection<CommitRequest<CobbleShardCommittable>> committables)
                throws IOException {
            if (committables.isEmpty()) {
                return;
            }
            List<CobbleShardCommittable> shardCommittables = new ArrayList<>(committables.size());
            for (CommitRequest<CobbleShardCommittable> request : committables) {
                shardCommittables.add(request.getCommittable());
            }
            commitCommittables(
                    commitId(shardCommittables), shardCommittables, Collections.emptyList());
            for (CommitRequest<CobbleShardCommittable> request : committables) {
                request.signalAlreadyCommitted();
            }
        }

        void commitCommittables(
                long checkpointId,
                List<CobbleShardCommittable> committables,
                List<CobbleShardCommittable> abandonedCommittables)
                throws IOException {
            if (committables.isEmpty()) {
                expireAbandonedCommittables(abandonedCommittables, Collections.emptyList());
                return;
            }
            materialize(resolveCommitId(checkpointId, committables), committables);
            expireAbandonedCommittables(abandonedCommittables, committables);
        }

        @Override
        public void close() throws Exception {
            if (catalogOpened != null) {
                try {
                    tableCommitter.close();
                } finally {
                    try {
                        coordinator.close();
                    } finally {
                        catalogOpened.close();
                    }
                }
                return;
            }
            try {
                waitForEndOfInputMarkers();
                if (CobbleSinkPaths.countEndOfInputMarkers(config) > 0) {
                    refreshLatestSnapshotOnClose();
                    CobbleSinkPaths.clearEndOfInputMarkers(config);
                }
            } finally {
                try {
                    tableCommitter.close();
                } finally {
                    coordinator.close();
                }
            }
        }

        private void waitForEndOfInputMarkers() throws InterruptedException {
            long deadlineNanos = System.nanoTime() + 5_000_000_000L;
            int target = config.bucketCount;
            while (System.nanoTime() < deadlineNanos) {
                if (CobbleSinkPaths.countEndOfInputMarkers(config) >= target) {
                    return;
                }
                Thread.sleep(50L);
            }
        }

        private void materialize(long checkpointId, List<CobbleShardCommittable> committables)
                throws IOException {
            int totalBuckets = committables.get(0).totalBuckets;
            for (CobbleShardCommittable committable : committables) {
                if (committable.totalBuckets != totalBuckets) {
                    throw new IOException(
                            "Mismatched total bucket count across Cobble committables.");
                }
            }
            List<ShardSnapshot> shardSnapshots = new ArrayList<>(committables.size());
            for (CobbleShardCommittable committable : committables) {
                shardSnapshots.add(committable.shardSnapshot);
            }
            validateCompleteCoverage(shardSnapshots, totalBuckets);

            if (catalogOpened != null) {
                try (CobbleCatalogTableReference.Opened ignored =
                        config.catalogTable.openValidated()) {
                    // Re-check the captured identity at every materialization boundary.
                }
                GlobalSnapshot committed = tableCommitter.commitBatch(checkpointId, shardSnapshots);
                GlobalSnapshot current =
                        committed == null ? coordinator.loadCurrentGlobalSnapshot() : committed;
                if (current == null) {
                    throw new IOException(
                            "Cobble catalog table commit did not produce a global snapshot.");
                }
                greatestCommitId = Math.max(greatestCommitId, checkpointId);
                expireOlderSnapshots(current.id, Collections.emptyMap());
                return;
            }

            Map<String, String> writerPathByDbId = CobbleSinkPaths.loadWriterPathIndex(config);
            for (CobbleShardCommittable committable : committables) {
                writerPathByDbId.put(
                        committable.shardSnapshot.dbId,
                        CobbleSinkPaths.coordinatorWriterPath(config, committable));
            }
            CobbleSinkPaths.storeWriterPathIndex(config, writerPathByDbId);
            GlobalSnapshot committed = tableCommitter.commitBatch(checkpointId, shardSnapshots);
            GlobalSnapshot current =
                    committed == null ? coordinator.loadCurrentGlobalSnapshot() : committed;
            if (current == null) {
                throw new IOException("Cobble table commit did not produce a global snapshot.");
            }
            greatestCommitId = Math.max(greatestCommitId, checkpointId);
            expireOlderSnapshots(current.id, writerPathByDbId);
        }

        private static long commitId(List<CobbleShardCommittable> committables) {
            long commitId = 0L;
            for (CobbleShardCommittable committable : committables) {
                commitId = Math.max(commitId, committable.shardSnapshot.snapshotId);
            }
            return commitId;
        }

        private void validateCompleteCoverage(List<ShardSnapshot> snapshots, int totalBuckets)
                throws IOException {
            ShardSnapshot[] bucketOwners = new ShardSnapshot[totalBuckets];
            assignBuckets(bucketOwners, snapshots, totalBuckets);
            for (int bucket = 0; bucket < totalBuckets; bucket++) {
                if (bucketOwners[bucket] == null) {
                    throw new IOException(
                            "Missing shard snapshot coverage for bucket " + bucket + ".");
                }
            }
        }

        private void assignBuckets(
                ShardSnapshot[] bucketOwners, List<ShardSnapshot> snapshots, int totalBuckets)
                throws IOException {
            for (ShardSnapshot snapshot : snapshots) {
                if (snapshot == null || snapshot.ranges == null) {
                    continue;
                }
                for (ShardSnapshot.Range range : snapshot.ranges) {
                    if (range == null
                            || range.start < 0
                            || range.end < range.start
                            || range.end >= totalBuckets) {
                        throw new IOException("Invalid shard range in snapshot materialization.");
                    }
                    for (int bucket = range.start; bucket <= range.end; bucket++) {
                        bucketOwners[bucket] = snapshot;
                    }
                }
            }
        }

        private void pruneWriterSnapshot(ShardSnapshot shardSnapshot, String writerPath)
                throws IOException {
            if (writerPath == null || writerPath.isEmpty()) {
                throw new IOException("Missing writer path while pruning shard snapshot.");
            }
            try {
                SnapshotTools.pruneShardSnapshot(
                        CobbleSinkPaths.createWriterConfigForWriterPath(config, writerPath),
                        shardSnapshot.dbId,
                        shardSnapshot.snapshotId);
            } catch (RuntimeException e) {
                throw new IOException("Failed to prune writer shard snapshot", e);
            }
        }

        private void expireAbandonedCommittables(
                List<CobbleShardCommittable> abandonedCommittables,
                List<CobbleShardCommittable> retainedCommittables)
                throws IOException {
            if (catalogOpened != null) {
                // Catalog storage has no writer-local path namespace. Preserve unreferenced
                // shard snapshots until the native catalog gains ownership-aware shard GC.
                return;
            }
            if (abandonedCommittables == null || abandonedCommittables.isEmpty()) {
                return;
            }
            Set<String> retainedSnapshotIdentities = new HashSet<>();
            for (CobbleShardCommittable retained : retainedCommittables) {
                retainedSnapshotIdentities.add(snapshotIdentity(retained.shardSnapshot));
            }
            Set<String> prunedSnapshotIdentities = new HashSet<>();
            for (CobbleShardCommittable abandoned : abandonedCommittables) {
                String snapshotIdentity = snapshotIdentity(abandoned.shardSnapshot);
                if (retainedSnapshotIdentities.contains(snapshotIdentity)
                        || !prunedSnapshotIdentities.add(snapshotIdentity)) {
                    continue;
                }
                pruneWriterSnapshot(abandoned.shardSnapshot, abandoned.writerPath);
            }
        }

        private void expireOlderSnapshots(
                long retainedSnapshotId, Map<String, String> writerPathByDbId) throws IOException {
            if (config.snapshotRetention <= 0) {
                return;
            }
            List<GlobalSnapshot> snapshots = coordinator.listGlobalSnapshots();
            Collections.sort(snapshots, Comparator.comparingLong(left -> left.id));

            int toExpire = snapshots.size() - config.snapshotRetention;
            for (GlobalSnapshot snapshot : snapshots) {
                if (toExpire <= 0) {
                    break;
                }
                if (snapshot.id == retainedSnapshotId) {
                    continue;
                }
                if (catalogOpened == null) {
                    for (ShardSnapshot shardSnapshot : snapshot.shardSnapshots) {
                        String writerPath = writerPathByDbId.get(shardSnapshot.dbId);
                        if (writerPath == null) {
                            throw new IOException(
                                    "Missing writer path mapping for shard dbId "
                                            + shardSnapshot.dbId);
                        }
                        pruneWriterSnapshot(shardSnapshot, writerPath);
                    }
                }
                coordinator.expireSnapshot(snapshot.id);
                toExpire--;
            }
        }

        private void refreshLatestSnapshotOnClose() throws IOException {
            GlobalSnapshot latest = coordinator.loadCurrentGlobalSnapshot();
            Map<String, String> writerPathByDbId = CobbleSinkPaths.loadWriterPathIndex(config);
            List<ShardSnapshot> refreshed = collectEndOfInputLatestShards(writerPathByDbId);
            materializeRefreshedSnapshot(latest, refreshed, writerPathByDbId);
        }

        private void materializeRefreshedSnapshot(
                GlobalSnapshot latest,
                List<ShardSnapshot> refreshed,
                Map<String, String> writerPathByDbId)
                throws IOException {
            if (latest == null
                    || latest.shardSnapshots == null
                    || latest.shardSnapshots.isEmpty()) {
                CobbleSinkPaths.storeWriterPathIndex(config, writerPathByDbId);
                commitEndOfInputSnapshot(refreshed, writerPathByDbId);
                return;
            }
            if (hasSameBucketCoverage(latest, refreshed, latest.totalBuckets)) {
                return;
            }
            CobbleSinkPaths.storeWriterPathIndex(config, writerPathByDbId);
            commitEndOfInputSnapshot(refreshed, writerPathByDbId);
        }

        private void commitEndOfInputSnapshot(
                List<ShardSnapshot> snapshots, Map<String, String> writerPathByDbId)
                throws IOException {
            long commitId = nextCommitIdForSnapshots(snapshots);
            GlobalSnapshot committed = tableCommitter.commitBatch(commitId, snapshots);
            GlobalSnapshot current =
                    committed == null ? coordinator.loadCurrentGlobalSnapshot() : committed;
            if (current == null) {
                throw new IOException("Cobble end-of-input commit did not produce a snapshot.");
            }
            greatestCommitId = Math.max(greatestCommitId, commitId);
            expireOlderSnapshots(current.id, writerPathByDbId);
        }

        private long resolveCommitId(long checkpointId, List<CobbleShardCommittable> committables)
                throws IOException {
            if (checkpointId != Long.MAX_VALUE) {
                return checkpointId;
            }
            List<ShardSnapshot> snapshots = new ArrayList<>(committables.size());
            for (CobbleShardCommittable committable : committables) {
                snapshots.add(committable.shardSnapshot);
            }
            return nextCommitIdForSnapshots(snapshots);
        }

        private long nextCommitIdForSnapshots(List<ShardSnapshot> snapshots) throws IOException {
            long latestSnapshotId = -1L;
            for (ShardSnapshot snapshot : snapshots) {
                latestSnapshotId = Math.max(latestSnapshotId, snapshot.snapshotId);
            }
            if (latestSnapshotId == Long.MAX_VALUE || greatestCommitId == Long.MAX_VALUE) {
                throw new IOException("Cobble end-of-input commit id cannot be incremented.");
            }
            return Math.max(latestSnapshotId + 1L, greatestCommitId + 1L);
        }

        private List<ShardSnapshot> collectEndOfInputLatestShards(
                Map<String, String> writerPathByDbId) throws IOException {
            return CobbleSinkPaths.resolveEndOfInputSnapshots(
                    config, CobbleSinkPaths.listEndOfInputCommittables(config), writerPathByDbId);
        }
    }

    private static GlobalSnapshot loadCurrentGlobalSnapshot(
            CobbleDynamicTableSink.SerializableConfig config) throws IOException {
        CobbleLoader.ensureCobbleLoaded();
        DbCoordinator coordinator = null;
        try {
            coordinator = DbCoordinator.open(CobbleSinkPaths.createCoordinatorConfig(config));
            return coordinator.loadCurrentGlobalSnapshot();
        } finally {
            if (coordinator != null) {
                coordinator.close();
            }
        }
    }

    private static GlobalSnapshot loadCurrentTableSnapshot(
            CobbleDynamicTableSink.SerializableConfig config) throws IOException {
        if (!config.isCatalogTable()) {
            return loadCurrentGlobalSnapshot(config);
        }
        try (CobbleCatalogTableReference.Opened opened = config.catalogTable.openValidated();
                DbCoordinator coordinator =
                        opened.table()
                                .coordinator(
                                        config.catalogTable
                                                .runtimeConfig()
                                                .totalBuckets(config.bucketCount))) {
            return coordinator.loadCurrentGlobalSnapshot();
        }
    }

    private static String snapshotIdentity(ShardSnapshot shardSnapshot) {
        return shardSnapshot.dbId
                + "#"
                + shardSnapshot.snapshotId
                + "#"
                + shardSnapshot.manifestPath;
    }

    private static boolean hasSameBucketCoverage(
            GlobalSnapshot latest, List<ShardSnapshot> refreshed, int totalBuckets)
            throws IOException {
        String[] latestByBucket = buildBucketIdentities(latest.shardSnapshots, totalBuckets);
        String[] refreshedByBucket = buildBucketIdentities(refreshed, totalBuckets);
        for (int i = 0; i < totalBuckets; i++) {
            if (!latestByBucket[i].equals(refreshedByBucket[i])) {
                return false;
            }
        }
        return true;
    }

    private static String[] buildBucketIdentities(List<ShardSnapshot> snapshots, int totalBuckets)
            throws IOException {
        String[] byBucket = new String[totalBuckets];
        for (ShardSnapshot snapshot : snapshots) {
            if (snapshot == null || snapshot.ranges == null) {
                continue;
            }
            String identity = snapshotIdentity(snapshot);
            for (ShardSnapshot.Range range : snapshot.ranges) {
                if (range == null
                        || range.start < 0
                        || range.end < range.start
                        || range.end >= totalBuckets) {
                    throw new IOException(
                            "Invalid shard range while comparing global snapshot coverage.");
                }
                for (int bucket = range.start; bucket <= range.end; bucket++) {
                    byBucket[bucket] = identity;
                }
            }
        }
        for (int bucket = 0; bucket < totalBuckets; bucket++) {
            if (byBucket[bucket] == null) {
                throw new IOException("Missing bucket coverage for bucket " + bucket + ".");
            }
        }
        return byBucket;
    }

    static MutationStats applyRowChange(
            Table table,
            CobbleTableRowConverter rowConverter,
            CobbleDynamicTableSink.SerializableConfig config,
            byte[] encodedKey,
            RowData element) {
        List<Value> values = rowConverter.toValues(element);
        return applyRowChange(table, config, encodedKey, element, values);
    }

    private static MutationStats applyRowChange(
            Table table,
            CobbleDynamicTableSink.SerializableConfig config,
            byte[] encodedKey,
            RowData element,
            List<Value> values) {
        switch (element.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                table.put(values);
                return new MutationStats(true, encodedKey.length);
            case UPDATE_BEFORE:
                return MutationStats.IGNORED;
            case DELETE:
                table.delete(buildTableKey(table, config, values));
                return new MutationStats(true, encodedKey.length);
            default:
                throw new UnsupportedOperationException(
                        "Cobble SQL sink only supports INSERT, UPDATE_BEFORE, UPDATE_AFTER, and"
                                + " DELETE rows, but received "
                                + element.getRowKind());
        }
    }

    private static TableKey buildTableKey(
            Table table, CobbleDynamicTableSink.SerializableConfig config, List<Value> rowValues) {
        TableKeyBuilder builder = table.keyBuilder();
        for (CobbleDynamicTableSink.SerializableField field : config.keyFields) {
            builder.push(rowValues.get(field.rowIndex));
        }
        return builder.build();
    }

    static int bucketOwnerSubtask(int bucket, int totalBuckets, int sinkParallelism) {
        if (sinkParallelism <= 0) {
            throw new IllegalArgumentException("sinkParallelism must be > 0");
        }
        if (bucket < 0 || bucket >= totalBuckets) {
            throw new IllegalArgumentException(
                    "bucket must be in [0, totalBuckets), got " + bucket);
        }
        return (int) ((((long) bucket + 1L) * (long) sinkParallelism - 1L) / (long) totalBuckets);
    }

    static final class MutationStats {
        private static final MutationStats IGNORED = new MutationStats(false, 0L);
        final boolean mutated;
        final long bytes;

        private MutationStats(boolean mutated, long bytes) {
            this.mutated = mutated;
            this.bytes = bytes;
        }
    }
}
