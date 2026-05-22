package io.cobble.flink.table;

import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.PendingSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.SnapshotTools;
import io.cobble.structured.Db;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Sink implementation with a global post-commit topology for snapshot materialization. */
final class CobbleSqlSink
        implements TwoPhaseCommittingSink<RowData, CobbleShardCommittable>,
                WithPostCommitTopology<RowData, CobbleShardCommittable> {

    private static final long serialVersionUID = 1L;

    private final CobbleDynamicTableSink.SerializableConfig config;

    CobbleSqlSink(CobbleDynamicTableSink.SerializableConfig config) {
        this.config = config;
    }

    @Override
    public PrecommittingSinkWriter<RowData, CobbleShardCommittable> createWriter(
            Sink.InitContext context) throws IOException {
        return new Writer(config, context);
    }

    @Override
    public Committer<CobbleShardCommittable> createCommitter() {
        return new PassthroughCommitter();
    }

    @Override
    public SimpleVersionedSerializer<CobbleShardCommittable> getCommittableSerializer() {
        return new CobbleShardCommittable.Serializer();
    }

    @Override
    public void addPostCommitTopology(
            DataStream<CommittableMessage<CobbleShardCommittable>> committables) {
        committables
                .global()
                .transform(
                        "Cobble Global Commit Operator",
                        Types.VOID,
                        new GlobalCommitOperatorFactory(config))
                .setParallelism(1)
                .name("Cobble Global Commit Operator");
    }

    private static final class Writer
            implements TwoPhaseCommittingSink.PrecommittingSinkWriter<
                    RowData, CobbleShardCommittable> {
        private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

        private final CobbleDynamicTableSink.SerializableConfig config;
        private final int subtaskId;
        private final int totalBuckets;
        private final int ownedRangeStart;
        private final int ownedRangeEnd;
        private final String writerPath;
        private final CobbleRowDataCodecs.RuntimeKeyEncoder keyEncoder;
        private final List<CobbleRowDataCodecs.RuntimeFieldEncoder> valueEncoders;
        private final Db db;
        private boolean hasStateToRetain;
        private boolean dirty;
        private CobbleShardCommittable endOfInputCommittable;

        private Writer(CobbleDynamicTableSink.SerializableConfig config, Sink.InitContext context)
                throws IOException {
            this.config = config;
            this.subtaskId = context.getSubtaskId();
            this.totalBuckets = config.bucketCount;
            this.ownedRangeStart = CobbleSinkPaths.writerRangeStart(config, subtaskId);
            this.ownedRangeEnd = CobbleSinkPaths.writerRangeEnd(config, subtaskId);
            this.writerPath =
                    CobbleSinkPaths.writerLocalDirectory(config, subtaskId).getAbsolutePath();
            this.keyEncoder = new CobbleRowDataCodecs.RuntimeKeyEncoder(config.keyFields);
            this.valueEncoders = new ArrayList<>(config.valueFields.size());
            for (CobbleDynamicTableSink.SerializableField field : config.valueFields) {
                this.valueEncoders.add(new CobbleRowDataCodecs.RuntimeFieldEncoder(field));
            }
            OpenedDb openedDb = restoreOrCreateDb(config, context);
            this.db = openedDb.db;
            this.hasStateToRetain = openedDb.restoredFromSnapshot;
            this.dirty = false;
        }

        @Override
        public void write(RowData element, Context context) throws IOException {
            byte[] encodedKey = keyEncoder.encode(element);
            int bucket = hashFixedBucket(encodedKey, totalBuckets);
            if (bucket < ownedRangeStart || bucket > ownedRangeEnd) {
                throw new IOException(
                        "Record bucket "
                                + bucket
                                + " is outside writer-owned range ["
                                + ownedRangeStart
                                + ", "
                                + ownedRangeEnd
                                + "] for subtask "
                                + subtaskId
                                + ".");
            }
            applyRowChange(db, valueEncoders, bucket, encodedKey, element);
            dirty = true;
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            if (!endOfInput || endOfInputCommittable != null) {
                return;
            }
            if (!dirty && !hasStateToRetain) {
                return;
            }
            endOfInputCommittable = snapshotCommittable();
            CobbleSinkPaths.markEndOfInputSnapshot(
                    config, endOfInputCommittable.shardSnapshot.dbId);
        }

        @Override
        public Collection<CobbleShardCommittable> prepareCommit()
                throws IOException, InterruptedException {
            if (endOfInputCommittable != null) {
                CobbleShardCommittable committable = endOfInputCommittable;
                endOfInputCommittable = null;
                LOG.info(
                        "Writer {} emitting endOfInput committable snapshotId={}",
                        Integer.valueOf(subtaskId),
                        Long.valueOf(committable.shardSnapshot.snapshotId));
                return Collections.singletonList(committable);
            }
            if (!dirty && !hasStateToRetain) {
                return Collections.emptyList();
            }
            return Collections.singletonList(snapshotCommittable());
        }

        private CobbleShardCommittable snapshotCommittable()
                throws IOException, InterruptedException {
            PendingSnapshot<ShardSnapshot> pending = db.startAsyncSnapshot();
            ShardSnapshot shardSnapshot;
            try {
                shardSnapshot = pending.future().get();
            } catch (java.util.concurrent.ExecutionException e) {
                throw new IOException("Failed to prepare Cobble shard snapshot", e.getCause());
            }
            dirty = false;
            hasStateToRetain = true;
            LOG.info(
                    "Prepared Cobble shard snapshot {} for bucket {} at {}",
                    Long.valueOf(shardSnapshot.snapshotId),
                    Integer.valueOf(subtaskId),
                    shardSnapshot.manifestPath);
            return new CobbleShardCommittable(totalBuckets, subtaskId, writerPath, shardSnapshot);
        }

        @Override
        public void close() throws Exception {
            db.close();
        }

        private static OpenedDb restoreOrCreateDb(
                CobbleDynamicTableSink.SerializableConfig config, Sink.InitContext context)
                throws IOException {
            int subtaskId = context.getSubtaskId();
            int targetRangeStart = CobbleSinkPaths.writerRangeStart(config, subtaskId);
            int targetRangeEnd = CobbleSinkPaths.writerRangeEnd(config, subtaskId);
            GlobalSnapshot globalSnapshot = loadCurrentGlobalSnapshot(config);
            if (globalSnapshot == null) {
                return new OpenedDb(
                        Db.open(
                                CobbleSinkPaths.createWriterConfig(config, subtaskId),
                                targetRangeStart,
                                targetRangeEnd),
                        false);
            }
            return new OpenedDb(
                    restoreRescaledDb(
                            config, subtaskId, globalSnapshot, targetRangeStart, targetRangeEnd),
                    true);
        }

        private static Db restoreRescaledDb(
                CobbleDynamicTableSink.SerializableConfig config,
                int subtaskId,
                GlobalSnapshot globalSnapshot,
                int targetRangeStart,
                int targetRangeEnd)
                throws IOException {
            List<RestoreSource> relevantSources =
                    collectRelevantSources(globalSnapshot, targetRangeStart, targetRangeEnd);
            LOG.info(
                    "Cobble sink writer {} restore target range [{}-{}], relevant sources={}",
                    Integer.valueOf(subtaskId),
                    Integer.valueOf(targetRangeStart),
                    Integer.valueOf(targetRangeEnd),
                    Integer.valueOf(relevantSources.size()));
            if (relevantSources.isEmpty()) {
                throw new IOException(
                        "Cobble sink restore could not find any checkpoint shard covering writer range ["
                                + targetRangeStart
                                + ", "
                                + targetRangeEnd
                                + "].");
            }
            ensureRestoreCoverage(relevantSources, targetRangeStart, targetRangeEnd);

            RestoreSource baseSource = selectBaseSource(relevantSources);
            LOG.info(
                    "Cobble sink writer {} base restore source dbId={} snapshotId={} intersections={}",
                    Integer.valueOf(subtaskId),
                    baseSource.shardSnapshot.dbId,
                    Long.valueOf(baseSource.shardSnapshot.snapshotId),
                    baseSource.intersections);
            Db db =
                    Db.restoreWithManifest(
                            CobbleSinkPaths.createWriterConfig(config, subtaskId),
                            baseSource.shardSnapshot.manifestPath);
            boolean success = false;
            try {
                shrinkBaseSourceToTargetRange(db, baseSource, targetRangeStart, targetRangeEnd);
                for (RestoreSource source : relevantSources) {
                    if (source == baseSource) {
                        continue;
                    }
                    materializeSourceSnapshotLocally(config, subtaskId, source.shardSnapshot);
                    int[] starts = new int[source.intersections.size()];
                    int[] ends = new int[source.intersections.size()];
                    for (int i = 0; i < source.intersections.size(); i++) {
                        starts[i] = source.intersections.get(i).start;
                        ends[i] = source.intersections.get(i).end;
                    }
                    LOG.info(
                            "Cobble sink writer {} expanding from dbId={} snapshotId={} intersections={}",
                            Integer.valueOf(subtaskId),
                            source.shardSnapshot.dbId,
                            Long.valueOf(source.shardSnapshot.snapshotId),
                            source.intersections);
                    db.expandBucket(
                            source.shardSnapshot.dbId,
                            source.shardSnapshot.snapshotId,
                            starts,
                            ends);
                }
                success = true;
                return db;
            } finally {
                if (!success) {
                    db.close();
                }
            }
        }

        private static void materializeSourceSnapshotLocally(
                CobbleDynamicTableSink.SerializableConfig config,
                int subtaskId,
                ShardSnapshot sourceSnapshot)
                throws IOException {
            File localSourceSnapshotManifest =
                    new File(
                            new File(
                                    new File(
                                            CobbleSinkPaths.writerLocalDirectory(config, subtaskId),
                                            sourceSnapshot.dbId),
                                    "snapshot"),
                            "SNAPSHOT-" + sourceSnapshot.snapshotId);
            if (localSourceSnapshotManifest.exists()) {
                return;
            }
            try (Db ignored =
                    Db.restoreWithManifest(
                            CobbleSinkPaths.createWriterConfig(config, subtaskId),
                            sourceSnapshot.manifestPath)) {
                // Materialize source shard snapshot into current writer scope for expandBucket
                // lookup.
            }
        }

        private static final class OpenedDb {
            private final Db db;
            private final boolean restoredFromSnapshot;

            private OpenedDb(Db db, boolean restoredFromSnapshot) {
                this.db = db;
                this.restoredFromSnapshot = restoredFromSnapshot;
            }
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

        Global(CobbleDynamicTableSink.SerializableConfig config) throws IOException {
            this.config = config;
            this.coordinator = DbCoordinator.open(CobbleSinkPaths.createCoordinatorConfig(config));
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
            commitCommittables(shardCommittables, Collections.emptyList());
            for (CommitRequest<CobbleShardCommittable> request : committables) {
                request.signalAlreadyCommitted();
            }
        }

        void commitCommittables(
                List<CobbleShardCommittable> committables,
                List<CobbleShardCommittable> abandonedCommittables)
                throws IOException {
            if (committables.isEmpty()) {
                expireAbandonedCommittables(abandonedCommittables, Collections.emptyList());
                return;
            }
            materialize(committables);
            expireAbandonedCommittables(abandonedCommittables, committables);
        }

        @Override
        public void close() throws Exception {
            try {
                waitForEndOfInputMarkers();
                if (CobbleSinkPaths.hasEndOfInputMarkers(config)) {
                    refreshLatestSnapshotOnClose();
                    CobbleSinkPaths.clearEndOfInputMarkers(config);
                }
            } finally {
                coordinator.close();
            }
        }

        private void waitForEndOfInputMarkers() throws InterruptedException {
            long deadlineNanos = System.nanoTime() + 5_000_000_000L;
            int target = Math.max(1, config.sinkParallelism);
            while (System.nanoTime() < deadlineNanos) {
                if (CobbleSinkPaths.countEndOfInputMarkers(config) >= target) {
                    return;
                }
                Thread.sleep(50L);
            }
        }

        private void materialize(List<CobbleShardCommittable> committables) throws IOException {
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

            Map<String, String> writerPathByDbId = loadWriterPathIndex();
            GlobalSnapshot latest = coordinator.loadCurrentGlobalSnapshot();
            for (CobbleShardCommittable committable : committables) {
                writerPathByDbId.put(committable.shardSnapshot.dbId, committable.writerPath);
            }
            storeWriterPathIndex(writerPathByDbId);
            long globalSnapshotId = latest == null ? 1L : latest.id + 1L;
            coordinator.materializeGlobalSnapshot(totalBuckets, globalSnapshotId, shardSnapshots);
            expireOlderSnapshots(globalSnapshotId, writerPathByDbId);
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
                for (ShardSnapshot shardSnapshot : snapshot.shardSnapshots) {
                    String writerPath = writerPathByDbId.get(shardSnapshot.dbId);
                    if (writerPath == null) {
                        throw new IOException(
                                "Missing writer path mapping for shard dbId " + shardSnapshot.dbId);
                    }
                    pruneWriterSnapshot(shardSnapshot, writerPath);
                }
                coordinator.expireSnapshot(snapshot.id);
                toExpire--;
            }
        }

        private Map<String, String> loadWriterPathIndex() throws IOException {
            File pathIndexFile = CobbleSinkPaths.writerPathIndexFile(config);
            Map<String, String> writerPathByDbId = new HashMap<>();
            if (!pathIndexFile.exists()) {
                return writerPathByDbId;
            }
            Properties properties = new Properties();
            try (FileInputStream in = new FileInputStream(pathIndexFile)) {
                properties.load(in);
            }
            for (String dbId : properties.stringPropertyNames()) {
                writerPathByDbId.put(dbId, properties.getProperty(dbId));
            }
            return writerPathByDbId;
        }

        private void storeWriterPathIndex(Map<String, String> writerPathByDbId) throws IOException {
            File pathIndexFile = CobbleSinkPaths.writerPathIndexFile(config);
            File parent = pathIndexFile.getParentFile();
            if (parent != null) {
                parent.mkdirs();
            }
            Properties properties = new Properties();
            for (Map.Entry<String, String> entry : writerPathByDbId.entrySet()) {
                properties.setProperty(entry.getKey(), entry.getValue());
            }
            try (FileOutputStream out = new FileOutputStream(pathIndexFile)) {
                properties.store(out, "Cobble writer path index by dbId");
            }
        }

        private void refreshLatestSnapshotOnClose() throws IOException {
            GlobalSnapshot latest = coordinator.loadCurrentGlobalSnapshot();
            Map<String, String> writerPathByDbId = loadWriterPathIndex();
            if (latest == null
                    || latest.shardSnapshots == null
                    || latest.shardSnapshots.isEmpty()) {
                List<ShardSnapshot> initial = collectEndOfInputLatestShards(writerPathByDbId);
                if (initial.isEmpty()) {
                    return;
                }
                storeWriterPathIndex(writerPathByDbId);
                coordinator.materializeGlobalSnapshot(config.bucketCount, 1L, initial);
                expireOlderSnapshots(1L, writerPathByDbId);
                return;
            }
            List<ShardSnapshot> refreshed = new ArrayList<>(latest.shardSnapshots.size());
            for (ShardSnapshot shard : latest.shardSnapshots) {
                refreshed.add(loadLatestShardSnapshot(shard.dbId, writerPathByDbId, shard));
            }
            if (hasSameBucketCoverage(latest, refreshed, latest.totalBuckets)) {
                return;
            }
            long globalSnapshotId = latest.id + 1L;
            coordinator.materializeGlobalSnapshot(latest.totalBuckets, globalSnapshotId, refreshed);
            expireOlderSnapshots(globalSnapshotId, writerPathByDbId);
        }

        private List<ShardSnapshot> collectEndOfInputLatestShards(
                Map<String, String> writerPathByDbId) throws IOException {
            List<String> dbIds = CobbleSinkPaths.listEndOfInputMarkerDbIds(config);
            List<ShardSnapshot> refreshed =
                    new ArrayList<>(Math.min(dbIds.size(), config.sinkParallelism));
            for (String dbId : dbIds) {
                ShardSnapshot latestShard = loadLatestShardSnapshot(dbId, writerPathByDbId, null);
                if (latestShard != null) {
                    refreshed.add(latestShard);
                    if (refreshed.size() >= config.sinkParallelism) {
                        break;
                    }
                }
            }
            if (refreshed.size() < config.sinkParallelism) {
                throw new IOException(
                        "Failed to resolve enough end-of-input shard snapshots. resolved="
                                + refreshed.size()
                                + ", expected="
                                + config.sinkParallelism
                                + ".");
            }
            return refreshed;
        }

        private ShardSnapshot loadLatestShardSnapshot(
                String dbId, Map<String, String> writerPathByDbId, ShardSnapshot fallback)
                throws IOException {
            String writerPath =
                    writerPathByDbId.computeIfAbsent(
                            dbId,
                            ignored -> CobbleSinkPaths.tableRootPath(config).getAbsolutePath());
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            while (true) {
                try (Db db =
                        Db.resume(
                                CobbleSinkPaths.createWriterConfigForWriterPath(config, writerPath),
                                dbId)) {
                    ShardSnapshot latestShard = db.getShardSnapshot(-1L);
                    if (latestShard != null) {
                        return latestShard;
                    }
                } catch (IllegalArgumentException e) {
                    if (e.getMessage() == null
                            || !e.getMessage().contains("snapshotId out of range")) {
                        throw new IOException(
                                "Failed to refresh end-of-input shard snapshot for dbId " + dbId,
                                e);
                    }
                }
                if (System.nanoTime() >= deadlineNanos) {
                    return fallback;
                }
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Interrupted while waiting for end-of-input shard snapshot for dbId "
                                    + dbId,
                            e);
                }
            }
        }
    }

    private static GlobalSnapshot loadCurrentGlobalSnapshot(
            CobbleDynamicTableSink.SerializableConfig config) throws IOException {
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

    private static List<RestoreSource> collectRelevantSources(
            GlobalSnapshot globalSnapshot, int targetRangeStart, int targetRangeEnd) {
        if (globalSnapshot == null || globalSnapshot.shardSnapshots == null) {
            return Collections.emptyList();
        }
        Map<String, RestoreSource> byIdentity = new LinkedHashMap<>();
        for (ShardSnapshot shardSnapshot : globalSnapshot.shardSnapshots) {
            if (shardSnapshot == null
                    || shardSnapshot.ranges == null
                    || shardSnapshot.ranges.isEmpty()) {
                continue;
            }
            String identity = snapshotIdentity(shardSnapshot);
            for (ShardSnapshot.Range range : shardSnapshot.ranges) {
                if (range == null) {
                    continue;
                }
                int start = Math.max(range.start, targetRangeStart);
                int end = Math.min(range.end, targetRangeEnd);
                if (start > end) {
                    continue;
                }
                RestoreSource source =
                        byIdentity.computeIfAbsent(
                                identity, ignored -> new RestoreSource(shardSnapshot));
                source.intersections.add(new BucketRange(start, end));
            }
        }
        List<RestoreSource> relevantSources = new ArrayList<>(byIdentity.values());
        for (RestoreSource source : relevantSources) {
            source.intersections.sort(Comparator.comparingInt(range -> range.start));
        }
        return relevantSources;
    }

    private static RestoreSource selectBaseSource(List<RestoreSource> restoreSources) {
        RestoreSource baseSource = null;
        int bestOverlap = -1;
        for (RestoreSource source : restoreSources) {
            int overlapSize = source.intersectionSize();
            if (overlapSize > bestOverlap) {
                bestOverlap = overlapSize;
                baseSource = source;
            }
        }
        return baseSource;
    }

    private static void shrinkBaseSourceToTargetRange(
            Db db, RestoreSource baseSource, int targetRangeStart, int targetRangeEnd) {
        List<Integer> starts = new ArrayList<>();
        List<Integer> ends = new ArrayList<>();
        for (ShardSnapshot.Range sourceRange : baseSource.shardSnapshot.ranges) {
            if (sourceRange == null) {
                continue;
            }
            if (sourceRange.start < targetRangeStart) {
                int leftEnd = Math.min(sourceRange.end, targetRangeStart - 1);
                if (sourceRange.start <= leftEnd) {
                    starts.add(sourceRange.start);
                    ends.add(leftEnd);
                }
            }
            if (sourceRange.end > targetRangeEnd) {
                int rightStart = Math.max(sourceRange.start, targetRangeEnd + 1);
                if (rightStart <= sourceRange.end) {
                    starts.add(rightStart);
                    ends.add(sourceRange.end);
                }
            }
        }
        if (starts.isEmpty()) {
            return;
        }
        db.shrinkBucket(
                starts.stream().mapToInt(Integer::intValue).toArray(),
                ends.stream().mapToInt(Integer::intValue).toArray());
    }

    private static void ensureRestoreCoverage(
            List<RestoreSource> restoreSources, int targetRangeStart, int targetRangeEnd)
            throws IOException {
        List<BucketRange> ranges = new ArrayList<>();
        for (RestoreSource source : restoreSources) {
            ranges.addAll(source.intersections);
        }
        ranges.sort(
                Comparator.comparingInt((BucketRange range) -> range.start)
                        .thenComparingInt(range -> range.end));

        int nextExpected = targetRangeStart;
        for (BucketRange range : ranges) {
            if (range.start > nextExpected) {
                throw new IOException(
                        "Cobble sink restore is missing checkpoint shard coverage for bucket range ["
                                + nextExpected
                                + ", "
                                + (range.start - 1)
                                + "].");
            }
            nextExpected = Math.max(nextExpected, range.end + 1);
            if (nextExpected > targetRangeEnd) {
                return;
            }
        }
        throw new IOException(
                "Cobble sink restore is missing checkpoint shard coverage for bucket range ["
                        + nextExpected
                        + ", "
                        + targetRangeEnd
                        + "].");
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

    private static final class RestoreSource {
        private final ShardSnapshot shardSnapshot;
        private final List<BucketRange> intersections = new ArrayList<>();

        private RestoreSource(ShardSnapshot shardSnapshot) {
            this.shardSnapshot = shardSnapshot;
        }

        private int intersectionSize() {
            int total = 0;
            for (BucketRange range : intersections) {
                total += range.end - range.start + 1;
            }
            return total;
        }
    }

    private static final class BucketRange {
        private final int start;
        private final int end;

        private BucketRange(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public String toString() {
            return "[" + start + "-" + end + "]";
        }
    }

    static int hashFixedBucket(byte[] encodedKey, int totalBuckets) {
        return Math.floorMod(java.util.Arrays.hashCode(encodedKey), totalBuckets);
    }

    static void applyRowChange(
            Db db,
            List<CobbleRowDataCodecs.RuntimeFieldEncoder> valueEncoders,
            int bucket,
            byte[] encodedKey,
            RowData element)
            throws IOException {
        switch (element.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                upsertRow(db, valueEncoders, bucket, encodedKey, element);
                return;
            case UPDATE_BEFORE:
                return;
            case DELETE:
                deleteRow(db, valueEncoders, bucket, encodedKey);
                return;
            default:
                throw new UnsupportedOperationException(
                        "Cobble SQL sink only supports INSERT, UPDATE_BEFORE, UPDATE_AFTER, and"
                                + " DELETE rows, but received "
                                + element.getRowKind());
        }
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

    private static void upsertRow(
            Db db,
            List<CobbleRowDataCodecs.RuntimeFieldEncoder> valueEncoders,
            int bucket,
            byte[] encodedKey,
            RowData element)
            throws IOException {
        for (CobbleRowDataCodecs.RuntimeFieldEncoder encoder : valueEncoders) {
            byte[] encodedValue = encoder.encodeNullable(element);
            if (encodedValue == null) {
                db.delete(bucket, encodedKey, encoder.structuredColumnIndex);
            } else {
                db.put(bucket, encodedKey, encoder.structuredColumnIndex, encodedValue);
            }
        }
    }

    private static void deleteRow(
            Db db,
            List<CobbleRowDataCodecs.RuntimeFieldEncoder> valueEncoders,
            int bucket,
            byte[] encodedKey)
            throws IOException {
        for (CobbleRowDataCodecs.RuntimeFieldEncoder encoder : valueEncoders) {
            db.delete(bucket, encodedKey, encoder.structuredColumnIndex);
        }
    }
}
