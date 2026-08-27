package io.cobble.flink.inspect.internal;

import io.cobble.Config;
import io.cobble.Db;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleLoader;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.DataField;
import io.cobble.table.LogicalTypes;
import io.cobble.table.Table;
import io.cobble.table.TableSchema;
import io.cobble.table.TableSnapshotCommitter;
import io.cobble.table.Value;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Native Cobble Table fixtures shared by inspect integration tests. */
final class CobbleTableInspectTestData {

    private CobbleTableInspectTestData() {}

    static GlobalSnapshot write(
            Path root, int buckets, long checkpointId, TableSchema schema, List<List<Value>> rows)
            throws Exception {
        CobbleLoader.ensureCobbleLoaded();
        Files.createDirectories(root);
        Config config = config(root, buckets);
        ShardSnapshot shard;
        try (Db db = Db.open(config, 0, buckets - 1);
                Table table = Table.create(db, CobbleTableRowConverter.TABLE_NAME, schema)) {
            for (List<Value> row : rows) {
                table.put(row);
            }
            shard = db.startAsyncSnapshot().future().get();
        }
        try (TableSnapshotCommitter committer = TableSnapshotCommitter.open(config, buckets, 4)) {
            GlobalSnapshot snapshot =
                    committer.commitBatch(checkpointId, Collections.singletonList(shard));
            if (snapshot == null) {
                throw new IllegalStateException("Table snapshot commit did not complete");
            }
            return snapshot;
        }
    }

    static TableSchema stringKeyValueSchema() {
        return new TableSchema(
                Arrays.asList(
                        new DataField(0L, "id", LogicalTypes.string().notNull()),
                        new DataField(1L, "payload", LogicalTypes.string())),
                Collections.singletonList(0L),
                Collections.singletonList(0L));
    }

    static TableSchema compositeSchema() {
        return new TableSchema(
                Arrays.asList(
                        new DataField(0L, "region", LogicalTypes.string().notNull()),
                        new DataField(1L, "id", LogicalTypes.string().notNull()),
                        new DataField(2L, "payload", LogicalTypes.string())),
                Arrays.asList(0L, 1L),
                Arrays.asList(0L, 1L));
    }

    static Config config(Path root, int buckets) {
        Config config = new Config().totalBuckets(buckets);
        config.walEnabled = false;
        config.snapshotOnlyTrack = true;
        config.snapshotDisableIncrementalBaseLink = true;
        config.logConsole = false;
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = root.toAbsolutePath().toString();
        volume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT,
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH);
        config.addVolume(volume);
        return config;
    }
}
