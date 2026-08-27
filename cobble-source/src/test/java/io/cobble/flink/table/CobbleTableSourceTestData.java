package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.Db;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleLoader;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.Table;
import io.cobble.table.TableSchema;
import io.cobble.table.TableSnapshotCommitter;
import io.cobble.table.Value;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Creates committed native Cobble Table snapshots for source tests. */
final class CobbleTableSourceTestData {

    private static final AtomicLong CHECKPOINT_IDS = new AtomicLong(1L);

    private CobbleTableSourceTestData() {}

    static GlobalSnapshot write(
            Path root, int bucketCount, TableSchema schema, List<List<Value>> rows)
            throws Exception {
        CobbleLoader.ensureCobbleLoaded();
        Files.createDirectories(root);
        Config config = config(root, bucketCount);
        ShardSnapshot shard;
        try (Db db = Db.open(config, 0, bucketCount - 1);
                Table table = Table.create(db, CobbleTableRowConverter.TABLE_NAME, schema)) {
            for (List<Value> row : rows) {
                table.put(row);
            }
            shard = db.startAsyncSnapshot().future().get();
        }
        try (TableSnapshotCommitter committer =
                TableSnapshotCommitter.open(config, bucketCount, 4)) {
            GlobalSnapshot snapshot =
                    committer.commitBatch(
                            CHECKPOINT_IDS.getAndIncrement(), Collections.singletonList(shard));
            if (snapshot == null) {
                throw new IllegalStateException("Table snapshot commit did not complete.");
            }
            return snapshot;
        }
    }

    static TableSchema idNameSchema() {
        return new CobbleDynamicTableSource.SerializableConfig(
                        "file:///unused",
                        2,
                        "latest",
                        "batch",
                        50L,
                        0L,
                        Collections.singletonList(
                                new CobbleDynamicTableSource.SerializableField(
                                        "id", "BIGINT", 0, -1)),
                        Collections.singletonList(
                                new CobbleDynamicTableSource.SerializableField(
                                        "name", "VARCHAR(2147483647)", 1, 0)))
                .tableSchema();
    }

    static List<List<Value>> idNameRows() {
        return Collections.singletonList(Arrays.asList(Value.int64(1L), Value.string("one")));
    }

    private static Config config(Path root, int bucketCount) {
        Config config = new Config().totalBuckets(bucketCount);
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
