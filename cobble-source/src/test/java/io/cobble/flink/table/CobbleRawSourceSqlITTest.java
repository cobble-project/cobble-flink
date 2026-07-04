package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.ShardSnapshot;
import io.cobble.structured.Db;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * SQL-level E2E test for {@code source.kind='raw'}: writes a Cobble table via the direct Java API
 * (no sink sidecar), then reads it back through the Flink SQL planner using the raw source DDL.
 */
class CobbleRawSourceSqlITTest {

    @TempDir private Path tempDir;

    @Test
    void rawSourceSqlQueryReturnsDirectWrittenBytes() throws Exception {
        Path tablePath = tempDir.resolve("raw-sql-e2e");
        int bucketCount = 2;
        int numColumns = 2;

        // Write data directly — no Flink sink, no inspect-schema sidecar.
        byte[][][] written = {
            {bytes("key-1"), bytes("val-1-a"), bytes("val-1-b")},
            {bytes("key-2"), bytes("val-2-a"), null},
            {bytes("key-3"), bytes("val-3-a"), bytes("val-3-b")},
        };

        Config writerConfig = new Config().numColumns(numColumns).totalBuckets(bucketCount);
        writerConfig.governanceMode = Config.GovernanceMode.NOOP;
        writerConfig.logConsole = false;
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = tablePath.toAbsolutePath().toString();
        volume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT);
        writerConfig.addVolume(volume);

        List<ShardSnapshot> shardSnapshots = new ArrayList<>();
        try (Db db = Db.open(writerConfig, 0, bucketCount - 1)) {
            for (byte[][] row : written) {
                int bucket = Math.floorMod(Arrays.hashCode(row[0]), bucketCount);
                db.put(bucket, row[0], 0, row[1]);
                if (row[2] != null) {
                    db.put(bucket, row[0], 1, row[2]);
                }
            }
            shardSnapshots.add(db.snapshot());
        }

        Config coordConfig = new Config().totalBuckets(bucketCount);
        coordConfig.governanceMode = Config.GovernanceMode.NOOP;
        coordConfig.logConsole = false;
        Config.VolumeDescriptor coordVolume = new Config.VolumeDescriptor();
        coordVolume.baseDir = tablePath.toAbsolutePath().toString();
        coordVolume.kinds =
                Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        coordConfig.addVolume(coordVolume);
        try (DbCoordinator coordinator = DbCoordinator.open(coordConfig)) {
            coordinator.materializeGlobalSnapshot(bucketCount, 1L, shardSnapshots);
        }

        // Read through the Flink SQL planner.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "CREATE TABLE raw_cobble ("
                        + "  `key` BYTES,"
                        + "  `columns` ARRAY<BYTES>"
                        + ") WITH ("
                        + "  'connector' = 'cobble',"
                        + "  'source.kind' = 'raw',"
                        + "  'path' = '"
                        + escape(tablePath)
                        + "',"
                        + "  'bucket' = '"
                        + bucketCount
                        + "',"
                        + "  'raw.columns' = '0,1',"
                        + "  'scan.checkpoint-id' = '1',"
                        + "  'scan.mode' = 'batch'"
                        + ")");

        List<Row> results = new ArrayList<>();
        try (org.apache.flink.util.CloseableIterator<Row> iter =
                tableEnv.executeSql("SELECT `key`, `columns` FROM raw_cobble").collect()) {
            while (iter.hasNext()) {
                results.add(iter.next());
            }
        }

        assertEquals(written.length, results.size());

        // Sort by key for deterministic comparison.
        results.sort(
                (r1, r2) -> {
                    byte[] k1 = (byte[]) r1.getField(0);
                    byte[] k2 = (byte[]) r2.getField(0);
                    return new String(k1, StandardCharsets.UTF_8)
                            .compareTo(new String(k2, StandardCharsets.UTF_8));
                });

        for (int i = 0; i < written.length; i++) {
            byte[] expectedKey = written[i][0];
            byte[] actualKey = (byte[]) results.get(i).getField(0);
            assertTrue(Arrays.equals(expectedKey, actualKey), "key mismatch at row " + i);

            byte[][] actualColumns = (byte[][]) results.get(i).getField(1);
            assertEquals(2, actualColumns.length, "columns array size at row " + i);
            assertTrue(
                    Arrays.equals(written[i][1], actualColumns[0]),
                    "column 0 mismatch at row " + i);
            if (written[i][2] == null) {
                assertTrue(actualColumns[1] == null, "column 1 should be null at row " + i);
            } else {
                assertTrue(
                        Arrays.equals(written[i][2], actualColumns[1]),
                        "column 1 mismatch at row " + i);
            }
        }
    }

    @Test
    void rawSourceRejectsLookupJoin() throws Exception {
        Path tablePath = tempDir.resolve("raw-lookup-reject");
        int bucketCount = 1;
        int numColumns = 1;

        Config writerConfig = new Config().numColumns(numColumns).totalBuckets(bucketCount);
        writerConfig.governanceMode = Config.GovernanceMode.NOOP;
        writerConfig.logConsole = false;
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = tablePath.toAbsolutePath().toString();
        volume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT);
        writerConfig.addVolume(volume);

        List<ShardSnapshot> shardSnapshots = new ArrayList<>();
        try (Db db = Db.open(writerConfig, 0, 0)) {
            db.put(0, bytes("k1"), 0, bytes("v1"));
            shardSnapshots.add(db.snapshot());
        }

        Config coordConfig = new Config().totalBuckets(bucketCount);
        coordConfig.governanceMode = Config.GovernanceMode.NOOP;
        coordConfig.logConsole = false;
        Config.VolumeDescriptor coordVolume = new Config.VolumeDescriptor();
        coordVolume.baseDir = tablePath.toAbsolutePath().toString();
        coordVolume.kinds =
                Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        coordConfig.addVolume(coordVolume);
        try (DbCoordinator coordinator = DbCoordinator.open(coordConfig)) {
            coordinator.materializeGlobalSnapshot(bucketCount, 1L, shardSnapshots);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "CREATE TABLE raw_cobble ("
                        + "  `key` BYTES,"
                        + "  `columns` ARRAY<BYTES>"
                        + ") WITH ("
                        + "  'connector' = 'cobble',"
                        + "  'source.kind' = 'raw',"
                        + "  'path' = '"
                        + escape(tablePath)
                        + "',"
                        + "  'bucket' = '1',"
                        + "  'raw.columns' = '0',"
                        + "  'scan.checkpoint-id' = '1',"
                        + "  'scan.mode' = 'batch'"
                        + ")");

        tableEnv.executeSql(
                "CREATE TABLE probe (k BYTES, proc_time AS PROCTIME()) WITH ('connector' = 'datagen', 'number-of-rows' = '1')");

        // A lookup join should fail because raw source does not support lookup. The planner may
        // wrap the ValidationException in a RuntimeException, so we check the full cause chain.
        Exception error =
                org.junit.jupiter.api.Assertions.assertThrows(
                        Exception.class,
                        () ->
                                tableEnv.executeSql(
                                                "SELECT * FROM probe LEFT JOIN raw_cobble FOR"
                                                        + " SYSTEM_TIME AS OF probe.proc_time ON"
                                                        + " probe.k = raw_cobble.`key`")
                                        .await(30, TimeUnit.SECONDS));
        boolean foundLookupMessage = false;
        Throwable current = error;
        while (current != null) {
            if (current.getMessage() != null
                    && current.getMessage().contains("lookup is not supported")) {
                foundLookupMessage = true;
                break;
            }
            current = current.getCause();
        }
        assertTrue(foundLookupMessage, "expected lookup-not-supported error, got: " + error);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String escape(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "\\\\");
    }
}
