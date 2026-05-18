package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.structured.Db;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class CobbleTableSinkITTest {

    private static final TypeInformation<Row> ROW_TYPE =
            Types.ROW_NAMED(
                    new String[] {"id", "name", "score"}, Types.LONG, Types.STRING, Types.INT);

    @TempDir private Path tempDir;

    @Test
    void sqlSinkWritesParallelShardsAndCommitsGlobalSnapshot() throws Exception {
        Path tablePath = tempDir.resolve("table");

        CobbleDynamicTableSink.SerializableConfig sinkConfig =
                new CobbleDynamicTableSink.SerializableConfig(
                        tablePath.toUri().toString(),
                        2,
                        2,
                        2,
                        java.util.Collections.singletonList(
                                new CobbleDynamicTableSink.SerializableField(
                                        "id", "BIGINT", 0, -1)),
                        java.util.Arrays.asList(
                                new CobbleDynamicTableSink.SerializableField(
                                        "name", "VARCHAR(2147483647)", 1, 0),
                                new CobbleDynamicTableSink.SerializableField(
                                        "score", "INT", 2, 1)));

        List<Row> inputRows = createInputRows();
        Map<Long, Row> expectedRows = expectedRows(inputRows);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.enableCheckpointing(500L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        org.apache.flink.streaming.api.datastream.DataStream<Row> sourceStream =
                env.addSource(new FiniteThenIdleSource(inputRows), ROW_TYPE);
        tableEnv.createTemporaryView(
                "src_rows",
                tableEnv.fromDataStream(
                        sourceStream,
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT())
                                .column("name", DataTypes.STRING())
                                .column("score", DataTypes.INT())
                                .build()));

        tableEnv.executeSql(
                "CREATE TABLE cobble_sink ("
                        + " id BIGINT,"
                        + " name STRING,"
                        + " score INT,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'path' = '"
                        + escape(tablePath)
                        + "',"
                        + " 'bucket' = '2',"
                        + " 'sink.parallelism' = '2',"
                        + " 'snapshot.retention' = '2'"
                        + ")");

        TableResult result = tableEnv.executeSql("INSERT INTO cobble_sink SELECT * FROM src_rows");
        JobClient jobClient = result.getJobClient().orElseThrow(IllegalStateException::new);

        GlobalSnapshot globalSnapshot =
                waitForCommittedSnapshot(
                        jobClient, sinkConfig, expectedRows, Duration.ofSeconds(30));
        jobClient.cancel().get(30L, TimeUnit.SECONDS);

        assertNotNull(globalSnapshot);
        assertEquals(2, globalSnapshot.totalBuckets);
        assertEquals(2, globalSnapshot.shardSnapshots.size());

        for (Map.Entry<Long, Row> entry : expectedRows.entrySet()) {
            verifyRowFromAnyShardSnapshot(
                    sinkConfig,
                    globalSnapshot,
                    entry.getKey().longValue(),
                    (String) entry.getValue().getField(1),
                    ((Integer) entry.getValue().getField(2)).intValue());
        }
    }

    private List<Row> createInputRows() {
        List<Row> rows = new ArrayList<>();
        for (long id = 1L; id <= 8L; id++) {
            rows.add(Row.of(Long.valueOf(id), "name-" + id, Integer.valueOf((int) id)));
        }
        return rows;
    }

    private Map<Long, Row> expectedRows(List<Row> inputRows) {
        Map<Long, Row> expected = new LinkedHashMap<>();
        for (Row row : inputRows) {
            expected.put(((Long) row.getField(0)).longValue(), row);
        }
        return expected;
    }

    private void verifyRowFromAnyShardSnapshot(
            CobbleDynamicTableSink.SerializableConfig sinkConfig,
            GlobalSnapshot globalSnapshot,
            long id,
            String expectedName,
            int expectedScore)
            throws Exception {
        CobbleRowDataCodecs.RuntimeKeyEncoder keyEncoder =
                new CobbleRowDataCodecs.RuntimeKeyEncoder(sinkConfig.keyFields);
        GenericRowData keyRow = new GenericRowData(1);
        keyRow.setRowKind(RowKind.INSERT);
        keyRow.setField(0, Long.valueOf(id));
        byte[] encodedKey = keyEncoder.encode(keyRow);

        for (ShardSnapshot shardSnapshot : globalSnapshot.shardSnapshots) {
            int bucket = shardSnapshot.ranges.get(0).start;
            Path restoreDir = tempDir.resolve("restore-" + bucket + "-" + id);
            Db restoredDb =
                    Db.restoreWithManifest(
                            createRestoreConfig(restoreDir), shardSnapshot.manifestPath);
            try {
                io.cobble.structured.Row row = restoredDb.get(bucket, encodedKey);
                if (row != null) {
                    assertEquals(expectedName, decodeString(row.getBytes(0)));
                    assertEquals(expectedScore, decodeInt(row.getBytes(1)));
                    return;
                }
            } finally {
                restoredDb.close();
            }
        }
        throw new AssertionError("Did not find row for id " + id + " in any shard snapshot.");
    }

    private Config createRestoreConfig(Path restoreDir) {
        Config config = new Config().numColumns(2).totalBuckets(2);
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = restoreDir.toAbsolutePath().toString();
        volume.kinds =
                java.util.Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META);
        config.addVolume(volume);
        Config.VolumeDescriptor writerSnapshotVolume = new Config.VolumeDescriptor();
        writerSnapshotVolume.baseDir = tempDir.resolve("table").toAbsolutePath().toString();
        writerSnapshotVolume.kinds =
                java.util.Collections.singletonList(Config.VolumeUsageKind.SNAPSHOT);
        config.addVolume(writerSnapshotVolume);
        config.governanceMode = Config.GovernanceMode.NOOP;
        config.logConsole = Boolean.FALSE;
        config.logPath = restoreDir.resolve("restore.log").toString();
        return config;
    }

    private String decodeString(byte[] encoded) throws Exception {
        TypeSerializer<StringData> serializer =
                InternalSerializers.create(DataTypes.STRING().getLogicalType());
        return serializer.deserialize(new DataInputDeserializer(encoded)).toString();
    }

    private int decodeInt(byte[] encoded) throws Exception {
        TypeSerializer<Integer> serializer =
                InternalSerializers.create(DataTypes.INT().getLogicalType());
        return serializer.deserialize(new DataInputDeserializer(encoded)).intValue();
    }

    private static String escape(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "\\\\");
    }

    private GlobalSnapshot waitForCommittedSnapshot(
            JobClient jobClient,
            CobbleDynamicTableSink.SerializableConfig sinkConfig,
            Map<Long, Row> expectedRows,
            Duration timeout)
            throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            JobStatus status;
            try {
                status = jobClient.getJobStatus().get(5L, TimeUnit.SECONDS);
            } catch (IllegalStateException e) {
                throw new AssertionError(
                        "MiniCluster shut down while waiting for checkpoint commit.",
                        jobFailure(jobClient, e));
            }
            if (status.isGloballyTerminalState()) {
                throw new AssertionError(
                        "Flink job reached terminal state "
                                + status
                                + " before a Cobble global snapshot was committed.");
            }
            DbCoordinator coordinator = null;
            try {
                coordinator =
                        DbCoordinator.open(CobbleSinkPaths.createCoordinatorConfig(sinkConfig));
                GlobalSnapshot snapshot = coordinator.loadCurrentGlobalSnapshot();
                if (snapshot != null
                        && snapshot.shardSnapshots.size() == sinkConfig.sinkParallelism
                        && containsExpectedRows(sinkConfig, snapshot, expectedRows)) {
                    return snapshot;
                }
            } finally {
                if (coordinator != null) {
                    coordinator.close();
                }
            }
            Thread.sleep(500L);
        }
        throw new AssertionError("Timed out waiting for a committed Cobble global snapshot.");
    }

    private boolean containsExpectedRows(
            CobbleDynamicTableSink.SerializableConfig sinkConfig,
            GlobalSnapshot globalSnapshot,
            Map<Long, Row> expectedRows)
            throws Exception {
        for (Map.Entry<Long, Row> entry : expectedRows.entrySet()) {
            if (!rowMatchesInAnyShard(
                    sinkConfig,
                    globalSnapshot,
                    entry.getKey().longValue(),
                    (String) entry.getValue().getField(1),
                    ((Integer) entry.getValue().getField(2)).intValue())) {
                return false;
            }
        }
        return true;
    }

    private boolean rowMatchesInAnyShard(
            CobbleDynamicTableSink.SerializableConfig sinkConfig,
            GlobalSnapshot globalSnapshot,
            long id,
            String expectedName,
            int expectedScore)
            throws Exception {
        CobbleRowDataCodecs.RuntimeKeyEncoder keyEncoder =
                new CobbleRowDataCodecs.RuntimeKeyEncoder(sinkConfig.keyFields);
        GenericRowData keyRow = new GenericRowData(1);
        keyRow.setRowKind(RowKind.INSERT);
        keyRow.setField(0, Long.valueOf(id));
        byte[] encodedKey = keyEncoder.encode(keyRow);

        for (ShardSnapshot shardSnapshot : globalSnapshot.shardSnapshots) {
            int bucket = shardSnapshot.ranges.get(0).start;
            Path restoreDir = tempDir.resolve("restore-" + bucket + "-" + id + "-probe");
            Db restoredDb =
                    Db.restoreWithManifest(
                            createRestoreConfig(restoreDir), shardSnapshot.manifestPath);
            try {
                io.cobble.structured.Row row = restoredDb.get(bucket, encodedKey);
                if (row != null) {
                    return expectedName.equals(decodeString(row.getBytes(0)))
                            && expectedScore == decodeInt(row.getBytes(1));
                }
            } finally {
                restoredDb.close();
            }
        }
        return false;
    }

    private Throwable jobFailure(JobClient jobClient, Throwable fallback) {
        try {
            jobClient.getJobExecutionResult().get(5L, TimeUnit.SECONDS);
            return fallback;
        } catch (java.util.concurrent.ExecutionException e) {
            return e.getCause() == null ? e : e.getCause();
        } catch (Exception e) {
            return e;
        }
    }

    private static final class FiniteThenIdleSource extends RichSourceFunction<Row> {
        private final List<Row> rows;
        private volatile boolean running = true;

        private FiniteThenIdleSource(List<Row> rows) {
            this.rows = new ArrayList<>(rows);
        }

        @Override
        public void run(SourceContext<Row> ctx) throws Exception {
            for (Row row : rows) {
                if (!running) {
                    return;
                }
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(row);
                }
                Thread.sleep(20L);
            }
            while (running) {
                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
