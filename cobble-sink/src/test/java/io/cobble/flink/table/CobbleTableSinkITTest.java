package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.View;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
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
                        true,
                        16L * 1024L * 1024L,
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
                        + " 'sink.use-managed-memory-allocator' = 'true',"
                        + " 'sink.writer-buffer-memory' = '16 mb',"
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

    @Test
    void sinkDeclaresUpsertModeAndAppliesFinalPrimaryKeyState() throws Exception {
        Path tablePath = tempDir.resolve("upsert-local");

        CobbleDynamicTableSink.SerializableConfig sinkConfig =
                new CobbleDynamicTableSink.SerializableConfig(
                        tablePath.toUri().toString(),
                        2,
                        2,
                        2,
                        false,
                        256L * 1024L * 1024L,
                        java.util.Collections.singletonList(
                                new CobbleDynamicTableSink.SerializableField(
                                        "id", "BIGINT", 0, -1)),
                        java.util.Arrays.asList(
                                new CobbleDynamicTableSink.SerializableField(
                                        "name", "VARCHAR(2147483647)", 1, 0),
                                new CobbleDynamicTableSink.SerializableField(
                                        "score", "INT", 2, 1)));
        CobbleDynamicTableSink sink = new CobbleDynamicTableSink(sinkConfig, "test");
        assertEquals(ChangelogMode.upsert(), sink.getChangelogMode(ChangelogMode.all()));

        CobbleRowDataCodecs.RuntimeKeyEncoder keyEncoder =
                new CobbleRowDataCodecs.RuntimeKeyEncoder(sinkConfig.keyFields);
        List<CobbleRowDataCodecs.RuntimeFieldEncoder> valueEncoders = new ArrayList<>();
        for (CobbleDynamicTableSink.SerializableField field : sinkConfig.valueFields) {
            valueEncoders.add(new CobbleRowDataCodecs.RuntimeFieldEncoder(field));
        }

        try (Db db = Db.open(CobbleSinkPaths.createWriterConfig(sinkConfig, 0), 0, 1)) {
            GenericRowData insertRow = rowData(RowKind.INSERT, 2L, "name-2", 2);
            byte[] encodedKey = keyEncoder.encode(insertRow);
            int bucket = CobbleSqlSink.hashFixedBucket(encodedKey, sinkConfig.bucketCount);

            CobbleSqlSink.MutationStats insertStats =
                    CobbleSqlSink.applyRowChange(db, valueEncoders, bucket, encodedKey, insertRow);
            assertEquals(true, insertStats.mutated);
            assertEquals(true, insertStats.bytes > encodedKey.length);
            verifyRow(bucket, encodedKey, db, "name-2", 2);

            GenericRowData updateBeforeRow = rowData(RowKind.UPDATE_BEFORE, 2L, "name-2", 2);
            CobbleSqlSink.MutationStats updateBeforeStats =
                    CobbleSqlSink.applyRowChange(
                            db, valueEncoders, bucket, encodedKey, updateBeforeRow);
            assertEquals(false, updateBeforeStats.mutated);
            assertEquals(0L, updateBeforeStats.bytes);
            verifyRow(bucket, encodedKey, db, "name-2", 2);

            GenericRowData updateRow = rowData(RowKind.UPDATE_AFTER, 2L, "updated-2", 20);
            CobbleSqlSink.MutationStats updateStats =
                    CobbleSqlSink.applyRowChange(db, valueEncoders, bucket, encodedKey, updateRow);
            assertEquals(true, updateStats.mutated);
            verifyRow(bucket, encodedKey, db, "updated-2", 20);

            GenericRowData deleteRow = rowData(RowKind.DELETE, 2L, "updated-2", 20);
            CobbleSqlSink.MutationStats deleteStats =
                    CobbleSqlSink.applyRowChange(db, valueEncoders, bucket, encodedKey, deleteRow);
            assertEquals(true, deleteStats.mutated);
            assertEquals((long) encodedKey.length, deleteStats.bytes);
            assertNull(db.get(bucket, encodedKey));
        }
    }

    @Test
    void sinkWriterConfigDisablesBlockCacheAndUsesManagedWriterBudget() throws Exception {
        Path tablePath = tempDir.resolve("managed-memory-config");
        CobbleDynamicTableSink.SerializableConfig sinkConfig =
                new CobbleDynamicTableSink.SerializableConfig(
                        tablePath.toUri().toString(),
                        2,
                        2,
                        2,
                        true,
                        32L * 1024L * 1024L,
                        java.util.Collections.singletonList(
                                new CobbleDynamicTableSink.SerializableField(
                                        "id", "BIGINT", 0, -1)),
                        java.util.Collections.singletonList(
                                new CobbleDynamicTableSink.SerializableField(
                                        "name", "VARCHAR(2147483647)", 1, 0)));

        Config writerConfig = CobbleSinkPaths.createWriterConfig(sinkConfig, 0);
        assertEquals(0, writerConfig.blockCacheSize.intValue());
        assertEquals(false, writerConfig.blockCacheHybridEnabled.booleanValue());
        assertEquals(0, writerConfig.blockCacheHybridDiskSize.intValue());
        assertEquals(32 * 1024 * 1024, writerConfig.memtableCapacity.intValue());
        assertEquals(1, writerConfig.memtableBufferCount.intValue());
    }

    @Test
    void writerRecordsStandardMetricsAndClosesNativeMonitor() throws Exception {
        Path tablePath = tempDir.resolve("writer-metrics");
        CobbleDynamicTableSink.SerializableConfig config = writerMetricsConfig(tablePath);
        CapturingSinkMetrics metrics = new CapturingSinkMetrics();
        Sink.InitContext context = sinkInitContext(metrics);
        CobbleSqlSink sink = new CobbleSqlSink(config);
        TwoPhaseCommittingSink.PrecommittingSinkWriter<RowData, CobbleShardCommittable> writer =
                sink.createWriter(context);
        GenericRowData insert = rowData(RowKind.INSERT, 1L, "one", 1);
        long expectedInsertBytes = encodedUpsertBytes(config, insert);
        long expectedDeleteBytes =
                new CobbleRowDataCodecs.RuntimeKeyEncoder(config.keyFields).encode(insert).length;
        writer.write(insert, null);
        writer.write(rowData(RowKind.UPDATE_BEFORE, 1L, "one", 1), null);
        writer.write(rowData(RowKind.DELETE, 1L, "one", 1), null);
        GenericRowData invalid = rowData(RowKind.INSERT, 0L, "bad", 1);
        invalid.setField(0, null);
        assertThrows(RuntimeException.class, () -> writer.write(invalid, null));
        assertEquals(2L, metrics.count("send"));
        assertEquals(expectedInsertBytes + expectedDeleteBytes, metrics.count("bytes"));
        assertEquals(1L, metrics.count("errors"));
        View nativeView = (View) metrics.metric("cobble.memtableFlushesTotal");
        assertNotNull(nativeView);
        nativeView.update();
        writer.close();
        nativeView.update();
    }

    @Test
    void updateBeforeKeepsWriterDirtyWithoutCountingSendMetrics() throws Exception {
        CobbleDynamicTableSink.SerializableConfig config =
                writerMetricsConfig(tempDir.resolve("update-before-dirty"));
        CapturingSinkMetrics metrics = new CapturingSinkMetrics();
        TwoPhaseCommittingSink.PrecommittingSinkWriter<RowData, CobbleShardCommittable> writer =
                new CobbleSqlSink(config).createWriter(sinkInitContext(metrics));
        try {
            writer.write(rowData(RowKind.UPDATE_BEFORE, 1L, "one", 1), null);
            assertEquals(0L, metrics.count("send"));
            assertEquals(0L, metrics.count("bytes"));
            assertEquals(0L, metrics.count("errors"));
            assertEquals(
                    1,
                    writer.prepareCommit().size(),
                    "successful UPDATE_BEFORE must retain the writer's dirty checkpoint state");
        } finally {
            writer.close();
        }
    }

    private CobbleDynamicTableSink.SerializableConfig writerMetricsConfig(Path tablePath) {
        return new CobbleDynamicTableSink.SerializableConfig(
                tablePath.toUri().toString(),
                1,
                1,
                1,
                false,
                1024L * 1024L,
                java.util.Collections.singletonList(
                        new CobbleDynamicTableSink.SerializableField("id", "BIGINT", 0, -1)),
                java.util.Arrays.asList(
                        new CobbleDynamicTableSink.SerializableField("name", "STRING", 1, 0),
                        new CobbleDynamicTableSink.SerializableField("score", "INT", 2, 1)));
    }

    private Sink.InitContext sinkInitContext(CapturingSinkMetrics metrics) {
        return (Sink.InitContext)
                Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {Sink.InitContext.class},
                        (proxy, method, args) -> {
                            if ("getSubtaskId".equals(method.getName())) return 0;
                            if ("getNumberOfParallelSubtasks".equals(method.getName())) return 1;
                            if ("getAttemptNumber".equals(method.getName())) return 0;
                            if ("metricGroup".equals(method.getName())) return metrics.group();
                            return null;
                        });
    }

    private static long encodedUpsertBytes(
            CobbleDynamicTableSink.SerializableConfig config, RowData row) throws Exception {
        long bytes = new CobbleRowDataCodecs.RuntimeKeyEncoder(config.keyFields).encode(row).length;
        for (CobbleDynamicTableSink.SerializableField field : config.valueFields) {
            byte[] encoded = new CobbleRowDataCodecs.RuntimeFieldEncoder(field).encodeNullable(row);
            if (encoded != null) {
                bytes += encoded.length;
            }
        }
        return bytes;
    }

    @Test
    void sqlSinkMaterializesGlobalSnapshotOnEndOfInputWithoutCheckpoint() throws Exception {
        Path tablePath = tempDir.resolve("eoi-table");

        CobbleDynamicTableSink.SerializableConfig sinkConfig =
                new CobbleDynamicTableSink.SerializableConfig(
                        tablePath.toUri().toString(),
                        2,
                        2,
                        2,
                        false,
                        256L * 1024L * 1024L,
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

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        org.apache.flink.streaming.api.datastream.DataStream<Row> sourceStream =
                env.fromCollection(inputRows, ROW_TYPE);
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
        result.getJobClient()
                .orElseThrow(IllegalStateException::new)
                .getJobExecutionResult()
                .get(30L, TimeUnit.SECONDS);

        try (DbCoordinator coordinator =
                DbCoordinator.open(CobbleSinkPaths.createCoordinatorConfig(sinkConfig))) {
            GlobalSnapshot snapshot = coordinator.loadCurrentGlobalSnapshot();
            assertNotNull(snapshot);
            assertEquals(2, snapshot.totalBuckets);
            assertEquals(2, snapshot.shardSnapshots.size());
            for (Map.Entry<Long, Row> entry : expectedRows.entrySet()) {
                verifyRowFromAnyShardSnapshot(
                        sinkConfig,
                        snapshot,
                        entry.getKey().longValue(),
                        (String) entry.getValue().getField(1),
                        ((Integer) entry.getValue().getField(2)).intValue());
            }
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
                            createRestoreConfig(restoreDir, sinkConfig),
                            shardSnapshot.manifestPath);
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

    private Config createRestoreConfig(
            Path restoreDir, CobbleDynamicTableSink.SerializableConfig sinkConfig) {
        Config config = new Config().numColumns(2).totalBuckets(2);
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = restoreDir.toAbsolutePath().toString();
        volume.kinds =
                java.util.Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META);
        config.addVolume(volume);
        Config.VolumeDescriptor writerSnapshotVolume = new Config.VolumeDescriptor();
        writerSnapshotVolume.baseDir =
                CobbleSinkPaths.tableRootPath(sinkConfig).toPath().toAbsolutePath().toString();
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
                        && snapshot.shardSnapshots.size() == sinkConfig.sinkParallelism) {
                    try {
                        if (containsExpectedRows(sinkConfig, snapshot, expectedRows)) {
                            return snapshot;
                        }
                    } catch (IllegalStateException e) {
                        // A freshly materialized global snapshot may become visible slightly before
                        // all referenced shard files are readable. Retry until the snapshot is
                        // fully materialized.
                    }
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
                            createRestoreConfig(restoreDir, sinkConfig),
                            shardSnapshot.manifestPath);
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

    private GenericRowData rowData(RowKind rowKind, long id, String name, int score) {
        GenericRowData row = new GenericRowData(3);
        row.setRowKind(rowKind);
        row.setField(0, Long.valueOf(id));
        row.setField(1, StringData.fromString(name));
        row.setField(2, Integer.valueOf(score));
        return row;
    }

    private void verifyRow(
            int bucket, byte[] encodedKey, Db db, String expectedName, int expectedScore)
            throws Exception {
        io.cobble.structured.Row row = db.get(bucket, encodedKey);
        assertNotNull(row);
        assertEquals(expectedName, decodeString(row.getBytes(0)));
        assertEquals(expectedScore, decodeInt(row.getBytes(1)));
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

    private static final class CapturingSinkMetrics {
        private final Map<String, Counter> counters = new LinkedHashMap<>();
        private final Map<String, Metric> metrics = new LinkedHashMap<>();

        private SinkWriterMetricGroup group() {
            return (SinkWriterMetricGroup)
                    Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[] {SinkWriterMetricGroup.class},
                            (proxy, method, args) -> {
                                String name = method.getName();
                                if ("getNumRecordsSendCounter".equals(name)) return counter("send");
                                if ("getNumBytesSendCounter".equals(name)) return counter("bytes");
                                if ("getNumRecordsSendErrorsCounter".equals(name))
                                    return counter("errors");
                                if ("counter".equals(name)) {
                                    if (args.length == 2) {
                                        metrics.put((String) args[0], (Metric) args[1]);
                                        return args[1];
                                    }
                                    return counter((String) args[0]);
                                }
                                if ("gauge".equals(name)) {
                                    metrics.put((String) args[0], (Metric) args[1]);
                                    return args[1];
                                }
                                if ("addGroup".equals(name)) return proxy;
                                return null;
                            });
        }

        private Counter counter(String name) {
            return counters.computeIfAbsent(name, ignored -> new TestCounter());
        }

        private long count(String name) {
            return counter(name).getCount();
        }

        private Metric metric(String name) {
            return metrics.get(name);
        }
    }

    private static final class TestCounter implements Counter {
        private long count;

        @Override
        public void inc() {
            count++;
        }

        @Override
        public void inc(long n) {
            count += n;
        }

        @Override
        public void dec() {
            count--;
        }

        @Override
        public void dec(long n) {
            count -= n;
        }

        @Override
        public long getCount() {
            return count;
        }
    }
}
