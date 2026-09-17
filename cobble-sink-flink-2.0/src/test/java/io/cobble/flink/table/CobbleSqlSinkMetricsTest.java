package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.Db;
import io.cobble.flink.catalog.CobbleCatalogTableReference;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.CatalogTable;
import io.cobble.table.FileCatalog;
import io.cobble.table.Table;
import io.cobble.table.TableIdentifier;
import io.cobble.table.TableKey;
import io.cobble.table.TableReader;
import io.cobble.table.TableWritePlan;
import io.cobble.table.Value;

import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class CobbleSqlSinkMetricsTest {

    @TempDir private Path tempDir;

    @Test
    void synchronousSnapshotCapturesThePreparedBoundary() throws Exception {
        CobbleDynamicTableSink.SerializableConfig config = writerConfig(tempDir.resolve("sync"));
        try (CommittingSinkWriter<RowData, CobbleShardCommittable> writer =
                new CobbleSqlSink(config)
                        .createWriter(writerInitContext(new CapturingSinkMetrics()))) {
            writer.write(rowData(RowKind.INSERT, 1L, "before", 1), null);
            CobbleShardCommittable prepared = writer.prepareCommit().iterator().next();
            assertNotNull(prepared.shardSnapshot);
            writer.write(rowData(RowKind.INSERT, 1L, "after", 2), null);
            CobbleShardCommittable.Serializer serializer = new CobbleShardCommittable.Serializer();
            CobbleShardCommittable complete =
                    serializer.deserialize(serializer.getVersion(), serializer.serialize(prepared));
            try (Db reader =
                            Db.restoreWithManifest(
                                    CobbleSinkPaths.createWriterConfigForWriterPath(
                                            config, tempDir.resolve("read").toString()),
                                    complete.shardSnapshot.manifestPath);
                    Table table = Table.open(reader, CobbleTableRowConverter.TABLE_NAME)) {
                assertEquals(
                        Arrays.asList(Value.int64(1L), Value.string("before"), Value.int32(1)),
                        table.get(table.keyBuilder().push(Value.int64(1L)).build()));
            }
        }
    }

    @Test
    void catalogWriterCommitsThroughCapturedCatalogTable() throws Exception {
        Path warehouse = tempDir.resolve("catalog");
        CobbleDynamicTableSink.SerializableConfig legacy = writerConfig(warehouse, 2);
        Config runtime = new Config().addVolume(warehouse.toUri().toString()).totalBuckets(2);
        TableIdentifier identifier = new TableIdentifier(Collections.singletonList("db"), "orders");
        try (FileCatalog catalog = FileCatalog.open(runtime, "test")) {
            catalog.createNamespace(Collections.singletonList("db"));
            try (CatalogTable created = catalog.createTable(identifier, legacy.tableSchema())) {
                CobbleCatalogTableReference reference =
                        new CobbleCatalogTableReference(
                                warehouse.toUri().toString(),
                                "test",
                                "db",
                                "orders",
                                created.tableId(),
                                created.catalogSchemaId());
                TableWritePlan plan = created.newWriteBuilder().totalBuckets(2).build();
                CobbleDynamicTableSink.SerializableConfig config =
                        new CobbleDynamicTableSink.SerializableConfig(
                                legacy, reference, plan, created.schema());
                CobbleSqlSink sink = new CobbleSqlSink(config);
                CommittingSinkWriter<RowData, CobbleShardCommittable> writer =
                        sink.createWriter(writerInitContext(new CapturingSinkMetrics()));
                try (CobbleSqlSink.Global global = new CobbleSqlSink.Global(config)) {
                    List<CobbleShardCommittable> pending =
                            new java.util.ArrayList<CobbleShardCommittable>(writer.prepareCommit());
                    assertEquals(2, pending.size());
                    assertTrue(pending.stream().allMatch(value -> value.shardSnapshot != null));
                    List<CobbleBucketWriterState> state =
                            ((StatefulSinkWriter<RowData, CobbleBucketWriterState>) writer)
                                    .snapshotState(0L);
                    assertEquals(2, state.size());
                    global.commitCommittables(0L, pending, Collections.emptyList());
                    writer.close();
                    writer =
                            (CommittingSinkWriter<RowData, CobbleShardCommittable>)
                                    sink.restoreWriter(
                                            writerInitContext(new CapturingSinkMetrics()), state);

                    writer.write(rowData(RowKind.INSERT, 9L, "catalog", 5), null);
                    pending =
                            new java.util.ArrayList<CobbleShardCommittable>(writer.prepareCommit());
                    state =
                            ((StatefulSinkWriter<RowData, CobbleBucketWriterState>) writer)
                                    .snapshotState(1L);
                    assertEquals(2, state.size());
                    global.commitCommittables(1L, pending, Collections.emptyList());
                    writer.close();
                    writer =
                            (CommittingSinkWriter<RowData, CobbleShardCommittable>)
                                    sink.restoreWriter(
                                            writerInitContext(new CapturingSinkMetrics()), state);

                    pending =
                            new java.util.ArrayList<CobbleShardCommittable>(writer.prepareCommit());
                    assertEquals(2, pending.size());
                    assertTrue(pending.stream().allMatch(value -> value.shardSnapshot != null));
                    assertEquals(
                            2,
                            ((StatefulSinkWriter<RowData, CobbleBucketWriterState>) writer)
                                    .snapshotState(2L)
                                    .size());
                    global.commitCommittables(2L, pending, Collections.emptyList());
                } finally {
                    writer.close();
                }
                try (TableReader reader =
                        created.readerBuilder(runtime).currentGlobalSnapshot().open()) {
                    TableKey key = reader.keyBuilder().push(Value.int64(9L)).build();
                    assertEquals("catalog", reader.get(key).get(1).raw());
                }
            }
        }
    }

    @Test
    void topologyBuildsWithoutAnAsyncFinalizer() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000L);
        CobbleDynamicTableSink.SerializableConfig config =
                writerConfig(tempDir.resolve("topology"));
        env.fromData(
                        Collections.singletonList(rowData(RowKind.INSERT, 1L, "one", 1)),
                        InternalTypeInfo.<RowData>of(config.rowType()))
                .sinkTo(new CobbleSqlSink(config));
        assertTrue(
                env.getStreamGraph(false).getStreamNodes().stream()
                        .anyMatch(
                                node ->
                                        node.getOperatorName()
                                                .contains("Cobble Global Commit Operator")));
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getStreamGraph(false);
    }

    @Test
    void writerRegistersMetricsCountsSuccessfulWritesAndRecordsFailures() throws Exception {
        CobbleDynamicTableSink.SerializableConfig config = writerConfig(tempDir.resolve("metrics"));
        CapturingSinkMetrics metrics = new CapturingSinkMetrics();
        CommittingSinkWriter<RowData, CobbleShardCommittable> writer =
                new CobbleSqlSink(config).createWriter(writerInitContext(metrics));
        GenericRowData insert = rowData(RowKind.INSERT, 1L, "one", 1);
        long expectedInsertBytes = encodedUpsertBytes(config, insert);
        long expectedDeleteBytes =
                new CobbleRowDataCodecs.RuntimeKeyEncoder(config.keyFields).encode(insert).length;
        try {
            assertTrue(
                    metrics.metrics.keySet().stream().anyMatch(name -> name.startsWith("cobble.")),
                    "sink writer must register native table metrics");
            writer.write(insert, null);
            writer.write(rowData(RowKind.UPDATE_BEFORE, 1L, "one", 1), null);
            writer.write(rowData(RowKind.DELETE, 1L, "one", 1), null);

            GenericRowData invalid = rowData(RowKind.INSERT, 0L, "bad", 1);
            invalid.setField(0, null);
            assertThrows(RuntimeException.class, () -> writer.write(invalid, null));

            assertEquals(2L, metrics.count("send"));
            assertEquals(expectedInsertBytes + expectedDeleteBytes, metrics.count("bytes"));
            assertEquals(1L, metrics.count("errors"));

        } finally {
            writer.close();
        }
    }

    private static CobbleDynamicTableSink.SerializableConfig writerConfig(Path tablePath) {
        return writerConfig(tablePath, 1);
    }

    private static CobbleDynamicTableSink.SerializableConfig writerConfig(
            Path tablePath, int buckets) {
        return new CobbleDynamicTableSink.SerializableConfig(
                tablePath.toUri().toString(),
                buckets,
                buckets,
                1,
                false,
                1024L * 1024L,
                Collections.singletonList(
                        new CobbleDynamicTableSink.SerializableField("id", "BIGINT", 0, -1)),
                Arrays.asList(
                        new CobbleDynamicTableSink.SerializableField("name", "STRING", 1, 0),
                        new CobbleDynamicTableSink.SerializableField("score", "INT", 2, 1)));
    }

    private static WriterInitContext writerInitContext(CapturingSinkMetrics metrics) {
        return (WriterInitContext)
                Proxy.newProxyInstance(
                        CobbleSqlSinkMetricsTest.class.getClassLoader(),
                        new Class<?>[] {WriterInitContext.class},
                        (proxy, method, args) -> {
                            if ("getTaskInfo".equals(method.getName())) {
                                return new TaskInfoImpl("test", 1, 0, 1, 0);
                            }
                            if ("metricGroup".equals(method.getName())) {
                                return metrics.group();
                            }
                            return null;
                        });
    }

    private static GenericRowData rowData(RowKind kind, long id, String name, int score) {
        GenericRowData row = new GenericRowData(kind, 3);
        row.setField(0, id);
        row.setField(1, StringData.fromString(name));
        row.setField(2, score);
        return row;
    }

    private static long encodedUpsertBytes(
            CobbleDynamicTableSink.SerializableConfig config, RowData row) throws Exception {
        return new CobbleRowDataCodecs.RuntimeKeyEncoder(config.keyFields).encode(row).length;
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
                                if ("getNumRecordsSendErrorsCounter".equals(name)) {
                                    return counter("errors");
                                }
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
