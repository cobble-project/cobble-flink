package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.View;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

class CobbleSqlSinkMetricsTest {

    @TempDir private Path tempDir;

    @Test
    void writerRegistersMetricsCountsSuccessfulWritesRecordsFailuresAndClosesMonitor()
            throws Exception {
        CobbleDynamicTableSink.SerializableConfig config = writerConfig(tempDir.resolve("metrics"));
        CapturingSinkMetrics metrics = new CapturingSinkMetrics();
        CommittingSinkWriter<RowData, CobbleShardCommittable> writer =
                new CobbleSqlSink(config).createWriter(writerInitContext(metrics));
        GenericRowData insert = rowData(RowKind.INSERT, 1L, "one", 1);
        long expectedInsertBytes = encodedUpsertBytes(config, insert);
        long expectedDeleteBytes =
                new CobbleRowDataCodecs.RuntimeKeyEncoder(config.keyFields).encode(insert).length;
        try {
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
        } finally {
            writer.close();
        }

        ((View) metrics.metric("cobble.memtableFlushesTotal")).update();
    }

    private static CobbleDynamicTableSink.SerializableConfig writerConfig(Path tablePath) {
        return new CobbleDynamicTableSink.SerializableConfig(
                tablePath.toUri().toString(),
                1,
                1,
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
        long bytes = new CobbleRowDataCodecs.RuntimeKeyEncoder(config.keyFields).encode(row).length;
        for (CobbleDynamicTableSink.SerializableField field : config.valueFields) {
            byte[] encoded = new CobbleRowDataCodecs.RuntimeFieldEncoder(field).encodeNullable(row);
            if (encoded != null) {
                bytes += encoded.length;
            }
        }
        return bytes;
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
