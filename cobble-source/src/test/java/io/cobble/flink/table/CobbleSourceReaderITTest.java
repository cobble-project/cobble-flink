package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cobble.GlobalSnapshot;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Integration test that exercises wide-bucket source scans through the reader directly. */
class CobbleSourceReaderITTest {

    @TempDir private Path tempDir;

    @Test
    void readerEmitsAllKeysAcrossLargeBucketRanges() throws Exception {
        Path tablePath = tempDir.resolve("large-bucket-source");
        writeRows(tablePath, rows("v1", 1000));

        CobbleDynamicTableSource.SerializableConfig config =
                new CobbleDynamicTableSource.SerializableConfig(
                        tablePath.toUri().toString(),
                        32,
                        "1",
                        "batch",
                        50L,
                        Collections.singletonList(
                                new CobbleDynamicTableSource.SerializableField(
                                        "id", "BIGINT", 0, 0)),
                        Arrays.asList(
                                new CobbleDynamicTableSource.SerializableField(
                                        "name", "VARCHAR(2147483647)", 1, 0),
                                new CobbleDynamicTableSource.SerializableField(
                                        "score", "INT", 2, 1)));

        GlobalSnapshot snapshot = CobbleSourceRuntime.loadSnapshotById(config, 1L);
        List<CobbleSourceSplit> splits = CobbleSourceRuntime.createSourceSplits(config, snapshot);
        CobbleSourceReader reader = new CobbleSourceReader(config, new TestingContext());
        CollectingOutput output = new CollectingOutput();
        try {
            reader.start();
            reader.addSplits(splits);
            reader.notifyNoMoreSplits();
            while (true) {
                InputStatus status = reader.pollNext(output);
                if (status == InputStatus.END_OF_INPUT) {
                    break;
                }
                if (status == InputStatus.NOTHING_AVAILABLE) {
                    throw new IllegalStateException("Reader became idle before end of input.");
                }
            }
        } finally {
            reader.close();
        }

        List<Long> ids = new ArrayList<>();
        for (RowData row : output.rows) {
            ids.add(Long.valueOf(row.getLong(0)));
        }
        Collections.sort(ids);
        assertEquals(64, ids.size());
        assertEquals(64, ids.stream().distinct().count());
        for (int i = 0; i < 64; i++) {
            assertEquals(Long.valueOf(i + 1L), ids.get(i));
        }
    }

    private void writeRows(Path tablePath, List<String> rows) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
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
                        + " 'bucket' = '32',"
                        + " 'sink.parallelism' = '8',"
                        + " 'snapshot.retention' = '4'"
                        + ")");
        awaitJobCompletion(
                tableEnv.executeSql(
                        "INSERT INTO cobble_sink SELECT * FROM (VALUES "
                                + String.join(", ", rows)
                                + ") AS src(id, name, score)"));
    }

    private static List<String> rows(String suffix, int scoreBase) {
        List<String> rows = new ArrayList<>();
        for (int i = 1; i <= 64; i++) {
            rows.add("(" + i + ", 'reader-" + suffix + "-" + i + "', " + (scoreBase + i) + ")");
        }
        return rows;
    }

    private static void awaitJobCompletion(TableResult result) throws Exception {
        JobClient jobClient = result.getJobClient().orElseThrow(IllegalStateException::new);
        jobClient.getJobExecutionResult().get(120L, TimeUnit.SECONDS);
    }

    private static String escape(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "\\\\");
    }

    /** Minimal reader context for exercising the source reader without a full runtime. */
    private static final class TestingContext implements SourceReaderContext {
        @Override
        public SourceReaderMetricGroup metricGroup() {
            return UnregisteredMetricsGroup.createSourceReaderMetricGroup();
        }

        @Override
        public Configuration getConfiguration() {
            return new Configuration();
        }

        @Override
        public String getLocalHostName() {
            return "localhost";
        }

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {}

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(getClass().getClassLoader());
        }

        @Override
        public int currentParallelism() {
            return 1;
        }
    }

    /** Reader output that collects emitted rows for assertions. */
    private static final class CollectingOutput implements ReaderOutput<RowData> {
        private final List<RowData> rows = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            rows.add(record);
        }

        @Override
        public void collect(RowData record, long timestamp) {
            rows.add(record);
        }

        @Override
        public void emitWatermark(org.apache.flink.api.common.eventtime.Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public SourceOutput<RowData> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}
    }
}
