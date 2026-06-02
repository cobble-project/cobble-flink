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
        writeRows(tablePath, 32, rows("v1", 1000));

        CobbleDynamicTableSource.SerializableConfig config =
                sourceConfig(tablePath, 32, "1", "batch");

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

    @Test
    void readerRestoresProgressFromCheckpointAcrossLargeBucketRanges() throws Exception {
        Path tablePath = tempDir.resolve("checkpoint-resume");
        writeRows(tablePath, 32, rows("v1", 1000));

        CobbleDynamicTableSource.SerializableConfig config =
                sourceConfig(tablePath, 32, "1", "batch");
        GlobalSnapshot snapshot = CobbleSourceRuntime.loadSnapshotById(config, 1L);
        List<CobbleSourceSplit> initialSplits =
                CobbleSourceRuntime.createSourceSplits(config, snapshot);

        CollectingOutput firstOutput = new CollectingOutput();
        List<CobbleSourceSplit> checkpointedSplits;
        CobbleSourceReader firstReader = new CobbleSourceReader(config, new TestingContext());
        try {
            firstReader.start();
            firstReader.addSplits(initialSplits);
            firstReader.notifyNoMoreSplits();
            pollUntilRowCount(firstReader, firstOutput, 12);
            checkpointedSplits = firstReader.snapshotState(1L);
        } finally {
            firstReader.close();
        }

        CollectingOutput resumedOutput = new CollectingOutput();
        CobbleSourceReader resumedReader = new CobbleSourceReader(config, new TestingContext());
        try {
            resumedReader.start();
            resumedReader.addSplits(checkpointedSplits);
            resumedReader.notifyNoMoreSplits();
            drainBoundedReader(resumedReader, resumedOutput);
        } finally {
            resumedReader.close();
        }

        assertRowsCoverExactlyOnce(combine(firstOutput.rows, resumedOutput.rows), "v1");
    }

    @Test
    void readerContinuesFromLastKeyWhenSnapshotIsReplaced() throws Exception {
        Path tablePath = tempDir.resolve("replacement-resume");
        writeRows(tablePath, 1, rows("v1", 1000));

        CobbleDynamicTableSource.SerializableConfig config =
                sourceConfig(tablePath, 1, "latest", "streaming");
        GlobalSnapshot snapshot1 = CobbleSourceRuntime.loadSnapshotById(config, 1L);
        List<CobbleSourceSplit> initialSplits =
                CobbleSourceRuntime.createSourceSplits(config, snapshot1);

        CollectingOutput output = new CollectingOutput();
        CobbleSourceReader reader = new CobbleSourceReader(config, new TestingContext());
        List<Long> idsSeenBeforeReplacement;
        try {
            reader.start();
            reader.addSplits(initialSplits);
            pollUntilRowCount(reader, output, 20);
            idsSeenBeforeReplacement = extractIds(output.rows);

            writeRows(tablePath, 1, rows("v2", 2000));
            GlobalSnapshot snapshot2 = CobbleSourceRuntime.loadSnapshotById(config, 2L);
            for (CobbleSourceSplit split :
                    CobbleSourceRuntime.createSourceSplits(config, snapshot2)) {
                reader.handleSourceEvents(new CobbleSourceEvents.ReplaceSplitEvent(split));
            }
            drainStreamingReader(reader, output);
        } finally {
            reader.close();
        }

        java.util.Map<Long, String> namesById = new java.util.HashMap<>();
        for (RowData row : output.rows) {
            namesById.put(Long.valueOf(row.getLong(0)), row.getString(1).toString());
        }
        assertEquals(64, output.rows.size());
        assertEquals(64, namesById.size());
        for (long id = 1L; id <= 64L; id++) {
            String expectedSuffix =
                    idsSeenBeforeReplacement.contains(Long.valueOf(id)) ? "v1" : "v2";
            assertEquals("reader-" + expectedSuffix + "-" + id, namesById.get(Long.valueOf(id)));
        }
    }

    @Test
    void readerContinuesLargeBucketReplacementAfterCheckpointRestore() throws Exception {
        Path tablePath = tempDir.resolve("replacement-resume-large-buckets");
        writeRows(tablePath, 32, 1, rows("v1", 1000, 640));

        CobbleDynamicTableSource.SerializableConfig config =
                sourceConfig(tablePath, 32, "latest", "streaming");
        GlobalSnapshot snapshot1 = CobbleSourceRuntime.loadSnapshotById(config, 1L);
        List<CobbleSourceSplit> initialSplits =
                CobbleSourceRuntime.createSourceSplits(config, snapshot1);

        CollectingOutput initialOutput = new CollectingOutput();
        List<CobbleSourceSplit> checkpointedSplits;
        CobbleSourceReader initialReader = new CobbleSourceReader(config, new TestingContext());
        try {
            initialReader.start();
            initialReader.addSplits(initialSplits);
            pollUntilRowCount(initialReader, initialOutput, 80);
            checkpointedSplits = initialReader.snapshotState(1L);
        } finally {
            initialReader.close();
        }

        CollectingOutput resumedOutput = new CollectingOutput();
        CobbleSourceReader resumedReader = new CobbleSourceReader(config, new TestingContext());
        try {
            resumedReader.start();
            resumedReader.addSplits(checkpointedSplits);
            pollUntilRowCount(resumedReader, resumedOutput, 40);

            writeRows(tablePath, 32, 1, rows("v2", 2000, 640));
            GlobalSnapshot snapshot2 = CobbleSourceRuntime.loadSnapshotById(config, 2L);
            for (CobbleSourceSplit split :
                    CobbleSourceRuntime.createSourceSplits(config, snapshot2)) {
                resumedReader.handleSourceEvents(new CobbleSourceEvents.ReplaceSplitEvent(split));
            }
            drainStreamingReader(resumedReader, resumedOutput);
        } finally {
            resumedReader.close();
        }

        java.util.Map<Long, List<String>> namesById = new java.util.HashMap<>();
        List<RowData> combined = combine(initialOutput.rows, resumedOutput.rows);
        for (RowData row : combined) {
            namesById
                    .computeIfAbsent(Long.valueOf(row.getLong(0)), ignored -> new ArrayList<>())
                    .add(row.getString(1).toString());
        }
        List<Long> missingIds = new ArrayList<>();
        for (long id = 1L; id <= 640L; id++) {
            if (!namesById.containsKey(Long.valueOf(id))) {
                missingIds.add(Long.valueOf(id));
            }
        }
        assertEquals(640, namesById.size(), "missing=" + missingIds);
        for (long id = 1L; id <= 640L; id++) {
            org.junit.jupiter.api.Assertions.assertTrue(namesById.containsKey(Long.valueOf(id)));
        }
    }

    @Test
    void readerWrapsReplacementAfterCheckpointRestoreToConsumeEarlierNewKeys() throws Exception {
        Path tablePath = tempDir.resolve("replacement-wrap-after-restore");
        writeRows(tablePath, 1, rangedRows("v1", 1000, 101, 200));

        CobbleDynamicTableSource.SerializableConfig config =
                sourceConfig(tablePath, 1, "latest", "streaming");
        GlobalSnapshot snapshot1 = CobbleSourceRuntime.loadSnapshotById(config, 1L);
        List<CobbleSourceSplit> initialSplits =
                CobbleSourceRuntime.createSourceSplits(config, snapshot1);

        CollectingOutput initialOutput = new CollectingOutput();
        List<CobbleSourceSplit> checkpointedSplits;
        CobbleSourceReader initialReader = new CobbleSourceReader(config, new TestingContext());
        try {
            initialReader.start();
            initialReader.addSplits(initialSplits);
            pollUntilRowCount(initialReader, initialOutput, 20);
            checkpointedSplits = initialReader.snapshotState(1L);
        } finally {
            initialReader.close();
        }

        CollectingOutput resumedOutput = new CollectingOutput();
        CobbleSourceReader resumedReader = new CobbleSourceReader(config, new TestingContext());
        List<Long> idsSeenBeforeReplacement = new ArrayList<>(extractIds(initialOutput.rows));
        try {
            resumedReader.start();
            resumedReader.addSplits(checkpointedSplits);
            pollUntilRowCount(resumedReader, resumedOutput, 10);
            idsSeenBeforeReplacement.addAll(extractIds(resumedOutput.rows));

            writeRows(tablePath, 1, rangedRows("v2", 2000, 1, 200));
            GlobalSnapshot snapshot2 = CobbleSourceRuntime.loadSnapshotById(config, 2L);
            for (CobbleSourceSplit split :
                    CobbleSourceRuntime.createSourceSplits(config, snapshot2)) {
                resumedReader.handleSourceEvents(new CobbleSourceEvents.ReplaceSplitEvent(split));
            }
            drainStreamingReader(resumedReader, resumedOutput);
        } finally {
            resumedReader.close();
        }

        java.util.Map<Long, List<String>> namesById = new java.util.HashMap<>();
        List<RowData> combined = combine(initialOutput.rows, resumedOutput.rows);
        for (RowData row : combined) {
            namesById
                    .computeIfAbsent(Long.valueOf(row.getLong(0)), ignored -> new ArrayList<>())
                    .add(row.getString(1).toString());
        }
        assertEquals(200, namesById.size());
        for (long id = 1L; id <= 200L; id++) {
            List<String> names = namesById.get(Long.valueOf(id));
            org.junit.jupiter.api.Assertions.assertTrue(names != null && !names.isEmpty());
            if (!idsSeenBeforeReplacement.contains(Long.valueOf(id))) {
                org.junit.jupiter.api.Assertions.assertTrue(names.contains("reader-v2-" + id));
            }
        }
    }

    private void writeRows(Path tablePath, int bucketCount, List<String> rows) throws Exception {
        writeRows(tablePath, bucketCount, Math.min(bucketCount, 8), rows);
    }

    private void writeRows(Path tablePath, int bucketCount, int sinkParallelism, List<String> rows)
            throws Exception {
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
                        + " 'bucket' = '"
                        + bucketCount
                        + "',"
                        + " 'sink.parallelism' = '"
                        + sinkParallelism
                        + "',"
                        + " 'snapshot.retention' = '4'"
                        + ")");
        awaitJobCompletion(
                tableEnv.executeSql(
                        "INSERT INTO cobble_sink SELECT * FROM (VALUES "
                                + String.join(", ", rows)
                                + ") AS src(id, name, score)"));
    }

    private static List<String> rows(String suffix, int scoreBase) {
        return rows(suffix, scoreBase, 64);
    }

    private static List<String> rows(String suffix, int scoreBase, int rowCount) {
        List<String> rows = new ArrayList<>();
        for (int i = 1; i <= rowCount; i++) {
            rows.add("(" + i + ", 'reader-" + suffix + "-" + i + "', " + (scoreBase + i) + ")");
        }
        return rows;
    }

    private static List<String> rangedRows(
            String suffix, int scoreBase, int startInclusive, int endInclusive) {
        List<String> rows = new ArrayList<>();
        for (int i = startInclusive; i <= endInclusive; i++) {
            rows.add("(" + i + ", 'reader-" + suffix + "-" + i + "', " + (scoreBase + i) + ")");
        }
        return rows;
    }

    private static CobbleDynamicTableSource.SerializableConfig sourceConfig(
            Path tablePath, int bucketCount, String checkpointId, String scanMode) {
        return new CobbleDynamicTableSource.SerializableConfig(
                tablePath.toUri().toString(),
                bucketCount,
                checkpointId,
                scanMode,
                50L,
                256L * 1024L * 1024L,
                Collections.singletonList(
                        new CobbleDynamicTableSource.SerializableField("id", "BIGINT", 0, 0)),
                Arrays.asList(
                        new CobbleDynamicTableSource.SerializableField(
                                "name", "VARCHAR(2147483647)", 1, 0),
                        new CobbleDynamicTableSource.SerializableField("score", "INT", 2, 1)));
    }

    private static void pollUntilRowCount(
            CobbleSourceReader reader, CollectingOutput output, int expectedRows) throws Exception {
        while (output.rows.size() < expectedRows) {
            InputStatus status = reader.pollNext(output);
            if (status == InputStatus.END_OF_INPUT) {
                throw new IllegalStateException(
                        "Reader finished before reaching " + expectedRows + " rows.");
            }
            if (status == InputStatus.NOTHING_AVAILABLE) {
                throw new IllegalStateException(
                        "Reader became idle before reaching " + expectedRows + " rows.");
            }
        }
    }

    private static void drainBoundedReader(CobbleSourceReader reader, CollectingOutput output)
            throws Exception {
        while (true) {
            InputStatus status = reader.pollNext(output);
            if (status == InputStatus.END_OF_INPUT) {
                return;
            }
            if (status == InputStatus.NOTHING_AVAILABLE) {
                throw new IllegalStateException("Bounded reader became idle before end of input.");
            }
        }
    }

    private static void drainStreamingReader(CobbleSourceReader reader, CollectingOutput output)
            throws Exception {
        int idlePolls = 0;
        while (idlePolls < 3) {
            int previousSize = output.rows.size();
            InputStatus status = reader.pollNext(output);
            if (status == InputStatus.END_OF_INPUT) {
                throw new IllegalStateException("Streaming reader ended unexpectedly.");
            }
            if (status == InputStatus.NOTHING_AVAILABLE && output.rows.size() == previousSize) {
                idlePolls++;
                continue;
            }
            idlePolls = 0;
        }
    }

    private static List<RowData> combine(List<RowData> first, List<RowData> second) {
        List<RowData> combined = new ArrayList<>(first.size() + second.size());
        combined.addAll(first);
        combined.addAll(second);
        return combined;
    }

    private static List<Long> extractIds(List<RowData> rows) {
        List<Long> ids = new ArrayList<>(rows.size());
        for (RowData row : rows) {
            ids.add(Long.valueOf(row.getLong(0)));
        }
        return ids;
    }

    private static void assertRowsCoverExactlyOnce(List<RowData> rows, String expectedSuffix) {
        assertRowsCoverExactlyOnce(rows, expectedSuffix, 64);
    }

    private static void assertRowsCoverExactlyOnce(
            List<RowData> rows, String expectedSuffix, int rowCount) {
        List<Long> ids = new ArrayList<>(rows.size());
        java.util.Map<Long, String> namesById = new java.util.HashMap<>();
        for (RowData row : rows) {
            Long id = Long.valueOf(row.getLong(0));
            ids.add(id);
            namesById.put(id, row.getString(1).toString());
        }
        Collections.sort(ids);
        java.util.Set<Long> distinctIds = new java.util.TreeSet<>(ids);
        List<Long> missingIds = new ArrayList<>();
        List<Long> unexpectedIds = new ArrayList<>();
        for (long id = 1L; id <= rowCount; id++) {
            if (!distinctIds.contains(Long.valueOf(id))) {
                missingIds.add(Long.valueOf(id));
            }
        }
        for (Long id : distinctIds) {
            if (id.longValue() < 1L || id.longValue() > rowCount) {
                unexpectedIds.add(id);
            }
        }
        String coverageMessage =
                "missing=" + missingIds + ", unexpected=" + unexpectedIds + ", ids=" + ids;
        assertEquals(rowCount, rows.size(), coverageMessage);
        assertEquals(rowCount, distinctIds.size(), coverageMessage);
        for (int i = 0; i < rowCount; i++) {
            long id = i + 1L;
            assertEquals(Long.valueOf(id), ids.get(i));
            assertEquals("reader-" + expectedSuffix + "-" + id, namesById.get(Long.valueOf(id)));
        }
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
