package io.cobble.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Regression coverage for WindowJoin's Cobble-backed state and timer lifecycle. */
class CobbleWindowJoinOperatorRegressionTest {

    private static final int STRESS_KEY_COUNT = 2_048;
    private static final int ROWS_PER_KEY = 3;
    private static final TypeInformation<Row> INPUT_TYPE =
            Types.ROW_NAMED(new String[] {"timestamp", "join_key"}, Types.LONG, Types.LONG);

    @Test
    void joinsEachKeyOnceWhenBothSourceWatermarksFireTheWindow(@TempDir Path tempDir)
            throws Exception {
        List<Row> joinedRows =
                runWindowJoin(
                        tempDir,
                        Arrays.asList(row(1L), row(2L)),
                        Arrays.asList(row(2L), row(1L)),
                        true);

        assertEquals(2, joinedRows.size());
        assertSameKeyRows(joinedRows);
        assertThat(joinedRows.stream().map(row -> row.getField(0)).collect(Collectors.toSet()))
                .isEqualTo(new HashSet<>(Arrays.asList(1L, 2L)));
    }

    @Test
    void joinsManyKeysAcrossSmallMemtablesAndTimerBatches(@TempDir Path tempDir) throws Exception {
        List<Row> leftRows = rowsForKeys(STRESS_KEY_COUNT, ROWS_PER_KEY);
        List<Row> rightRows = rowsForKeys(STRESS_KEY_COUNT, ROWS_PER_KEY);
        int expectedRows = STRESS_KEY_COUNT * ROWS_PER_KEY * ROWS_PER_KEY;

        List<Row> heapRows = runWindowJoin(tempDir.resolve("heap"), leftRows, rightRows, false);
        assertEquals(expectedRows, heapRows.size());

        List<Row> joinedRows = runWindowJoin(tempDir.resolve("cobble"), leftRows, rightRows, true);

        assertSameKeyRows(joinedRows);
        Map<Long, Long> rowsPerKey =
                joinedRows.stream()
                        .collect(
                                Collectors.groupingBy(
                                        row -> (Long) row.getField(0), Collectors.counting()));
        assertThat(rowsPerKey.values()).allMatch(count -> count == ROWS_PER_KEY * ROWS_PER_KEY);
        assertEquals(expectedRows, joinedRows.size());
        assertEquals(STRESS_KEY_COUNT, rowsPerKey.size());
    }

    @Test
    void joinsGroupedWindowResultsWithoutRepeatingWindowTimers(@TempDir Path tempDir)
            throws Exception {
        int groupedKeyCount = STRESS_KEY_COUNT;
        List<Row> leftRows = rowsForKeys(groupedKeyCount, ROWS_PER_KEY);
        List<Row> rightRows = rowsForKeys(groupedKeyCount / 2, ROWS_PER_KEY);

        List<Row> heapRows =
                runGroupedWindowJoin(tempDir.resolve("heap"), leftRows, rightRows, false);
        assertEquals(groupedKeyCount / 2, heapRows.size());

        List<Row> cobbleRows =
                runGroupedWindowJoin(tempDir.resolve("cobble"), leftRows, rightRows, true);
        assertThat(cobbleRows).containsExactlyInAnyOrderElementsOf(heapRows);
    }

    @Test
    void joinsGroupedResultsAcrossManyWindowsWithoutRefiringTimers(@TempDir Path tempDir)
            throws Exception {
        int keyCount = 32;
        int windowCount = 64;
        List<Row> rows = rowsForWindows(keyCount, windowCount);

        List<Row> heapRows = runGroupedWindowJoin(tempDir.resolve("heap"), rows, rows, false);
        assertEquals(keyCount * windowCount, heapRows.size());

        List<Row> cobbleRows = runGroupedWindowJoin(tempDir.resolve("cobble"), rows, rows, true);
        assertThat(cobbleRows).containsExactlyInAnyOrderElementsOf(heapRows);
    }

    private List<Row> runWindowJoin(
            Path tempDir, List<Row> leftRows, List<Row> rightRows, boolean useCobbleStateBackend)
            throws Exception {
        StreamExecutionEnvironment env = createExecutionEnvironment(tempDir, useCobbleStateBackend);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        registerInput(tableEnv, "left_input", "left_rowtime", env, leftRows);
        registerInput(tableEnv, "right_input", "right_rowtime", env, rightRows);

        String query =
                "SELECT left_side.join_key AS left_key, right_side.join_key AS right_key "
                        + "FROM (SELECT * FROM TABLE(TUMBLE(TABLE left_input, "
                        + "DESCRIPTOR(left_rowtime), INTERVAL '10' SECOND))) AS left_side "
                        + "JOIN (SELECT * FROM TABLE(TUMBLE(TABLE right_input, "
                        + "DESCRIPTOR(right_rowtime), INTERVAL '10' SECOND))) AS right_side "
                        + "ON left_side.join_key = right_side.join_key "
                        + "AND left_side.window_start = right_side.window_start "
                        + "AND left_side.window_end = right_side.window_end";

        assertThat(tableEnv.explainSql(query)).contains("WindowJoin");
        return collectRows(tableEnv.executeSql(query), Duration.ofSeconds(60));
    }

    private List<Row> runGroupedWindowJoin(
            Path tempDir, List<Row> leftRows, List<Row> rightRows, boolean useCobbleStateBackend)
            throws Exception {
        StreamExecutionEnvironment env = createExecutionEnvironment(tempDir, useCobbleStateBackend);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        registerInput(tableEnv, "left_input", "left_rowtime", env, leftRows);
        registerInput(tableEnv, "right_input", "right_rowtime", env, rightRows);

        String query =
                "SELECT left_side.join_key "
                        + "FROM (SELECT join_key, window_start, window_end FROM TABLE(TUMBLE("
                        + "TABLE left_input, DESCRIPTOR(left_rowtime), INTERVAL '10' SECOND)) "
                        + "GROUP BY join_key, window_start, window_end) AS left_side "
                        + "JOIN (SELECT join_key, window_start, window_end FROM TABLE(TUMBLE("
                        + "TABLE right_input, DESCRIPTOR(right_rowtime), INTERVAL '10' SECOND)) "
                        + "GROUP BY join_key, window_start, window_end) AS right_side "
                        + "ON left_side.join_key = right_side.join_key "
                        + "AND left_side.window_start = right_side.window_start "
                        + "AND left_side.window_end = right_side.window_end";

        assertThat(tableEnv.explainSql(query)).contains("WindowJoin");
        return collectRows(tableEnv.executeSql(query), Duration.ofSeconds(60));
    }

    private StreamExecutionEnvironment createExecutionEnvironment(
            Path tempDir, boolean useCobbleStateBackend) {
        Configuration configuration = new Configuration();
        configuration.set(CobbleOptions.LOCAL_DIRECTORIES, tempDir.resolve("cobble").toString());
        configuration.set(CobbleOptions.FIX_PER_SLOT_MEMORY_SIZE, MemorySize.parse("256kb"));
        configuration.set(CobbleOptions.DIRECT_IO_BUFFER_SIZE, MemorySize.parse("8kb"));
        configuration.set(CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE, 128);
        configuration.set(CobbleOptions.MEMTABLE_BUFFER_RATIO, 0.5d);
        configuration.set(CobbleOptions.MEMTABLE_BUFFER_COUNT, 2);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.setMaxParallelism(1);
        env.setStateBackend(
                useCobbleStateBackend
                        ? newCobbleStateBackend(configuration)
                        : new HashMapStateBackend());
        return env;
    }

    private CobbleStateBackend newCobbleStateBackend(Configuration configuration) {
        CobbleStateBackend stateBackend = new CobbleStateBackend();
        stateBackend.setPriorityQueueStateType(CobbleStateBackend.PriorityQueueStateType.COBBLE);
        return stateBackend.configure(configuration, getClass().getClassLoader());
    }

    private static void registerInput(
            StreamTableEnvironment tableEnv,
            String name,
            String rowtimeColumn,
            StreamExecutionEnvironment env,
            List<Row> rows) {
        DataStream<Row> stream =
                env.fromCollection(rows, INPUT_TYPE)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Row>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                context ->
                                                        (row, previousTimestamp) ->
                                                                (Long) row.getField(0)));
        Table table =
                tableEnv.fromDataStream(
                        stream,
                        Schema.newBuilder()
                                .columnByMetadata(
                                        rowtimeColumn, "TIMESTAMP_LTZ(3)", "rowtime", true)
                                .watermark(rowtimeColumn, "SOURCE_WATERMARK()")
                                .build());
        tableEnv.createTemporaryView(name, table);
    }

    private static Row row(long key) {
        return Row.of(1L, key);
    }

    private static List<Row> rowsForKeys(int keyCount, int rowsPerKey) {
        List<Row> rows = new ArrayList<>(keyCount * rowsPerKey);
        for (int rowIndex = 0; rowIndex < rowsPerKey; rowIndex++) {
            for (long key = 0; key < keyCount; key++) {
                rows.add(row(key));
            }
        }
        return rows;
    }

    private static List<Row> rowsForWindows(int keyCount, int windowCount) {
        List<Row> rows = new ArrayList<>(keyCount * windowCount);
        for (int window = 0; window < windowCount; window++) {
            long timestamp = window * 10_000L + 1L;
            for (long key = 0; key < keyCount; key++) {
                rows.add(Row.of(timestamp, key));
            }
        }
        return rows;
    }

    private static void assertSameKeyRows(List<Row> joinedRows) {
        assertThat(joinedRows)
                .allSatisfy(row -> assertThat(row.getField(0)).isEqualTo(row.getField(1)));
    }

    private static List<Row> collectRows(TableResult result, Duration timeout) throws Exception {
        ExecutorService executor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "cobble-window-join-collector");
                            thread.setDaemon(true);
                            return thread;
                        });
        Future<List<Row>> future =
                executor.submit(
                        () -> {
                            List<Row> rows = new ArrayList<>();
                            try (CloseableIterator<Row> iterator = result.collect()) {
                                while (iterator.hasNext()) {
                                    rows.add(iterator.next());
                                }
                            }
                            return rows;
                        });
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            future.cancel(true);
            executor.shutdownNow();
        }
    }
}
