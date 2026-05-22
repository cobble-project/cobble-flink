package io.cobble.flink.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

class CobbleSqlConnectorE2eITTest {

    @TempDir private Path tempDir;

    @Test
    void sqlSinkRoundTripsIntoSqlSource() throws Exception {
        Path tablePath = tempDir.resolve("cobble-table");

        StreamTableEnvironment writerTableEnv = createTableEnvironment();
        writerTableEnv.executeSql(
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

        awaitJobCompletion(
                writerTableEnv.executeSql(
                        "INSERT INTO cobble_sink "
                                + "SELECT * FROM (VALUES "
                                + " (1, 'name-1', 10),"
                                + " (2, 'name-2', 20),"
                                + " (7, 'name-7', 70),"
                                + " (8, 'name-8', 80)"
                                + ") AS src(id, name, score)"));

        StreamTableEnvironment readerTableEnv = createTableEnvironment();
        readerTableEnv.executeSql(
                "CREATE TABLE cobble_source ("
                        + " id BIGINT,"
                        + " name STRING,"
                        + " score INT,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'path' = '"
                        + escape(tablePath)
                        + "',"
                        + " 'scan.checkpoint-id' = 'latest',"
                        + " 'scan.mode' = 'batch'"
                        + ")");

        assertEquals(
                Arrays.asList("1,name-1,10", "2,name-2,20", "7,name-7,70", "8,name-8,80"),
                collectRows(
                        readerTableEnv.executeSql("SELECT id, name, score FROM cobble_source")));
    }

    private StreamTableEnvironment createTableEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        return StreamTableEnvironment.create(env);
    }

    private void awaitJobCompletion(TableResult result) throws Exception {
        JobClient jobClient = result.getJobClient().orElseThrow(IllegalStateException::new);
        jobClient.getJobExecutionResult().get(30L, TimeUnit.SECONDS);
    }

    private List<String> collectRows(TableResult result) throws Exception {
        List<String> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                rows.add(row.getField(0) + "," + row.getField(1) + "," + row.getField(2));
            }
        }
        Collections.sort(rows);
        return rows;
    }

    private static String escape(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "\\\\");
    }
}
