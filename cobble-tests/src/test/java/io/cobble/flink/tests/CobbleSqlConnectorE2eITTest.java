package io.cobble.flink.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
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

    private static final TypeInformation<Row> LOOKUP_PROBE_TYPE =
            Types.ROW_NAMED(new String[] {"order_id", "id"}, Types.LONG, Types.LONG);

    @TempDir private Path tempDir;

    @Test
    void sqlSinkRoundTripsIntoSqlSource() throws Exception {
        Path tablePath = tempDir.resolve("cobble-table");

        populateDimensionTable(tablePath);

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
                        + " 'source.block-cache-memory' = '256mb',"
                        + " 'scan.checkpoint-id' = 'latest',"
                        + " 'scan.mode' = 'batch'"
                        + ")");

        assertEquals(
                Arrays.asList("1,name-1,10", "2,name-2,20", "7,name-7,70", "8,name-8,80"),
                collectRows(
                        readerTableEnv.executeSql("SELECT id, name, score FROM cobble_source")));
    }

    @Test
    void sqlLookupJoinEnrichesStreamingRows() throws Exception {
        Path tablePath = tempDir.resolve("lookup-table");
        populateDimensionTable(tablePath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "CREATE TABLE cobble_dim ("
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

        tableEnv.createTemporaryView(
                "orders",
                tableEnv.fromDataStream(
                        env.fromCollection(
                                Arrays.asList(
                                        Row.ofKind(RowKind.INSERT, 100L, 2L),
                                        Row.ofKind(RowKind.INSERT, 101L, 7L),
                                        Row.ofKind(RowKind.INSERT, 102L, 99L)),
                                LOOKUP_PROBE_TYPE),
                        Schema.newBuilder()
                                .column("order_id", DataTypes.BIGINT())
                                .column("id", DataTypes.BIGINT())
                                .columnByExpression("pt", "PROCTIME()")
                                .build()));

        String lookupQuery =
                "SELECT o.order_id, o.id, d.name, d.score "
                        + "FROM orders AS o "
                        + "LEFT JOIN cobble_dim FOR SYSTEM_TIME AS OF o.pt AS d "
                        + "ON o.id = d.id";
        assertTrue(
                tableEnv.explainSql(lookupQuery).contains("LookupJoin"),
                "Expected Flink to plan the Cobble dimension access as a LookupJoin.");
        assertEquals(
                Arrays.asList("100,2,name-2,20", "101,7,name-7,70", "102,99,null,null"),
                collectRows(tableEnv.executeSql(lookupQuery)));
    }

    private StreamTableEnvironment createTableEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        return StreamTableEnvironment.create(env);
    }

    private void populateDimensionTable(Path tablePath) throws Exception {
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
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < row.getArity(); i++) {
                    if (i > 0) {
                        builder.append(',');
                    }
                    builder.append(row.getField(i));
                }
                rows.add(builder.toString());
            }
        }
        Collections.sort(rows);
        return rows;
    }

    private static String escape(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "\\\\");
    }
}
