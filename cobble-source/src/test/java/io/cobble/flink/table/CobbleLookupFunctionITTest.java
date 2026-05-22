package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

class CobbleLookupFunctionITTest {

    @TempDir private Path tempDir;

    @Test
    void latestBatchLookupKeepsInitialSnapshot() throws Exception {
        Path tablePath = tempDir.resolve("latest-batch");
        writeDimensionRows(tablePath, Arrays.asList("2,name-2,20", "7,name-7,70"));

        CobbleLookupFunction lookup = openLookupFunction(tablePath, "latest", "batch");
        try {
            assertEquals("name-2,20", lookupValue(lookup, 2L));

            writeDimensionRows(tablePath, Arrays.asList("2,name-2-updated,200", "7,name-7,70"));

            assertEquals("name-2,20", lookupValue(lookup, 2L));
        } finally {
            lookup.close();
        }
    }

    @Test
    void latestStreamingLookupTracksNewSnapshots() throws Exception {
        Path tablePath = tempDir.resolve("latest-streaming");
        writeDimensionRows(tablePath, Arrays.asList("2,name-2,20", "7,name-7,70"));

        CobbleLookupFunction lookup = openLookupFunction(tablePath, "latest", "streaming");
        try {
            assertEquals("name-2,20", lookupValue(lookup, 2L));

            writeDimensionRows(tablePath, Arrays.asList("2,name-2-updated,200", "7,name-7,70"));

            assertEquals("name-2-updated,200", lookupValue(lookup, 2L));
        } finally {
            lookup.close();
        }
    }

    @Test
    void snapshotIdLookupStaysPinnedToConfiguredSnapshot() throws Exception {
        Path tablePath = tempDir.resolve("snapshot-id");
        writeDimensionRows(tablePath, Arrays.asList("2,name-2,20", "7,name-7,70"));

        CobbleLookupFunction lookup = openLookupFunction(tablePath, "1", "batch");
        try {
            assertEquals("name-2,20", lookupValue(lookup, 2L));

            writeDimensionRows(tablePath, Arrays.asList("2,name-2-updated,200", "7,name-7,70"));

            assertEquals("name-2,20", lookupValue(lookup, 2L));
        } finally {
            lookup.close();
        }
    }

    @Test
    void lookupDoesNotCreateRestoreWorkspace() throws Exception {
        Path tablePath = tempDir.resolve("reader-path");
        writeDimensionRows(tablePath, Arrays.asList("2,name-2,20", "7,name-7,70"));

        Path workspaceRoot = tablePath.resolve(".cobble-flink-lookup");
        CobbleLookupFunction lookup = openLookupFunction(tablePath, "latest", "batch");
        try {
            assertEquals("name-2,20", lookupValue(lookup, 2L));
        } finally {
            lookup.close();
        }
        assertFalse(Files.exists(workspaceRoot));
    }

    private CobbleLookupFunction openLookupFunction(
            Path tablePath, String checkpointId, String scanMode) throws Exception {
        CobbleLookupFunction lookup =
                new CobbleLookupFunction(
                        buildLookupConfig(tablePath, checkpointId, scanMode), new int[] {0});
        lookup.open(new FunctionContext((RuntimeContext) null));
        return lookup;
    }

    private CobbleDynamicTableSource.SerializableConfig buildLookupConfig(
            Path tablePath, String checkpointId, String scanMode) {
        return new CobbleDynamicTableSource.SerializableConfig(
                tablePath.toUri().toString(),
                -1,
                checkpointId,
                scanMode,
                50L,
                Collections.singletonList(
                        new CobbleDynamicTableSource.SerializableField("id", "BIGINT", 0, -1)),
                Arrays.asList(
                        new CobbleDynamicTableSource.SerializableField(
                                "name", "VARCHAR(2147483647)", 1, 0),
                        new CobbleDynamicTableSource.SerializableField("score", "INT", 2, 1)));
    }

    private void writeDimensionRows(Path tablePath, Collection<String> rows) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
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
                        + " 'bucket' = '2',"
                        + " 'sink.parallelism' = '2',"
                        + " 'snapshot.retention' = '4'"
                        + ")");

        awaitJobCompletion(
                tableEnv.executeSql(
                        "INSERT INTO cobble_sink "
                                + "SELECT * FROM (VALUES "
                                + String.join(", ", toSqlRows(rows))
                                + ") AS src(id, name, score)"));
    }

    private String[] toSqlRows(Collection<String> rows) {
        String[] sqlRows = new String[rows.size()];
        int index = 0;
        for (String row : rows) {
            String[] fields = row.split(",", -1);
            sqlRows[index] = "(" + fields[0] + ", '" + fields[1] + "', " + fields[2] + ")";
            index++;
        }
        return sqlRows;
    }

    private String lookupValue(CobbleLookupFunction lookup, long id) throws Exception {
        GenericRowData keyRow = new GenericRowData(1);
        keyRow.setRowKind(RowKind.INSERT);
        keyRow.setField(0, Long.valueOf(id));
        Collection<RowData> result = lookup.lookup(keyRow);
        if (result.isEmpty()) {
            return "null";
        }
        GenericRowData row = (GenericRowData) result.iterator().next();
        return row.getField(1) + "," + row.getField(2);
    }

    private void awaitJobCompletion(TableResult result) throws Exception {
        JobClient jobClient = result.getJobClient().orElseThrow(IllegalStateException::new);
        jobClient.getJobExecutionResult().get(30L, TimeUnit.SECONDS);
    }

    private static String escape(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "\\\\");
    }
}
