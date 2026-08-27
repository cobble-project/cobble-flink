package io.cobble.flink.inspect.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.inspect.CobbleInspectClient;
import io.cobble.flink.inspect.FieldValue;
import io.cobble.flink.inspect.InspectException;
import io.cobble.flink.inspect.InspectPage;
import io.cobble.flink.inspect.InspectSession;
import io.cobble.flink.inspect.LookupKey;
import io.cobble.flink.inspect.LookupRequest;
import io.cobble.flink.inspect.LookupResult;
import io.cobble.flink.inspect.ScanFilter;
import io.cobble.flink.inspect.ScanRequest;
import io.cobble.flink.inspect.TypedLookupKey;
import io.cobble.flink.inspect.TypedValue;
import io.cobble.table.Value;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

class InspectTypedSinkIT {
    @TempDir private Path tempDir;

    @Test
    void tableScanLookupAndGeneratedSqlUseNativeSchema() throws Exception {
        Path table = tempDir.resolve("table");
        List<List<Value>> rows = new ArrayList<List<Value>>();
        for (int index = 0; index < 8; index++) {
            rows.add(row("us", "a" + index, "value"));
        }
        rows.add(row("us", "target-1", "one"));
        rows.add(row("us", "target-2", "two"));
        CobbleTableInspectTestData.write(
                table, 1, 8L, CobbleTableInspectTestData.compositeSchema(), rows);

        try (CobbleInspectClient client = CobbleInspectClient.builder().totalBuckets(1).build();
                InspectSession session = client.openDataSource(table.toString())) {
            List<FieldValue> filter = Arrays.asList(field("region", "us"), field("id", "target"));
            InspectPage first =
                    session.scan(
                            new ScanRequest(
                                    "sink", 1, null, null, null, null, ScanFilter.sink(filter)));
            assertEquals(1, first.rows().size());
            assertTrue(first.nextPageToken() != null);
            InspectPage second =
                    session.scan(
                            new ScanRequest(
                                    "sink",
                                    1,
                                    first.nextPageToken(),
                                    null,
                                    null,
                                    null,
                                    ScanFilter.sink(filter)));
            assertEquals(1, second.rows().size());
            assertTrue(second.nextPageToken() == null);

            LookupResult lookup =
                    session.lookup(
                            new LookupRequest(
                                    "sink",
                                    Collections.singletonList(
                                            LookupKey.typed(
                                                    TypedLookupKey.sink(
                                                            Arrays.asList(
                                                                    field("region", "us"),
                                                                    field("id", "target-1")))))));
            assertTrue(lookup.rows().get(0).found());
            assertEquals("one", lookup.rows().get(0).decodedColumns().get(0).scalar());

            InspectException partial =
                    assertThrows(
                            InspectException.class,
                            () ->
                                    session.lookup(
                                            new LookupRequest(
                                                    "sink",
                                                    Collections.singletonList(
                                                            LookupKey.typed(
                                                                    TypedLookupKey.sink(
                                                                            Collections
                                                                                    .singletonList(
                                                                                            field(
                                                                                                    "region",
                                                                                                    "us"))))))));
            assertTrue(partial.getMessage().contains("requires all fields"));

            InspectException wrongType =
                    assertThrows(
                            InspectException.class,
                            () ->
                                    session.lookup(
                                            new LookupRequest(
                                                    "sink",
                                                    Collections.singletonList(
                                                            LookupKey.typed(
                                                                    TypedLookupKey.sink(
                                                                            Arrays.asList(
                                                                                    new FieldValue(
                                                                                            "region",
                                                                                            TypedValue
                                                                                                    .integer(
                                                                                                            1)),
                                                                                    field(
                                                                                            "id",
                                                                                            "target-1"))))))));
            assertFalse(wrongType.getMessage().isEmpty());

            String ddl = session.overview().items().get(0).sourceSql().ddl();
            assertTrue(ddl.contains("`region` STRING NOT NULL"));
            assertTrue(ddl.contains("PRIMARY KEY (`region`, `id`) NOT ENFORCED"));
            StreamExecutionEnvironment environment =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            environment.setParallelism(1);
            StreamTableEnvironment tables = StreamTableEnvironment.create(environment);
            tables.executeSql(ddl);
            List<String> sqlRows = new ArrayList<String>();
            try (CloseableIterator<Row> results =
                    tables.executeSql("SELECT region, id FROM sink").collect()) {
                while (results.hasNext()) {
                    Row row = results.next();
                    sqlRows.add(row.getFieldAs(0) + ":" + row.getFieldAs(1));
                }
            }
            assertEquals(rows.size(), sqlRows.size());
            assertTrue(sqlRows.contains("us:target-1"));
        }
    }

    private static List<Value> row(String region, String id, String payload) {
        return Arrays.asList(Value.string(region), Value.string(id), Value.string(payload));
    }

    private static FieldValue field(String name, String value) {
        return new FieldValue(name, TypedValue.string(value));
    }
}
