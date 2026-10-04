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
import io.cobble.table.DataField;
import io.cobble.table.LogicalTypes;
import io.cobble.table.TableSchema;
import io.cobble.table.Value;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
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
import java.util.stream.Collectors;

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
            assertTrue(session.targets().get(0).exactLookupSupported());
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
            assertEquals(
                    "one",
                    lookup.rows().get(0).decodedColumns().get(0).fields().get(0).value().scalar());
            LookupResult keyOnly =
                    session.lookup(
                            new LookupRequest(
                                    "sink",
                                    Arrays.asList(
                                            LookupKey.typed(
                                                    TypedLookupKey.sink(
                                                            Arrays.asList(
                                                                    field("region", "us"),
                                                                    field("id", "target-1")))),
                                            LookupKey.typed(
                                                    TypedLookupKey.sink(
                                                            Arrays.asList(
                                                                    field("region", "us"),
                                                                    field("id", "missing"))))),
                                    new int[0]));
            assertTrue(keyOnly.rows().get(0).found());
            assertTrue(keyOnly.rows().get(0).decodedColumns().isEmpty());
            assertFalse(keyOnly.rows().get(1).found());

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
            String tableName = ddl.substring("CREATE TABLE ".length(), ddl.indexOf(" ("));
            StreamExecutionEnvironment environment =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            environment.setParallelism(1);
            StreamTableEnvironment tables = StreamTableEnvironment.create(environment);
            tables.executeSql(ddl);
            List<String> sqlRows = new ArrayList<String>();
            try (CloseableIterator<Row> results =
                    tables.executeSql("SELECT region, id, payload FROM " + tableName).collect()) {
                while (results.hasNext()) {
                    Row row = results.next();
                    sqlRows.add(
                            row.getFieldAs(0) + ":" + row.getFieldAs(1) + ":" + row.getFieldAs(2));
                }
            }
            assertEquals(rows.size(), sqlRows.size());
            assertTrue(sqlRows.contains("us:target-1:one"));
            assertTrue(sqlRows.contains("us:target-2:two"));

            tables.createTemporaryView(
                    "table_probes",
                    tables.fromDataStream(
                            environment.fromCollection(
                                    Arrays.asList(
                                            Row.of("us", "target-1"),
                                            Row.of("us", "target-2"),
                                            Row.of("us", "absent")),
                                    Types.ROW_NAMED(
                                            new String[] {"probe_region", "probe_id"},
                                            Types.STRING,
                                            Types.STRING)),
                            Schema.newBuilder()
                                    .column("probe_region", "STRING")
                                    .column("probe_id", "STRING")
                                    .columnByExpression("pt", "PROCTIME()")
                                    .build()));
            String lookupSql =
                    "SELECT p.probe_id, d.payload FROM table_probes p LEFT JOIN "
                            + tableName
                            + " FOR SYSTEM_TIME AS OF p.pt d "
                            + "ON p.probe_region = d.region AND p.probe_id = d.id";
            assertTrue(tables.explainSql(lookupSql).contains("LookupJoin"));
            List<String> lookupRows = new ArrayList<>();
            try (CloseableIterator<Row> results = tables.executeSql(lookupSql).collect()) {
                while (results.hasNext()) {
                    Row row = results.next();
                    lookupRows.add(row.getFieldAs(0) + ":" + row.getFieldAs(1));
                }
            }
            assertEquals(
                    Arrays.asList("absent:null", "target-1:one", "target-2:two"),
                    lookupRows.stream().sorted().collect(Collectors.toList()));
        }
    }

    @Test
    void projectedTableScanAndLookupPreserveFieldOrderAndMisses() throws Exception {
        Path table = tempDir.resolve("projected-table");
        TableSchema schema =
                new TableSchema(
                        Arrays.asList(
                                new DataField(0L, "id", LogicalTypes.string().notNull()),
                                new DataField(1L, "first", LogicalTypes.string()),
                                new DataField(2L, "second", LogicalTypes.string())),
                        Collections.singletonList(0L),
                        Collections.singletonList(0L));
        CobbleTableInspectTestData.write(
                table,
                1,
                12L,
                schema,
                Arrays.asList(
                        Arrays.asList(Value.string("a"), Value.string("A1"), Value.string("A2")),
                        Arrays.asList(Value.string("b"), Value.string("B1"), Value.string("B2"))));

        try (CobbleInspectClient client = CobbleInspectClient.builder().totalBuckets(1).build();
                InspectSession session = client.openDataSource(table.toString())) {
            InspectPage first =
                    session.scan(new ScanRequest("sink", 1, null, null, null, new int[] {1, 0}));
            assertEquals(1, first.rows().size());
            assertEquals(
                    "second", first.rows().get(0).decodedColumns().get(0).fields().get(0).name());
            assertEquals(
                    "first", first.rows().get(0).decodedColumns().get(1).fields().get(0).name());
            InspectPage second =
                    session.scan(
                            new ScanRequest(
                                    "sink",
                                    1,
                                    first.nextPageToken(),
                                    null,
                                    null,
                                    new int[] {1, 0}));
            assertEquals(1, second.rows().size());
            assertEquals(null, second.nextPageToken());
            assertFalse(
                    first.rows()
                            .get(0)
                            .decodedKey()
                            .fields()
                            .get(0)
                            .value()
                            .scalar()
                            .equals(
                                    second.rows()
                                            .get(0)
                                            .decodedKey()
                                            .fields()
                                            .get(0)
                                            .value()
                                            .scalar()));

            LookupResult lookup =
                    session.lookup(
                            new LookupRequest(
                                    "sink",
                                    Arrays.asList(
                                            LookupKey.typed(
                                                    TypedLookupKey.sink(
                                                            Collections.singletonList(
                                                                    field("id", "a")))),
                                            LookupKey.typed(
                                                    TypedLookupKey.sink(
                                                            Collections.singletonList(
                                                                    field("id", "missing"))))),
                                    new int[] {1}));
            assertTrue(lookup.rows().get(0).found());
            assertEquals(
                    "A2",
                    lookup.rows().get(0).decodedColumns().get(0).fields().get(0).value().scalar());
            assertEquals(1, lookup.rows().get(0).decodedColumns().size());
            assertFalse(lookup.rows().get(1).found());
            assertEquals(0, lookup.rows().get(1).decodedColumns().size());
        }
    }

    @Test
    void numericTextPrefixDoesNotSkipEarlierEncodedKeys() throws Exception {
        Path table = tempDir.resolve("numeric-keys");
        TableSchema schema =
                new TableSchema(
                        Arrays.asList(
                                new DataField(0L, "id", LogicalTypes.int64().notNull()),
                                new DataField(1L, "payload", LogicalTypes.string())),
                        Collections.singletonList(0L),
                        Collections.singletonList(0L));
        CobbleTableInspectTestData.write(
                table,
                1,
                13L,
                schema,
                Arrays.asList(
                        Arrays.asList(Value.int64(-10L), Value.string("ten")),
                        Arrays.asList(Value.int64(-1L), Value.string("one")),
                        Arrays.asList(Value.int64(2L), Value.string("two"))));
        try (CobbleInspectClient client = CobbleInspectClient.builder().totalBuckets(1).build();
                InspectSession session = client.openDataSource(table.toString())) {
            InspectPage result =
                    session.scan(
                            new ScanRequest(
                                    "sink",
                                    10,
                                    null,
                                    null,
                                    null,
                                    null,
                                    ScanFilter.sink(
                                            Collections.singletonList(
                                                    new FieldValue(
                                                            "id", TypedValue.integer(-1L))))));
            assertEquals(2, result.rows().size());
        }
    }

    private static List<Value> row(String region, String id, String payload) {
        return Arrays.asList(Value.string(region), Value.string(id), Value.string(payload));
    }

    private static FieldValue field(String name, String value) {
        return new FieldValue(name, TypedValue.string(value));
    }
}
