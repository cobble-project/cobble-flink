package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.flink.catalog.CobbleCatalogTableReference;
import io.cobble.table.CatalogTable;
import io.cobble.table.FileCatalog;
import io.cobble.table.Table;
import io.cobble.table.TableIdentifier;
import io.cobble.table.TableSnapshotCommitter;
import io.cobble.table.Value;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Real Catalog SPI and SQL readers over native catalog-owned writes. */
@Timeout(120)
class CobbleCatalogSourceITTest {
    @Test
    void sqlPreservesDeclaredCompositeKeyOrder(@TempDir Path directory) throws Exception {
        TableEnvironment tables = createCatalog(directory);
        tables.executeSql(
                "CREATE TABLE composite_keys (id INT, region STRING, v BIGINT, PRIMARY KEY (region, id) NOT ENFORCED)");
        tables.executeSql("INSERT INTO composite_keys VALUES (1, 'east', 10), (1, 'west', 20)")
                .await();
        assertEquals(
                Arrays.asList("+I[1, east, 10]", "+I[1, west, 20]"),
                collect(tables, "SELECT * FROM composite_keys"));
        tables.executeSql(
                "CREATE TEMPORARY VIEW composite_probe AS SELECT id, region, PROCTIME() AS pt FROM (VALUES (1, 'east'), (1, 'missing')) AS p(id, region)");
        String query =
                "SELECT p.region, d.v FROM composite_probe AS p LEFT JOIN composite_keys FOR SYSTEM_TIME AS OF p.pt AS d ON p.id = d.id AND p.region = d.region";
        assertTrue(tables.explainSql(query).contains("LookupJoin"));
        assertEquals(Arrays.asList("+I[east, 10]", "+I[missing, null]"), collect(tables, query));
    }

    @Test
    void sqlSinkAndSourceShareCatalogIdentityAcrossInsertsAndSchemaChanges(@TempDir Path directory)
            throws Exception {
        TableEnvironment tables = createCatalog(directory);
        tables.executeSql("INSERT INTO items VALUES (1, 'one')").await();
        tables.executeSql("INSERT INTO items VALUES (2, 'two')").await();
        assertEquals(
                Arrays.asList("+I[1, one]", "+I[2, two]"), collect(tables, "SELECT * FROM items"));
        tables.executeSql("ALTER TABLE items ADD extra BIGINT");
        tables.executeSql("INSERT INTO items VALUES (3, 'three', 30)").await();
        assertEquals(
                Arrays.asList("+I[1, one, null]", "+I[2, two, null]", "+I[3, three, 30]"),
                collect(tables, "SELECT * FROM items"));
        tables.executeSql("ALTER TABLE items DROP v");
        tables.executeSql("INSERT INTO items VALUES (4, 40)").await();
        assertEquals(
                Arrays.asList("+I[1, null]", "+I[2, null]", "+I[3, 30]", "+I[4, 40]"),
                collect(tables, "SELECT * FROM items"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void restoresNativeScanSplitWithoutRepeatingEmittedRows(@TempDir Path directory)
            throws Exception {
        TableEnvironment tables = createCatalog(directory);
        write(directory);
        CobbleCatalogTableReference reference =
                CobbleCatalogTableReference.fromOptions(
                        tables.getCatalog("native_catalog")
                                .get()
                                .getTable(new ObjectPath("default", "items"))
                                .getOptions(),
                        "default",
                        "items");
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT().notNull()),
                                        DataTypes.FIELD("v", DataTypes.STRING()))
                                .getLogicalType();
        CobbleCatalogScanSource source = new CobbleCatalogScanSource(reference, rowType, "latest");
        CobbleCatalogScanSource.Split split;
        try (io.cobble.table.TableReader nativeReader =
                CobbleCatalogDynamicTableSource.openReader(reference, "latest")) {
            split =
                    new CobbleCatalogScanSource.Split(
                            "0", nativeReader.scanPlan().splits().get(0), 0);
        }
        SourceReaderContext context =
                (SourceReaderContext)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {SourceReaderContext.class},
                                (proxy, method, args) ->
                                        method.getName().equals("metricGroup")
                                                ? UnregisteredMetricsGroup
                                                        .createSourceReaderMetricGroup()
                                                : null);
        List<Integer> ids = new ArrayList<>();
        ReaderOutput<RowData> output =
                (ReaderOutput<RowData>)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {ReaderOutput.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("collect"))
                                        ids.add(((RowData) args[0]).getInt(0));
                                    return null;
                                });
        byte[] checkpoint;
        try (SourceReader<RowData, CobbleCatalogScanSource.Split> reader =
                source.createReader(context)) {
            reader.addSplits(Collections.singletonList(split));
            reader.pollNext(output);
            checkpoint = source.getSplitSerializer().serialize(reader.snapshotState(1).get(0));
        }
        try (SourceReader<RowData, CobbleCatalogScanSource.Split> reader =
                source.createReader(context)) {
            reader.addSplits(
                    Collections.singletonList(
                            source.getSplitSerializer().deserialize(1, checkpoint)));
            reader.notifyNoMoreSplits();
            for (int attempts = 0; attempts < 10; attempts++) {
                if (reader.pollNext(output) == InputStatus.END_OF_INPUT) break;
            }
        }
        Collections.sort(ids);
        assertEquals(Arrays.asList(1, 2), ids);
    }

    @Test
    void sqlScanUsesCatalogPlanAndKeepsDataAfterRename(@TempDir Path directory) throws Exception {
        TableEnvironment tables = createCatalog(directory);
        assertEquals(Collections.emptyList(), collect(tables, "SELECT * FROM items"));
        write(directory);
        assertEquals(
                Arrays.asList("+I[1, one]", "+I[2, two]"), collect(tables, "SELECT * FROM items"));
        tables.executeSql("ALTER TABLE items RENAME TO renamed_items");
        assertEquals(
                Arrays.asList("+I[1, one]", "+I[2, two]"),
                collect(tables, "SELECT * FROM renamed_items"));
    }

    @Test
    void sqlLookupUsesCatalogIdentityAndFullPrimaryKey(@TempDir Path directory) throws Exception {
        TableEnvironment tables = createCatalog(directory);
        write(directory);
        tables.executeSql(
                "CREATE TEMPORARY VIEW probe AS SELECT id, PROCTIME() AS pt FROM (VALUES (1), (99)) AS ids(id)");
        String sql =
                "SELECT p.id, d.v FROM probe AS p LEFT JOIN items FOR SYSTEM_TIME AS OF p.pt AS d ON p.id = d.id";
        assertTrue(tables.explainSql(sql).contains("LookupJoin"));
        assertEquals(Arrays.asList("+I[1, one]", "+I[99, null]"), collect(tables, sql));
    }

    @Test
    void sqlReadsAndLooksUpAcrossCatalogBuckets(@TempDir Path directory) throws Exception {
        TableEnvironment tables = createCatalog(directory, 2);
        tables.executeSql(
                        "INSERT INTO items VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')")
                .await();
        assertEquals(
                Arrays.asList("+I[1, one]", "+I[2, two]", "+I[3, three]", "+I[4, four]"),
                collect(tables, "SELECT * FROM items"));
        tables.executeSql(
                "CREATE TEMPORARY VIEW multi_bucket_probe AS "
                        + "SELECT id, PROCTIME() AS pt FROM (VALUES (1), (2), (3), (4), (99)) AS ids(id)");
        String sql =
                "SELECT p.id, d.v FROM multi_bucket_probe AS p "
                        + "LEFT JOIN items FOR SYSTEM_TIME AS OF p.pt AS d ON p.id = d.id";
        assertTrue(tables.explainSql(sql).contains("LookupJoin"));
        assertEquals(
                Arrays.asList(
                        "+I[1, one]", "+I[2, two]", "+I[3, three]", "+I[4, four]", "+I[99, null]"),
                collect(tables, sql));
    }

    private static TableEnvironment createCatalog(Path directory) {
        return createCatalog(directory, 1);
    }

    private static TableEnvironment createCatalog(Path directory, int buckets) {
        TableEnvironment tables = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tables.getConfig().getConfiguration().setInteger("parallelism.default", 1);
        tables.executeSql(
                "CREATE CATALOG native_catalog WITH ('type'='cobble', 'path'='"
                        + directory.toUri()
                        + "', 'buckets'='"
                        + buckets
                        + "')");
        tables.executeSql("USE CATALOG native_catalog");
        tables.executeSql("CREATE TABLE items (id INT, v STRING, PRIMARY KEY (id) NOT ENFORCED)");
        return tables;
    }

    private static void write(Path directory) {
        Config config = new Config().addVolume(directory.toUri().toString()).totalBuckets(1);
        try (FileCatalog catalog = FileCatalog.open(config, "flink");
                CatalogTable table =
                        catalog.loadTable(
                                new TableIdentifier(
                                        Collections.singletonList("default"), "items"));
                Table writer =
                        table.newWriteBuilder()
                                .totalBuckets(1)
                                .build()
                                .writerBuilder(config)
                                .bucket(0)
                                .open();
                TableSnapshotCommitter committer = table.snapshotCommitter(config, 4)) {
            writer.put(Arrays.asList(Value.int32(1), Value.string("one")));
            writer.put(Arrays.asList(Value.int32(2), Value.string("two")));
            committer.commitBatch(1, Collections.singletonList(writer.snapshot()));
        }
    }

    private static List<String> collect(TableEnvironment tables, String sql) throws Exception {
        List<String> rows = new ArrayList<>();
        try (CloseableIterator<Row> result = tables.executeSql(sql).collect()) {
            while (result.hasNext()) rows.add(result.next().toString());
        }
        Collections.sort(rows);
        return rows;
    }
}
