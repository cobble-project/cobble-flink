package io.cobble.flink.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.cobble.table.TableSchema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

class CobbleCatalogTest {
    @TempDir Path temporaryDirectory;

    @Test
    void managesTableIdentityAndExplicitSchemaEvolution() throws Exception {
        CobbleCatalog catalog =
                new CobbleCatalog("c", temporaryDirectory.toUri().toString(), "test", 2);
        catalog.open();
        try {
            catalog.createDatabase(
                    "db", new CatalogDatabaseImpl(Collections.emptyMap(), null), false);
            ObjectPath original = new ObjectPath("db", "orders");
            catalog.createTable(original, table(), false);
            CatalogTable loaded = (CatalogTable) catalog.getTable(original);
            String tableId = loaded.getOptions().get(CobbleCatalog.OPTION_TABLE_ID);
            catalog.alterTable(
                    original,
                    loaded,
                    Collections.singletonList(
                            TableChange.add(Column.physical("note", DataTypes.STRING()))),
                    false);
            assertEquals(
                    3,
                    ((CatalogTable) catalog.getTable(original))
                            .getUnresolvedSchema()
                            .getColumns()
                            .size());
            catalog.renameTable(original, "renamed", false);
            CatalogTable renamed = (CatalogTable) catalog.getTable(new ObjectPath("db", "renamed"));
            assertEquals(tableId, renamed.getOptions().get(CobbleCatalog.OPTION_TABLE_ID));
            catalog.dropTable(new ObjectPath("db", "renamed"), false);
            catalog.createTable(new ObjectPath("db", "renamed"), table(), false);
            CobbleCatalogTableReference stale =
                    CobbleCatalogTableReference.fromOptions(renamed.getOptions(), "db", "renamed");
            assertThrows(IllegalStateException.class, stale::openValidated);
        } finally {
            catalog.close();
        }
    }

    @Test
    void rejectsLossyBoundedVarchar() throws Exception {
        CobbleCatalog catalog =
                new CobbleCatalog("c", temporaryDirectory.toUri().toString(), "test", 1);
        catalog.open();
        try {
            catalog.createDatabase(
                    "db", new CatalogDatabaseImpl(Collections.emptyMap(), null), false);
            CatalogTable bounded =
                    CatalogTable.of(
                            Schema.newBuilder()
                                    .column("id", DataTypes.BIGINT().notNull())
                                    .column("v", DataTypes.VARCHAR(10))
                                    .primaryKey("id")
                                    .build(),
                            null,
                            Collections.emptyList(),
                            Collections.emptyMap());
            assertThrows(
                    ValidationException.class,
                    () -> catalog.createTable(new ObjectPath("db", "bad"), bounded, false));
        } finally {
            catalog.close();
        }
    }

    @Test
    void roundTripsSupportedTypesAndRejectsBoundedAndNestedTypes() {
        Schema flinkSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().notNull())
                        .column("bool", DataTypes.BOOLEAN())
                        .column("tiny", DataTypes.TINYINT())
                        .column("small", DataTypes.SMALLINT())
                        .column("integer", DataTypes.INT())
                        .column("big", DataTypes.BIGINT())
                        .column("float", DataTypes.FLOAT())
                        .column("double", DataTypes.DOUBLE())
                        .column("string", DataTypes.STRING())
                        .column("bytes", DataTypes.BYTES())
                        .column("decimal", DataTypes.DECIMAL(18, 4))
                        .column("date", DataTypes.DATE())
                        .column("time", DataTypes.TIME(3))
                        .column("timestamp", DataTypes.TIMESTAMP(6))
                        .primaryKey("id")
                        .build();

        TableSchema converted = CobbleCatalogTypes.toCobble(flinkSchema);
        TableSchema roundTripped =
                CobbleCatalogTypes.toCobble(CobbleCatalogTypes.toFlink(converted));

        assertEquals(converted.primaryKey(), roundTripped.primaryKey());
        assertEquals(converted.fields().size(), roundTripped.fields().size());
        for (int index = 0; index < converted.fields().size(); index++) {
            assertEquals(converted.fields().get(index).id(), roundTripped.fields().get(index).id());
            assertEquals(
                    converted.fields().get(index).name(), roundTripped.fields().get(index).name());
            assertEquals(
                    converted.fields().get(index).logicalType(),
                    roundTripped.fields().get(index).logicalType());
        }

        assertThrows(
                ValidationException.class,
                () -> CobbleCatalogTypes.toCobble(DataTypes.VARCHAR(10)));
        assertThrows(
                ValidationException.class,
                () -> CobbleCatalogTypes.toCobble(DataTypes.VARBINARY(10)));
        assertThrows(
                ValidationException.class,
                () -> CobbleCatalogTypes.toCobble(DataTypes.ARRAY(DataTypes.STRING())));
        assertThrows(
                ValidationException.class,
                () ->
                        CobbleCatalogTypes.toCobble(
                                DataTypes.ROW(DataTypes.FIELD("nested", DataTypes.STRING()))));
    }

    @Test
    void rejectsWatermarksAndColumnComments() throws Exception {
        CobbleCatalog catalog =
                new CobbleCatalog("c", temporaryDirectory.toUri().toString(), "test", 1);
        catalog.open();
        try {
            catalog.createDatabase(
                    "db", new CatalogDatabaseImpl(Collections.emptyMap(), null), false);
            CatalogTable watermarkTable =
                    CatalogTable.of(
                            Schema.newBuilder()
                                    .column("id", DataTypes.BIGINT().notNull())
                                    .column("updated_at", DataTypes.TIMESTAMP(3))
                                    .column("value", DataTypes.STRING())
                                    .watermark("updated_at", "updated_at - INTERVAL '1' SECOND")
                                    .primaryKey("id")
                                    .build(),
                            null,
                            Collections.emptyList(),
                            Collections.emptyMap());
            assertThrows(
                    CatalogException.class,
                    () ->
                            catalog.createTable(
                                    new ObjectPath("db", "watermarked"), watermarkTable, false));
            CatalogTable commentedTable =
                    CatalogTable.of(
                            Schema.newBuilder()
                                    .column("id", DataTypes.BIGINT().notNull())
                                    .withComment("stable key")
                                    .column("value", DataTypes.STRING())
                                    .primaryKey("id")
                                    .build(),
                            null,
                            Collections.emptyList(),
                            Collections.emptyMap());
            assertThrows(
                    CatalogException.class,
                    () ->
                            catalog.createTable(
                                    new ObjectPath("db", "commented"), commentedTable, false));
        } finally {
            catalog.close();
        }
    }

    @Test
    void rejectsKeyChangesAndLossyNarrowing() throws Exception {
        CobbleCatalog catalog =
                new CobbleCatalog("c", temporaryDirectory.toUri().toString(), "test", 1);
        catalog.open();
        try {
            catalog.createDatabase(
                    "db", new CatalogDatabaseImpl(Collections.emptyMap(), null), false);
            ObjectPath path = new ObjectPath("db", "orders");
            catalog.createTable(path, table(), false);
            CatalogTable loaded = (CatalogTable) catalog.getTable(path);

            assertThrows(
                    ValidationException.class,
                    () ->
                            catalog.alterTable(
                                    path,
                                    loaded,
                                    Collections.singletonList(TableChange.dropColumn("id")),
                                    false));
            assertThrows(
                    ValidationException.class,
                    () ->
                            catalog.alterTable(
                                    path,
                                    loaded,
                                    Collections.singletonList(
                                            TableChange.modifyPhysicalColumnType(
                                                    Column.physical(
                                                            "id", DataTypes.BIGINT().notNull()),
                                                    DataTypes.INT().notNull())),
                                    false));
            assertThrows(
                    ValidationException.class,
                    () ->
                            catalog.alterTable(
                                    path,
                                    loaded,
                                    Collections.singletonList(
                                            TableChange.modifyPhysicalColumnType(
                                                    Column.physical("amount", DataTypes.INT()),
                                                    DataTypes.SMALLINT())),
                                    false));
        } finally {
            catalog.close();
        }
    }

    @Test
    void preservesDeclaredCompositePrimaryKeyOrder() {
        TableSchema schema =
                CobbleCatalogTypes.toCobble(
                        Schema.newBuilder()
                                .column("first", DataTypes.BIGINT().notNull())
                                .column("second", DataTypes.BIGINT().notNull())
                                .column("value", DataTypes.STRING())
                                .primaryKey("second", "first")
                                .build());
        assertEquals(Arrays.asList(Long.valueOf(1L), Long.valueOf(0L)), schema.primaryKey());
    }

    private static CatalogBaseTable table() {
        return CatalogTable.of(
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().notNull())
                        .column("amount", DataTypes.INT())
                        .primaryKey("id")
                        .build(),
                null,
                Collections.emptyList(),
                Collections.emptyMap());
    }
}
