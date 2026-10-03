package io.cobble.flink.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogTable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class CobbleCatalogTableTest {
    @Test
    void defensivelyCopiesOptionsAndPartitionKeys() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "cobble");
        List<String> partitions = new ArrayList<>(Collections.singletonList("region"));
        CatalogTable table = new CobbleCatalogTable(schema(), "items", partitions, options);
        options.clear();
        partitions.clear();

        assertEquals(Collections.singletonMap("connector", "cobble"), table.getOptions());
        assertEquals(Collections.singletonList("region"), table.getPartitionKeys());
        assertTrue(table.isPartitioned());
        assertEquals(TableKind.TABLE, table.getTableKind());
        assertThrows(
                UnsupportedOperationException.class,
                () -> table.getOptions().put("connector", "other"));
        assertThrows(UnsupportedOperationException.class, () -> table.getPartitionKeys().add("id"));
    }

    @Test
    void copiesPreserveSchemaPrimaryKeyAndMetadata() {
        Schema schema = schema();
        CatalogTable table =
                new CobbleCatalogTable(
                        schema,
                        "items",
                        Collections.singletonList("region"),
                        Collections.singletonMap("connector", "cobble"));
        Map<String, String> replacement = new HashMap<>();
        replacement.put("bucket", "2");
        CatalogTable copied = (CatalogTable) table.copy();
        CatalogTable withOptions = table.copy(replacement);
        replacement.clear();

        assertNotSame(table, copied);
        for (CatalogTable copy : Arrays.asList(copied, withOptions)) {
            assertSame(schema, copy.getUnresolvedSchema());
            assertEquals(
                    Arrays.asList("region", "id"),
                    copy.getUnresolvedSchema().getPrimaryKey().get().getColumnNames());
            assertEquals(2, copy.getUnresolvedSchema().getColumns().size());
            assertEquals("items", copy.getComment());
            assertEquals(Optional.of("items"), copy.getDescription());
            assertEquals(Optional.of("items"), copy.getDetailedDescription());
            assertEquals(Collections.singletonList("region"), copy.getPartitionKeys());
        }
        assertEquals(table.getOptions(), copied.getOptions());
        assertEquals(Collections.singletonMap("bucket", "2"), withOptions.getOptions());
        assertEquals(Collections.singletonMap("connector", "cobble"), table.getOptions());
        assertThrows(
                UnsupportedOperationException.class,
                () -> withOptions.getOptions().put("bucket", "3"));
    }

    @Test
    void supportsAbsentCommentAndUnpartitionedTables() {
        CatalogTable table =
                new CobbleCatalogTable(
                        schema(), null, Collections.emptyList(), Collections.emptyMap());
        assertFalse(table.isPartitioned());
        assertEquals(Optional.empty(), table.getDescription());
        assertEquals(Optional.empty(), table.getDetailedDescription());
    }

    private static Schema schema() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("region", DataTypes.STRING().notNull())
                .primaryKey("region", "id")
                .build();
    }
}
