package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.GlobalSnapshot;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.TableReader;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class SourceKeyFilterTest {
    private final RowType type =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.INT()),
                                    DataTypes.FIELD("flag", DataTypes.BOOLEAN()),
                                    DataTypes.FIELD("value", DataTypes.STRING()))
                            .getLogicalType();

    @Test
    void acceptsOnlyFullKeyAndRetainsEveryPredicate() {
        ResolvedExpression id =
                equals(field("id", DataTypes.INT(), 0), new ValueLiteralExpression(3));
        ResolvedExpression flag =
                equals(new ValueLiteralExpression(true), field("flag", DataTypes.BOOLEAN(), 1));
        ResolvedExpression value =
                equals(field("value", DataTypes.STRING(), 2), new ValueLiteralExpression("x"));
        List<ResolvedExpression> filters = Arrays.asList(id, flag, value);
        SourceKeyFilter.Selection selection =
                select(filters, new int[] {0, 1, 2}, new int[] {1, 0});
        assertNotNull(selection.filter);
        assertEquals(Arrays.asList(id, flag), selection.result.getAcceptedFilters());
        assertEquals(filters, selection.result.getRemainingFilters());
        assertNull(
                select(Collections.singletonList(id), new int[] {0, 1, 2}, new int[] {0, 1})
                        .filter);
        ResolvedExpression and =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.AND,
                        Arrays.asList(id, flag),
                        DataTypes.BOOLEAN());
        assertEquals(
                Collections.singletonList(and),
                select(Collections.singletonList(and), new int[] {0, 1, 2}, new int[] {0, 1})
                        .result
                        .getAcceptedFilters());
    }

    @Test
    void unsupportedScalarFamiliesNeverRoute() {
        List<DataType> types =
                Arrays.asList(
                        DataTypes.STRING(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.CHAR(3),
                        DataTypes.DECIMAL(10, 2),
                        DataTypes.TIMESTAMP(3));
        List<Object> values =
                Arrays.asList(
                        "abc",
                        1.0f,
                        1.0d,
                        "abc",
                        new BigDecimal("1.00"),
                        LocalDateTime.of(2026, 1, 1, 0, 0));
        for (int i = 0; i < types.size(); i++) {
            DataType dataType = types.get(i);
            RowType row = (RowType) DataTypes.ROW(DataTypes.FIELD("id", dataType)).getLogicalType();
            ResolvedExpression predicate =
                    equals(
                            field("id", dataType, 0),
                            new ValueLiteralExpression(values.get(i), dataType.notNull()));
            assertNull(
                    SourceKeyFilter.select(
                                    Collections.singletonList(predicate),
                                    row,
                                    new int[] {0},
                                    new int[] {0})
                            .filter);
        }
    }

    @Test
    void serializedProjectedConfigKeepsFixedSnapshotRouting(@TempDir Path directory)
            throws Exception {
        GlobalSnapshot snapshot =
                CobbleTableSourceTestData.write(
                        directory,
                        8,
                        CobbleTableSourceTestData.idNameSchema(),
                        CobbleTableSourceTestData.idNameRows());
        CobbleDynamicTableSource.SerializableConfig config =
                new CobbleDynamicTableSource.SerializableConfig(
                        directory.toUri().toString(),
                        8,
                        Long.toString(snapshot.id),
                        "batch",
                        50,
                        0,
                        Collections.singletonList(
                                new CobbleDynamicTableSource.SerializableField(
                                        "id", "BIGINT", 0, -1)),
                        Collections.singletonList(
                                new CobbleDynamicTableSource.SerializableField(
                                        "name", "STRING", 1, 0)));
        SourceKeyFilter filter =
                SourceKeyFilter.select(
                                Collections.singletonList(
                                        equals(
                                                field("id", DataTypes.BIGINT(), 0),
                                                new ValueLiteralExpression(1L))),
                                config.rowType(),
                                config.outputProjection,
                                new int[] {0})
                        .filter;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(config.withFilter(filter).withProjection(new int[] {1}));
        }
        CobbleDynamicTableSource.SerializableConfig restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (CobbleDynamicTableSource.SerializableConfig) input.readObject();
        }
        assertEquals("name", restored.projectedFields().get(0).name);
        assertEquals(1, CobbleSourceRuntime.createSourceSplits(restored, snapshot).size());
        try (TableReader reader =
                TableReader.open(
                        CobbleSourceRuntime.createSourceScanConfig(restored, 8),
                        CobbleTableRowConverter.TABLE_NAME,
                        snapshot.id)) {
            assertEquals(filter.bucket(reader), restored.keyFilter.bucket(reader));
        }
    }

    @Test
    void mapsProjectedIndexesAndRejectsUnsafeExpressions() {
        ResolvedExpression id =
                equals(field("id", DataTypes.INT(), 1), new ValueLiteralExpression(3));
        assertNotNull(
                select(Collections.singletonList(id), new int[] {2, 0}, new int[] {0}).filter);
        ResolvedExpression nullId =
                equals(
                        field("id", DataTypes.INT(), 0),
                        new ValueLiteralExpression(null, DataTypes.INT()));
        ResolvedExpression coerced =
                equals(field("id", DataTypes.INT(), 0), new ValueLiteralExpression(3L));
        ResolvedExpression or =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.OR, Arrays.asList(id, id), DataTypes.BOOLEAN());
        for (ResolvedExpression expression : Arrays.asList(nullId, coerced, or)) {
            SourceKeyFilter.Selection selection =
                    select(
                            Collections.singletonList(expression),
                            new int[] {0, 1, 2},
                            new int[] {0});
            assertNull(selection.filter);
            assertEquals(
                    Collections.singletonList(expression), selection.result.getRemainingFilters());
            assertTrue(selection.result.getAcceptedFilters().isEmpty());
        }
    }

    @Test
    void copyAndProjectionKeepIndependentRoutingAndRepeatedFiltersMayClearIt() {
        CobbleDynamicTableSource source =
                new CobbleDynamicTableSource(
                        new CobbleDynamicTableSource.SerializableConfig(
                                "file:///unused",
                                8,
                                "latest",
                                "batch",
                                50,
                                0,
                                Collections.singletonList(
                                        new CobbleDynamicTableSource.SerializableField(
                                                "id", "INT", 0, -1)),
                                Collections.singletonList(
                                        new CobbleDynamicTableSource.SerializableField(
                                                "value", "STRING", 1, 0))),
                        "test");
        ResolvedExpression id =
                equals(field("id", DataTypes.INT(), 0), new ValueLiteralExpression(3));
        assertEquals(
                Collections.singletonList(id),
                source.applyFilters(Collections.singletonList(id)).getAcceptedFilters());
        CobbleDynamicTableSource copy = (CobbleDynamicTableSource) source.copy();
        copy.applyProjection(
                new int[][] {{1}, {0}},
                DataTypes.ROW(
                        DataTypes.FIELD("value", DataTypes.STRING()),
                        DataTypes.FIELD("id", DataTypes.INT())));
        ResolvedExpression projectedId =
                equals(field("id", DataTypes.INT(), 1), new ValueLiteralExpression(3));
        assertEquals(
                Collections.singletonList(projectedId),
                copy.applyFilters(Collections.singletonList(projectedId)).getAcceptedFilters());
        assertTrue(copy.applyFilters(Collections.emptyList()).getAcceptedFilters().isEmpty());
        assertEquals(
                Collections.singletonList(id),
                source.applyFilters(Collections.singletonList(id)).getAcceptedFilters());
    }

    private SourceKeyFilter.Selection select(
            List<ResolvedExpression> filters, int[] projection, int[] primary) {
        return SourceKeyFilter.select(filters, type, projection, primary);
    }

    static FieldReferenceExpression field(String name, DataType type, int index) {
        return new FieldReferenceExpression(name, type, 0, index);
    }

    static CallExpression equals(ResolvedExpression left, ResolvedExpression right) {
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.EQUALS, Arrays.asList(left, right), DataTypes.BOOLEAN());
    }
}
