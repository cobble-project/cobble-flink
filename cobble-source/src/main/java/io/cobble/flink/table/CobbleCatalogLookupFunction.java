package io.cobble.flink.table;

import io.cobble.flink.catalog.CobbleCatalogTableReference;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.TableReadEntry;
import io.cobble.table.Value;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Full-primary-key lookup against one catalog-selected snapshot. */
public final class CobbleCatalogLookupFunction extends LookupFunction {
    private final CobbleCatalogTableReference reference;
    private final RowType rowType;
    private final String snapshot;
    private final int[] primaryPositions;
    private final int[] mapping;
    private transient io.cobble.table.TableReader reader;
    private transient CobbleTableRowConverter converter;
    private transient RowData.FieldGetter[] keyGetters;

    CobbleCatalogLookupFunction(
            CobbleCatalogTableReference reference,
            RowType rowType,
            String snapshot,
            int[] primaryPositions,
            int[] mapping) {
        this.reference = reference;
        this.rowType = rowType;
        this.snapshot = snapshot;
        this.primaryPositions = primaryPositions.clone();
        this.mapping = mapping.clone();
    }

    @Override
    public void open(FunctionContext context) {
        converter = new CobbleTableRowConverter(rowType);
        keyGetters = new RowData.FieldGetter[mapping.length];
        for (int i = 0; i < mapping.length; i++) {
            keyGetters[i] =
                    RowData.createFieldGetter(rowType.getTypeAt(primaryPositions[i]), mapping[i]);
        }
        reader = CobbleCatalogDynamicTableSource.openReader(reference, snapshot);
    }

    @Override
    public Collection<RowData> lookup(RowData arguments) throws IOException {
        try {
            if (reader == null) return Collections.emptyList();
            List<Value> key = new ArrayList<>(mapping.length);
            for (int i = 0; i < mapping.length; i++) {
                Object value = keyGetters[i].getFieldOrNull(arguments);
                if (value == null) return Collections.emptyList();
                key.add(
                        CobbleTableRowConverter.toValue(
                                rowType.getTypeAt(primaryPositions[i]), value));
            }
            Collection<TableReadEntry<List<Value>>> rows = reader.lookup(key);
            return rows.isEmpty()
                    ? Collections.emptyList()
                    : Collections.singletonList(
                            converter.toRowData(rows.iterator().next().value()));
        } catch (RuntimeException error) {
            throw new IOException("Cobble catalog lookup failed.", error);
        } catch (Exception error) {
            throw new IOException("Cobble catalog lookup failed.", error);
        }
    }

    @Override
    public void close() {
        if (reader != null) reader.close();
        reader = null;
    }
}
