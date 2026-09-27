package io.cobble.flink.table;

import io.cobble.flink.catalog.CobbleCatalogTableReference;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.TableKeyBuilder;
import io.cobble.table.TableProjection;
import io.cobble.table.TableReader;
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
    private final int[] outputProjection;
    private transient TableReader reader;
    private transient TableProjection projection;
    private transient CobbleTableRowConverter converter;
    private transient RowData.FieldGetter[] keyGetters;

    CobbleCatalogLookupFunction(
            CobbleCatalogTableReference reference,
            RowType rowType,
            String snapshot,
            int[] primaryPositions,
            int[] mapping,
            int[] outputProjection) {
        this.reference = reference;
        this.rowType = rowType;
        this.snapshot = snapshot;
        this.primaryPositions = primaryPositions.clone();
        this.mapping = mapping.clone();
        this.outputProjection = outputProjection.clone();
    }

    @Override
    public void open(FunctionContext context) {
        converter =
                new CobbleTableRowConverter(SourceProjection.rowType(rowType, outputProjection));
        keyGetters = new RowData.FieldGetter[mapping.length];
        for (int i = 0; i < mapping.length; i++) {
            keyGetters[i] =
                    RowData.createFieldGetter(rowType.getTypeAt(primaryPositions[i]), mapping[i]);
        }
        TableReader opened = CobbleCatalogDynamicTableSource.openReader(reference, snapshot);
        if (opened == null) return;
        try {
            List<String> selectedNames = new ArrayList<>(Math.max(1, outputProjection.length));
            if (outputProjection.length == 0) {
                selectedNames.add(rowType.getFieldNames().get(primaryPositions[0]));
            } else {
                for (int index : outputProjection) {
                    selectedNames.add(rowType.getFieldNames().get(index));
                }
            }
            projection = opened.projectByNames(selectedNames);
            reader = opened;
        } catch (RuntimeException | Error error) {
            opened.close();
            throw error;
        }
    }

    @Override
    public Collection<RowData> lookup(RowData arguments) throws IOException {
        try {
            if (projection == null) return Collections.emptyList();
            TableKeyBuilder key = reader.keyBuilder();
            for (int i = 0; i < mapping.length; i++) {
                Object value = keyGetters[i].getFieldOrNull(arguments);
                if (value == null) return Collections.emptyList();
                key.push(
                        CobbleTableRowConverter.toValue(
                                rowType.getTypeAt(primaryPositions[i]), value));
            }
            List<Value> values = projection.get(key.build());
            return values == null
                    ? Collections.emptyList()
                    : Collections.singletonList(
                            converter.toRowData(
                                    outputProjection.length == 0
                                            ? Collections.emptyList()
                                            : values));
        } catch (Exception error) {
            throw new IOException("Cobble catalog lookup failed.", error);
        }
    }

    @Override
    public void close() {
        try {
            if (projection != null) projection.close();
        } finally {
            projection = null;
            if (reader != null) reader.close();
            reader = null;
        }
    }
}
