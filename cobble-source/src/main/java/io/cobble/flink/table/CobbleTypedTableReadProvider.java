package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.DataField;
import io.cobble.table.TableReadCapabilities;
import io.cobble.table.TableReadCursor;
import io.cobble.table.TableReadEntry;
import io.cobble.table.TableReadPosition;
import io.cobble.table.TableReadProvider;
import io.cobble.table.TableReadRange;
import io.cobble.table.TableReadSchema;
import io.cobble.table.TableReadSession;
import io.cobble.table.TableScanPlan;
import io.cobble.table.TableSchema;
import io.cobble.table.Value;

import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Converts one projected native Table scan assignment into Flink's internal row format. */
final class CobbleTypedTableReadProvider implements TableReadProvider<RowData, Void> {
    private final TableScanPlan plan;
    private final CobbleTableRowConverter converter;
    private final TableReadProvider<List<Value>, ?> delegate;

    static TableReadSchema schemaFor(CobbleDynamicTableSource.SerializableConfig config) {
        TableSchema full = config.tableSchema();
        List<DataField> selected = new ArrayList<>();
        for (CobbleDynamicTableSource.SerializableField field : config.projectedFields()) {
            selected.add(full.fields().get(field.rowIndex));
        }
        return new TableReadSchema(selected);
    }

    CobbleTypedTableReadProvider(
            CobbleDynamicTableSource.SerializableConfig config,
            TableScanPlan plan,
            Config readerConfig)
            throws Exception {
        this.plan = plan;
        this.converter = new CobbleTableRowConverter(config.projectedRowType());
        this.delegate = plan.open(readerConfig, plan.splits().get(0));
    }

    @Override
    public TableReadCapabilities capabilities() {
        return delegate.capabilities();
    }

    @Override
    public TableReadSession<RowData, Void> open() throws Exception {
        return new Session(delegate.open());
    }

    @Override
    public void close() {
        delegate.close();
    }

    private final class Session implements TableReadSession<RowData, Void> {
        private final TableReadSession<List<Value>, ?> source;

        private Session(TableReadSession<List<Value>, ?> source) {
            this.source = source;
        }

        @Override
        public TableReadSchema schema() {
            return plan.readSchema();
        }

        @Override
        public TableReadCapabilities capabilities() {
            return source.capabilities();
        }

        @Override
        public TableReadCursor<RowData> scan(TableReadRange range, TableReadPosition position)
                throws Exception {
            TableReadCursor<List<Value>> cursor = source.scan(range, position);
            return new TableReadCursor<RowData>() {
                @Override
                public TableReadEntry<RowData> next() throws Exception {
                    TableReadEntry<List<Value>> entry = cursor.next();
                    return entry == null
                            ? null
                            : new TableReadEntry<RowData>(
                                    entry.position(),
                                    converter.toRowData(entry.value()),
                                    entry.physicalBytes(),
                                    entry.countsPhysicalEntry());
                }

                @Override
                public void close() {
                    cursor.close();
                }
            };
        }

        @Override
        public Collection<TableReadEntry<RowData>> lookup(Void key) {
            throw new UnsupportedOperationException("scan assignment does not support lookup");
        }

        @Override
        public void close() {
            source.close();
        }
    }
}
