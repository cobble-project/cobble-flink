package io.cobble.flink.table;

import io.cobble.ScanOptions;
import io.cobble.ScanSplit;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.DataField;
import io.cobble.table.LogicalTypes;
import io.cobble.table.TableDirectScanCursor;
import io.cobble.table.TableReadCapabilities;
import io.cobble.table.TableReadCursor;
import io.cobble.table.TableReadEntry;
import io.cobble.table.TableReadPosition;
import io.cobble.table.TableReadProvider;
import io.cobble.table.TableReadRange;
import io.cobble.table.TableReadSchema;
import io.cobble.table.TableReadSession;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Keeps sink-table native scan bytes and row decoding behind the table read SPI. */
final class CobbleSinkTableReadProvider implements TableReadProvider<RowData, Void> {
    private static final TableReadCapabilities CAPABILITIES =
            new TableReadCapabilities(true, false, false);

    private final CobbleTableScanConfig config;
    private final ScanSplit split;
    private final int totalBuckets;
    private boolean opened;

    CobbleSinkTableReadProvider(CobbleTableScanConfig config, ScanSplit split, int totalBuckets) {
        this.config = config;
        this.split = split;
        this.totalBuckets = totalBuckets;
    }

    @Override
    public TableReadCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public synchronized TableReadSession<RowData, Void> open() throws Exception {
        if (opened)
            throw new IllegalStateException("sink table provider already has an open session");
        opened = true;
        ScannedRowDecoder decoder = config.createDecoder();
        TableReadSchema schema = schemaFor(config);
        try (ScanOptions options = ScanOptions.forColumns(config.projectedColumnIndexes())) {
            if (config.columnFamily() != null) options.columnFamily(config.columnFamily());
            return new Session(
                    TableDirectScanCursor.fixed(
                            split.openDirectScannerWithOptions(
                                    CobbleSourceRuntime.createSourceScanConfig(
                                            config, totalBuckets),
                                    options),
                            (entry, ownedKey) ->
                                    Collections.singletonList(
                                            decoder.decode(
                                                    ownedKey,
                                                    TableDirectScanCursor.copyColumns(entry)))),
                    schema);
        }
    }

    @Override
    public void close() {}

    private static final class Session implements TableReadSession<RowData, Void> {
        private final TableDirectScanCursor<RowData> cursor;
        private final TableReadSchema schema;
        private boolean closed;

        private Session(TableDirectScanCursor<RowData> cursor, TableReadSchema schema) {
            this.cursor = cursor;
            this.schema = schema;
        }

        @Override
        public TableReadSchema schema() {
            return schema;
        }

        @Override
        public TableReadCapabilities capabilities() {
            return CAPABILITIES;
        }

        @Override
        public TableReadCursor<RowData> scan(TableReadRange range, TableReadPosition position) {
            if (closed) throw new IllegalStateException("sink table read session is closed");
            if (position != null)
                throw new UnsupportedOperationException("sink scan is pre-positioned by ScanSplit");
            if (range.firstBucket() != 0 || range.lastBucket() != Integer.MAX_VALUE) {
                throw new UnsupportedOperationException(
                        "sink scan range is fixed by its ScanSplit");
            }
            return cursor;
        }

        @Override
        public Collection<TableReadEntry<RowData>> lookup(Void ignored) {
            throw new UnsupportedOperationException("sink table scan does not support lookup");
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            cursor.close();
        }
    }

    private static TableReadSchema schemaFor(CobbleTableScanConfig config) {
        List<DataField> fields = new ArrayList<DataField>();
        if (config instanceof CobbleDynamicTableSource.SerializableConfig) {
            int index = 0;
            for (CobbleDynamicTableSource.SerializableField field :
                    ((CobbleDynamicTableSource.SerializableConfig) config).physicalFields()) {
                fields.add(
                        new DataField(
                                index++,
                                field.name,
                                CobbleTableRowConverter.toCobbleType(
                                        LogicalTypeParser.parse(
                                                field.logicalType,
                                                CobbleSinkTableReadProvider.class
                                                        .getClassLoader()))));
            }
        } else {
            fields.add(new DataField(0, "key", LogicalTypes.binary()));
            fields.add(new DataField(1, "columns", LogicalTypes.list(LogicalTypes.binary())));
        }
        return new TableReadSchema(fields);
    }
}
