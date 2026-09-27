package io.cobble.flink.table;

import io.cobble.ScanOptions;
import io.cobble.ScanSplit;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Keeps raw source bytes and their row decoding behind the table read SPI. */
final class CobbleRawTableReadProvider implements TableReadProvider<RowData, Void> {
    private static final TableReadCapabilities CAPABILITIES =
            new TableReadCapabilities(true, false, false);

    private final RawSourceConfig config;
    private final ScanSplit split;
    private final int totalBuckets;
    private boolean opened;

    CobbleRawTableReadProvider(RawSourceConfig config, ScanSplit split, int totalBuckets) {
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
            throw new IllegalStateException("raw source provider already has an open session");
        opened = true;
        CobbleRawRowDecoder decoder =
                new CobbleRawRowDecoder(config.projectedColumnIndexes().length);
        TableReadSchema schema = schemaForRaw();
        try (ScanOptions options = ScanOptions.forColumns(config.projectedColumnIndexes())) {
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
            if (closed) throw new IllegalStateException("raw source read session is closed");
            if (position != null)
                throw new UnsupportedOperationException("raw scan is pre-positioned by ScanSplit");
            if (range.firstBucket() != 0 || range.lastBucket() != Integer.MAX_VALUE) {
                throw new UnsupportedOperationException("raw scan range is fixed by its ScanSplit");
            }
            return cursor;
        }

        @Override
        public Collection<TableReadEntry<RowData>> lookup(Void ignored) {
            throw new UnsupportedOperationException("raw source scan does not support lookup");
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            cursor.close();
        }
    }

    private static TableReadSchema schemaForRaw() {
        List<DataField> fields = new ArrayList<DataField>();
        fields.add(new DataField(0, "key", LogicalTypes.binary()));
        fields.add(new DataField(1, "columns", LogicalTypes.list(LogicalTypes.binary())));
        return new TableReadSchema(fields);
    }
}
