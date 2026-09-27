package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.DataField;
import io.cobble.table.TableKeyBuilder;
import io.cobble.table.TableProjection;
import io.cobble.table.TableReadCapabilities;
import io.cobble.table.TableReadCursor;
import io.cobble.table.TableReadEntry;
import io.cobble.table.TableReadPosition;
import io.cobble.table.TableReadProvider;
import io.cobble.table.TableReadRange;
import io.cobble.table.TableReadSchema;
import io.cobble.table.TableReadSession;
import io.cobble.table.TableReader;
import io.cobble.table.Value;

import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Owns a reusable typed Table reader and projection for full-primary-key lookup. */
final class CobbleSinkLookupReadProvider implements TableReadProvider<RowData, RowData> {
    private static final TableReadCapabilities CAPABILITIES =
            new TableReadCapabilities(false, true, false);

    private final CobbleDynamicTableSource.SerializableConfig config;
    private final GlobalSnapshot initialSnapshot;
    private final CobbleLookupFunction.RuntimeLookupKeyConverter keyConverter;
    private final CobbleTableRowConverter converter;
    private final List<String> selectedNames;
    private TableReader reader;
    private TableProjection projection;
    private boolean opened;

    CobbleSinkLookupReadProvider(
            CobbleDynamicTableSource.SerializableConfig config,
            GlobalSnapshot initialSnapshot,
            int[] lookupKeyPositions) {
        this.config = config;
        this.initialSnapshot = initialSnapshot;
        this.keyConverter =
                new CobbleLookupFunction.RuntimeLookupKeyConverter(
                        config.keyFields, lookupKeyPositions);
        this.converter = new CobbleTableRowConverter(config.projectedRowType());
        List<String> names = new ArrayList<>();
        for (CobbleDynamicTableSource.SerializableField field : config.projectedFields()) {
            names.add(field.name);
        }
        if (names.isEmpty()) names.add(config.keyFields.get(0).name);
        this.selectedNames = Collections.unmodifiableList(names);
    }

    @Override
    public TableReadCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public synchronized TableReadSession<RowData, RowData> open() {
        if (opened) throw new IllegalStateException("sink lookup provider already opened");
        opened = true;
        Config readerConfig =
                CobbleSourceRuntime.createLookupReaderConfig(config, initialSnapshot.totalBuckets);
        TableReader openedReader =
                config.isStreamingLatest()
                        ? TableReader.openCurrent(readerConfig, CobbleTableRowConverter.TABLE_NAME)
                        : TableReader.open(
                                readerConfig,
                                CobbleTableRowConverter.TABLE_NAME,
                                initialSnapshot.id);
        try {
            projection = openedReader.projectByNames(selectedNames);
            reader = openedReader;
            return new Session();
        } catch (RuntimeException | Error error) {
            openedReader.close();
            throw error;
        }
    }

    synchronized void refresh() {
        if (reader == null || !config.isStreamingLatest() || !reader.refresh()) return;
        TableProjection next = reader.projectByNames(selectedNames);
        TableProjection previous = projection;
        projection = next;
        previous.close();
    }

    @Override
    public synchronized void close() {
        try {
            if (projection != null) projection.close();
        } finally {
            projection = null;
            if (reader != null) reader.close();
            reader = null;
        }
    }

    private final class Session implements TableReadSession<RowData, RowData> {
        private boolean closed;

        @Override
        public List<DataField> keyFields() {
            return reader.keyFields();
        }

        @Override
        public TableReadSchema schema() {
            return CobbleTypedTableReadProvider.schemaFor(config);
        }

        @Override
        public TableReadCapabilities capabilities() {
            return CAPABILITIES;
        }

        @Override
        public TableReadCursor<RowData> scan(TableReadRange range, TableReadPosition position) {
            throw new UnsupportedOperationException("sink lookup session does not scan");
        }

        @Override
        public Collection<TableReadEntry<RowData>> lookup(RowData key) {
            synchronized (CobbleSinkLookupReadProvider.this) {
                if (closed) throw new IllegalStateException("sink lookup session is closed");
                TableKeyBuilder builder = reader.keyBuilder();
                keyConverter.pushInto(key, builder);
                TableReadEntry<List<Value>> found = projection.getEntry(builder.build());
                if (found == null) return Collections.emptyList();
                return Collections.singletonList(
                        new TableReadEntry<RowData>(
                                found.position(),
                                converter.toRowData(
                                        config.outputProjection.length == 0
                                                ? Collections.emptyList()
                                                : found.value()),
                                found.physicalBytes(),
                                found.countsPhysicalEntry()));
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
