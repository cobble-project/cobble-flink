package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.ReadOptions;
import io.cobble.Reader;
import io.cobble.flink.common.CobbleConnectorMetrics;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.BucketHash;
import io.cobble.table.DataField;
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

/** Owns native sink-table lookup I/O, decoding, and bucket routing. */
final class CobbleSinkLookupReadProvider implements TableReadProvider<RowData, RowData> {

    private static final TableReadCapabilities CAPABILITIES =
            new TableReadCapabilities(false, true, false);

    private final CobbleDynamicTableSource.SerializableConfig config;
    private final GlobalSnapshot initialSnapshot;
    private final CobbleLookupFunction.RuntimeLookupKeyEncoder keyEncoder;
    private Reader reader;
    private ReadOptions readOptions;
    private ScannedRowDecoder decoder;
    private int totalBuckets;
    private boolean opened;

    CobbleSinkLookupReadProvider(
            CobbleDynamicTableSource.SerializableConfig config,
            GlobalSnapshot initialSnapshot,
            int[] lookupKeyPositions) {
        this.config = config;
        this.initialSnapshot = initialSnapshot;
        this.keyEncoder =
                new CobbleLookupFunction.RuntimeLookupKeyEncoder(
                        config.keyFields, lookupKeyPositions);
    }

    @Override
    public TableReadCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public synchronized TableReadSession<RowData, RowData> open() throws Exception {
        if (opened)
            throw new IllegalStateException("sink lookup provider already has an open session");
        opened = true;
        totalBuckets = initialSnapshot.totalBuckets;
        decoder = config.createDecoder();
        Config readerConfig = CobbleSourceRuntime.createLookupReaderConfig(config, totalBuckets);
        Reader openedReader =
                config.isStreamingLatest()
                        ? Reader.openCurrent(readerConfig)
                        : Reader.open(readerConfig, initialSnapshot.id);
        try {
            readOptions =
                    ReadOptions.forColumnsInFamily(
                            CobbleTableRowConverter.TABLE_NAME, config.projectedColumnIndexes());
            reader = openedReader;
            return new Session();
        } catch (RuntimeException error) {
            openedReader.close();
            throw error;
        }
    }

    synchronized void refresh() {
        if (reader != null && config.isStreamingLatest()) {
            reader.refresh();
            totalBuckets = reader.currentGlobalSnapshot().totalBuckets;
        }
    }

    @Override
    public synchronized void close() {
        if (readOptions != null) readOptions.close();
        readOptions = null;
        if (reader != null) reader.close();
        reader = null;
    }

    private final class Session implements TableReadSession<RowData, RowData> {
        private boolean closed;

        @Override
        public TableReadSchema schema() {
            List<DataField> fields = new ArrayList<DataField>();
            for (int index = 0; index < config.physicalFields().size(); index++) {
                CobbleDynamicTableSource.SerializableField field =
                        config.physicalFields().get(index);
                fields.add(
                        new DataField(
                                index,
                                field.name,
                                CobbleTableRowConverter.toCobbleType(
                                        LogicalTypeParser.parse(
                                                field.logicalType,
                                                CobbleSinkLookupReadProvider.class
                                                        .getClassLoader()))));
            }
            return new TableReadSchema(fields);
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
        public Collection<TableReadEntry<RowData>> lookup(RowData key) throws Exception {
            if (closed) throw new IllegalStateException("sink lookup session is closed");
            byte[] encoded = keyEncoder.encode(key);
            int bucket = new BucketHash(totalBuckets).bucket(encoded);
            byte[][] columns = reader.getWithOptions(bucket, encoded, readOptions);
            if (columns == null) return Collections.emptyList();
            return Collections.singletonList(
                    new TableReadEntry<RowData>(
                            new TableReadPosition(bucket, encoded, 1),
                            decoder.decode(encoded, columns),
                            CobbleConnectorMetrics.nativeEntryBytes(encoded, columns),
                            true));
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
