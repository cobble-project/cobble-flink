package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.DataField;
import io.cobble.table.TableFormatPlugin;
import io.cobble.table.TableFormatPluginRegistry;
import io.cobble.table.TablePathRequest;
import io.cobble.table.TableReadCapabilities;
import io.cobble.table.TableReadCursor;
import io.cobble.table.TableReadEntry;
import io.cobble.table.TableReadPosition;
import io.cobble.table.TableReadProvider;
import io.cobble.table.TableReadRange;
import io.cobble.table.TableReadSchema;
import io.cobble.table.TableReadSession;
import io.cobble.table.TableReadSnapshot;
import io.cobble.table.TableReader;
import io.cobble.table.TableScanPlan;
import io.cobble.table.TableScanSplit;
import io.cobble.table.Value;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Flink row adapter over the fixed public {@link TableReader} state session.
 *
 * <p>The source retains its SQL field order and lookup-key reorder contract, while snapshot
 * opening, state decoding, bucket routing, direct scanning, and resource ownership all belong to
 * the common table reader session.
 */
final class CobbleStateTableReadProvider implements TableReadProvider<RowData, RowData> {
    private final StateSourceConfig sourceConfig;
    private final long checkpointId;
    private final int[] lookupKeyPositions;
    private boolean opened;

    static CobbleStateTableReadProvider forScan(StateSourceConfig config, long checkpointId) {
        return new CobbleStateTableReadProvider(config, checkpointId, null);
    }

    static CobbleStateTableReadProvider forLookup(
            StateSourceConfig config, int[] lookupKeyPositions) throws IOException {
        return new CobbleStateTableReadProvider(
                config, CobbleStateSourceRuntime.resolveCheckpointId(config), lookupKeyPositions);
    }

    static List<CobbleStateSourceSplit> createSourceSplits(StateSourceConfig sourceConfig)
            throws Exception {
        long checkpointId = CobbleStateSourceRuntime.resolveCheckpointId(sourceConfig);
        Config config = CobbleStateSourceRuntime.tableReadConfig(sourceConfig);
        try (TableReader reader =
                TableReader.open(
                        config,
                        fixedSnapshot(config, sourceConfig, checkpointId),
                        (TableFormatPlugin) new CobbleStateTableFormatPlugin())) {
            TableScanPlan plan = reader.scanPlan();
            if (plan.snapshotId() != checkpointId) {
                throw new IOException(
                        "state table reader resolved checkpoint "
                                + plan.snapshotId()
                                + " instead of fixed checkpoint "
                                + checkpointId);
            }
            List<CobbleStateSourceSplit> result = new ArrayList<CobbleStateSourceSplit>();
            for (TableScanSplit split : plan.splits()) {
                for (io.cobble.ShardSnapshot.Range range : split.shardSnapshot().ranges) {
                    result.add(
                            CobbleStateSourceSplit.forRange(
                                    checkpointId,
                                    plan.totalBuckets(),
                                    range.start,
                                    range.end,
                                    sourceConfig.operatorId(),
                                    sourceConfig.stateName(),
                                    sourceConfig.stateKind()));
                }
            }
            return result;
        }
    }

    private CobbleStateTableReadProvider(
            StateSourceConfig sourceConfig, long checkpointId, int[] lookupKeyPositions) {
        this.sourceConfig = sourceConfig;
        this.checkpointId = checkpointId;
        this.lookupKeyPositions =
                lookupKeyPositions == null
                        ? null
                        : Arrays.copyOf(lookupKeyPositions, lookupKeyPositions.length);
    }

    @Override
    public TableReadCapabilities capabilities() {
        boolean lookup =
                lookupKeyPositions != null
                        && !"list".equals(sourceConfig.stateKind())
                        && !"timer".equals(sourceConfig.stateKind());
        return new TableReadCapabilities(true, lookup, true);
    }

    @Override
    public synchronized TableReadSession<RowData, RowData> open() throws Exception {
        if (opened) {
            throw new IllegalStateException(
                    "state table read provider already has an open session");
        }
        opened = true;
        Config config = CobbleStateSourceRuntime.tableReadConfig(sourceConfig);
        TableReader reader = null;
        try {
            reader =
                    TableReader.open(
                            config,
                            fixedSnapshot(config, sourceConfig, checkpointId),
                            (TableFormatPlugin) new CobbleStateTableFormatPlugin());
            return new Session(reader);
        } catch (Exception error) {
            if (reader != null) reader.close();
            throw error;
        }
    }

    @Override
    public void close() {}

    private static TableReadSnapshot fixedSnapshot(
            Config config, StateSourceConfig sourceConfig, long checkpointId) throws Exception {
        Map<String, String> options = new LinkedHashMap<String, String>();
        options.put(
                CobbleEmbeddedCheckpointReadPlanner.CHECKPOINT_ID_OPTION,
                Long.toString(checkpointId));
        options.put(
                CobbleEmbeddedCheckpointReadPlanner.OPERATOR_ID_OPTION, sourceConfig.operatorId());
        TablePathRequest request =
                new TablePathRequest(
                        sourceConfig.pathUri(), sourceConfig.stateName(), null, options);
        Optional<TableReadSnapshot> embedded =
                CobbleEmbeddedCheckpointReadPlanner.resolve(config, request);
        if (embedded.isPresent()) return embedded.get();
        try {
            return TableFormatPluginRegistry.resolvePath(config, request);
        } catch (IllegalArgumentException error) {
            throw new IOException(
                    "state source path is not a readable fixed checkpoint or Cobble global root: "
                            + sourceConfig.pathUri(),
                    error);
        }
    }

    private final class Session implements TableReadSession<RowData, RowData> {
        private final TableReader reader;
        private final TableReadSchema schema;
        private final CobbleTableRowConverter outputConverter;
        private final List<LogicalType> lookupTypes;
        private final RowData.FieldGetter[] lookupFieldGetters;
        private final int[] sourceValueIndexes;
        private final boolean identitySourceMapping;
        private final TableReadCapabilities readCapabilities;
        private boolean closed;

        private Session(TableReader reader) {
            this.reader = reader;
            List<String> names = new ArrayList<String>();
            List<String> types = new ArrayList<String>();
            List<DataField> fields = new ArrayList<DataField>();
            for (int index = 0; index < sourceConfig.outputFields().size(); index++) {
                StateSourceField field = sourceConfig.outputFields().get(index);
                names.add(field.name());
                types.add(field.logicalType());
                fields.add(
                        new DataField(
                                index,
                                field.name(),
                                CobbleTableRowConverter.toCobbleType(
                                        LogicalTypeParser.parse(
                                                field.logicalType(),
                                                CobbleStateTableReadProvider.class
                                                        .getClassLoader()))));
            }
            schema = new TableReadSchema(fields);
            outputConverter =
                    new CobbleTableRowConverter(CobbleTableRowConverter.parseRowType(names, types));
            sourceValueIndexes = sourceIndexes(reader.schema(), fields);
            identitySourceMapping = identityMapping(reader.schema(), sourceValueIndexes);
            lookupTypes = lookupTypes(sourceConfig.lookupKeyContract().requiredFields());
            lookupFieldGetters = lookupFieldGetters(lookupTypes, lookupKeyPositions);
            validateLookupContract(reader.keyFields());
            TableReadCapabilities readerCapabilities = reader.capabilities();
            readCapabilities =
                    new TableReadCapabilities(
                            readerCapabilities.scan(),
                            lookupKeyPositions != null && readerCapabilities.exactLookup(),
                            readerCapabilities.restartableScan());
        }

        @Override
        public TableReadSchema schema() {
            return schema;
        }

        @Override
        public TableReadCapabilities capabilities() {
            return readCapabilities;
        }

        @Override
        public TableReadCursor<RowData> scan(TableReadRange range, TableReadPosition position)
                throws Exception {
            ensureOpen();
            return rows(reader.scan(range, position));
        }

        @Override
        public Collection<TableReadEntry<RowData>> lookup(RowData key) throws Exception {
            ensureOpen();
            if (!capabilities().exactLookup()) {
                throw new UnsupportedOperationException(
                        "exact lookup is not supported for this state kind or session");
            }
            Collection<TableReadEntry<List<Value>>> matches = reader.lookup(lookupValues(key));
            if (matches.isEmpty()) return Collections.emptyList();
            List<TableReadEntry<RowData>> rows =
                    new ArrayList<TableReadEntry<RowData>>(matches.size());
            for (TableReadEntry<List<Value>> match : matches) rows.add(row(match));
            return rows;
        }

        @Override
        public synchronized void close() {
            if (closed) return;
            closed = true;
            reader.close();
        }

        private TableReadCursor<RowData> rows(final TableReadCursor<List<Value>> delegate) {
            return new TableReadCursor<RowData>() {
                private boolean cursorClosed;

                @Override
                public TableReadEntry<RowData> next() throws Exception {
                    if (cursorClosed) {
                        throw new IllegalStateException("state table scan cursor is closed");
                    }
                    try {
                        TableReadEntry<List<Value>> value = delegate.next();
                        return value == null ? null : row(value);
                    } catch (Exception error) {
                        close();
                        throw error;
                    }
                }

                @Override
                public void close() {
                    if (cursorClosed) return;
                    cursorClosed = true;
                    delegate.close();
                }
            };
        }

        private TableReadEntry<RowData> row(TableReadEntry<List<Value>> entry) {
            List<Value> source = entry.value();
            List<Value> values;
            if (identitySourceMapping) {
                values = source;
            } else {
                values = new ArrayList<Value>(sourceValueIndexes.length);
                for (int sourceIndex : sourceValueIndexes) values.add(source.get(sourceIndex));
            }
            return new TableReadEntry<RowData>(
                    entry.position(),
                    outputConverter.toRowData(values),
                    entry.physicalBytes(),
                    entry.countsPhysicalEntry());
        }

        private List<Value> lookupValues(RowData key) throws IOException {
            List<Value> values = new ArrayList<Value>(lookupTypes.size());
            for (int index = 0; index < lookupTypes.size(); index++) {
                Object value = lookupFieldGetters[index].getFieldOrNull(key);
                if (value == null) {
                    throw new IOException(
                            "Lookup key column '"
                                    + sourceConfig
                                            .lookupKeyContract()
                                            .requiredFields()
                                            .get(index)
                                            .name()
                                    + "' must not be null.");
                }
                values.add(CobbleTableRowConverter.toValue(lookupTypes.get(index), value));
            }
            return values;
        }

        private void ensureOpen() {
            if (closed) throw new IllegalStateException("state table read session is closed");
        }
    }

    private static int[] sourceIndexes(TableReadSchema source, List<DataField> output) {
        int[] indexes = new int[output.size()];
        for (int index = 0; index < output.size(); index++) {
            DataField expected = output.get(index);
            int sourceIndex = find(source, expected.name());
            if (sourceIndex < 0) {
                // SQL state sources may rename a derived semantic field (for example LIST
                // `value` to `element`). Both layouts originate from the same state metadata,
                // so preserve that configured output order only when the complete row shape and
                // the corresponding logical type agree.
                if (source.fields().size() != output.size()) {
                    throw new IllegalArgumentException(
                            "state reader does not expose expected field '"
                                    + expected.name()
                                    + "'");
                }
                DataField positional = source.fields().get(index);
                if (!expected.logicalType().equals(positional.logicalType())) {
                    throw new IllegalArgumentException(
                            "state reader does not expose expected field '"
                                    + expected.name()
                                    + "' with compatible logical type");
                }
                sourceIndex = index;
            }
            indexes[index] = sourceIndex;
        }
        return indexes;
    }

    private static boolean identityMapping(TableReadSchema source, int[] indexes) {
        if (source.fields().size() != indexes.length) return false;
        for (int index = 0; index < indexes.length; index++) {
            if (indexes[index] != index) return false;
        }
        return true;
    }

    private static int find(TableReadSchema schema, String name) {
        for (int index = 0; index < schema.fields().size(); index++) {
            if (name.equals(schema.fields().get(index).name())) return index;
        }
        return -1;
    }

    private static List<LogicalType> lookupTypes(List<StateSourceField> fields) {
        List<LogicalType> types = new ArrayList<LogicalType>(fields.size());
        for (StateSourceField field : fields) {
            types.add(
                    LogicalTypeParser.parse(
                            field.logicalType(),
                            CobbleStateTableReadProvider.class.getClassLoader()));
        }
        return types;
    }

    private static RowData.FieldGetter[] lookupFieldGetters(
            List<LogicalType> types, int[] positions) {
        if (positions == null) return new RowData.FieldGetter[0];
        RowData.FieldGetter[] getters = new RowData.FieldGetter[types.size()];
        for (int index = 0; index < getters.length; index++) {
            getters[index] = RowData.createFieldGetter(types.get(index), positions[index]);
        }
        return getters;
    }

    private void validateLookupContract(List<DataField> keyFields) {
        if (lookupKeyPositions == null) return;
        List<StateSourceField> required = sourceConfig.lookupKeyContract().requiredFields();
        if (required.size() != keyFields.size() || required.size() != lookupKeyPositions.length) {
            throw new IllegalArgumentException(
                    "state reader exact lookup key does not match source contract");
        }
        for (int index = 0; index < required.size(); index++) {
            if (!required.get(index).name().equals(keyFields.get(index).name())) {
                throw new IllegalArgumentException(
                        "state reader exact lookup key order does not match source contract");
            }
        }
    }
}
