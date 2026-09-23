package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.flink.common.CobbleStateDescriptor;
import io.cobble.flink.common.CobbleStateReadFormatMetadata;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateKind;
import io.cobble.flink.common.inspect.StateSourceSchemaLayout;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.DataField;
import io.cobble.table.TableFormatBinding;
import io.cobble.table.TableFormatPlugin;
import io.cobble.table.TablePathRequest;
import io.cobble.table.TablePhysicalKey;
import io.cobble.table.TableReadCapabilities;
import io.cobble.table.TableReadSchema;
import io.cobble.table.TableReadSnapshot;
import io.cobble.table.Value;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Reads snapshot-described Flink keyed state through Cobble's Java table format SPI. */
public final class CobbleStateTableFormatPlugin implements TableFormatPlugin {

    @Override
    public String formatId() {
        return CobbleStateReadFormatMetadata.FORMAT_ID;
    }

    @Override
    public TableFormatBinding bind(TableReadSnapshot snapshot) throws IOException {
        if (!formatId().equals(snapshot.formatId())) {
            throw new IOException("State reader cannot bind format '" + snapshot.formatId() + "'");
        }
        GlobalSnapshot global = snapshot.globalSnapshot();
        if (global == null) {
            throw new IOException("State table reads require a fixed global snapshot");
        }
        List<ShardBinding> shardBindings = new ArrayList<ShardBinding>();
        Layout expected = null;
        for (TableReadSnapshot.ShardDescriptor shard : snapshot.shards()) {
            Layout layout = layout(snapshot.columnFamily(), shard.metadataJson());
            if (expected != null
                    && !expected.readSchema.fields().equals(layout.readSchema.fields())) {
                throw new IOException(
                        "State schema differs between snapshot shards for '"
                                + snapshot.columnFamily()
                                + "'");
            }
            expected = layout;
            shardBindings.add(new ShardBinding(shard.snapshot().ranges, layout));
        }
        if (expected == null) throw new IOException("State snapshot has no readable shards");
        return new StateBinding(
                snapshot.columnFamily(), global.totalBuckets, expected, shardBindings);
    }

    @Override
    public Optional<TableReadSnapshot> resolvePath(Config config, TablePathRequest request)
            throws Exception {
        return CobbleEmbeddedCheckpointReadPlanner.resolve(config, request);
    }

    private static Layout layout(String columnFamily, String json) throws IOException {
        CobbleStateReadFormatMetadata.Decoded metadata = CobbleStateReadFormatMetadata.decode(json);
        if (metadata.stateKind() == CobbleStateDescriptor.StateKind.TIMER) {
            throw new IOException("Timer state table scans are not supported");
        }
        CobbleStateDescriptor expected =
                CobbleStateDescriptor.forKeyValue(
                        metadata.stateName(), columnFamily, metadata.stateKind());
        if (!columnFamily.equals(metadata.columnFamily())
                || metadata.rowKeyEncoding() != expected.rowKeyEncoding()
                || metadata.rowValueEncoding() != expected.rowValueEncoding()
                || metadata.rowKeyFormatVersion() != expected.rowKeyFormatVersion()
                || metadata.rowValueFormatVersion() != expected.rowValueFormatVersion()) {
            throw new IOException("Unsupported physical state format for '" + columnFamily + "'");
        }
        if (metadata.schemaStore().schemas().size() != 1) {
            throw new IOException("State format metadata must describe exactly one state");
        }
        StateInspectSchema schema = metadata.schemaStore().schemas().get(0);
        if (!columnFamily.equals(schema.columnFamily())
                || !metadata.stateName().equals(schema.stateName())
                || schema.stateKind() != StateKind.valueOf(metadata.stateKind().name())) {
            throw new IOException("State inspect schema does not match its physical descriptor");
        }
        StateInspectSemanticSchema semantic =
                metadata.schemaStore().semanticSchema(schema.stateName());
        try {
            List<StateSourceField> fields = new ArrayList<>();
            List<String> names = new ArrayList<>();
            List<String> types = new ArrayList<>();
            for (StateSourceSchemaLayout.Field field :
                    StateSourceSchemaLayout.derive(schema, semantic)) {
                fields.add(
                        new StateSourceField(
                                field.name(),
                                field.logicalType(),
                                StateSourceField.Group.valueOf(field.group().name()),
                                field.groupFieldIndex()));
                names.add(field.name());
                types.add(field.logicalType());
            }
            RowType rowType = CobbleTableRowConverter.parseRowType(names, types);
            List<DataField> readFields = new ArrayList<>();
            for (int i = 0; i < fields.size(); i++) {
                readFields.add(
                        new DataField(
                                i,
                                names.get(i),
                                CobbleTableRowConverter.toCobbleType(rowType.getTypeAt(i))));
            }
            return new Layout(schema, semantic, fields, rowType, new TableReadSchema(readFields));
        } catch (IllegalArgumentException error) {
            throw new IOException(
                    "Cannot derive readable columns for state '" + columnFamily + "'", error);
        }
    }

    private static final class Layout {
        private final StateInspectSchema schema;
        private final StateInspectSemanticSchema semantic;
        private final List<StateSourceField> fields;
        private final RowType rowType;
        private final TableReadSchema readSchema;

        private Layout(
                StateInspectSchema schema,
                StateInspectSemanticSchema semantic,
                List<StateSourceField> fields,
                RowType rowType,
                TableReadSchema readSchema) {
            this.schema = schema;
            this.semantic = semantic;
            this.fields = fields;
            this.rowType = rowType;
            this.readSchema = readSchema;
        }
    }

    /** Session-private Flink serializer binding over the generic physical reader. */
    private static final class StateBinding implements TableFormatBinding {
        private final Layout layout;
        private final int totalBuckets;
        private final List<StateSourceField> lookupFields;
        private final List<DataField> keyFields;
        private final TableReadCapabilities capabilities;
        private final List<ShardBinding> shardBindings;
        private final ShardBinding[] bucketBindings;
        private final ShardBinding lookupRoutingBinding;
        private final boolean sharedLookupEncoding;

        private StateBinding(
                String columnFamily,
                int totalBuckets,
                Layout layout,
                List<ShardBinding> shardBindings)
                throws IOException {
            this.layout = layout;
            this.totalBuckets = totalBuckets;
            this.lookupFields = lookupFields(layout);
            this.capabilities = new TableReadCapabilities(true, !lookupFields.isEmpty(), true);
            this.keyFields = lookupKeyFields(layout.readSchema, lookupFields);
            this.shardBindings =
                    Collections.unmodifiableList(new ArrayList<ShardBinding>(shardBindings));
            for (ShardBinding shard : this.shardBindings) {
                shard.initializeDecoder(columnFamily);
            }
            this.bucketBindings = bucketBindings(totalBuckets, this.shardBindings);
            this.lookupRoutingBinding = this.shardBindings.get(0);
            this.sharedLookupEncoding = hasSharedLookupEncoding(this.shardBindings);
        }

        @Override
        public TableReadSchema schema() {
            return layout.readSchema;
        }

        @Override
        public List<DataField> keyFields() {
            return keyFields;
        }

        @Override
        public TableReadCapabilities capabilities() {
            return capabilities;
        }

        @Override
        public int[] physicalColumns() {
            return new int[] {0};
        }

        @Override
        public TablePhysicalKey encodeKey(List<Value> values) throws Exception {
            if (lookupFields.isEmpty() || values.size() != lookupFields.size()) {
                throw new IllegalArgumentException(
                        "state exact lookup requires " + lookupFields.size() + " full key values");
            }
            if (sharedLookupEncoding) {
                CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                        lookupRoutingBinding.encode(values, lookupFields, totalBuckets);
                return new TablePhysicalKey(encoded.keyGroup(), encoded.rowKey());
            }
            CobbleStateLookupKeyEncoder.EncodedStateLookupKey fallback = null;
            for (ShardBinding shard : shardBindings) {
                CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                        shard.encode(values, lookupFields, totalBuckets);
                if (shard.owns(encoded.keyGroup())) {
                    return new TablePhysicalKey(encoded.keyGroup(), encoded.rowKey());
                }
                fallback = encoded;
            }
            // A key group outside this state family's selected shards is a valid fixed-snapshot
            // miss. The common physical session recognizes the unowned bucket before opening it.
            return new TablePhysicalKey(fallback.keyGroup(), fallback.rowKey());
        }

        @Override
        public List<List<Value>> decode(int bucket, byte[] key, byte[][] columns) throws Exception {
            ShardBinding shard = shardForBucket(bucket);
            List<RowData> rows = shard.decoder.decode(key, columns, "physical", bucket);
            List<List<Value>> values = new ArrayList<List<Value>>(rows.size());
            for (RowData row : rows) values.add(shard.rowConverter.toValues(row));
            return values;
        }

        @Override
        public boolean isMissingColumnFamily(RuntimeException error) {
            return CobbleStateRowDecoder.isUnknownColumnFamily(error);
        }

        private ShardBinding shardForBucket(int bucket) throws IOException {
            if (bucket >= 0 && bucket < bucketBindings.length && bucketBindings[bucket] != null) {
                return bucketBindings[bucket];
            }
            throw new IOException("state snapshot has no descriptor for key group " + bucket);
        }

        private static ShardBinding[] bucketBindings(
                int totalBuckets, List<ShardBinding> shardBindings) throws IOException {
            ShardBinding[] result = new ShardBinding[totalBuckets];
            for (ShardBinding shard : shardBindings) {
                for (io.cobble.ShardSnapshot.Range range : shard.ranges) {
                    if (range.start < 0 || range.end < range.start || range.end >= totalBuckets) {
                        throw new IOException(
                                "state snapshot has invalid key-group range "
                                        + range.start
                                        + ".."
                                        + range.end);
                    }
                    for (int bucket = range.start; bucket <= range.end; bucket++) {
                        // Preserve the pre-index traversal's first matching descriptor when
                        // checkpoint metadata has overlapping ranges.
                        if (result[bucket] == null) result[bucket] = shard;
                    }
                }
            }
            return result;
        }

        private static boolean hasSharedLookupEncoding(List<ShardBinding> shardBindings) {
            ShardBinding first = shardBindings.get(0);
            for (int index = 1; index < shardBindings.size(); index++) {
                ShardBinding candidate = shardBindings.get(index);
                if (!lookupEncodingCompatible(
                        first.layout.schema,
                        first.layout.semantic,
                        candidate.layout.schema,
                        candidate.layout.semantic)) {
                    return false;
                }
            }
            return true;
        }
    }

    static boolean lookupEncodingCompatible(
            StateInspectSchema firstSchema,
            StateInspectSemanticSchema firstSemantic,
            StateInspectSchema candidateSchema,
            StateInspectSemanticSchema candidateSemantic) {
        return firstSchema.equals(candidateSchema)
                && Objects.equals(firstSemantic, candidateSemantic);
    }

    private static final class ShardBinding {
        private final List<io.cobble.ShardSnapshot.Range> ranges;
        private final Layout layout;
        private CobbleStateRowDecoder decoder;
        private CobbleTableRowConverter rowConverter;
        private CobbleStateLookupKeyEncoder lookupEncoder;
        private CobbleTableRowConverter lookupConverter;

        private ShardBinding(List<io.cobble.ShardSnapshot.Range> ranges, Layout layout) {
            this.ranges = new ArrayList<io.cobble.ShardSnapshot.Range>(ranges);
            this.layout = layout;
        }

        private boolean owns(int bucket) {
            for (io.cobble.ShardSnapshot.Range range : ranges) {
                if (bucket >= range.start && bucket <= range.end) return true;
            }
            return false;
        }

        private void initializeDecoder(String columnFamily) throws IOException {
            decoder =
                    new CobbleStateRowDecoder(
                            layout.schema, layout.semantic, layout.fields, "state=" + columnFamily);
            rowConverter = new CobbleTableRowConverter(layout.rowType);
        }

        private synchronized CobbleStateLookupKeyEncoder.EncodedStateLookupKey encode(
                List<Value> values, List<StateSourceField> lookupFields, int totalBuckets)
                throws Exception {
            if (lookupEncoder == null) {
                int[] positions = new int[lookupFields.size()];
                for (int index = 0; index < positions.length; index++) positions[index] = index;
                lookupEncoder =
                        new CobbleStateLookupKeyEncoder(
                                layout.schema, layout.semantic, lookupFields, positions);
                lookupConverter = new CobbleTableRowConverter(keyRowType(lookupFields));
            }
            return lookupEncoder.encode(lookupConverter.toRowData(values), totalBuckets);
        }
    }

    private static List<StateSourceField> lookupFields(Layout layout) {
        if (layout.schema.stateKind() == StateKind.LIST
                || layout.schema.stateKind() == StateKind.TIMER) {
            return Collections.emptyList();
        }
        List<StateSourceField> fields = new ArrayList<StateSourceField>();
        for (StateSourceField field : layout.fields) {
            if (field.group() == StateSourceField.Group.STATE_KEY
                    || field.group() == StateSourceField.Group.NAMESPACE
                    || (layout.schema.stateKind() == StateKind.MAP
                            && field.group() == StateSourceField.Group.MAP_KEY)) {
                fields.add(field);
            }
        }
        return fields;
    }

    private static List<DataField> lookupKeyFields(
            TableReadSchema schema, List<StateSourceField> lookupFields) {
        List<DataField> result = new ArrayList<DataField>(lookupFields.size());
        for (StateSourceField lookupField : lookupFields) {
            for (DataField field : schema.fields()) {
                if (lookupField.name().equals(field.name())) {
                    result.add(field);
                    break;
                }
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static RowType keyRowType(List<StateSourceField> fields) {
        List<String> names = new ArrayList<String>(fields.size());
        List<String> types = new ArrayList<String>(fields.size());
        for (StateSourceField field : fields) {
            names.add(field.name());
            types.add(field.logicalType());
        }
        return CobbleTableRowConverter.parseRowType(names, types);
    }
}
