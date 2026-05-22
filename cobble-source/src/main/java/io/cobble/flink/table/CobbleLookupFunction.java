package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.Reader;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Lookup function that resolves Cobble rows by PRIMARY KEY from a configured snapshot. */
public final class CobbleLookupFunction extends LookupFunction {

    private final CobbleDynamicTableSource.SerializableConfig config;
    private final int[] lookupKeyPositions;
    private transient RuntimeLookupKeyEncoder keyEncoder;
    private transient CobbleRowDataDecoders.RuntimeRowDecoder rowDecoder;
    private transient Reader reader;
    private transient int totalBuckets;

    CobbleLookupFunction(
            CobbleDynamicTableSource.SerializableConfig config, int[] lookupKeyPositions) {
        this.config = config;
        this.lookupKeyPositions = Arrays.copyOf(lookupKeyPositions, lookupKeyPositions.length);
    }

    @Override
    public void open(FunctionContext context) {
        this.keyEncoder = new RuntimeLookupKeyEncoder(config.keyFields, lookupKeyPositions);
        this.rowDecoder = new CobbleRowDataDecoders.RuntimeRowDecoder(config);
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        if (!ensureReaderLoaded()) {
            return Collections.emptyList();
        }

        if (config.isStreamingLatest()) {
            reader.refresh();
            totalBuckets = reader.currentGlobalSnapshot().totalBuckets;
        }

        byte[] encodedKey = keyEncoder.encode(keyRow);
        int bucket = hashFixedBucket(encodedKey, totalBuckets);
        byte[][] columns = reader.get(bucket, encodedKey);
        if (columns == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(rowDecoder.decode(encodedKey, columns));
    }

    @Override
    public void close() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    private boolean ensureReaderLoaded() throws IOException {
        if (reader != null) {
            return true;
        }
        GlobalSnapshot initialSnapshot = CobbleSourceRuntime.loadConfiguredSnapshot(config);
        if (initialSnapshot == null) {
            return false;
        }
        this.totalBuckets = initialSnapshot.totalBuckets;
        Config readerConfig = CobbleSourceRuntime.createLookupReaderConfig(config, totalBuckets);
        if (config.isStreamingLatest()) {
            this.reader = Reader.openCurrent(readerConfig);
        } else {
            this.reader = Reader.open(readerConfig, initialSnapshot.id);
        }
        return true;
    }

    private static int hashFixedBucket(byte[] encodedKey, int totalBuckets) {
        return Math.floorMod(Arrays.hashCode(encodedKey), totalBuckets);
    }

    private static final class RuntimeLookupKeyEncoder {
        private final List<RuntimeLookupFieldEncoder> encoders;
        private final DataOutputSerializer keyOutput;

        private RuntimeLookupKeyEncoder(
                List<CobbleDynamicTableSource.SerializableField> keyFields,
                int[] lookupKeyPositions) {
            this.encoders = new ArrayList<>(keyFields.size());
            for (int i = 0; i < keyFields.size(); i++) {
                this.encoders.add(
                        new RuntimeLookupFieldEncoder(keyFields.get(i), lookupKeyPositions[i]));
            }
            this.keyOutput = new DataOutputSerializer(128);
        }

        private byte[] encode(RowData row) throws IOException {
            keyOutput.clear();
            for (RuntimeLookupFieldEncoder encoder : encoders) {
                byte[] encoded = encoder.encodeRequired(row);
                keyOutput.writeInt(encoded.length);
                keyOutput.write(encoded);
            }
            return keyOutput.getCopyOfBuffer();
        }
    }

    private static final class RuntimeLookupFieldEncoder {
        private final String name;
        private final TypeSerializer<Object> serializer;
        private final RowData.FieldGetter fieldGetter;
        private final DataOutputSerializer valueOutput;

        private RuntimeLookupFieldEncoder(
                CobbleDynamicTableSource.SerializableField field, int lookupKeyPosition) {
            this.name = field.name;
            LogicalType logicalType = LogicalTypeParser.parse(field.logicalType);
            this.serializer = InternalSerializers.create(logicalType);
            this.fieldGetter = RowData.createFieldGetter(logicalType, lookupKeyPosition);
            this.valueOutput = new DataOutputSerializer(64);
        }

        private byte[] encodeRequired(RowData row) throws IOException {
            Object value = fieldGetter.getFieldOrNull(row);
            if (value == null) {
                throw new IllegalArgumentException(
                        "Lookup key column " + name + " must not be null.");
            }
            valueOutput.clear();
            serializer.serialize(value, valueOutput);
            return valueOutput.getCopyOfBuffer();
        }
    }
}
