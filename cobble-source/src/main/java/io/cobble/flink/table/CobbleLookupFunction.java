package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.ReadOptions;
import io.cobble.Reader;
import io.cobble.flink.common.CobbleConnectorMetrics;
import io.cobble.flink.common.CobbleLoader;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.BucketHash;
import io.cobble.table.KeyCodec;
import io.cobble.table.Value;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
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
    private transient ReadOptions readOptions;
    private transient int totalBuckets;
    private transient CobbleConnectorMetrics.LookupMetrics metrics;

    CobbleLookupFunction(
            CobbleDynamicTableSource.SerializableConfig config, int[] lookupKeyPositions) {
        this.config = config;
        this.lookupKeyPositions = Arrays.copyOf(lookupKeyPositions, lookupKeyPositions.length);
    }

    @Override
    public void open(FunctionContext context) {
        this.keyEncoder = new RuntimeLookupKeyEncoder(config.keyFields, lookupKeyPositions);
        this.rowDecoder = new CobbleRowDataDecoders.RuntimeRowDecoder(config);
        this.metrics = lookupMetrics(context);
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        metrics.request();
        try {
            if (!ensureReaderLoaded()) {
                metrics.miss();
                return Collections.emptyList();
            }
            if (config.isStreamingLatest()) {
                reader.refresh();
                totalBuckets = reader.currentGlobalSnapshot().totalBuckets;
            }
            byte[] encodedKey = keyEncoder.encode(keyRow);
            int bucket = hashFixedBucket(encodedKey, totalBuckets);
            byte[][] columns = reader.getWithOptions(bucket, encodedKey, readOptions);
            if (columns == null) {
                metrics.miss();
                return Collections.emptyList();
            }
            RowData decoded = rowDecoder.decode(encodedKey, columns);
            metrics.hit(encodedKey, columns);
            return Collections.singletonList(decoded);
        } catch (IOException e) {
            metrics.error();
            throw e;
        } catch (RuntimeException e) {
            metrics.error();
            throw e;
        }
    }

    @Override
    public void close() {
        if (readOptions != null) {
            readOptions.close();
            readOptions = null;
        }
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    private boolean ensureReaderLoaded() throws IOException {
        if (reader != null) {
            return true;
        }
        CobbleLoader.ensureCobbleLoaded();
        GlobalSnapshot initialSnapshot = CobbleSourceRuntime.loadConfiguredSnapshot(config);
        if (initialSnapshot == null) {
            return false;
        }
        this.totalBuckets = initialSnapshot.totalBuckets;
        Config readerConfig = CobbleSourceRuntime.createLookupReaderConfig(config, totalBuckets);
        Reader openedReader =
                config.isStreamingLatest()
                        ? Reader.openCurrent(readerConfig)
                        : Reader.open(readerConfig, initialSnapshot.id);
        try {
            int[] columns = config.projectedColumnIndexes();
            this.readOptions =
                    ReadOptions.forColumnsInFamily(CobbleTableRowConverter.TABLE_NAME, columns);
            this.reader = openedReader;
        } catch (RuntimeException | LinkageError e) {
            openedReader.close();
            throw e;
        }
        return true;
    }

    private static int hashFixedBucket(byte[] encodedKey, int totalBuckets) {
        return new BucketHash(totalBuckets).bucket(encodedKey);
    }

    private static CobbleConnectorMetrics.LookupMetrics lookupMetrics(FunctionContext context) {
        return CobbleConnectorMetrics.lookup(context == null ? null : context.getMetricGroup());
    }

    private static final class RuntimeLookupKeyEncoder {
        private final List<RuntimeLookupFieldEncoder> encoders;
        private final List<io.cobble.table.LogicalType> keyTypes;

        private RuntimeLookupKeyEncoder(
                List<CobbleDynamicTableSource.SerializableField> keyFields,
                int[] lookupKeyPositions) {
            this.encoders = new ArrayList<>(keyFields.size());
            this.keyTypes = new ArrayList<io.cobble.table.LogicalType>(keyFields.size());
            for (int i = 0; i < keyFields.size(); i++) {
                LogicalType type =
                        LogicalTypeParser.parse(
                                keyFields.get(i).logicalType,
                                CobbleLookupFunction.class.getClassLoader());
                this.encoders.add(
                        new RuntimeLookupFieldEncoder(
                                keyFields.get(i), lookupKeyPositions[i], type));
                this.keyTypes.add(CobbleTableRowConverter.toCobbleType(type.copy(false)));
            }
        }

        private byte[] encode(RowData row) {
            List<Value> values = new ArrayList<Value>(encoders.size());
            for (RuntimeLookupFieldEncoder encoder : encoders) {
                values.add(encoder.encodeRequired(row));
            }
            return KeyCodec.encode(keyTypes, values);
        }
    }

    private static final class RuntimeLookupFieldEncoder {
        private final String name;
        private final LogicalType logicalType;
        private final RowData.FieldGetter fieldGetter;

        private RuntimeLookupFieldEncoder(
                CobbleDynamicTableSource.SerializableField field,
                int lookupKeyPosition,
                LogicalType logicalType) {
            this.name = field.name;
            this.logicalType = logicalType;
            this.fieldGetter = RowData.createFieldGetter(logicalType, lookupKeyPosition);
        }

        private Value encodeRequired(RowData row) {
            Object value = fieldGetter.getFieldOrNull(row);
            if (value == null) {
                throw new IllegalArgumentException(
                        "Lookup key column " + name + " must not be null.");
            }
            return CobbleTableRowConverter.toValue(logicalType, value);
        }
    }
}
