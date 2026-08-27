package io.cobble.flink.common.table;

import io.cobble.table.BucketHash;
import io.cobble.table.DataField;
import io.cobble.table.KeyCodec;
import io.cobble.table.LogicalTypes;
import io.cobble.table.RecordType;
import io.cobble.table.TableSchema;
import io.cobble.table.TimestampKind;
import io.cobble.table.Value;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Converts between Flink internal rows and Cobble's schema-directed table values. */
public final class CobbleTableRowConverter {

    public static final String TABLE_NAME = "data";

    private final RowType rowType;
    private final List<RowData.FieldGetter> fieldGetters;

    public CobbleTableRowConverter(RowType rowType) {
        this.rowType = Objects.requireNonNull(rowType, "rowType");
        this.fieldGetters = new ArrayList<RowData.FieldGetter>(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            fieldGetters.add(RowData.createFieldGetter(rowType.getTypeAt(i), i));
        }
    }

    public static RowType parseRowType(List<String> fieldNames, List<String> logicalTypes) {
        if (fieldNames.size() != logicalTypes.size()) {
            throw new IllegalArgumentException(
                    "field names and logical types must have equal size");
        }
        List<RowType.RowField> fields = new ArrayList<RowType.RowField>(fieldNames.size());
        for (int i = 0; i < fieldNames.size(); i++) {
            fields.add(
                    new RowType.RowField(
                            fieldNames.get(i),
                            LogicalTypeParser.parse(
                                    logicalTypes.get(i),
                                    CobbleTableRowConverter.class.getClassLoader())));
        }
        return new RowType(false, fields);
    }

    public static TableSchema toTableSchema(RowType rowType, List<String> primaryKeyColumns) {
        Objects.requireNonNull(rowType, "rowType");
        if (primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
            throw new IllegalArgumentException("Cobble tables require a primary key");
        }
        long[] nextNestedId = new long[] {rowType.getFieldCount()};
        Set<String> primaryKeyNames = new HashSet<String>(primaryKeyColumns);
        List<DataField> fields = new ArrayList<DataField>(rowType.getFieldCount());
        Map<String, Long> idsByName = new LinkedHashMap<String, Long>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            RowType.RowField field = rowType.getFields().get(i);
            long id = i;
            fields.add(
                    new DataField(
                            id,
                            field.getName(),
                            toCobbleType(
                                    primaryKeyNames.contains(field.getName())
                                            ? field.getType().copy(false)
                                            : field.getType(),
                                    nextNestedId)));
            idsByName.put(field.getName(), Long.valueOf(id));
        }
        List<Long> primaryKey = new ArrayList<Long>(primaryKeyColumns.size());
        for (String column : primaryKeyColumns) {
            Long id = idsByName.get(column);
            if (id == null) {
                throw new IllegalArgumentException(
                        "primary key column is not a physical field: " + column);
            }
            primaryKey.add(id);
        }
        return new TableSchema(fields, primaryKey, primaryKey);
    }

    public List<Value> toValues(RowData row) {
        List<Value> values = new ArrayList<Value>(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            Object value = fieldGetters.get(i).getFieldOrNull(row);
            values.add(toValue(rowType.getTypeAt(i), value));
        }
        return values;
    }

    public RowData toRowData(List<Value> values) {
        if (values.size() != rowType.getFieldCount()) {
            throw new IllegalArgumentException("table row does not match the Flink row arity");
        }
        GenericRowData row = new GenericRowData(values.size());
        for (int i = 0; i < values.size(); i++) {
            row.setField(i, toInternal(rowType.getTypeAt(i), values.get(i)));
        }
        return row;
    }

    public static int bucket(TableSchema schema, List<Value> row, int totalBuckets) {
        Map<Long, Integer> positionsById = new LinkedHashMap<Long, Integer>();
        for (int i = 0; i < schema.fields().size(); i++) {
            positionsById.put(Long.valueOf(schema.fields().get(i).id()), Integer.valueOf(i));
        }
        List<io.cobble.table.LogicalType> keyTypes =
                new ArrayList<io.cobble.table.LogicalType>(schema.bucketKey().size());
        List<Value> keyValues = new ArrayList<Value>(schema.bucketKey().size());
        for (Long fieldId : schema.bucketKey()) {
            Integer position = positionsById.get(fieldId);
            if (position == null) {
                throw new IllegalArgumentException("bucket key field is missing from table schema");
            }
            keyTypes.add(schema.fields().get(position.intValue()).logicalType());
            keyValues.add(row.get(position.intValue()));
        }
        return new BucketHash(totalBuckets).bucket(KeyCodec.encode(keyTypes, keyValues));
    }

    public static io.cobble.table.LogicalType toCobbleType(
            org.apache.flink.table.types.logical.LogicalType type) {
        return toCobbleType(type, new long[] {0L});
    }

    private static io.cobble.table.LogicalType toCobbleType(
            org.apache.flink.table.types.logical.LogicalType type, long[] nextFieldId) {
        io.cobble.table.LogicalType converted;
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                converted = LogicalTypes.bool();
                break;
            case TINYINT:
                converted = LogicalTypes.int8();
                break;
            case SMALLINT:
                converted = LogicalTypes.int16();
                break;
            case INTEGER:
                converted = LogicalTypes.int32();
                break;
            case BIGINT:
                converted = LogicalTypes.int64();
                break;
            case FLOAT:
                converted = LogicalTypes.float32();
                break;
            case DOUBLE:
                converted = LogicalTypes.float64();
                break;
            case DECIMAL:
                DecimalType decimal = (DecimalType) type;
                converted = LogicalTypes.decimal(decimal.getPrecision(), decimal.getScale());
                break;
            case DATE:
                converted = LogicalTypes.date();
                break;
            case TIME_WITHOUT_TIME_ZONE:
                converted = LogicalTypes.time(((TimeType) type).getPrecision());
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                converted =
                        LogicalTypes.timestamp(
                                ((TimestampType) type).getPrecision(),
                                TimestampKind.WITHOUT_TIME_ZONE);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                converted =
                        LogicalTypes.timestamp(
                                ((LocalZonedTimestampType) type).getPrecision(),
                                TimestampKind.WITH_LOCAL_TIME_ZONE);
                break;
            case CHAR:
            case VARCHAR:
                converted = LogicalTypes.string();
                break;
            case BINARY:
            case VARBINARY:
                converted = LogicalTypes.binary();
                break;
            case ARRAY:
                converted =
                        LogicalTypes.list(
                                toCobbleType(((ArrayType) type).getElementType(), nextFieldId));
                break;
            case MAP:
                org.apache.flink.table.types.logical.MapType mapType =
                        (org.apache.flink.table.types.logical.MapType) type;
                converted =
                        LogicalTypes.map(
                                toCobbleType(mapType.getKeyType().copy(false), nextFieldId),
                                toCobbleType(mapType.getValueType(), nextFieldId));
                break;
            case ROW:
                RowType nestedRow = (RowType) type;
                List<DataField> nestedFields = new ArrayList<DataField>(nestedRow.getFieldCount());
                for (RowType.RowField field : nestedRow.getFields()) {
                    nestedFields.add(
                            new DataField(
                                    nextFieldId[0]++,
                                    field.getName(),
                                    toCobbleType(field.getType(), nextFieldId)));
                }
                converted = LogicalTypes.struct(new RecordType(nestedFields));
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Flink table type: " + type.asSummaryString());
        }
        return type.isNullable() ? converted.nullable() : converted.notNull();
    }

    public static Value toValue(
            org.apache.flink.table.types.logical.LogicalType type, Object value) {
        if (value == null) {
            return Value.nullValue();
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Value.bool((Boolean) value);
            case TINYINT:
                return Value.int8((Byte) value);
            case SMALLINT:
                return Value.int16((Short) value);
            case INTEGER:
                return Value.int32((Integer) value);
            case BIGINT:
                return Value.int64((Long) value);
            case FLOAT:
                return Value.float32((Float) value);
            case DOUBLE:
                return Value.float64((Double) value);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                DecimalData decimalData = (DecimalData) value;
                return Value.decimal(
                        decimalType.getPrecision(),
                        decimalType.getScale(),
                        decimalData.toBigDecimal().unscaledValue());
            case DATE:
                return Value.date((Integer) value);
            case TIME_WITHOUT_TIME_ZONE:
                return Value.time(((Integer) value).longValue() * 1_000_000L);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return timestampValue(
                        (TimestampData) value,
                        ((TimestampType) type).getPrecision(),
                        TimestampKind.WITHOUT_TIME_ZONE);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return timestampValue(
                        (TimestampData) value,
                        ((LocalZonedTimestampType) type).getPrecision(),
                        TimestampKind.WITH_LOCAL_TIME_ZONE);
            case CHAR:
            case VARCHAR:
                return Value.string(((StringData) value).toString());
            case BINARY:
            case VARBINARY:
                return Value.binary((byte[]) value);
            case ARRAY:
                return arrayValue((ArrayType) type, (ArrayData) value);
            case MAP:
                return mapValue(
                        (org.apache.flink.table.types.logical.MapType) type, (MapData) value);
            case ROW:
                return rowValue((RowType) type, (RowData) value);
            default:
                throw new IllegalArgumentException(
                        "Unsupported Flink table type: " + type.asSummaryString());
        }
    }

    private static Value timestampValue(TimestampData value, int precision, TimestampKind kind) {
        Instant instant;
        if (kind == TimestampKind.WITH_LOCAL_TIME_ZONE) {
            instant = value.toInstant();
        } else {
            instant = value.toLocalDateTime().toInstant(ZoneOffset.UTC);
        }
        return Value.timestamp(precision, kind, instant.getEpochSecond(), instant.getNano());
    }

    private static Value arrayValue(ArrayType type, ArrayData array) {
        ArrayData.ElementGetter getter = ArrayData.createElementGetter(type.getElementType());
        List<Value> values = new ArrayList<Value>(array.size());
        for (int i = 0; i < array.size(); i++) {
            values.add(toValue(type.getElementType(), getter.getElementOrNull(array, i)));
        }
        return Value.list(values);
    }

    private static Value mapValue(org.apache.flink.table.types.logical.MapType type, MapData map) {
        ArrayData keys = map.keyArray();
        ArrayData values = map.valueArray();
        ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(type.getKeyType());
        ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(type.getValueType());
        List<Map.Entry<Value, Value>> entries = new ArrayList<Map.Entry<Value, Value>>(map.size());
        for (int i = 0; i < map.size(); i++) {
            entries.add(
                    new AbstractMap.SimpleImmutableEntry<Value, Value>(
                            toValue(type.getKeyType(), keyGetter.getElementOrNull(keys, i)),
                            toValue(type.getValueType(), valueGetter.getElementOrNull(values, i))));
        }
        return Value.map(entries);
    }

    private static Value rowValue(RowType type, RowData row) {
        List<Value> values = new ArrayList<Value>(type.getFieldCount());
        for (int i = 0; i < type.getFieldCount(); i++) {
            RowData.FieldGetter getter = RowData.createFieldGetter(type.getTypeAt(i), i);
            values.add(toValue(type.getTypeAt(i), getter.getFieldOrNull(row)));
        }
        return Value.struct(values);
    }

    private static Object toInternal(
            org.apache.flink.table.types.logical.LogicalType type, Value value) {
        if (value.kind() == Value.Kind.NULL) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
                return value.raw();
            case DECIMAL:
                Value.Decimal decimal = (Value.Decimal) value.raw();
                return DecimalData.fromBigDecimal(
                        new BigDecimal(decimal.unscaled, decimal.scale),
                        decimal.precision,
                        decimal.scale);
            case TIME_WITHOUT_TIME_ZONE:
                return Integer.valueOf((int) (((Long) value.raw()).longValue() / 1_000_000L));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                Value.Timestamp timestamp = (Value.Timestamp) value.raw();
                Instant instant = Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos);
                if (timestamp.kind == TimestampKind.WITH_LOCAL_TIME_ZONE) {
                    return TimestampData.fromInstant(instant);
                }
                return TimestampData.fromLocalDateTime(
                        LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
            case CHAR:
            case VARCHAR:
                return StringData.fromString((String) value.raw());
            case BINARY:
            case VARBINARY:
                ByteBuffer bytes = ((ByteBuffer) value.raw()).duplicate();
                byte[] binary = new byte[bytes.remaining()];
                bytes.get(binary);
                return binary;
            case ARRAY:
                return toInternalArray((ArrayType) type, value);
            case MAP:
                return toInternalMap((org.apache.flink.table.types.logical.MapType) type, value);
            case ROW:
                return toInternalRow((RowType) type, value);
            default:
                throw new IllegalArgumentException(
                        "Unsupported Flink table type: " + type.asSummaryString());
        }
    }

    @SuppressWarnings("unchecked")
    private static GenericArrayData toInternalArray(ArrayType type, Value value) {
        List<Value> values = (List<Value>) value.raw();
        Object[] converted = new Object[values.size()];
        for (int i = 0; i < values.size(); i++) {
            converted[i] = toInternal(type.getElementType(), values.get(i));
        }
        return new GenericArrayData(converted);
    }

    @SuppressWarnings("unchecked")
    private static GenericMapData toInternalMap(
            org.apache.flink.table.types.logical.MapType type, Value value) {
        List<Map.Entry<Value, Value>> values = (List<Map.Entry<Value, Value>>) value.raw();
        Map<Object, Object> converted = new LinkedHashMap<Object, Object>();
        for (Map.Entry<Value, Value> entry : values) {
            converted.put(
                    toInternal(type.getKeyType(), entry.getKey()),
                    toInternal(type.getValueType(), entry.getValue()));
        }
        return new GenericMapData(converted);
    }

    @SuppressWarnings("unchecked")
    private static GenericRowData toInternalRow(RowType type, Value value) {
        List<Value> values = (List<Value>) value.raw();
        GenericRowData row = new GenericRowData(values.size());
        for (int i = 0; i < values.size(); i++) {
            row.setField(i, toInternal(type.getTypeAt(i), values.get(i)));
        }
        return row;
    }
}
