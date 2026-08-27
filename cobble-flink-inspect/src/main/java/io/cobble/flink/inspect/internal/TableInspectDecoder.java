package io.cobble.flink.inspect.internal;

import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.BucketHash;
import io.cobble.table.KeyCodec;
import io.cobble.table.ListType;
import io.cobble.table.LogicalType;
import io.cobble.table.MapType;
import io.cobble.table.StructType;
import io.cobble.table.Value;
import io.cobble.table.ValueCodec;

import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Decodes and encodes native Cobble Table rows for inspection. */
final class TableInspectDecoder {

    private TableInspectDecoder() {}

    static byte[] encodeKeyPrefix(InspectTarget target, List<String> values) throws IOException {
        TableInspectSchema schema = requireSchema(target);
        if (values == null || values.isEmpty()) {
            return new byte[0];
        }
        if (values.size() > schema.keyFields.size()) {
            throw new IOException(
                    "Too many table key values (expected at most " + schema.keyFields.size() + ")");
        }
        List<LogicalType> types = new ArrayList<LogicalType>(values.size());
        List<Value> encoded = new ArrayList<Value>(values.size());
        for (int index = 0; index < values.size(); index++) {
            TableInspectSchema.Field field = schema.keyFields.get(index);
            try {
                org.apache.flink.table.types.logical.LogicalType flinkType =
                        LogicalTypeParser.parse(
                                field.logicalType(), TableInspectDecoder.class.getClassLoader());
                Object parsed = InspectFieldInputParser.parse(flinkType, values.get(index));
                types.add(field.type());
                encoded.add(CobbleTableRowConverter.toValue(flinkType, parsed));
            } catch (Exception error) {
                throw new IOException(
                        "Failed to encode table key field "
                                + field.name()
                                + " ("
                                + field.logicalType()
                                + "): "
                                + message(error),
                        error);
            }
        }
        return KeyCodec.encode(types, encoded);
    }

    static int bucket(InspectTarget target, byte[] key, int totalBuckets) throws IOException {
        requireSchema(target);
        return new BucketHash(totalBuckets).bucket(key);
    }

    static DecodedRow decode(InspectTarget target, byte[] key, byte[][] columns, int[] projection) {
        if (target == null || target.tableSchema == null) {
            return DecodedRow.empty();
        }
        DecodedPart keyPart = decodeKey(target.tableSchema, key);
        DecodedPart columnPart = decodeColumns(target.tableSchema, columns, projection);
        return new DecodedRow(
                keyPart.fields,
                columnPart.fields,
                appendError(keyPart.decodeError, columnPart.decodeError));
    }

    private static DecodedPart decodeKey(TableInspectSchema schema, byte[] key) {
        List<Map<String, Object>> output = new ArrayList<Map<String, Object>>();
        if (key == null) {
            return new DecodedPart(output, "Missing table key bytes");
        }
        try {
            List<LogicalType> types = new ArrayList<LogicalType>(schema.keyFields.size());
            for (TableInspectSchema.Field field : schema.keyFields) {
                types.add(field.type());
            }
            List<Value> values = KeyCodec.decode(types, ByteBuffer.wrap(key));
            for (int index = 0; index < values.size(); index++) {
                TableInspectSchema.Field field = schema.keyFields.get(index);
                output.add(fieldToJson(field, null, render(field.type(), values.get(index)), null));
            }
            return new DecodedPart(output, null);
        } catch (RuntimeException error) {
            return new DecodedPart(output, "Failed to decode table key: " + message(error));
        }
    }

    private static DecodedPart decodeColumns(
            TableInspectSchema schema, byte[][] columns, int[] projection) {
        List<Map<String, Object>> output = new ArrayList<Map<String, Object>>();
        String decodeError = null;
        for (TableInspectSchema.Field field : schema.valueFields) {
            int position = columnPosition(field.valueColumnIndex, projection);
            if (position < 0) {
                continue;
            }
            byte[] bytes = columns == null || position >= columns.length ? null : columns[position];
            Object value = null;
            try {
                if (bytes == null) {
                    throw new IllegalArgumentException(
                            "Missing table value column " + field.valueColumnIndex);
                }
                Value decoded = ValueCodec.decodeOwned(field.type(), ByteBuffer.wrap(bytes));
                value = render(field.type(), decoded);
            } catch (RuntimeException error) {
                decodeError =
                        appendError(
                                decodeError,
                                "Failed to decode table field "
                                        + field.name()
                                        + ": "
                                        + message(error));
                value = bytes == null ? null : InspectJsonValues.bytesJson(bytes);
            }
            output.add(fieldToJson(field, Integer.valueOf(field.valueColumnIndex), value, bytes));
        }
        return new DecodedPart(output, decodeError);
    }

    private static int columnPosition(int valueColumnIndex, int[] projection) {
        if (projection == null) {
            return valueColumnIndex;
        }
        for (int index = 0; index < projection.length; index++) {
            if (projection[index] == valueColumnIndex) {
                return index;
            }
        }
        return -1;
    }

    private static Object render(LogicalType type, Value value) {
        if (value.kind() == Value.Kind.NULL) {
            return null;
        }
        switch (type.kind()) {
            case INT64:
                return DisplayLong.forJson((Long) value.raw());
            case DECIMAL:
                Value.Decimal decimal = (Value.Decimal) value.raw();
                return new BigDecimal(decimal.unscaled, decimal.scale);
            case DATE:
                return LocalDate.ofEpochDay(((Integer) value.raw()).longValue()).toString();
            case TIME:
                return LocalTime.ofNanoOfDay(((Long) value.raw()).longValue()).toString();
            case TIMESTAMP:
                Value.Timestamp timestamp = (Value.Timestamp) value.raw();
                Instant instant = Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos);
                return timestamp.kind == io.cobble.table.TimestampKind.WITH_LOCAL_TIME_ZONE
                        ? instant.toString()
                        : instant.atOffset(ZoneOffset.UTC).toLocalDateTime().toString();
            case BINARY:
                ByteBuffer buffer = ((ByteBuffer) value.raw()).duplicate();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                return InspectJsonValues.bytesJson(bytes);
            case LIST:
                return list((ListType) type, castValues(value.raw()));
            case MAP:
                return map((MapType) type, castEntries(value.raw()));
            case STRUCT:
                return struct((StructType) type, castValues(value.raw()));
            case EXTENSION:
                Value.Extension extension = (Value.Extension) value.raw();
                Map<String, Object> extended = new LinkedHashMap<String, Object>();
                extended.put("type_id", extension.typeId);
                extended.put("value", extension.value.raw());
                return extended;
            default:
                return value.raw();
        }
    }

    private static Map<String, Object> list(ListType type, List<Value> values) {
        List<Object> elements = new ArrayList<Object>(values.size());
        for (Value value : values) {
            elements.add(render(type.elementType(), value));
        }
        Map<String, Object> output = new LinkedHashMap<String, Object>();
        output.put("kind", "LIST");
        output.put("elements", elements);
        return output;
    }

    private static Map<String, Object> map(MapType type, List<Map.Entry<Value, Value>> values) {
        List<Map<String, Object>> entries = new ArrayList<Map<String, Object>>(values.size());
        for (Map.Entry<Value, Value> value : values) {
            Map<String, Object> entry = new LinkedHashMap<String, Object>();
            entry.put("key", render(type.keyType(), value.getKey()));
            entry.put("value", render(type.valueType(), value.getValue()));
            entries.add(entry);
        }
        Map<String, Object> output = new LinkedHashMap<String, Object>();
        output.put("kind", "MAP");
        output.put("entries", entries);
        return output;
    }

    private static Map<String, Object> struct(StructType type, List<Value> values) {
        List<Map<String, Object>> fields = new ArrayList<Map<String, Object>>(values.size());
        for (int index = 0; index < values.size(); index++) {
            io.cobble.table.DataField field = type.recordType().fields().get(index);
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("name", field.name());
            item.put("value", render(field.logicalType(), values.get(index)));
            fields.add(item);
        }
        Map<String, Object> output = new LinkedHashMap<String, Object>();
        output.put("kind", "ROW");
        output.put("fields", fields);
        return output;
    }

    @SuppressWarnings("unchecked")
    private static List<Value> castValues(Object value) {
        return (List<Value>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Map.Entry<Value, Value>> castEntries(Object value) {
        return (List<Map.Entry<Value, Value>>) value;
    }

    private static Map<String, Object> fieldToJson(
            TableInspectSchema.Field field, Integer index, Object value, byte[] rawBytes) {
        Map<String, Object> output = new LinkedHashMap<String, Object>();
        if (index != null) {
            output.put("index", index);
        }
        output.put("name", field.name());
        output.put("logical_type", field.logicalType());
        output.put("value", value);
        if (value instanceof Map && rawBytes != null) {
            output.put("raw_b64", Base64.getEncoder().encodeToString(rawBytes));
        }
        return output;
    }

    private static TableInspectSchema requireSchema(InspectTarget target) throws IOException {
        if (target == null || target.tableSchema == null) {
            throw new IOException("Cobble Table schema is not available for key filtering");
        }
        return target.tableSchema;
    }

    private static String appendError(String existing, String next) {
        if (next == null || next.isEmpty()) {
            return existing;
        }
        return existing == null || existing.isEmpty() ? next : existing + "; " + next;
    }

    private static String message(Throwable error) {
        String message = error.getMessage();
        return message == null || message.isEmpty() ? error.getClass().getName() : message;
    }

    static final class DecodedRow {
        final List<Map<String, Object>> decodedKey;
        final List<Map<String, Object>> decodedColumns;
        final String decodeError;

        private DecodedRow(
                List<Map<String, Object>> decodedKey,
                List<Map<String, Object>> decodedColumns,
                String decodeError) {
            this.decodedKey = decodedKey;
            this.decodedColumns = decodedColumns;
            this.decodeError = decodeError;
        }

        private static DecodedRow empty() {
            return new DecodedRow(null, null, null);
        }
    }

    private static final class DecodedPart {
        private final List<Map<String, Object>> fields;
        private final String decodeError;

        private DecodedPart(List<Map<String, Object>> fields, String decodeError) {
            this.fields = fields;
            this.decodeError = decodeError;
        }
    }
}
