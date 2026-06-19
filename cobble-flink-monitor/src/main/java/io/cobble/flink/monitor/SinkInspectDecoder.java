package io.cobble.flink.monitor;

import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Best-effort decoder for raw Cobble sink rows using persisted table schema metadata. */
final class SinkInspectDecoder {

    private SinkInspectDecoder() {}

    /** Encodes a leading subset of the ordered sink primary key for prefix scans. */
    static byte[] encodeKeyPrefix(InspectTarget target, List<String> values) throws IOException {
        if (target == null || target.sinkSchema == null) {
            throw new IOException("Sink schema is not available for key filtering");
        }
        List<SinkInspectField> fields = target.sinkSchema.keyFields();
        if (values == null || values.isEmpty()) {
            return new byte[0];
        }
        if (values.size() > fields.size()) {
            throw new IOException(
                    "Too many sink key values (expected at most " + fields.size() + ")");
        }
        DataOutputSerializer output = new DataOutputSerializer(128);
        for (int index = 0; index < values.size(); index++) {
            SinkInspectField field = fields.get(index);
            byte[] bytes = encodeField(field, values.get(index));
            output.writeInt(bytes.length);
            output.write(bytes);
        }
        return output.getCopyOfBuffer();
    }

    static DecodedRow decode(InspectTarget target, byte[] key, byte[][] columns, int[] projection) {
        if (target == null || target.sinkSchema == null) {
            return DecodedRow.empty();
        }
        List<Map<String, Object>> decodedKey = new ArrayList<>();
        List<Map<String, Object>> decodedColumns = new ArrayList<>();
        String decodeError = null;
        DecodedPart keyPart = decodeKey(target.sinkSchema, key);
        DecodedPart columnPart = decodeColumns(target.sinkSchema, columns, projection);
        decodeError = appendError(decodeError, keyPart.decodeError);
        decodeError = appendError(decodeError, columnPart.decodeError);
        return new DecodedRow(keyPart.fields, columnPart.fields, decodeError);
    }

    private static DecodedPart decodeKey(SinkInspectSchema schema, byte[] key) {
        List<Map<String, Object>> output = new ArrayList<>(schema.keyFields().size());
        String decodeError = null;
        if (key == null) {
            return new DecodedPart(output, "Missing sink key bytes");
        }
        DataInputDeserializer input = new DataInputDeserializer(key);
        try {
            for (SinkInspectField field : schema.keyFields()) {
                if (input.available() < Integer.BYTES) {
                    decodeError =
                            appendError(
                                    decodeError,
                                    "Sink key is truncated before field " + field.name());
                    break;
                }
                int length = input.readInt();
                if (length < 0 || input.available() < length) {
                    decodeError =
                            appendError(
                                    decodeError,
                                    "Invalid sink key field length for "
                                            + field.name()
                                            + ": "
                                            + length);
                    break;
                }
                byte[] fieldBytes = new byte[length];
                input.readFully(fieldBytes);
                try {
                    output.add(
                            fieldToJson(field, null, decodeField(field, fieldBytes), fieldBytes));
                } catch (Exception e) {
                    decodeError = appendError(decodeError, message(e));
                    output.add(fieldToJson(field, null, fallbackBytes(fieldBytes), fieldBytes));
                }
            }
            if (input.available() != 0) {
                decodeError =
                        appendError(
                                decodeError,
                                "Sink key has " + input.available() + " trailing bytes");
            }
        } catch (IOException e) {
            decodeError = appendError(decodeError, message(e));
        }
        return new DecodedPart(output, decodeError);
    }

    private static DecodedPart decodeColumns(
            SinkInspectSchema schema, byte[][] columns, int[] projection) {
        List<Map<String, Object>> output = new ArrayList<>(schema.valueFields().size());
        String decodeError = null;
        for (SinkInspectField field : schema.valueFields()) {
            int position = columnPosition(field.structuredColumnIndex(), projection);
            if (position < 0) {
                continue;
            }
            byte[] valueBytes =
                    columns == null || position >= columns.length ? null : columns[position];
            Object value;
            try {
                value = valueBytes == null ? null : decodeField(field, valueBytes);
            } catch (Exception e) {
                decodeError = appendError(decodeError, message(e));
                value = fallbackBytes(valueBytes);
            }
            output.add(
                    fieldToJson(
                            field,
                            Integer.valueOf(field.structuredColumnIndex()),
                            value,
                            valueBytes));
        }
        return new DecodedPart(output, decodeError);
    }

    private static int columnPosition(int structuredColumnIndex, int[] projection) {
        if (structuredColumnIndex < 0) {
            return -1;
        }
        if (projection == null) {
            return structuredColumnIndex;
        }
        for (int i = 0; i < projection.length; i++) {
            if (projection[i] == structuredColumnIndex) {
                return i;
            }
        }
        return -1;
    }

    private static Object decodeField(SinkInspectField field, byte[] bytes) throws IOException {
        try {
            LogicalType logicalType = LogicalTypeParser.parse(field.logicalType());
            @SuppressWarnings("unchecked")
            TypeSerializer<Object> serializer =
                    (TypeSerializer<Object>) InternalSerializers.create(logicalType);
            Object value = serializer.deserialize(new DataInputDeserializer(bytes));
            return render(value, bytes);
        } catch (Exception e) {
            throw new IOException(
                    "Failed to decode sink field "
                            + field.name()
                            + " ("
                            + field.logicalType()
                            + "): "
                            + message(e),
                    e);
        }
    }

    private static byte[] encodeField(SinkInspectField field, String text) throws IOException {
        try {
            LogicalType logicalType = LogicalTypeParser.parse(field.logicalType());
            @SuppressWarnings("unchecked")
            TypeSerializer<Object> serializer =
                    (TypeSerializer<Object>) InternalSerializers.create(logicalType);
            DataOutputSerializer output = new DataOutputSerializer(64);
            serializer.serialize(parseFieldInput(logicalType, text), output);
            return output.getCopyOfBuffer();
        } catch (Exception e) {
            throw new IOException(
                    "Failed to encode sink key field "
                            + field.name()
                            + " ("
                            + field.logicalType()
                            + "): "
                            + message(e),
                    e);
        }
    }

    private static Object parseFieldInput(LogicalType logicalType, String text) throws IOException {
        String value = text == null ? "" : text;
        try {
            LogicalTypeRoot root = logicalType.getTypeRoot();
            switch (root) {
                case CHAR:
                case VARCHAR:
                    return StringData.fromString(value);
                case BOOLEAN:
                    if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                        throw new IllegalArgumentException("expected true or false");
                    }
                    return Boolean.valueOf(value);
                case TINYINT:
                    return Byte.valueOf(value);
                case SMALLINT:
                    return Short.valueOf(value);
                case INTEGER:
                    return Integer.valueOf(value);
                case BIGINT:
                    return Long.valueOf(value);
                case FLOAT:
                    return Float.valueOf(value);
                case DOUBLE:
                    return Double.valueOf(value);
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) logicalType;
                    DecimalData decimal =
                            DecimalData.fromBigDecimal(
                                    new BigDecimal(value),
                                    decimalType.getPrecision(),
                                    decimalType.getScale());
                    if (decimal == null) {
                        throw new IllegalArgumentException(
                                "value is outside the declared precision");
                    }
                    return decimal;
                case BINARY:
                case VARBINARY:
                    return Base64.getDecoder().decode(value);
                case DATE:
                    return Math.toIntExact(LocalDate.parse(value).toEpochDay());
                case TIME_WITHOUT_TIME_ZONE:
                    return Math.toIntExact(LocalTime.parse(value).toNanoOfDay() / 1_000_000L);
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return TimestampData.fromLocalDateTime(LocalDateTime.parse(value));
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return TimestampData.fromInstant(Instant.parse(value));
                default:
                    throw new IllegalArgumentException("unsupported key input type " + logicalType);
            }
        } catch (RuntimeException e) {
            throw new IOException(message(e), e);
        }
    }

    private static Map<String, Object> fieldToJson(
            SinkInspectField field, Integer index, Object value, byte[] rawBytes) {
        Map<String, Object> output = new LinkedHashMap<>();
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

    private static Object render(Object value, byte[] rawBytes) {
        if (value == null
                || value instanceof String
                || value instanceof Number
                || value instanceof Boolean) {
            return value;
        }
        if (value instanceof StringData) {
            return value.toString();
        }
        if (value instanceof byte[]) {
            return CobbleFlinkMonitorServer.bytesJson((byte[]) value);
        }
        if (rawBytes != null) {
            return CobbleFlinkMonitorServer.bytesJson(rawBytes);
        }
        return value.toString();
    }

    private static Object fallbackBytes(byte[] bytes) {
        return bytes == null ? null : CobbleFlinkMonitorServer.bytesJson(bytes);
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

        boolean hasOutput() {
            return decodedKey != null || decodedColumns != null || decodeError != null;
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
