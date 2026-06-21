package io.cobble.flink.monitor;

import io.cobble.flink.common.inspect.SerializerInspectSchema;
import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.StateInspectTypeKind;
import io.cobble.flink.common.inspect.StateKind;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.util.MathUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PushbackInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Best-effort decoder for raw Cobble state rows using persisted Flink state schema metadata. */
final class StateInspectDecoder {

    private static final byte LIST_DELIMITER = ',';
    private static final String VOID_NAMESPACE_SERIALIZER_CLASS =
            "org.apache.flink.runtime.state.VoidNamespaceSerializer";
    private static final String VOID_NAMESPACE_LABEL = "VoidNamespace";

    private StateInspectDecoder() {}

    static DecodedRow decode(InspectTarget target, byte[] rowKey, byte[][] columns) {
        if (target == null || target.schema == null || rowKey == null) {
            return DecodedRow.empty();
        }
        StateInspectSchema schema = target.schema;
        Map<String, Object> decodedKey = null;
        Object decodedValue = null;
        Map<String, Object> decodedParts = null;
        String decodeError = null;
        try {
            decodedKey = decodeKey(schema, rowKey);
        } catch (Exception e) {
            decodeError = message(e);
        }
        try {
            decodedValue =
                    schema.stateKind() == StateKind.TIMER
                            ? null
                            : decodeValue(schema, firstColumn(columns));
        } catch (Exception e) {
            decodeError = appendError(decodeError, message(e));
        }
        try {
            decodedParts = decodeSemanticParts(target, rowKey, columns);
        } catch (Exception e) {
            decodeError = appendError(decodeError, message(e));
        }
        return new DecodedRow(decodedKey, decodedValue, decodedParts, decodeError);
    }

    static byte[] encodeStateKeyPrefix(
            InspectTarget target,
            String key,
            byte[] keyBytes,
            String namespace,
            byte[] namespaceBytes,
            String mapKey,
            byte[] mapKeyBytes)
            throws IOException {
        if (target == null || target.schema == null) {
            throw new IOException("Logical state-key filters require state schema metadata");
        }
        StateInspectSchema schema = target.schema;
        boolean hasKey = hasText(key) || keyBytes != null;
        boolean hasNamespace = hasText(namespace) || namespaceBytes != null;
        boolean hasMapKey = hasText(mapKey) || mapKeyBytes != null;
        if (!hasKey && (hasNamespace || hasMapKey)) {
            throw new IOException("State key is required when namespace or map key is set");
        }
        if (!hasKey) {
            return null;
        }

        byte[] encodedKey = encodeComponent(schema.keySerializer(), key, keyBytes, "key");
        byte[] encodedNamespace =
                encodeNamespace(schema.namespaceSerializer(), namespace, namespaceBytes);
        if (schema.stateKind() == StateKind.MAP) {
            byte[] encodedMapKey =
                    hasMapKey
                            ? encodeComponent(
                                    schema.mapUserKeySerializer(), mapKey, mapKeyBytes, "map key")
                            : null;
            return encodeMapStateKeyPrefix(schema, encodedKey, encodedNamespace, encodedMapKey);
        }
        if (hasMapKey) {
            throw new IOException("Map key filter is only valid for MapState");
        }
        return encodeKeyAndNamespace(schema, encodedKey, encodedNamespace);
    }

    private static Map<String, Object> decodeKey(StateInspectSchema schema, byte[] rowKey)
            throws IOException {
        if (schema.stateKind() == StateKind.TIMER) {
            return decodeTimerKey(schema, rowKey);
        }
        KeySlices slices =
                schema.stateKind() == StateKind.MAP
                        ? splitMapKey(schema, rowKey)
                        : splitKeyAndNamespace(schema, rowKey);

        Map<String, Object> output = new LinkedHashMap<>();
        output.put("key", decodeDisplay(schema.keySerializer(), slices.key));
        output.put("namespace", renderNamespace(schema.namespaceSerializer(), slices.namespace));
        if (schema.stateKind() == StateKind.MAP) {
            output.put("map_key", decodeDisplay(schema.mapUserKeySerializer(), slices.mapKey));
        }
        return output;
    }

    private static Map<String, Object> decodeTimerKey(StateInspectSchema schema, byte[] rowKey)
            throws IOException {
        requireLength(rowKey, Long.BYTES, "timer timestamp");
        ByteArrayInputStream bytes = new ByteArrayInputStream(rowKey);
        DataInputViewStreamWrapper input = new DataInputViewStreamWrapper(bytes);
        long timestamp = MathUtils.flipSignBit(input.readLong());
        int keyStart = rowKey.length - bytes.available();
        Object key = restore(schema.keySerializer()).deserialize(input);
        int namespaceStart = rowKey.length - bytes.available();
        Object namespace;
        if (isVoidNamespaceSerializer(schema.namespaceSerializer())) {
            requireLength(
                    slice(rowKey, namespaceStart, rowKey.length - namespaceStart),
                    1,
                    "timer namespace");
            input.readByte();
            namespace = VOID_NAMESPACE_LABEL;
        } else {
            namespace = restore(schema.namespaceSerializer()).deserialize(input);
        }
        int end = rowKey.length - bytes.available();

        Map<String, Object> output = new LinkedHashMap<>();
        output.put("timestamp", timestamp);
        output.put("key", render(key, slice(rowKey, keyStart, namespaceStart - keyStart), null));
        output.put(
                "namespace",
                isVoidNamespaceSerializer(schema.namespaceSerializer())
                        ? VOID_NAMESPACE_LABEL
                        : render(
                                namespace,
                                slice(rowKey, namespaceStart, end - namespaceStart),
                                null));
        return output;
    }

    static boolean mapKeyBytesStartsWith(InspectTarget target, byte[] rowKey, byte[] prefix)
            throws IOException {
        if (target == null
                || target.schema == null
                || target.schema.stateKind() != StateKind.MAP
                || prefix == null) {
            return false;
        }
        return startsWith(splitMapKey(target.schema, rowKey).mapKey, prefix);
    }

    private static byte[] encodeKeyAndNamespace(
            StateInspectSchema schema, byte[] keyBytes, byte[] namespaceBytes) throws IOException {
        DataOutputSerializer output =
                new DataOutputSerializer(keyBytes.length + namespaceBytes.length + Integer.BYTES);
        output.write(keyBytes);
        output.write(namespaceBytes);
        if (schema.keyLengthStored()) {
            output.writeInt(keyBytes.length);
        }
        return output.getCopyOfBuffer();
    }

    private static byte[] encodeMapStateKeyPrefix(
            StateInspectSchema schema, byte[] keyBytes, byte[] namespaceBytes, byte[] mapKeyBytes)
            throws IOException {
        int capacity =
                keyBytes.length
                        + namespaceBytes.length
                        + 1
                        + (mapKeyBytes == null ? 0 : mapKeyBytes.length)
                        + Integer.BYTES * 2;
        DataOutputSerializer output = new DataOutputSerializer(capacity);
        output.write(keyBytes);
        output.write(namespaceBytes);
        output.writeByte(0);
        if (mapKeyBytes != null) {
            output.write(mapKeyBytes);
            if (schema.mapKeyLengthStored()) {
                output.writeInt(keyBytes.length);
            }
            if (schema.mapNamespaceLengthStored()) {
                output.writeInt(namespaceBytes.length);
            }
        }
        return output.getCopyOfBuffer();
    }

    private static Object decodeValue(StateInspectSchema schema, byte[] valueBytes)
            throws IOException {
        if (valueBytes == null) {
            return null;
        }
        if (schema.stateKind() == StateKind.VALUE) {
            return decodeDisplay(schema.valueSerializer(), valueBytes);
        }
        if (schema.stateKind() == StateKind.LIST) {
            TypeSerializer<Object> serializer = restore(schema.listElementSerializer());
            List<Object> values = deserializeListElements(serializer, valueBytes);
            List<Object> rendered = new ArrayList<>(values.size());
            for (Object value : values) {
                rendered.add(render(value, null, serializer));
            }
            return rendered;
        }
        if (schema.stateKind() == StateKind.MAP) {
            Map<String, Object> output = new LinkedHashMap<>();
            output.put("map_value", decodeDisplay(schema.mapUserValueSerializer(), valueBytes));
            return output;
        }
        throw new IOException("Unsupported state kind: " + schema.stateKind());
    }

    private static List<Object> deserializeListElements(
            TypeSerializer<Object> serializer, byte[] valueBytes) throws IOException {
        List<Object> values = new ArrayList<>();
        PushbackInputStream input =
                new PushbackInputStream(new ByteArrayInputStream(valueBytes), 1);
        DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(input);
        while (true) {
            int first = input.read();
            if (first < 0) {
                break;
            }
            input.unread(first);
            values.add(serializer.deserialize(inputView));
            int delimiter = input.read();
            if (delimiter < 0) {
                break;
            }
            if ((byte) delimiter != LIST_DELIMITER) {
                throw new IOException("Invalid Cobble list delimiter: " + delimiter);
            }
        }
        return values;
    }

    private static Map<String, Object> decodeSemanticParts(
            InspectTarget target, byte[] rowKey, byte[][] columns) throws IOException {
        if (target == null
                || target.schema == null
                || target.semanticSchema == null
                || target.semanticSchema.isEmpty()) {
            return null;
        }
        StateInspectSchema schema = target.schema;
        StateInspectSemanticSchema semanticSchema = target.semanticSchema;
        if (schema.stateKind() == StateKind.TIMER) {
            return decodeTimerSemanticParts(schema, semanticSchema, rowKey);
        }
        KeySlices slices =
                schema.stateKind() == StateKind.MAP
                        ? splitMapKey(schema, rowKey)
                        : splitKeyAndNamespace(schema, rowKey);
        Map<String, Object> output = new LinkedHashMap<>();
        String decodeError = null;
        decodeError =
                addSemanticPart(
                        output,
                        decodeError,
                        "state_key",
                        semanticSchema.stateKey(),
                        schema.keySerializer(),
                        slices.key);
        if (!isVoidNamespaceSerializer(schema.namespaceSerializer())) {
            decodeError =
                    addSemanticPart(
                            output,
                            decodeError,
                            "namespace",
                            semanticSchema.namespace(),
                            schema.namespaceSerializer(),
                            slices.namespace);
        }
        if (schema.stateKind() == StateKind.MAP) {
            decodeError =
                    addSemanticPart(
                            output,
                            decodeError,
                            "map_key",
                            semanticSchema.mapUserKey(),
                            schema.mapUserKeySerializer(),
                            slices.mapKey);
            decodeError =
                    addSemanticPart(
                            output,
                            decodeError,
                            "map_value",
                            semanticSchema.mapUserValue(),
                            schema.mapUserValueSerializer(),
                            firstColumn(columns));
        } else if (schema.stateKind() == StateKind.VALUE) {
            decodeError =
                    addSemanticPart(
                            output,
                            decodeError,
                            "value",
                            semanticSchema.value(),
                            schema.valueSerializer(),
                            firstColumn(columns));
        } else if (schema.stateKind() == StateKind.LIST) {
            decodeError =
                    addSemanticListPart(
                            output,
                            decodeError,
                            semanticSchema.listElement(),
                            schema.listElementSerializer(),
                            firstColumn(columns));
        }
        if (decodeError != null) {
            throw new IOException(decodeError);
        }
        return output.isEmpty() ? null : output;
    }

    private static Map<String, Object> decodeTimerSemanticParts(
            StateInspectSchema schema, StateInspectSemanticSchema semanticSchema, byte[] rowKey)
            throws IOException {
        requireLength(rowKey, Long.BYTES, "timer timestamp");
        ByteArrayInputStream bytes = new ByteArrayInputStream(rowKey);
        DataInputViewStreamWrapper input = new DataInputViewStreamWrapper(bytes);
        input.readLong();
        int keyStart = rowKey.length - bytes.available();
        restore(schema.keySerializer()).deserialize(input);
        int namespaceStart = rowKey.length - bytes.available();
        if (isVoidNamespaceSerializer(schema.namespaceSerializer())) {
            requireLength(
                    slice(rowKey, namespaceStart, rowKey.length - namespaceStart),
                    1,
                    "timer namespace");
            input.readByte();
        } else {
            restore(schema.namespaceSerializer()).deserialize(input);
        }
        int end = rowKey.length - bytes.available();

        Map<String, Object> output = new LinkedHashMap<>();
        String decodeError =
                addSemanticPart(
                        output,
                        null,
                        "state_key",
                        semanticSchema.stateKey(),
                        schema.keySerializer(),
                        slice(rowKey, keyStart, namespaceStart - keyStart));
        if (!isVoidNamespaceSerializer(schema.namespaceSerializer())) {
            decodeError =
                    addSemanticPart(
                            output,
                            decodeError,
                            "namespace",
                            semanticSchema.namespace(),
                            schema.namespaceSerializer(),
                            slice(rowKey, namespaceStart, end - namespaceStart));
        }
        if (decodeError != null) {
            throw new IOException(decodeError);
        }
        return output.isEmpty() ? null : output;
    }

    private static String addSemanticPart(
            Map<String, Object> output,
            String decodeError,
            String partName,
            StateInspectType type,
            SerializerInspectSchema serializer,
            byte[] bytes) {
        if (!isKnownSemanticType(type)) {
            return decodeError;
        }
        try {
            if (bytes == null) {
                output.put(partName, semanticValueToJson(type, null));
            } else {
                output.put(partName, semanticValueToJson(type, deserialize(serializer, bytes)));
            }
        } catch (Exception e) {
            return appendError(decodeError, partName + ": " + message(e));
        }
        return decodeError;
    }

    private static String addSemanticListPart(
            Map<String, Object> output,
            String decodeError,
            StateInspectType elementType,
            SerializerInspectSchema serializer,
            byte[] bytes) {
        if (!isKnownSemanticType(elementType)) {
            return decodeError;
        }
        try {
            List<Object> values =
                    bytes == null
                            ? new ArrayList<>()
                            : deserializeListElements(restore(serializer), bytes);
            List<Object> decoded = new ArrayList<>(values.size());
            for (Object value : values) {
                decoded.add(semanticValueToJson(elementType, value));
            }
            Map<String, Object> list = new LinkedHashMap<>();
            list.put("kind", StateInspectTypeKind.LIST.name());
            list.put("values", decoded);
            output.put("value", list);
        } catch (Exception e) {
            return appendError(decodeError, "value: " + message(e));
        }
        return decodeError;
    }

    private static boolean isKnownSemanticType(StateInspectType type) {
        return type != null && type.kind() != StateInspectTypeKind.UNKNOWN;
    }

    static void validateSemanticPartFilter(
            StateInspectType type, List<String> values, String partLabel) throws IOException {
        List<StateInspectType> fields = semanticFilterFieldTypes(type, partLabel);
        if (values == null || values.isEmpty()) {
            throw new IOException(partLabel + " fields must not be empty");
        }
        if (values.size() > fields.size()) {
            throw new IOException(
                    "Too many " + partLabel + " fields (expected at most " + fields.size() + ")");
        }
        for (int index = 0; index < values.size(); index++) {
            if (values.get(index) == null || values.get(index).isEmpty()) {
                throw new IOException(partLabel + " fields must be supplied in order without gaps");
            }
        }
    }

    @SuppressWarnings("unchecked")
    static boolean matchesSemanticPartFilter(
            Map<String, Object> decodedParts,
            String partName,
            StateInspectType type,
            List<String> values) {
        if (decodedParts == null || values == null || values.isEmpty()) {
            return false;
        }
        Object part = decodedParts.get(partName);
        if (!(part instanceof Map)) {
            return false;
        }
        List<StateInspectType> fieldTypes;
        try {
            fieldTypes = semanticFilterFieldTypes(type, partName);
        } catch (IOException e) {
            return false;
        }
        if (values.size() > fieldTypes.size()) {
            return false;
        }
        List<Object> fieldValues = new ArrayList<>(fieldTypes.size());
        if (type.kind() == StateInspectTypeKind.SCALAR) {
            fieldValues.add(((Map<String, Object>) part).get("value"));
        } else {
            Object fields = ((Map<String, Object>) part).get("fields");
            if (!(fields instanceof List)) {
                return false;
            }
            for (Object field : (List<?>) fields) {
                if (!(field instanceof Map)) {
                    return false;
                }
                fieldValues.add(((Map<String, Object>) field).get("value"));
            }
        }
        if (fieldValues.size() < values.size()) {
            return false;
        }
        for (int index = 0; index < values.size(); index++) {
            if (!semanticValueMatches(
                    fieldValues.get(index), values.get(index), index == values.size() - 1)) {
                return false;
            }
        }
        return true;
    }

    private static List<StateInspectType> semanticFilterFieldTypes(
            StateInspectType type, String partLabel) throws IOException {
        if (!isKnownSemanticType(type)) {
            throw new IOException("Missing semantic metadata for " + partLabel);
        }
        if (type.kind() == StateInspectTypeKind.SCALAR) {
            return Arrays.asList(type);
        }
        if (type.kind() != StateInspectTypeKind.ROW && type.kind() != StateInspectTypeKind.TUPLE) {
            throw new IOException("Field filtering is not supported for " + partLabel);
        }
        List<StateInspectType> fields = new ArrayList<>(type.fields().size());
        for (StateInspectField field : type.fields()) {
            if (field.type().kind() != StateInspectTypeKind.SCALAR) {
                throw new IOException(
                        "Field filtering only supports scalar "
                                + partLabel
                                + " fields ("
                                + field.name()
                                + ")");
            }
            fields.add(field.type());
        }
        return fields;
    }

    @SuppressWarnings("unchecked")
    private static boolean semanticValueMatches(Object value, String input, boolean prefix) {
        if (value == null || input == null) {
            return false;
        }
        String candidate;
        if (value instanceof Map) {
            Object b64 = ((Map<String, Object>) value).get("b64");
            if (b64 == null) {
                return false;
            }
            candidate = String.valueOf(b64);
        } else {
            candidate = String.valueOf(value);
        }
        if (value instanceof Boolean) {
            return prefix
                    ? candidate.regionMatches(true, 0, input, 0, input.length())
                    : candidate.equalsIgnoreCase(input);
        }
        return prefix ? candidate.startsWith(input) : candidate.equals(input);
    }

    private static Map<String, Object> semanticValueToJson(StateInspectType type, Object value)
            throws IOException {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("kind", type.kind().name());
        if (type.logicalType() != null) {
            output.put("logical_type", type.logicalType());
        }
        switch (type.kind()) {
            case SCALAR:
                output.put("value", renderSemanticScalar(value));
                return output;
            case ROW:
                if (value != null && !(value instanceof RowData)) {
                    throw new IOException(
                            "Expected RowData but found " + value.getClass().getName());
                }
                output.put("fields", decodeRowFields(type, (RowData) value));
                return output;
            case TUPLE:
                if (value != null && !(value instanceof Tuple)) {
                    throw new IOException(
                            "Expected Flink Tuple but found " + value.getClass().getName());
                }
                output.put("fields", decodeTupleFields(type, (Tuple) value));
                return output;
            case LIST:
                if (value != null && !(value instanceof List)) {
                    throw new IOException("Expected List but found " + value.getClass().getName());
                }
                output.put("values", decodeListValues(type.elementType(), (List<?>) value));
                return output;
            case UNKNOWN:
                throw new IOException("Unknown semantic type");
            default:
                throw new IOException("Unsupported semantic type " + type.kind());
        }
    }

    private static List<Map<String, Object>> decodeRowFields(StateInspectType type, RowData row)
            throws IOException {
        List<Map<String, Object>> output = new ArrayList<>(type.fields().size());
        for (int index = 0; index < type.fields().size(); index++) {
            StateInspectField field = type.fields().get(index);
            Object value = null;
            if (row != null) {
                LogicalType logicalType = LogicalTypeParser.parse(field.type().logicalType());
                value = RowData.createFieldGetter(logicalType, index).getFieldOrNull(row);
            }
            output.add(semanticFieldToJson(field, value));
        }
        return output;
    }

    private static List<Map<String, Object>> decodeTupleFields(StateInspectType type, Tuple tuple)
            throws IOException {
        List<Map<String, Object>> output = new ArrayList<>(type.fields().size());
        for (int index = 0; index < type.fields().size(); index++) {
            output.add(
                    semanticFieldToJson(
                            type.fields().get(index),
                            tuple == null ? null : tuple.getField(index)));
        }
        return output;
    }

    private static List<Object> decodeListValues(StateInspectType elementType, List<?> values)
            throws IOException {
        List<Object> output = new ArrayList<>();
        if (values == null) {
            return output;
        }
        for (Object value : values) {
            output.add(semanticValueToJson(elementType, value));
        }
        return output;
    }

    private static Map<String, Object> semanticFieldToJson(StateInspectField field, Object value)
            throws IOException {
        Map<String, Object> output = semanticValueToJson(field.type(), value);
        output.put("name", field.name());
        return output;
    }

    private static Object renderSemanticScalar(Object value) throws IOException {
        if (value == null
                || value instanceof String
                || value instanceof Number
                || value instanceof Boolean) {
            return value;
        }
        if (value instanceof StringData
                || value instanceof DecimalData
                || value instanceof TimestampData) {
            return value.toString();
        }
        if (value instanceof byte[]) {
            return CobbleFlinkMonitorServer.bytesJson((byte[]) value);
        }
        throw new IOException("Semantic scalar is not displayable: " + value.getClass().getName());
    }

    private static KeySlices splitKeyAndNamespace(StateInspectSchema schema, byte[] rowKey)
            throws IOException {
        int payloadEnd = rowKey.length;
        Integer keyLength = fixedLength(schema.keySerializer());
        Integer namespaceLength = fixedLength(schema.namespaceSerializer());
        if (schema.keyLengthStored()) {
            requireLength(rowKey, Integer.BYTES, "row key length suffix");
            keyLength = readInt(rowKey, rowKey.length - Integer.BYTES);
            payloadEnd -= Integer.BYTES;
        }
        int[] lengths = inferTwoLengths(payloadEnd, keyLength, namespaceLength, "key", "namespace");
        return new KeySlices(
                slice(rowKey, 0, lengths[0]), slice(rowKey, lengths[0], lengths[1]), null);
    }

    private static KeySlices splitMapKey(StateInspectSchema schema, byte[] rowKey)
            throws IOException {
        int payloadEnd = rowKey.length;
        Integer keyLength = fixedLength(schema.keySerializer());
        Integer namespaceLength = fixedLength(schema.namespaceSerializer());
        Integer mapKeyLength = fixedLength(schema.mapUserKeySerializer());
        if (schema.mapNamespaceLengthStored()) {
            requireLength(rowKey, Integer.BYTES, "map namespace length suffix");
            payloadEnd -= Integer.BYTES;
            namespaceLength = readInt(rowKey, payloadEnd);
        }
        if (schema.mapKeyLengthStored()) {
            requireLength(
                    rowKey, rowKey.length - payloadEnd + Integer.BYTES, "map key length suffix");
            payloadEnd -= Integer.BYTES;
            keyLength = readInt(rowKey, payloadEnd);
        }

        int[] lengths =
                inferThreeLengths(
                        payloadEnd - 1,
                        keyLength,
                        namespaceLength,
                        mapKeyLength,
                        "key",
                        "namespace",
                        "map key");
        int separator = lengths[0] + lengths[1];
        if (separator < 0 || separator >= payloadEnd || rowKey[separator] != 0) {
            throw new IOException("Invalid MapState row-key separator");
        }
        return new KeySlices(
                slice(rowKey, 0, lengths[0]),
                slice(rowKey, lengths[0], lengths[1]),
                slice(rowKey, separator + 1, lengths[2]));
    }

    private static int[] inferTwoLengths(
            int totalLength,
            Integer firstLength,
            Integer secondLength,
            String firstName,
            String secondName)
            throws IOException {
        if (firstLength == null && secondLength == null) {
            throw new IOException(
                    "Cannot infer variable " + firstName + " and " + secondName + " lengths");
        }
        if (firstLength == null) {
            firstLength = totalLength - secondLength;
        }
        if (secondLength == null) {
            secondLength = totalLength - firstLength;
        }
        validateLength(firstLength, firstName);
        validateLength(secondLength, secondName);
        if (firstLength + secondLength != totalLength) {
            throw new IOException(
                    "Invalid row-key lengths: "
                            + firstName
                            + "="
                            + firstLength
                            + ", "
                            + secondName
                            + "="
                            + secondLength
                            + ", payload="
                            + totalLength);
        }
        return new int[] {firstLength, secondLength};
    }

    private static int[] inferThreeLengths(
            int totalLength,
            Integer firstLength,
            Integer secondLength,
            Integer thirdLength,
            String firstName,
            String secondName,
            String thirdName)
            throws IOException {
        int unknown = 0;
        unknown += firstLength == null ? 1 : 0;
        unknown += secondLength == null ? 1 : 0;
        unknown += thirdLength == null ? 1 : 0;
        if (unknown > 1) {
            throw new IOException("Cannot infer MapState row-key component lengths");
        }
        if (firstLength == null) {
            firstLength = totalLength - secondLength - thirdLength;
        } else if (secondLength == null) {
            secondLength = totalLength - firstLength - thirdLength;
        } else if (thirdLength == null) {
            thirdLength = totalLength - firstLength - secondLength;
        }
        validateLength(firstLength, firstName);
        validateLength(secondLength, secondName);
        validateLength(thirdLength, thirdName);
        if (firstLength + secondLength + thirdLength != totalLength) {
            throw new IOException("Invalid MapState row-key component lengths");
        }
        return new int[] {firstLength, secondLength, thirdLength};
    }

    private static Object deserialize(SerializerInspectSchema serializerSchema, byte[] bytes)
            throws IOException {
        TypeSerializer<Object> serializer = restore(serializerSchema);
        return serializer.deserialize(
                new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)));
    }

    private static Object decodeDisplay(SerializerInspectSchema serializerSchema, byte[] bytes)
            throws IOException {
        return render(deserialize(serializerSchema, bytes), bytes, null);
    }

    private static byte[] encodeComponent(
            SerializerInspectSchema serializerSchema, String value, byte[] bytes, String label)
            throws IOException {
        if (bytes != null) {
            return bytes;
        }
        TypeSerializer<Object> serializer = restore(serializerSchema);
        Object parsed = parseTextValue(serializerSchema, value, label);
        DataOutputSerializer output = new DataOutputSerializer(64);
        serializer.serialize(parsed, output);
        return output.getCopyOfBuffer();
    }

    private static byte[] encodeNamespace(
            SerializerInspectSchema serializerSchema, String namespace, byte[] namespaceBytes)
            throws IOException {
        if (isVoidNamespaceSerializer(serializerSchema)) {
            if (hasText(namespace) || namespaceBytes != null) {
                throw new IOException("VoidNamespace does not accept a namespace value");
            }
            return new byte[] {0};
        }
        if (!hasText(namespace) && namespaceBytes == null) {
            throw new IOException("Namespace is required for this state");
        }
        return encodeComponent(serializerSchema, namespace, namespaceBytes, "namespace");
    }

    private static Object parseTextValue(
            SerializerInspectSchema serializerSchema, String value, String label)
            throws IOException {
        String className = serializerSchema.serializerClassName();
        try {
            if (isSerializer(className, "StringSerializer")) {
                return value;
            }
            if (isSerializer(className, "IntSerializer")) {
                return Integer.parseInt(value);
            }
            if (isSerializer(className, "LongSerializer")) {
                return Long.parseLong(value);
            }
            if (isSerializer(className, "ShortSerializer")) {
                return Short.parseShort(value);
            }
            if (isSerializer(className, "ByteSerializer")) {
                return Byte.parseByte(value);
            }
            if (isSerializer(className, "BooleanSerializer")) {
                return Boolean.parseBoolean(value);
            }
            if (isSerializer(className, "FloatSerializer")) {
                return Float.parseFloat(value);
            }
            if (isSerializer(className, "DoubleSerializer")) {
                return Double.parseDouble(value);
            }
        } catch (NumberFormatException e) {
            throw new IOException(
                    "Invalid "
                            + label
                            + " value for "
                            + simpleSerializerName(className)
                            + ": "
                            + value);
        }
        throw new IOException(
                "Text input for "
                        + label
                        + " is not supported by "
                        + className
                        + "; use the *_b64 field with serialized bytes");
    }

    private static Object renderNamespace(
            SerializerInspectSchema serializerSchema, byte[] namespaceBytes) throws IOException {
        if (isVoidNamespaceSerializer(serializerSchema)) {
            return VOID_NAMESPACE_LABEL;
        }
        return decodeDisplay(serializerSchema, namespaceBytes);
    }

    @SuppressWarnings("unchecked")
    private static TypeSerializer<Object> restore(SerializerInspectSchema serializerSchema)
            throws IOException {
        if (serializerSchema == null) {
            throw new IOException("Missing serializer metadata");
        }
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = StateInspectDecoder.class.getClassLoader();
        }
        TypeSerializer<Object> serializer =
                (TypeSerializer<Object>) serializerSchema.restoreSerializer(classLoader);
        if (serializer == null) {
            throw new IOException(
                    "Failed to restore serializer " + serializerSchema.serializerClassName());
        }
        return serializer;
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static boolean isSerializer(String className, String simpleName) {
        return className != null && className.endsWith("." + simpleName);
    }

    static String simpleSerializerName(String className) {
        if (className == null || className.isEmpty()) {
            return "unknown";
        }
        int lastDot = className.lastIndexOf('.');
        String simple = lastDot < 0 ? className : className.substring(lastDot + 1);
        return simple.endsWith("Serializer")
                ? simple.substring(0, simple.length() - "Serializer".length())
                : simple;
    }

    private static boolean isVoidNamespaceSerializer(SerializerInspectSchema serializerSchema) {
        return serializerSchema != null
                && VOID_NAMESPACE_SERIALIZER_CLASS.equals(serializerSchema.serializerClassName());
    }

    private static Object render(Object value, byte[] rawBytes, TypeSerializer<Object> serializer)
            throws IOException {
        if (value == null
                || value instanceof String
                || value instanceof Number
                || value instanceof Boolean) {
            return value;
        }
        if (value instanceof byte[]) {
            return CobbleFlinkMonitorServer.bytesJson((byte[]) value);
        }
        byte[] fallbackBytes = rawBytes == null ? serializeFallback(serializer, value) : rawBytes;
        if (fallbackBytes != null) {
            return CobbleFlinkMonitorServer.bytesJson(fallbackBytes);
        }
        throw new IOException("Decoded value is not displayable: " + value.getClass().getName());
    }

    private static byte[] serializeFallback(TypeSerializer<Object> serializer, Object value)
            throws IOException {
        if (serializer == null || value == null) {
            return null;
        }
        DataOutputSerializer output = new DataOutputSerializer(64);
        serializer.serialize(value, output);
        return output.getCopyOfBuffer();
    }

    private static boolean startsWith(byte[] value, byte[] prefix) {
        if (value == null || prefix == null || prefix.length > value.length) {
            return false;
        }
        for (int index = 0; index < prefix.length; index++) {
            if (value[index] != prefix[index]) {
                return false;
            }
        }
        return true;
    }

    private static String appendError(String existing, String next) {
        if (existing == null || existing.isEmpty()) {
            return next;
        }
        return existing + "; " + next;
    }

    private static String message(Exception e) {
        return e.getMessage() == null ? e.getClass().getName() : e.getMessage();
    }

    private static Integer fixedLength(SerializerInspectSchema serializerSchema) {
        if (isVoidNamespaceSerializer(serializerSchema)) {
            return 1;
        }
        if (serializerSchema == null || serializerSchema.lengthTag() < 0) {
            return null;
        }
        return serializerSchema.lengthTag();
    }

    private static byte[] firstColumn(byte[][] columns) {
        return columns == null || columns.length == 0 ? null : columns[0];
    }

    private static byte[] slice(byte[] source, int offset, int length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > source.length) {
            throw new IOException("Invalid row-key slice offset=" + offset + ", length=" + length);
        }
        return Arrays.copyOfRange(source, offset, offset + length);
    }

    private static void validateLength(int length, String name) throws IOException {
        if (length < 0) {
            throw new IOException("Invalid " + name + " length: " + length);
        }
    }

    private static void requireLength(byte[] bytes, int required, String label) throws IOException {
        if (bytes.length < required) {
            throw new IOException("Row key too short for " + label);
        }
    }

    private static int readInt(byte[] bytes, int offset) throws IOException {
        if (offset < 0 || offset + Integer.BYTES > bytes.length) {
            throw new IOException("Invalid int offset in row key: " + offset);
        }
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }

    static final class DecodedRow {
        final Map<String, Object> decodedKey;
        final Object decodedValue;
        final Map<String, Object> decodedParts;
        final String decodeError;

        private DecodedRow(
                Map<String, Object> decodedKey,
                Object decodedValue,
                Map<String, Object> decodedParts,
                String decodeError) {
            this.decodedKey = decodedKey;
            this.decodedValue = decodedValue;
            this.decodedParts = decodedParts;
            this.decodeError = decodeError;
        }

        static DecodedRow empty() {
            return new DecodedRow(null, null, null, null);
        }

        boolean hasOutput() {
            return decodedKey != null
                    || decodedValue != null
                    || decodedParts != null
                    || decodeError != null;
        }
    }

    private static final class KeySlices {
        private final byte[] key;
        private final byte[] namespace;
        private final byte[] mapKey;

        private KeySlices(byte[] key, byte[] namespace, byte[] mapKey) {
            this.key = key;
            this.namespace = namespace;
            this.mapKey = mapKey;
        }
    }
}
