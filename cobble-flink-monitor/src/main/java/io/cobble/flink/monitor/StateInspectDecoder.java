package io.cobble.flink.monitor;

import io.cobble.flink.common.inspect.SerializerInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateKind;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
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
        return new DecodedRow(decodedKey, decodedValue, decodeError);
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
        output.put("key", render(deserialize(schema.keySerializer(), slices.key)));
        output.put("namespace", renderNamespace(schema.namespaceSerializer(), slices.namespace));
        if (schema.stateKind() == StateKind.MAP) {
            output.put(
                    "map_key", render(deserialize(schema.mapUserKeySerializer(), slices.mapKey)));
        }
        return output;
    }

    private static Map<String, Object> decodeTimerKey(StateInspectSchema schema, byte[] rowKey)
            throws IOException {
        requireLength(rowKey, Long.BYTES, "timer timestamp");
        ByteArrayInputStream bytes = new ByteArrayInputStream(rowKey);
        DataInputViewStreamWrapper input = new DataInputViewStreamWrapper(bytes);
        long timestamp = MathUtils.flipSignBit(input.readLong());
        Object key = restore(schema.keySerializer()).deserialize(input);
        Object namespace;
        if (isVoidNamespaceSerializer(schema.namespaceSerializer())) {
            if (bytes.available() < 1) {
                throw new IOException("Row key too short for timer namespace");
            }
            input.readByte();
            namespace = VOID_NAMESPACE_LABEL;
        } else {
            namespace = restore(schema.namespaceSerializer()).deserialize(input);
        }

        Map<String, Object> output = new LinkedHashMap<>();
        output.put("timestamp", timestamp);
        output.put("key", render(key));
        output.put(
                "namespace",
                isVoidNamespaceSerializer(schema.namespaceSerializer())
                        ? VOID_NAMESPACE_LABEL
                        : render(namespace));
        return output;
    }

    private static Object decodeValue(StateInspectSchema schema, byte[] valueBytes)
            throws IOException {
        if (valueBytes == null) {
            return null;
        }
        if (schema.stateKind() == StateKind.VALUE) {
            return render(deserialize(schema.valueSerializer(), valueBytes));
        }
        if (schema.stateKind() == StateKind.LIST) {
            TypeSerializer<Object> serializer = restore(schema.listElementSerializer());
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
                values.add(render(serializer.deserialize(inputView)));
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
        if (schema.stateKind() == StateKind.MAP) {
            Map<String, Object> output = new LinkedHashMap<>();
            output.put(
                    "map_value", render(deserialize(schema.mapUserValueSerializer(), valueBytes)));
            return output;
        }
        throw new IOException("Unsupported state kind: " + schema.stateKind());
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

    private static Object renderNamespace(
            SerializerInspectSchema serializerSchema, byte[] namespaceBytes) throws IOException {
        if (isVoidNamespaceSerializer(serializerSchema)) {
            return VOID_NAMESPACE_LABEL;
        }
        return render(deserialize(serializerSchema, namespaceBytes));
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

    private static boolean isVoidNamespaceSerializer(SerializerInspectSchema serializerSchema) {
        return serializerSchema != null
                && VOID_NAMESPACE_SERIALIZER_CLASS.equals(serializerSchema.serializerClassName());
    }

    private static Object render(Object value) {
        if (value == null
                || value instanceof String
                || value instanceof Number
                || value instanceof Boolean) {
            return value;
        }
        if (value instanceof byte[]) {
            return CobbleFlinkMonitorServer.bytesJson((byte[]) value);
        }
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("type", value.getClass().getName());
        output.put("value", String.valueOf(value));
        return output;
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
        final String decodeError;

        private DecodedRow(
                Map<String, Object> decodedKey, Object decodedValue, String decodeError) {
            this.decodedKey = decodedKey;
            this.decodedValue = decodedValue;
            this.decodeError = decodeError;
        }

        static DecodedRow empty() {
            return new DecodedRow(null, null, null);
        }

        boolean hasOutput() {
            return decodedKey != null || decodedValue != null || decodeError != null;
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
