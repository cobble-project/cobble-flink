package io.cobble.flink.table;

import io.cobble.flink.common.inspect.DescriptorCapability;
import io.cobble.flink.common.inspect.InspectDecoderDescriptor;
import io.cobble.flink.common.inspect.SerializerInspectSchema;
import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.StateInspectTypeKind;
import io.cobble.flink.common.inspect.StateKind;
import io.cobble.flink.common.inspect.decode.ClasslessDecodeFailureException;
import io.cobble.flink.common.inspect.decode.ClasslessPojoValue;
import io.cobble.flink.common.inspect.decode.ClasslessValueDecoder;

import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PushbackInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/** Decodes Cobble state rows into the SQL physical row shape resolved at planning time. */
final class CobbleStateRowDecoder {

    static final String TIMER_NOT_IMPLEMENTED =
            "Cobble state source timer runtime is not implemented yet.";

    private static final byte LIST_DELIMITER = ',';
    private static final String VOID_NAMESPACE_SERIALIZER_CLASS =
            "org.apache.flink.runtime.state.VoidNamespaceSerializer";

    private final StateSourceConfig config;
    private final StateInspectSchema schema;
    private final StateInspectSemanticSchema semanticSchema;
    private final Map<StateSourceField.Group, GroupDecoder> groupDecoders =
            new EnumMap<>(StateSourceField.Group.class);

    CobbleStateRowDecoder(
            StateSourceConfig config, CobbleStateSourceRuntime.RuntimeSchema runtimeSchema)
            throws IOException {
        this.config = config;
        this.schema = runtimeSchema.schema;
        this.semanticSchema = runtimeSchema.semanticSchema;
        initializeGroupDecoders();
    }

    List<RowData> decode(byte[] rowKey, byte[][] columns, String splitId, int keyGroup)
            throws IOException {
        return decode(rowKey, columns, splitId, keyGroup, 0);
    }

    /**
     * Decodes a native state row, dropping the first {@code skipRows} decoded rows. Used to resume
     * a partially-consumed LIST entry: on restore the reader re-reads the same entry and skips the
     * rows that were already emitted before the checkpoint.
     */
    List<RowData> decode(
            byte[] rowKey, byte[][] columns, String splitId, int keyGroup, int skipRows)
            throws IOException {
        if (schema.stateKind() == StateKind.TIMER) {
            throw new UnsupportedOperationException(TIMER_NOT_IMPLEMENTED);
        }
        try {
            KeySlices slices =
                    schema.stateKind() == StateKind.MAP
                            ? splitMapKey(rowKey)
                            : splitKeyAndNamespace(rowKey);
            Object[] stateKey = decodeGroup(StateSourceField.Group.STATE_KEY, slices.key);
            Object[] namespace =
                    isVoidNamespace(schema.namespaceSerializer())
                            ? null
                            : decodeGroup(StateSourceField.Group.NAMESPACE, slices.namespace);

            List<RowData> rows = new ArrayList<>();
            switch (schema.stateKind()) {
                case VALUE:
                case REDUCING:
                case AGGREGATING:
                    rows.add(
                            buildRow(
                                    stateKey,
                                    namespace,
                                    decodeGroup(StateSourceField.Group.VALUE, firstColumn(columns)),
                                    null,
                                    null,
                                    null));
                    break;
                case LIST:
                    for (Object[] listElement : decodeListElements(firstColumn(columns))) {
                        rows.add(buildRow(stateKey, namespace, null, listElement, null, null));
                    }
                    break;
                case MAP:
                    byte[] mapValueColumn = firstColumn(columns);
                    if (mapValueColumn == null) {
                        break;
                    }
                    rows.add(
                            buildRow(
                                    stateKey,
                                    namespace,
                                    null,
                                    null,
                                    decodeGroup(StateSourceField.Group.MAP_KEY, slices.mapKey),
                                    decodeMapValue(mapValueColumn)));
                    break;
                default:
                    throw new IOException("Unsupported state kind: " + schema.stateKind());
            }
            if (skipRows > 0) {
                if (skipRows >= rows.size()) {
                    rows.clear();
                } else {
                    rows = new ArrayList<>(rows.subList(skipRows, rows.size()));
                }
            }
            return rows;
        } catch (UnsupportedOperationException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(
                    "Failed to decode Cobble state source row (checkpoint="
                            + config.scanCheckpointId()
                            + ", operator="
                            + config.operatorId()
                            + ", state="
                            + config.stateName()
                            + ", kind="
                            + config.stateKind()
                            + ", split="
                            + splitId
                            + ", keyGroup="
                            + keyGroup
                            + "): "
                            + e.getMessage(),
                    e);
        }
    }

    private void initializeGroupDecoders() throws IOException {
        addGroup(
                StateSourceField.Group.STATE_KEY,
                semanticSchema.stateKey(),
                schema.keySerializer());
        if (!isVoidNamespace(schema.namespaceSerializer())) {
            addGroup(
                    StateSourceField.Group.NAMESPACE,
                    semanticSchema.namespace(),
                    schema.namespaceSerializer());
        }
        switch (schema.stateKind()) {
            case VALUE:
            case REDUCING:
            case AGGREGATING:
                addGroup(
                        StateSourceField.Group.VALUE,
                        semanticSchema.value(),
                        schema.valueSerializer());
                break;
            case LIST:
                addGroup(
                        StateSourceField.Group.LIST_ELEMENT,
                        semanticSchema.listElement(),
                        schema.listElementSerializer());
                break;
            case MAP:
                addGroup(
                        StateSourceField.Group.MAP_KEY,
                        semanticSchema.mapUserKey(),
                        schema.mapUserKeySerializer());
                addGroup(
                        StateSourceField.Group.MAP_VALUE,
                        semanticSchema.mapUserValue(),
                        schema.mapUserValueSerializer());
                break;
            case TIMER:
                break;
            default:
                throw new IOException("Unsupported state kind: " + schema.stateKind());
        }
    }

    private void addGroup(
            StateSourceField.Group group, StateInspectType type, SerializerInspectSchema serializer)
            throws IOException {
        groupDecoders.put(group, new GroupDecoder(group, type, serializer));
    }

    private Object[] decodeGroup(StateSourceField.Group group, byte[] bytes) throws IOException {
        GroupDecoder decoder = groupDecoders.get(group);
        if (decoder == null) {
            return null;
        }
        return decoder.decode(bytes);
    }

    private List<Object[]> decodeListElements(byte[] bytes) throws IOException {
        GroupDecoder decoder = groupDecoders.get(StateSourceField.Group.LIST_ELEMENT);
        return decoder.decodeListElements(bytes);
    }

    private Object[] decodeMapValue(byte[] mapValueColumn) throws IOException {
        if (mapValueColumn.length == 0) {
            throw new IOException(
                    "MapState row value column is empty; expected at least the isNull byte.");
        }
        if (mapValueColumn[0] != 0x00) {
            return groupDecoders.get(StateSourceField.Group.MAP_VALUE).nullValues();
        }
        byte[] payload = new byte[mapValueColumn.length - 1];
        System.arraycopy(mapValueColumn, 1, payload, 0, payload.length);
        return decodeGroup(StateSourceField.Group.MAP_VALUE, payload);
    }

    private RowData buildRow(
            Object[] stateKey,
            Object[] namespace,
            Object[] value,
            Object[] listElement,
            Object[] mapKey,
            Object[] mapValue) {
        GenericRowData row = new GenericRowData(RowKind.INSERT, config.outputFields().size());
        for (int index = 0; index < config.outputFields().size(); index++) {
            StateSourceField field = config.outputFields().get(index);
            row.setField(
                    index,
                    groupValues(
                            field.group(),
                            stateKey,
                            namespace,
                            value,
                            listElement,
                            mapKey,
                            mapValue)[field.groupFieldIndex()]);
        }
        return row;
    }

    private static Object[] groupValues(
            StateSourceField.Group group,
            Object[] stateKey,
            Object[] namespace,
            Object[] value,
            Object[] listElement,
            Object[] mapKey,
            Object[] mapValue) {
        switch (group) {
            case STATE_KEY:
                return stateKey;
            case NAMESPACE:
                return namespace;
            case VALUE:
                return value;
            case LIST_ELEMENT:
                return listElement;
            case MAP_KEY:
                return mapKey;
            case MAP_VALUE:
                return mapValue;
            default:
                throw new IllegalArgumentException("Unsupported group for data row: " + group);
        }
    }

    private KeySlices splitKeyAndNamespace(byte[] rowKey) throws IOException {
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

    private KeySlices splitMapKey(byte[] rowKey) throws IOException {
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
            int total, Integer first, Integer second, String firstName, String secondName)
            throws IOException {
        if (first != null && second != null) {
            if (first + second != total) {
                throw new IOException("Invalid " + firstName + "/" + secondName + " lengths");
            }
            return new int[] {first, second};
        }
        if (first != null) {
            return new int[] {first, total - first};
        }
        if (second != null) {
            return new int[] {total - second, second};
        }
        throw new IOException("Cannot infer variable " + firstName + "/" + secondName + " lengths");
    }

    private static int[] inferThreeLengths(
            int total,
            Integer first,
            Integer second,
            Integer third,
            String firstName,
            String secondName,
            String thirdName)
            throws IOException {
        int known = 0;
        int unknown = 0;
        if (first == null) {
            unknown++;
        } else {
            known += first;
        }
        if (second == null) {
            unknown++;
        } else {
            known += second;
        }
        if (third == null) {
            unknown++;
        } else {
            known += third;
        }
        if (unknown > 1) {
            throw new IOException(
                    "Cannot infer variable "
                            + firstName
                            + "/"
                            + secondName
                            + "/"
                            + thirdName
                            + " lengths");
        }
        int remaining = total - known;
        if (remaining < 0) {
            throw new IOException("Invalid encoded row-key lengths");
        }
        return new int[] {
            first == null ? remaining : first,
            second == null ? remaining : second,
            third == null ? remaining : third
        };
    }

    private static Integer fixedLength(SerializerInspectSchema serializerSchema) {
        if (isVoidNamespace(serializerSchema)) {
            return 1;
        }
        if (serializerSchema == null || serializerSchema.lengthTag() < 0) {
            return null;
        }
        return serializerSchema.lengthTag();
    }

    private static boolean isVoidNamespace(SerializerInspectSchema serializerSchema) {
        return serializerSchema != null
                && VOID_NAMESPACE_SERIALIZER_CLASS.equals(serializerSchema.serializerClassName());
    }

    private static byte[] firstColumn(byte[][] columns) {
        return columns == null || columns.length == 0 ? null : columns[0];
    }

    private static void requireLength(byte[] bytes, int minLength, String label)
            throws IOException {
        if (bytes == null || bytes.length < minLength) {
            throw new IOException(label + " requires at least " + minLength + " bytes");
        }
    }

    private static byte[] slice(byte[] bytes, int offset, int length) throws IOException {
        if (length < 0 || offset < 0 || offset + length > bytes.length) {
            throw new IOException("Invalid slice offset=" + offset + ", length=" + length);
        }
        byte[] copy = new byte[length];
        System.arraycopy(bytes, offset, copy, 0, length);
        return copy;
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }

    private static final class GroupDecoder {
        private final StateSourceField.Group group;
        private final StateInspectType type;
        private final SerializerInspectSchema serializerSchema;
        private final InspectDecoderDescriptor descriptor;
        private final boolean preferClassless;
        private TypeSerializer<Object> serializer;
        private final List<DataStructureConverter<Object, Object>> converters;
        private final List<LogicalType> logicalTypes;

        private GroupDecoder(
                StateSourceField.Group group,
                StateInspectType type,
                SerializerInspectSchema serializerSchema)
                throws IOException {
            this.group = group;
            this.type = type;
            this.serializerSchema = serializerSchema;
            this.descriptor =
                    serializerSchema == null ? null : serializerSchema.decoderDescriptor();
            this.preferClassless =
                    ClasslessValueDecoder.shouldPreferClasslessSemanticDecode(descriptor);
            this.logicalTypes = flattenedLogicalTypes(type);
            this.converters = new ArrayList<>(logicalTypes.size());
            for (LogicalType logicalType : logicalTypes) {
                this.converters.add(
                        DataStructureConverters.getConverter(
                                TypeConversions.fromLogicalToDataType(logicalType)));
            }
        }

        private Object[] decode(byte[] bytes) throws IOException {
            if (bytes == null) {
                return nullValues();
            }
            if (!preferClassless) {
                return decodeWithSerializer(bytes);
            }
            try {
                return decodeObject(ClasslessValueDecoder.decode(descriptor, bytes));
            } catch (IOException classlessFailure) {
                if (descriptor.capability() == DescriptorCapability.FULLY_CLASSLESS) {
                    throw classlessFailure("classless value decode", classlessFailure);
                }
                return decodeWithLiveFallback(bytes, classlessFailure);
            }
        }

        private List<Object[]> decodeListElements(byte[] bytes) throws IOException {
            List<Object[]> rows = new ArrayList<>();
            if (bytes == null) {
                return rows;
            }
            if (!preferClassless) {
                return decodeListElementsWithSerializer(bytes);
            }
            try {
                ClasslessValueDecoder.DecodeCursor cursor =
                        new ClasslessValueDecoder.DecodeCursor(bytes);
                while (cursor.remaining() > 0) {
                    rows.add(
                            decodeObject(
                                    ClasslessValueDecoder.decodeFromCursor(descriptor, cursor)));
                    if (cursor.remaining() > 0) {
                        int delimiter = cursor.input().readUnsignedByte();
                        if (delimiter != LIST_DELIMITER) {
                            throw ClasslessDecodeFailureException.malformed(
                                    "Invalid Cobble list delimiter after element: 0x"
                                            + Integer.toHexString(delimiter));
                        }
                    }
                }
                return rows;
            } catch (IOException classlessFailure) {
                if (descriptor.capability() == DescriptorCapability.FULLY_CLASSLESS) {
                    throw classlessFailure("classless list element decode", classlessFailure);
                }
                return decodeListElementsWithLiveFallback(bytes, classlessFailure);
            }
        }

        private Object[] decodeWithSerializer(byte[] bytes) throws IOException {
            return decodeObject(
                    serializer()
                            .deserialize(
                                    new DataInputViewStreamWrapper(
                                            new ByteArrayInputStream(bytes))));
        }

        private Object[] decodeWithLiveFallback(byte[] bytes, IOException classlessFailure)
                throws IOException {
            try {
                return decodeWithSerializer(bytes);
            } catch (Exception | LinkageError fallbackFailure) {
                throw new IOException(
                        "Classless value decode failed ("
                                + classlessFailure.getMessage()
                                + ") and live serializer fallback failed: "
                                + fallbackFailure.getMessage(),
                        fallbackFailure);
            }
        }

        private List<Object[]> decodeListElementsWithLiveFallback(
                byte[] bytes, IOException classlessFailure) throws IOException {
            try {
                return decodeListElementsWithSerializer(bytes);
            } catch (Exception | LinkageError fallbackFailure) {
                throw new IOException(
                        "Classless list element decode failed ("
                                + classlessFailure.getMessage()
                                + ") and live serializer fallback failed: "
                                + fallbackFailure.getMessage(),
                        fallbackFailure);
            }
        }

        private List<Object[]> decodeListElementsWithSerializer(byte[] bytes) throws IOException {
            List<Object[]> rows = new ArrayList<>();
            PushbackInputStream input = new PushbackInputStream(new ByteArrayInputStream(bytes), 1);
            DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(input);
            while (true) {
                int first = input.read();
                if (first < 0) {
                    return rows;
                }
                input.unread(first);
                rows.add(decodeObject(serializer().deserialize(inputView)));
                int delimiter = input.read();
                if (delimiter < 0) {
                    return rows;
                }
                if ((byte) delimiter != LIST_DELIMITER) {
                    throw new IOException("Invalid Cobble list delimiter: " + delimiter);
                }
            }
        }

        private TypeSerializer<Object> serializer() throws IOException {
            if (serializer == null) {
                serializer = restore(serializerSchema);
            }
            return serializer;
        }

        private IOException classlessFailure(String operation, IOException failure) {
            return new IOException(
                    "Failed " + operation + " for " + group + ": " + failure.getMessage(), failure);
        }

        private Object[] decodeObject(Object value) throws IOException {
            switch (type.kind()) {
                case SCALAR:
                    return new Object[] {toInternal(0, value)};
                case ROW:
                    return decodeRow(value);
                case TUPLE:
                    return decodeTuple(value);
                default:
                    throw new IOException(
                            "Unsupported semantic type " + type.kind() + " for " + group);
            }
        }

        private Object[] nullValues() {
            return new Object[logicalTypes.size()];
        }

        private Object[] decodeRow(Object value) throws IOException {
            Object[] output = new Object[type.fields().size()];
            if (value == null) {
                return output;
            }
            if (value instanceof ClasslessPojoValue) {
                ClasslessPojoValue pojo = (ClasslessPojoValue) value;
                if (pojo.isNull()) {
                    return output;
                }
                for (int index = 0; index < type.fields().size(); index++) {
                    StateInspectField field = type.fields().get(index);
                    if (!pojo.fields().containsKey(field.name())) {
                        throw new IOException(
                                "Classless POJO field '"
                                        + field.name()
                                        + "' is missing for "
                                        + group);
                    }
                    output[index] = toInternal(index, pojo.fields().get(field.name()));
                }
                return output;
            }
            if (value instanceof IndexedRecord) {
                IndexedRecord record = (IndexedRecord) value;
                for (int index = 0; index < type.fields().size(); index++) {
                    output[index] = toInternal(index, record.get(index));
                }
                return output;
            }
            if (!(value instanceof RowData)) {
                throw new IOException(
                        "Expected RowData for " + group + " but got " + value.getClass().getName());
            }
            RowData row = (RowData) value;
            for (int index = 0; index < type.fields().size(); index++) {
                Object field =
                        RowData.createFieldGetter(logicalTypes.get(index), index)
                                .getFieldOrNull(row);
                output[index] = field;
            }
            return output;
        }

        private Object[] decodeTuple(Object value) throws IOException {
            Object[] output = new Object[type.fields().size()];
            if (value == null) {
                return output;
            }
            if (!(value instanceof Tuple)) {
                throw new IOException(
                        "Expected Tuple for " + group + " but got " + value.getClass().getName());
            }
            Tuple tuple = (Tuple) value;
            for (int index = 0; index < type.fields().size(); index++) {
                output[index] = toInternal(index, tuple.getField(index));
            }
            return output;
        }

        private Object toInternal(int index, Object value) throws IOException {
            if (value == null) {
                return null;
            }
            try {
                return converters.get(index).toInternalOrNull(normalizeAvroScalar(value));
            } catch (RuntimeException e) {
                String fieldName =
                        type.kind() == StateInspectTypeKind.SCALAR
                                ? "value"
                                : type.fields().get(index).name();
                throw new IOException(
                        "Failed to convert "
                                + group
                                + " field '"
                                + fieldName
                                + "' to internal logical type "
                                + logicalTypes.get(index).asSummaryString()
                                + " from "
                                + value.getClass().getName(),
                        e);
            }
        }

        private static Object normalizeAvroScalar(Object value) {
            if (value instanceof Utf8) {
                return value.toString();
            }
            if (value instanceof GenericEnumSymbol) {
                return value.toString();
            }
            if (value instanceof ByteBuffer) {
                ByteBuffer buffer = ((ByteBuffer) value).duplicate();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                return bytes;
            }
            if (value instanceof GenericFixed) {
                return ((GenericFixed) value).bytes();
            }
            return value;
        }

        @SuppressWarnings("unchecked")
        private static TypeSerializer<Object> restore(SerializerInspectSchema serializerSchema)
                throws IOException {
            if (serializerSchema == null) {
                throw new IOException("Missing serializer metadata");
            }
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (classLoader == null) {
                classLoader = CobbleStateRowDecoder.class.getClassLoader();
            }
            TypeSerializer<Object> serializer =
                    (TypeSerializer<Object>) serializerSchema.restoreSerializer(classLoader);
            if (serializer == null) {
                throw new IOException(
                        "Failed to restore serializer " + serializerSchema.serializerClassName());
            }
            return serializer;
        }

        private static List<LogicalType> flattenedLogicalTypes(StateInspectType type)
                throws IOException {
            if (type == null || type.kind() == StateInspectTypeKind.UNKNOWN) {
                throw new IOException("Missing semantic type");
            }
            List<LogicalType> types = new ArrayList<>();
            if (type.kind() == StateInspectTypeKind.SCALAR) {
                types.add(parse(type.logicalType()));
            } else if (type.kind() == StateInspectTypeKind.ROW
                    || type.kind() == StateInspectTypeKind.TUPLE) {
                for (StateInspectField field : type.fields()) {
                    if (field.type().kind() != StateInspectTypeKind.SCALAR) {
                        throw new IOException("Nested non-scalar semantic field is unsupported");
                    }
                    types.add(parse(field.type().logicalType()));
                }
            } else {
                throw new IOException("Unsupported semantic type " + type.kind());
            }
            return types;
        }

        private static LogicalType parse(String logicalType) {
            return LogicalTypeParser.parse(
                    logicalType, CobbleStateRowDecoder.class.getClassLoader());
        }
    }

    private static final class KeySlices {
        final byte[] key;
        final byte[] namespace;
        final byte[] mapKey;

        private KeySlices(byte[] key, byte[] namespace, byte[] mapKey) {
            this.key = key;
            this.namespace = namespace;
            this.mapKey = mapKey;
        }
    }
}
