package io.cobble.flink.table;

import io.cobble.flink.common.inspect.InspectDecoderDescriptor;
import io.cobble.flink.common.inspect.SerializerInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.StateInspectTypeKind;
import io.cobble.flink.common.inspect.StateKind;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Runtime-only encoder that turns a lookup {@link RowData} carrying the full exact state lookup key
 * into the Cobble row-key bytes and the Cobble key group.
 *
 * <p>It is the exact inverse of {@link CobbleStateRowDecoder#splitKeyAndNamespace} and {@link
 * CobbleStateRowDecoder#splitMapKey}: the same length-suffix layout is used, serializers are
 * restored from the inspect sidecar, and the key group is computed from the logical Flink state key
 * object via {@link KeyGroupRangeAssignment#assignToKeyGroup(Object, int)} — matching the backend
 * writer's {@code bucketForKey(key)}.
 *
 * <p>This class is runtime-only and does not open a reader or call {@code Reader.get(...)}.
 */
final class CobbleStateLookupKeyEncoder {

    private static final String VOID_NAMESPACE_SERIALIZER_CLASS =
            VoidNamespaceSerializer.class.getName();
    private static final byte MAP_SEPARATOR = 0x00;

    private final StateSourceConfig config;
    private final CobbleStateSourceRuntime.RuntimeSchema runtimeSchema;
    private final int[] lookupKeyPositionsByRequiredField;

    private final GroupLookupEncoder stateKeyEncoder;
    private final GroupLookupEncoder namespaceEncoder;
    private final GroupLookupEncoder mapKeyEncoder;
    private final boolean isVoidNamespace;
    private final boolean isMap;

    CobbleStateLookupKeyEncoder(
            StateSourceConfig config,
            CobbleStateSourceRuntime.RuntimeSchema runtimeSchema,
            int[] lookupKeyPositionsByRequiredField) {
        this(
                config,
                runtimeSchema,
                lookupKeyPositionsByRequiredField,
                Thread.currentThread().getContextClassLoader() == null
                        ? CobbleStateLookupKeyEncoder.class.getClassLoader()
                        : Thread.currentThread().getContextClassLoader());
    }

    CobbleStateLookupKeyEncoder(
            StateSourceConfig config,
            CobbleStateSourceRuntime.RuntimeSchema runtimeSchema,
            int[] lookupKeyPositionsByRequiredField,
            ClassLoader classLoader) {
        this.config = config;
        this.runtimeSchema = runtimeSchema;
        this.lookupKeyPositionsByRequiredField =
                Arrays.copyOf(
                        lookupKeyPositionsByRequiredField,
                        lookupKeyPositionsByRequiredField.length);

        StateInspectSchema schema = runtimeSchema.schema;
        StateInspectSemanticSchema semantic = runtimeSchema.semanticSchema;
        this.isVoidNamespace = isVoidNamespace(schema.namespaceSerializer());
        this.isMap = schema.stateKind() == StateKind.MAP;

        try {
            this.stateKeyEncoder =
                    GroupLookupEncoder.create(
                            "state key",
                            semantic.stateKey(),
                            schema.keySerializer(),
                            config,
                            classLoader);
            this.namespaceEncoder =
                    isVoidNamespace
                            ? null
                            : GroupLookupEncoder.create(
                                    "namespace",
                                    semantic.namespace(),
                                    schema.namespaceSerializer(),
                                    config,
                                    classLoader);
            this.mapKeyEncoder =
                    isMap
                            ? GroupLookupEncoder.create(
                                    "map key",
                                    semantic.mapUserKey(),
                                    schema.mapUserKeySerializer(),
                                    config,
                                    classLoader)
                            : null;
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to initialize Cobble state lookup key encoder: " + e.getMessage(), e);
        }
    }

    EncodedStateLookupKey encode(RowData lookupKeyRow, int totalKeyGroups) throws IOException {
        if (lookupKeyRow == null) {
            throw new IOException("Cobble state lookup key row must not be null.");
        }
        List<StateSourceField> requiredFields = config.lookupKeyContract().requiredFields();
        if (lookupKeyPositionsByRequiredField.length != requiredFields.size()) {
            throw new IOException(
                    "Cobble state lookup encoder expects "
                            + requiredFields.size()
                            + " lookup key position(s) but received "
                            + lookupKeyPositionsByRequiredField.length
                            + ".");
        }

        Object stateKeyObject = buildGroupObject(stateKeyEncoder, lookupKeyRow, "state key");
        Object namespaceObject =
                isVoidNamespace ? VoidNamespace.INSTANCE : buildNamespaceObject(lookupKeyRow);
        Object mapKeyObject = isMap ? buildMapKeyObject(lookupKeyRow) : null;

        byte[] rowKey = buildRowKey(stateKeyObject, namespaceObject, mapKeyObject);
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(stateKeyObject, totalKeyGroups);
        return new EncodedStateLookupKey(keyGroup, rowKey);
    }

    private Object buildNamespaceObject(RowData lookupKeyRow) throws IOException {
        return buildGroupObject(namespaceEncoder, lookupKeyRow, "namespace");
    }

    private Object buildMapKeyObject(RowData lookupKeyRow) throws IOException {
        return buildGroupObject(mapKeyEncoder, lookupKeyRow, "map key");
    }

    private Object buildGroupObject(
            GroupLookupEncoder groupEncoder, RowData lookupKeyRow, String groupLabel)
            throws IOException {
        int[] requiredIndexes = positionsForGroup(groupEncoder.group());
        Object[] fieldValues = new Object[requiredIndexes.length];
        for (int i = 0; i < requiredIndexes.length; i++) {
            int requiredFieldIndex = requiredIndexes[i];
            int rowPosition = lookupKeyPositionsByRequiredField[requiredFieldIndex];
            if (rowPosition < 0) {
                throw new IOException(
                        "Lookup key position for required field "
                                + requiredFieldIndex
                                + " is invalid: "
                                + rowPosition
                                + ".");
            }
            Object value = groupEncoder.readField(i, rowPosition, lookupKeyRow);
            if (value == null) {
                StateSourceField field =
                        config.lookupKeyContract().requiredFields().get(requiredFieldIndex);
                throw new IOException(
                        "Lookup key column '"
                                + field.name()
                                + "' ("
                                + groupLabel
                                + ") must not be null.");
            }
            fieldValues[i] = value;
        }
        return groupEncoder.buildObject(fieldValues);
    }

    private int[] positionsForGroup(StateSourceField.Group group) {
        List<StateSourceField> requiredFields = config.lookupKeyContract().requiredFields();
        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < requiredFields.size(); i++) {
            if (requiredFields.get(i).group() == group) {
                indexes.add(i);
            }
        }
        int[] result = new int[indexes.size()];
        for (int i = 0; i < indexes.size(); i++) {
            result[i] = indexes.get(i);
        }
        return result;
    }

    private byte[] buildRowKey(Object stateKeyObject, Object namespaceObject, Object mapKeyObject)
            throws IOException {
        byte[] keyBytes = stateKeyEncoder.serializeObject(stateKeyObject);
        // VoidNamespaceSerializer.getLength() returns 0 but serialize() writes 1 byte (0x00).
        // Always emit it to match the backend writer.
        byte[] namespaceBytes =
                isVoidNamespace
                        ? namespaceBytes()
                        : namespaceEncoder.serializeObject(namespaceObject);
        DataOutputSerializer output =
                new DataOutputSerializer(keyBytes.length + namespaceBytes.length + 16);
        output.write(keyBytes);
        output.write(namespaceBytes);

        StateInspectSchema schema = runtimeSchema.schema;
        if (isMap) {
            output.writeByte(MAP_SEPARATOR);
            byte[] mapKeyBytes = mapKeyEncoder.serializeObject(mapKeyObject);
            output.write(mapKeyBytes);
            if (schema.mapKeyLengthStored()) {
                output.writeInt(keyBytes.length);
            }
            if (schema.mapNamespaceLengthStored()) {
                output.writeInt(namespaceBytes.length);
            }
        } else {
            if (schema.keyLengthStored()) {
                output.writeInt(keyBytes.length);
            }
        }
        return output.getCopyOfBuffer();
    }

    private static boolean isVoidNamespace(SerializerInspectSchema serializer) {
        return serializer != null
                && VOID_NAMESPACE_SERIALIZER_CLASS.equals(serializer.serializerClassName());
    }

    /**
     * The single {@code 0x00} byte emitted by {@link VoidNamespaceSerializer#serialize}. Mirrors
     * the backend writer, which always serializes the (non-null) {@link VoidNamespace} instance
     * rather than skipping it.
     */
    private static byte[] namespaceBytes() {
        return new byte[] {0x00};
    }

    /** Result of encoding a lookup key: the Cobble key group and row-key bytes. */
    static final class EncodedStateLookupKey {
        private final int keyGroup;
        private final byte[] rowKey;

        private EncodedStateLookupKey(int keyGroup, byte[] rowKey) {
            this.keyGroup = keyGroup;
            this.rowKey = rowKey;
        }

        int keyGroup() {
            return keyGroup;
        }

        byte[] rowKey() {
            return rowKey;
        }
    }

    /**
     * Builds the serializer object for one logical group (state key / namespace / map key) from
     * flattened lookup {@link RowData} fields, then serializes it.
     *
     * <p>Supported semantic shapes:
     *
     * <ul>
     *   <li>{@code SCALAR}: one field, passed through a {@link RowData.FieldGetter}.
     *   <li>{@code ROW}: a {@link GenericRowData} with one field per flattened scalar field, which
     *       {@code RowDataSerializer} can serialize.
     *   <li>{@code TUPLE}: not supported yet; fails with a clear message.
     *   <li>{@code UNKNOWN} / nested non-scalar: fails clearly.
     * </ul>
     */
    private static final class GroupLookupEncoder {
        private final String groupLabel;
        private final StateInspectType type;
        private final TypeSerializer<Object> serializer;
        private final List<LogicalType> logicalTypes;
        private final List<DataStructureConverter<Object, Object>> converters;
        private final StateSourceField.Group group;

        private GroupLookupEncoder(
                String groupLabel,
                StateInspectType type,
                TypeSerializer<Object> serializer,
                List<LogicalType> logicalTypes,
                List<DataStructureConverter<Object, Object>> converters,
                StateSourceField.Group group) {
            this.groupLabel = groupLabel;
            this.type = type;
            this.serializer = serializer;
            this.logicalTypes = logicalTypes;
            this.converters = converters;
            this.group = group;
        }

        static GroupLookupEncoder create(
                String groupLabel,
                StateInspectType type,
                SerializerInspectSchema serializerSchema,
                StateSourceConfig config,
                ClassLoader classLoader)
                throws IOException {
            StateSourceField.Group group = groupForLabel(groupLabel, config);
            rejectClasslessStructuredLookupKey(groupLabel, type, serializerSchema);
            TypeSerializer<Object> serializer =
                    restoreSerializer(serializerSchema, groupLabel, classLoader);
            List<LogicalType> logicalTypes = flattenedLogicalTypes(type, groupLabel);
            List<DataStructureConverter<Object, Object>> converters =
                    new ArrayList<>(logicalTypes.size());
            for (LogicalType logicalType : logicalTypes) {
                converters.add(
                        DataStructureConverters.getConverter(
                                TypeConversions.fromLogicalToDataType(logicalType)));
            }
            return new GroupLookupEncoder(
                    groupLabel, type, serializer, logicalTypes, converters, group);
        }

        private static void rejectClasslessStructuredLookupKey(
                String groupLabel, StateInspectType type, SerializerInspectSchema serializerSchema)
                throws IOException {
            InspectDecoderDescriptor descriptor =
                    serializerSchema == null ? null : serializerSchema.decoderDescriptor();
            if (descriptor == null
                    || (!descriptor.isPojo() && !descriptor.isAvro())
                    || (type.kind() != StateInspectTypeKind.ROW
                            && type.kind() != StateInspectTypeKind.TUPLE)) {
                return;
            }
            throw new IOException(
                    "Cobble state lookup does not support exact "
                            + groupLabel
                            + " reconstruction for classless "
                            + (descriptor.isPojo() ? "POJO" : "Avro")
                            + " structured keys. Use scan mode or a scalar key.");
        }

        StateSourceField.Group group() {
            return group;
        }

        /**
         * Reads the {@code groupIndex}-th flattened field of this group from {@code row} in its
         * Flink internal representation (e.g. {@link org.apache.flink.table.data.StringData} for
         * {@code VARCHAR}). Conversion to the external object shape happens in {@link
         * #buildObject(Object[])} only for {@code SCALAR} groups, because {@code ROW} groups must
         * keep internal values so {@code RowDataSerializer} and {@code GenericRowData.hashCode()}
         * (used for key-group assignment) match the backend writer.
         */
        Object readField(int groupIndex, int rowPosition, RowData row) {
            return RowData.createFieldGetter(logicalTypes.get(groupIndex), rowPosition)
                    .getFieldOrNull(row);
        }

        Object buildObject(Object[] fieldValues) throws IOException {
            switch (type.kind()) {
                case SCALAR:
                    // Scalar serializers (e.g. StringSerializer) expect external Java objects, not
                    // internal representations (e.g. StringData). Convert here so the object is
                    // correct for both serialization and key-group assignment.
                    Object external = toExternal(0, fieldValues[0]);
                    return external;
                case ROW:
                    // RowDataSerializer reads fields via RowData.FieldGetter, which expects
                    // internal representations. Keep internal values so serialization and
                    // hashCode() match the backend writer.
                    GenericRowData row = new GenericRowData(fieldValues.length);
                    for (int i = 0; i < fieldValues.length; i++) {
                        row.setField(i, fieldValues[i]);
                    }
                    return row;
                case TUPLE:
                    throw new IOException(
                            "Cobble state lookup tuple "
                                    + groupLabel
                                    + " encoding is not implemented yet.");
                case UNKNOWN:
                default:
                    throw new IOException(
                            "Cobble state lookup " + groupLabel + " semantic type is unsupported.");
            }
        }

        private Object toExternal(int index, Object internal) {
            if (internal == null) {
                return null;
            }
            try {
                return converters.get(index).toExternalOrNull(internal);
            } catch (RuntimeException ignored) {
                return internal;
            }
        }

        byte[] serializeObject(Object object) throws IOException {
            DataOutputSerializer output = new DataOutputSerializer(64);
            serializer.serialize(object, output);
            return output.getCopyOfBuffer();
        }

        @SuppressWarnings("unchecked")
        private static TypeSerializer<Object> restoreSerializer(
                SerializerInspectSchema serializerSchema,
                String groupLabel,
                ClassLoader classLoader)
                throws IOException {
            if (serializerSchema == null) {
                throw new IOException(
                        "Cobble state lookup " + groupLabel + " serializer is missing.");
            }
            TypeSerializer<Object> serializer =
                    (TypeSerializer<Object>) serializerSchema.restoreSerializer(classLoader);
            if (serializer == null) {
                throw new IOException(
                        "Cobble state lookup "
                                + groupLabel
                                + " serializer "
                                + serializerSchema.serializerClassName()
                                + " could not be restored.");
            }
            // The sidecar captured the writer serializer length; if the restored serializer reports
            // a different fixed length, the layout would mismatch, so fail fast.
            int restoredLength = safeLength(serializer);
            if (restoredLength != serializerSchema.lengthTag()) {
                throw new IOException(
                        "Cobble state lookup "
                                + groupLabel
                                + " serializer "
                                + serializerSchema.serializerClassName()
                                + " length mismatch: sidecar lengthTag="
                                + serializerSchema.lengthTag()
                                + " but restored getLength()="
                                + restoredLength
                                + ".");
            }
            return serializer;
        }

        private static int safeLength(TypeSerializer<?> serializer) {
            try {
                return serializer.getLength();
            } catch (RuntimeException e) {
                return -1;
            }
        }

        private static List<LogicalType> flattenedLogicalTypes(
                StateInspectType type, String groupLabel) throws IOException {
            List<LogicalType> types = new ArrayList<>();
            if (type == null || type.kind() == StateInspectTypeKind.UNKNOWN) {
                throw new IOException(
                        "Cobble state lookup " + groupLabel + " semantic type is missing.");
            }
            switch (type.kind()) {
                case SCALAR:
                    types.add(parse(type.logicalType()));
                    break;
                case ROW:
                case TUPLE:
                    for (io.cobble.flink.common.inspect.StateInspectField field : type.fields()) {
                        if (field.type().kind() != StateInspectTypeKind.SCALAR) {
                            throw new IOException(
                                    "Cobble state lookup "
                                            + groupLabel
                                            + " has a nested non-scalar field, which is unsupported.");
                        }
                        types.add(parse(field.type().logicalType()));
                    }
                    break;
                case LIST:
                case UNKNOWN:
                default:
                    throw new IOException(
                            "Cobble state lookup " + groupLabel + " semantic type is unsupported.");
            }
            return types;
        }

        private static LogicalType parse(String logicalType) {
            return LogicalTypeParser.parse(
                    logicalType, CobbleStateLookupKeyEncoder.class.getClassLoader());
        }

        private static StateSourceField.Group groupForLabel(
                String groupLabel, StateSourceConfig config) {
            switch (groupLabel) {
                case "state key":
                    return StateSourceField.Group.STATE_KEY;
                case "namespace":
                    return StateSourceField.Group.NAMESPACE;
                case "map key":
                    return StateSourceField.Group.MAP_KEY;
                default:
                    throw new IllegalArgumentException("Unknown group label: " + groupLabel);
            }
        }
    }
}
