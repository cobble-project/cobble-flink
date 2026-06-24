package io.cobble.flink.state;

import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Extracts optional SQL-oriented semantic state shapes without linking the generic backend to Flink
 * Table runtime classes.
 */
final class StateInspectSemanticSchemaExtractor {

    private static final String INTERNAL_TYPE_INFO_CLASS =
            "org.apache.flink.table.runtime.typeutils.InternalTypeInfo";
    private static final String ROW_DATA_SERIALIZER_CLASS =
            "org.apache.flink.table.runtime.typeutils.RowDataSerializer";

    private StateInspectSemanticSchemaExtractor() {}

    static StateInspectSemanticSchema forValue(
            TypeSerializer<?> stateKeySerializer,
            TypeSerializer<?> namespaceSerializer,
            ValueStateDescriptor<?> descriptor) {
        return StateInspectSemanticSchema.forValue(
                fromSerializer(stateKeySerializer),
                fromSerializer(namespaceSerializer),
                firstKnown(
                        describeDescriptorType(descriptor),
                        fromValueDescriptorSerializer(descriptor)));
    }

    static StateInspectSemanticSchema forReducing(
            TypeSerializer<?> stateKeySerializer,
            TypeSerializer<?> namespaceSerializer,
            ReducingStateDescriptor<?> descriptor) {
        return StateInspectSemanticSchema.forReducing(
                fromSerializer(stateKeySerializer),
                fromSerializer(namespaceSerializer),
                firstKnown(
                        describeDescriptorType(descriptor),
                        fromReducingDescriptorSerializer(descriptor)));
    }

    /**
     * Aggregating state stores the accumulator (ACC), so the value slot describes the accumulator
     * shape, not the output (OUT). We only consult the descriptor's value serializer (which is the
     * accumulator serializer); the descriptor's declared {@code typeInfo} is the output type and
     * would be misleading here.
     */
    static StateInspectSemanticSchema forAggregating(
            TypeSerializer<?> stateKeySerializer,
            TypeSerializer<?> namespaceSerializer,
            AggregatingStateDescriptor<?, ?, ?> descriptor) {
        return StateInspectSemanticSchema.forAggregating(
                fromSerializer(stateKeySerializer),
                fromSerializer(namespaceSerializer),
                firstKnown(
                        fromAggregatingDescriptorSerializer(descriptor),
                        StateInspectType.unknown()));
    }

    static StateInspectSemanticSchema forList(
            TypeSerializer<?> stateKeySerializer,
            TypeSerializer<?> namespaceSerializer,
            ListStateDescriptor<?> descriptor) {
        StateInspectType elementType = null;
        TypeInformation<?> typeInfo = descriptorTypeInfo(descriptor);
        if (typeInfo instanceof ListTypeInfo) {
            elementType = describeTypeInfo(((ListTypeInfo<?>) typeInfo).getElementTypeInfo());
        }
        return StateInspectSemanticSchema.forList(
                fromSerializer(stateKeySerializer),
                fromSerializer(namespaceSerializer),
                firstKnown(elementType, fromListElementSerializer(descriptor)));
    }

    static StateInspectSemanticSchema forMap(
            TypeSerializer<?> stateKeySerializer,
            TypeSerializer<?> namespaceSerializer,
            MapStateDescriptor<?, ?> descriptor) {
        StateInspectType mapKeyType = null;
        StateInspectType mapValueType = null;
        TypeInformation<?> typeInfo = descriptorTypeInfo(descriptor);
        if (typeInfo instanceof MapTypeInfo) {
            MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) typeInfo;
            mapKeyType = describeTypeInfo(mapTypeInfo.getKeyTypeInfo());
            mapValueType = describeTypeInfo(mapTypeInfo.getValueTypeInfo());
        }
        return StateInspectSemanticSchema.forMap(
                fromSerializer(stateKeySerializer),
                fromSerializer(namespaceSerializer),
                firstKnown(mapKeyType, fromMapKeySerializer(descriptor)),
                firstKnown(mapValueType, fromMapValueSerializer(descriptor)));
    }

    static StateInspectSemanticSchema forTimer(
            TypeSerializer<?> keySerializer, TypeSerializer<?> namespaceSerializer) {
        return StateInspectSemanticSchema.forValue(
                fromSerializer(keySerializer),
                fromSerializer(namespaceSerializer),
                StateInspectType.unknown());
    }

    private static StateInspectType describeDescriptorType(StateDescriptor<?, ?> descriptor) {
        return describeTypeInfo(descriptorTypeInfo(descriptor));
    }

    private static StateInspectType fromValueDescriptorSerializer(
            ValueStateDescriptor<?> descriptor) {
        try {
            return fromSerializer(descriptor.getSerializer());
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static StateInspectType fromReducingDescriptorSerializer(
            ReducingStateDescriptor<?> descriptor) {
        try {
            return fromSerializer(descriptor.getSerializer());
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static StateInspectType fromAggregatingDescriptorSerializer(
            AggregatingStateDescriptor<?, ?, ?> descriptor) {
        try {
            // getSerializer() returns the accumulator serializer for AggregatingStateDescriptor.
            return fromSerializer(descriptor.getSerializer());
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static StateInspectType fromListElementSerializer(ListStateDescriptor<?> descriptor) {
        try {
            return fromSerializer(descriptor.getElementSerializer());
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static StateInspectType fromMapKeySerializer(MapStateDescriptor<?, ?> descriptor) {
        try {
            return fromSerializer(descriptor.getKeySerializer());
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static StateInspectType fromMapValueSerializer(MapStateDescriptor<?, ?> descriptor) {
        try {
            return fromSerializer(descriptor.getValueSerializer());
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static TypeInformation<?> descriptorTypeInfo(StateDescriptor<?, ?> descriptor) {
        try {
            Field field = StateDescriptor.class.getDeclaredField("typeInfo");
            field.setAccessible(true);
            Object typeInfo = field.get(descriptor);
            return typeInfo instanceof TypeInformation ? (TypeInformation<?>) typeInfo : null;
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            return null;
        }
    }

    private static StateInspectType describeTypeInfo(TypeInformation<?> typeInfo) {
        if (typeInfo == null) {
            return null;
        }
        try {
            if (INTERNAL_TYPE_INFO_CLASS.equals(typeInfo.getClass().getName())) {
                return describeInternalRowType(typeInfo);
            }
            if (typeInfo instanceof ListTypeInfo) {
                return StateInspectType.list(
                        firstKnown(
                                describeTypeInfo(((ListTypeInfo<?>) typeInfo).getElementTypeInfo()),
                                StateInspectType.unknown()));
            }
            if (typeInfo instanceof TupleTypeInfoBase) {
                return describeTupleType((TupleTypeInfoBase<?>) typeInfo);
            }
            return scalarForClass(typeInfo.getTypeClass());
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            return null;
        }
    }

    private static StateInspectType describeInternalRowType(Object internalTypeInfo)
            throws ReflectiveOperationException {
        Method toRowType = internalTypeInfo.getClass().getMethod("toRowType");
        Object rowType = toRowType.invoke(internalTypeInfo);
        Method getFields = rowType.getClass().getMethod("getFields");
        List<?> rowFields = (List<?>) getFields.invoke(rowType);
        List<StateInspectField> fields = new ArrayList<>(rowFields.size());
        for (Object rowField : rowFields) {
            Method getName = rowField.getClass().getMethod("getName");
            Method getType = rowField.getClass().getMethod("getType");
            fields.add(
                    new StateInspectField(
                            (String) getName.invoke(rowField),
                            StateInspectType.scalar(logicalType(getType.invoke(rowField)))));
        }
        return fields.isEmpty() ? StateInspectType.unknown() : StateInspectType.row(fields);
    }

    private static StateInspectType describeTupleType(TupleTypeInfoBase<?> tupleTypeInfo) {
        TypeInformation<?>[] fieldTypes = tupleTypeInfo.getFieldTypes();
        List<StateInspectField> fields = new ArrayList<>(fieldTypes.length);
        for (int i = 0; i < fieldTypes.length; i++) {
            fields.add(
                    new StateInspectField(
                            "f" + i,
                            firstKnown(
                                    describeTypeInfo(fieldTypes[i]), StateInspectType.unknown())));
        }
        return fields.isEmpty() ? StateInspectType.unknown() : StateInspectType.tuple(fields);
    }

    private static StateInspectType fromSerializer(TypeSerializer<?> serializer) {
        if (serializer == null) {
            return null;
        }
        try {
            if (serializer instanceof ListSerializer) {
                ListSerializer<?> listSerializer = (ListSerializer<?>) serializer;
                return StateInspectType.list(
                        firstKnown(
                                fromSerializer(listSerializer.getElementSerializer()),
                                StateInspectType.unknown()));
            }
            if (ROW_DATA_SERIALIZER_CLASS.equals(serializer.getClass().getName())) {
                return describeRowDataSerializer(serializer);
            }
            return scalarForSerializer(serializer);
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            return null;
        }
    }

    private static StateInspectType describeRowDataSerializer(TypeSerializer<?> serializer)
            throws ReflectiveOperationException {
        Field typesField = serializer.getClass().getDeclaredField("types");
        typesField.setAccessible(true);
        Object types = typesField.get(serializer);
        int fieldCount = Array.getLength(types);
        List<StateInspectField> fields = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            fields.add(
                    new StateInspectField(
                            "f" + i, StateInspectType.scalar(logicalType(Array.get(types, i)))));
        }
        return fields.isEmpty() ? StateInspectType.unknown() : StateInspectType.row(fields);
    }

    private static String logicalType(Object logicalType) throws ReflectiveOperationException {
        Method method = logicalType.getClass().getMethod("asSerializableString");
        return (String) method.invoke(logicalType);
    }

    private static StateInspectType scalarForSerializer(TypeSerializer<?> serializer) {
        String className = serializer.getClass().getName();
        if (className.endsWith("IntSerializer")) {
            return StateInspectType.scalar("INT");
        }
        if (className.endsWith("LongSerializer")) {
            return StateInspectType.scalar("BIGINT");
        }
        if (className.endsWith("BooleanSerializer")) {
            return StateInspectType.scalar("BOOLEAN");
        }
        if (className.endsWith("StringSerializer")) {
            return StateInspectType.scalar("VARCHAR");
        }
        if (className.endsWith("FloatSerializer")) {
            return StateInspectType.scalar("FLOAT");
        }
        if (className.endsWith("DoubleSerializer")) {
            return StateInspectType.scalar("DOUBLE");
        }
        return StateInspectType.unknown();
    }

    private static StateInspectType scalarForClass(Class<?> typeClass) {
        if (typeClass == Integer.class || typeClass == Integer.TYPE) {
            return StateInspectType.scalar("INT");
        }
        if (typeClass == Long.class || typeClass == Long.TYPE) {
            return StateInspectType.scalar("BIGINT");
        }
        if (typeClass == Boolean.class || typeClass == Boolean.TYPE) {
            return StateInspectType.scalar("BOOLEAN");
        }
        if (typeClass == String.class) {
            return StateInspectType.scalar("VARCHAR");
        }
        if (typeClass == Float.class || typeClass == Float.TYPE) {
            return StateInspectType.scalar("FLOAT");
        }
        if (typeClass == Double.class || typeClass == Double.TYPE) {
            return StateInspectType.scalar("DOUBLE");
        }
        return StateInspectType.unknown();
    }

    private static StateInspectType firstKnown(
            StateInspectType preferred, StateInspectType fallback) {
        if (preferred != null
                && preferred.kind()
                        != io.cobble.flink.common.inspect.StateInspectTypeKind.UNKNOWN) {
            return preferred;
        }
        return fallback;
    }
}
