package io.cobble.flink.state;

import io.cobble.flink.common.inspect.SerializerInspectSchema;
import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.StateInspectTypeKind;

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Extracts optional SQL-oriented semantic state shapes.
 *
 * <p>The primary source is the snapshot-derived {@link StateInspectType} stored on each {@link
 * SerializerInspectSchema} (captured by {@link
 * io.cobble.flink.common.inspect.SerializerSnapshotInspectTypeExtractor}). Descriptor {@code
 * TypeInformation} is only used as an overlay for field names and more precise logical types (e.g.
 * {@code VARCHAR(100)}, {@code DECIMAL(p,s)}) when it has extra information and the arity matches.
 *
 * <p>This class no longer reflects on live serializers — all structural type information comes from
 * the snapshot-derived inspect type.
 */
final class StateInspectSemanticSchemaExtractor {

    private static final String INTERNAL_TYPE_INFO_CLASS =
            "org.apache.flink.table.runtime.typeutils.InternalTypeInfo";

    private StateInspectSemanticSchemaExtractor() {}

    static StateInspectSemanticSchema forValue(
            SerializerInspectSchema stateKeySerializer,
            SerializerInspectSchema namespaceSerializer,
            SerializerInspectSchema valueSerializer,
            ValueStateDescriptor<?> descriptor) {
        return StateInspectSemanticSchema.forValue(
                inspectTypeOrUnknown(stateKeySerializer),
                inspectTypeOrUnknown(namespaceSerializer),
                overlayDescriptor(
                        firstKnown(
                                inspectTypeOrUnknown(valueSerializer),
                                describeDescriptorType(descriptor)),
                        describeDescriptorType(descriptor)));
    }

    static StateInspectSemanticSchema forValue(
            SerializerInspectSchema stateKeySerializer,
            SerializerInspectSchema namespaceSerializer,
            SerializerInspectSchema valueSerializer) {
        return StateInspectSemanticSchema.forValue(
                inspectTypeOrUnknown(stateKeySerializer),
                inspectTypeOrUnknown(namespaceSerializer),
                inspectTypeOrUnknown(valueSerializer));
    }

    static StateInspectSemanticSchema forReducing(
            SerializerInspectSchema stateKeySerializer,
            SerializerInspectSchema namespaceSerializer,
            SerializerInspectSchema valueSerializer,
            ReducingStateDescriptor<?> descriptor) {
        return StateInspectSemanticSchema.forReducing(
                inspectTypeOrUnknown(stateKeySerializer),
                inspectTypeOrUnknown(namespaceSerializer),
                overlayDescriptor(
                        firstKnown(
                                inspectTypeOrUnknown(valueSerializer),
                                describeDescriptorType(descriptor)),
                        describeDescriptorType(descriptor)));
    }

    static StateInspectSemanticSchema forReducing(
            SerializerInspectSchema stateKeySerializer,
            SerializerInspectSchema namespaceSerializer,
            SerializerInspectSchema valueSerializer) {
        return StateInspectSemanticSchema.forReducing(
                inspectTypeOrUnknown(stateKeySerializer),
                inspectTypeOrUnknown(namespaceSerializer),
                inspectTypeOrUnknown(valueSerializer));
    }

    /**
     * Aggregating state stores the accumulator (ACC), so the value slot describes the accumulator
     * shape, not the output (OUT). We only consult the value serializer's inspect type (which is
     * the accumulator serializer); the descriptor's declared {@code typeInfo} is the output type and
     * would be misleading here.
     */
    static StateInspectSemanticSchema forAggregating(
            SerializerInspectSchema stateKeySerializer,
            SerializerInspectSchema namespaceSerializer,
            SerializerInspectSchema valueSerializer,
            AggregatingStateDescriptor<?, ?, ?> descriptor) {
        return StateInspectSemanticSchema.forAggregating(
                inspectTypeOrUnknown(stateKeySerializer),
                inspectTypeOrUnknown(namespaceSerializer),
                firstKnown(inspectTypeOrUnknown(valueSerializer), StateInspectType.unknown()));
    }

    static StateInspectSemanticSchema forAggregating(
            SerializerInspectSchema stateKeySerializer,
            SerializerInspectSchema namespaceSerializer,
            SerializerInspectSchema valueSerializer) {
        return StateInspectSemanticSchema.forAggregating(
                inspectTypeOrUnknown(stateKeySerializer),
                inspectTypeOrUnknown(namespaceSerializer),
                inspectTypeOrUnknown(valueSerializer));
    }

    static StateInspectSemanticSchema forList(
            SerializerInspectSchema stateKeySerializer,
            SerializerInspectSchema namespaceSerializer,
            SerializerInspectSchema elementSerializer,
            ListStateDescriptor<?> descriptor) {
        StateInspectType elementType = inspectTypeOrUnknown(elementSerializer);
        // Descriptor ListTypeInfo may carry a more specific element type.
        TypeInformation<?> typeInfo = descriptorTypeInfo(descriptor);
        if (typeInfo instanceof ListTypeInfo) {
            StateInspectType descriptorElementType =
                    describeTypeInfo(((ListTypeInfo<?>) typeInfo).getElementTypeInfo());
            elementType = firstKnown(elementType, descriptorElementType);
            elementType = overlayDescriptor(elementType, descriptorElementType);
        }
        return StateInspectSemanticSchema.forList(
                inspectTypeOrUnknown(stateKeySerializer),
                inspectTypeOrUnknown(namespaceSerializer),
                elementType);
    }

    static StateInspectSemanticSchema forList(
            SerializerInspectSchema stateKeySerializer,
            SerializerInspectSchema namespaceSerializer,
            SerializerInspectSchema elementSerializer) {
        return StateInspectSemanticSchema.forList(
                inspectTypeOrUnknown(stateKeySerializer),
                inspectTypeOrUnknown(namespaceSerializer),
                inspectTypeOrUnknown(elementSerializer));
    }

    static StateInspectSemanticSchema forMap(
            SerializerInspectSchema stateKeySerializer,
            SerializerInspectSchema namespaceSerializer,
            SerializerInspectSchema mapUserKeySerializer,
            SerializerInspectSchema mapUserValueSerializer,
            MapStateDescriptor<?, ?> descriptor) {
        StateInspectType mapKeyType = inspectTypeOrUnknown(mapUserKeySerializer);
        StateInspectType mapValueType = inspectTypeOrUnknown(mapUserValueSerializer);
        // Descriptor MapTypeInfo may carry more specific key/value types.
        TypeInformation<?> typeInfo = descriptorTypeInfo(descriptor);
        if (typeInfo instanceof MapTypeInfo) {
            MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) typeInfo;
            StateInspectType descriptorKeyType = describeTypeInfo(mapTypeInfo.getKeyTypeInfo());
            StateInspectType descriptorValueType = describeTypeInfo(mapTypeInfo.getValueTypeInfo());
            mapKeyType = firstKnown(mapKeyType, descriptorKeyType);
            mapValueType = firstKnown(mapValueType, descriptorValueType);
            mapKeyType = overlayDescriptor(mapKeyType, descriptorKeyType);
            mapValueType = overlayDescriptor(mapValueType, descriptorValueType);
        }
        return StateInspectSemanticSchema.forMap(
                inspectTypeOrUnknown(stateKeySerializer),
                inspectTypeOrUnknown(namespaceSerializer),
                mapKeyType,
                mapValueType);
    }

    static StateInspectSemanticSchema forMap(
            SerializerInspectSchema stateKeySerializer,
            SerializerInspectSchema namespaceSerializer,
            SerializerInspectSchema mapUserKeySerializer,
            SerializerInspectSchema mapUserValueSerializer) {
        return StateInspectSemanticSchema.forMap(
                inspectTypeOrUnknown(stateKeySerializer),
                inspectTypeOrUnknown(namespaceSerializer),
                inspectTypeOrUnknown(mapUserKeySerializer),
                inspectTypeOrUnknown(mapUserValueSerializer));
    }

    static StateInspectSemanticSchema forTimer(
            SerializerInspectSchema keySerializer,
            SerializerInspectSchema namespaceSerializer) {
        return StateInspectSemanticSchema.forValue(
                inspectTypeOrUnknown(keySerializer),
                inspectTypeOrUnknown(namespaceSerializer),
                StateInspectType.unknown());
    }

    // ---- Type resolution helpers ----

    /**
     * Returns the snapshot-derived inspect type, or {@link StateInspectType#unknown()} when the
     * schema has no inspect type (e.g. snapshot capture failed).
     */
    private static StateInspectType inspectTypeOrUnknown(SerializerInspectSchema schema) {
        if (schema == null) {
            return StateInspectType.unknown();
        }
        StateInspectType type = schema.inspectType();
        return type != null ? type : StateInspectType.unknown();
    }

    /**
     * Recursively overlays field names and structural precision from the descriptor-derived type
     * onto the snapshot-derived primary type. The snapshot type is structurally authoritative
     * (it reflects the actual serializer configuration), but may carry ordinal field names
     * ({@code f0}, {@code f1}, ...) for RowData whose field names live only in the descriptor's
     * {@code TypeInformation}.
     *
     * <p>Overlay rules by kind:
     *
     * <ul>
     *   <li>{@code ROW} (same arity): take descriptor field names, recurse on each field type.
     *   <li>{@code TUPLE} (same arity): recurse on each field type (tuple names are always
     *       {@code f0}/{@code f1} so they don't change, but nested types still need overlay).
     *   <li>{@code LIST}: recurse on the element type.
     *   <li>{@code MAP}: recurse on both key and value types.
     *   <li>{@code SCALAR} / {@code UNKNOWN} / mismatched kinds: return primary unchanged.
     * </ul>
     *
     * <p>If either type is {@code null}, the primary is returned unchanged.
     */
    private static StateInspectType overlayDescriptor(
            StateInspectType primary, StateInspectType descriptor) {
        if (primary == null || descriptor == null) {
            return primary;
        }
        switch (primary.kind()) {
            case ROW:
            case TUPLE:
                if (descriptor.kind() != primary.kind()
                        || primary.fields().size() != descriptor.fields().size()) {
                    return primary;
                }
                List<StateInspectField> overlaid =
                        new ArrayList<>(primary.fields().size());
                for (int i = 0; i < primary.fields().size(); i++) {
                    StateInspectField primaryField = primary.fields().get(i);
                    StateInspectField descriptorField = descriptor.fields().get(i);
                    overlaid.add(
                            new StateInspectField(
                                    descriptorField.name(),
                                    overlayDescriptor(primaryField.type(), descriptorField.type())));
                }
                return primary.kind() == StateInspectTypeKind.ROW
                        ? StateInspectType.row(overlaid)
                        : StateInspectType.tuple(overlaid);
            case LIST:
                if (descriptor.kind() != StateInspectTypeKind.LIST) {
                    return primary;
                }
                return StateInspectType.list(
                        overlayDescriptor(primary.elementType(), descriptor.elementType()));
            case MAP:
                if (descriptor.kind() != StateInspectTypeKind.MAP) {
                    return primary;
                }
                return StateInspectType.map(
                        overlayDescriptor(primary.keyType(), descriptor.keyType()),
                        overlayDescriptor(primary.valueType(), descriptor.valueType()));
            default:
                // SCALAR, UNKNOWN, or any future kind — no overlay.
                return primary;
        }
    }

    private static StateInspectType describeDescriptorType(StateDescriptor<?, ?> descriptor) {
        return describeTypeInfo(descriptorTypeInfo(descriptor));
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

    /**
     * Derives a {@link StateInspectType} from descriptor {@link TypeInformation}. Used only as a
     * fallback when the snapshot-derived type is {@link StateInspectTypeKind#UNKNOWN}, or as a
     * source of field-name overlay for {@code ROW} types.
     */
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

    private static String logicalType(Object logicalType) throws ReflectiveOperationException {
        Method method = logicalType.getClass().getMethod("asSerializableString");
        return (String) method.invoke(logicalType);
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
        if (preferred != null && preferred.kind() != StateInspectTypeKind.UNKNOWN) {
            return preferred;
        }
        if (fallback != null) {
            return fallback;
        }
        return StateInspectType.unknown();
    }
}
