package io.cobble.flink.table;

import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.StateInspectTypeKind;
import io.cobble.flink.common.inspect.StateSourceSchemaLayout;
import io.cobble.flink.common.inspect.decode.ClasslessPojoValue;

import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Session-owned recursive conversion tree shared by classless and restored serializer values. */
final class StateSemanticValueConverter {
    private final StateInspectType type;
    private final LogicalType logicalType;
    private final List<StateSemanticValueConverter> children = new ArrayList<>();
    private final DataStructureConverter<Object, Object> scalar;
    private final RowData.FieldGetter[] fieldGetters;
    private Class<?> pojoClass;
    private Field[] pojoFields;

    int flattenedArity() {
        return type.kind() == StateInspectTypeKind.ROW || type.kind() == StateInspectTypeKind.TUPLE
                ? children.size()
                : 1;
    }

    StateSemanticValueConverter(StateInspectType type) throws IOException {
        this.type = type;
        try {
            this.logicalType =
                    LogicalTypeParser.parse(
                            StateSourceSchemaLayout.logicalType(type), getClass().getClassLoader());
        } catch (RuntimeException unsupported) {
            throw new IOException("Unsupported or missing semantic type", unsupported);
        }
        switch (type.kind()) {
            case ROW:
            case TUPLE:
                for (StateInspectField field : type.fields())
                    children.add(new StateSemanticValueConverter(field.type()));
                break;
            case LIST:
                children.add(new StateSemanticValueConverter(type.elementType()));
                break;
            case MAP:
                children.add(new StateSemanticValueConverter(type.keyType()));
                children.add(new StateSemanticValueConverter(type.valueType()));
                break;
            default:
                break;
        }
        scalar =
                type.kind() == StateInspectTypeKind.SCALAR
                        ? DataStructureConverters.getConverter(
                                TypeConversions.fromLogicalToDataType(logicalType))
                        : null;
        fieldGetters = new RowData.FieldGetter[children.size()];
        if (type.kind() == StateInspectTypeKind.ROW || type.kind() == StateInspectTypeKind.TUPLE) {
            for (int i = 0; i < children.size(); i++)
                fieldGetters[i] = RowData.createFieldGetter(children.get(i).logicalType, i);
        }
    }

    Object[] flatten(Object value, boolean[] selected) throws IOException {
        Object[] result = new Object[selected.length];
        if (value == null
                || value instanceof ClasslessPojoValue && ((ClasslessPojoValue) value).isNull())
            return result;
        if (type.kind() == StateInspectTypeKind.ROW || type.kind() == StateInspectTypeKind.TUPLE) {
            for (int i = 0; i < selected.length; i++) {
                if (selected[i])
                    result[i] =
                            value instanceof RowData
                                    ? fieldGetters[i].getFieldOrNull((RowData) value)
                                    : children.get(i).convert(field(value, i));
            }
        } else if (selected[0]) {
            result[0] = convert(value);
        }
        return result;
    }

    private Object convert(Object value) throws IOException {
        if (value == null) return null;
        switch (type.kind()) {
            case ROW:
            case TUPLE:
                if (value instanceof RowData) return value;
                if (value instanceof ClasslessPojoValue && ((ClasslessPojoValue) value).isNull())
                    return null;
                GenericRowData row = new GenericRowData(children.size());
                for (int i = 0; i < children.size(); i++)
                    row.setField(i, children.get(i).convert(field(value, i)));
                return row;
            case LIST:
                if (value instanceof ArrayData) return value;
                List<Object> items = new ArrayList<>();
                if (value instanceof Iterable) {
                    for (Object item : (Iterable<?>) value)
                        items.add(children.get(0).convert(item));
                } else if (value.getClass().isArray()) {
                    for (int i = 0; i < Array.getLength(value); i++)
                        items.add(children.get(0).convert(Array.get(value, i)));
                } else throw new IOException("Expected collection or array for " + logicalType);
                return new GenericArrayData(items.toArray());
            case MAP:
                if (value instanceof MapData) return value;
                if (!(value instanceof Map))
                    throw new IOException("Expected Map for " + logicalType);
                Map<Object, Object> map = new LinkedHashMap<>();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                    map.put(
                            children.get(0).convert(entry.getKey()),
                            children.get(1).convert(entry.getValue()));
                }
                return new GenericMapData(map);
            case SCALAR:
                if (logicalType.getTypeRoot() == LogicalTypeRoot.ROW && value instanceof RowData
                        || logicalType.getTypeRoot() == LogicalTypeRoot.ARRAY
                                && value instanceof ArrayData
                        || logicalType.getTypeRoot() == LogicalTypeRoot.MAP
                                && value instanceof MapData) return value;
                return scalar.toInternalOrNull(normalize(value));
            default:
                throw new IOException("Unsupported semantic type " + type.kind());
        }
    }

    private Object field(Object value, int index) throws IOException {
        String name = type.fields().get(index).name();
        if (value instanceof ClasslessPojoValue) {
            Map<String, Object> fields = ((ClasslessPojoValue) value).fields();
            if (!fields.containsKey(name))
                throw new IOException("Classless POJO field '" + name + "' is missing");
            return fields.get(name);
        }
        if (value instanceof IndexedRecord) return ((IndexedRecord) value).get(index);
        if (value instanceof Tuple) return ((Tuple) value).getField(index);
        if (value instanceof Row) return ((Row) value).getField(index);
        try {
            if (pojoClass != value.getClass()) {
                pojoFields = new Field[children.size()];
                pojoClass = value.getClass();
            }
            if (pojoFields[index] == null) {
                Class<?> owner = pojoClass;
                while (owner != null) {
                    try {
                        Field field = owner.getDeclaredField(name);
                        field.setAccessible(true);
                        pojoFields[index] = field;
                        break;
                    } catch (NoSuchFieldException missing) {
                        owner = owner.getSuperclass();
                    }
                }
                if (pojoFields[index] == null)
                    throw new IOException("POJO field '" + name + "' is missing");
            }
            return pojoFields[index].get(value);
        } catch (IllegalAccessException | RuntimeException error) {
            throw new IOException("Unable to read semantic field '" + name + "'", error);
        }
    }

    private static Object normalize(Object value) {
        if (value instanceof Utf8 || value instanceof GenericEnumSymbol) return value.toString();
        if (value instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) value).duplicate();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
        return value instanceof GenericFixed ? ((GenericFixed) value).bytes() : value;
    }
}
