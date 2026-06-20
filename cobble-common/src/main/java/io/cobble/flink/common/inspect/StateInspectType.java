package io.cobble.flink.common.inspect;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Recursive, serializer-independent type description for state inspect. */
public final class StateInspectType {

    private final StateInspectTypeKind kind;
    private final String logicalType;
    private final List<StateInspectField> fields;
    private final StateInspectType elementType;

    private StateInspectType(
            StateInspectTypeKind kind,
            String logicalType,
            List<StateInspectField> fields,
            StateInspectType elementType) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.logicalType = logicalType;
        this.fields = immutableCopy(fields);
        this.elementType = elementType;
    }

    public static StateInspectType scalar(String logicalType) {
        if (logicalType == null || logicalType.trim().isEmpty()) {
            throw new IllegalArgumentException("Scalar logical type must not be empty");
        }
        return new StateInspectType(
                StateInspectTypeKind.SCALAR, logicalType, Collections.emptyList(), null);
    }

    public static StateInspectType row(List<StateInspectField> fields) {
        return structured(StateInspectTypeKind.ROW, fields);
    }

    public static StateInspectType tuple(List<StateInspectField> fields) {
        return structured(StateInspectTypeKind.TUPLE, fields);
    }

    public static StateInspectType list(StateInspectType elementType) {
        return new StateInspectType(
                StateInspectTypeKind.LIST,
                null,
                Collections.emptyList(),
                Objects.requireNonNull(elementType, "elementType"));
    }

    public static StateInspectType unknown() {
        return new StateInspectType(
                StateInspectTypeKind.UNKNOWN, null, Collections.emptyList(), null);
    }

    private static StateInspectType structured(
            StateInspectTypeKind kind, List<StateInspectField> fields) {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException(kind + " state inspect type requires fields");
        }
        return new StateInspectType(kind, null, fields, null);
    }

    public StateInspectTypeKind kind() {
        return kind;
    }

    public String logicalType() {
        return logicalType;
    }

    public List<StateInspectField> fields() {
        return fields;
    }

    public StateInspectType elementType() {
        return elementType;
    }

    void write(DataOutputView output) throws IOException {
        output.writeInt(kind.ordinal());
        switch (kind) {
            case SCALAR:
                output.writeUTF(logicalType);
                break;
            case ROW:
            case TUPLE:
                output.writeInt(fields.size());
                for (StateInspectField field : fields) {
                    field.write(output);
                }
                break;
            case LIST:
                elementType.write(output);
                break;
            case UNKNOWN:
                break;
        }
    }

    static StateInspectType read(DataInputView input) throws IOException {
        StateInspectTypeKind kind = typeKind(input.readInt());
        switch (kind) {
            case SCALAR:
                return scalar(input.readUTF());
            case ROW:
                return row(readFields(input));
            case TUPLE:
                return tuple(readFields(input));
            case LIST:
                return list(read(input));
            case UNKNOWN:
                return unknown();
            default:
                throw new IOException("Unsupported state inspect type kind: " + kind);
        }
    }

    private static StateInspectTypeKind typeKind(int ordinal) throws IOException {
        StateInspectTypeKind[] kinds = StateInspectTypeKind.values();
        if (ordinal < 0 || ordinal >= kinds.length) {
            throw new IOException("Unknown state inspect type kind ordinal: " + ordinal);
        }
        return kinds[ordinal];
    }

    private static List<StateInspectField> readFields(DataInputView input) throws IOException {
        int count = input.readInt();
        if (count <= 0) {
            throw new IOException("Structured state inspect type requires fields");
        }
        List<StateInspectField> fields = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            fields.add(StateInspectField.read(input));
        }
        return fields;
    }

    private static List<StateInspectField> immutableCopy(List<StateInspectField> fields) {
        if (fields == null || fields.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(fields));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StateInspectType)) {
            return false;
        }
        StateInspectType that = (StateInspectType) other;
        return kind == that.kind
                && Objects.equals(logicalType, that.logicalType)
                && Objects.equals(fields, that.fields)
                && Objects.equals(elementType, that.elementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, logicalType, fields, elementType);
    }
}
