package io.cobble.flink.common.inspect;

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
