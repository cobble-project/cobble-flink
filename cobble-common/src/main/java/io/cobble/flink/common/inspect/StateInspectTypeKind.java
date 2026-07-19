package io.cobble.flink.common.inspect;

/**
 * Shape of a decoded value used by state inspect metadata.
 *
 * <p>Enum ordinals are persisted inside the versioned {@link StateInspectSchemaStore} format.
 */
public enum StateInspectTypeKind {
    SCALAR,
    ROW,
    LIST,
    TUPLE,
    UNKNOWN,
    MAP
}
