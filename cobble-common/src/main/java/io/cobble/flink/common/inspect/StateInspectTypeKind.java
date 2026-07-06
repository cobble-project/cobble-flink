package io.cobble.flink.common.inspect;

/**
 * Shape of a decoded value used by state inspect metadata.
 *
 * <p>Enum ordinals are persisted in the inspect schema store. The store format is not
 * backward-compatible across releases (see {@link StateInspectSchemaStore} version), so ordinals
 * need not be frozen indefinitely. {@code MAP} is appended after {@code UNKNOWN} only to minimize
 * churn against in-flight development, not to preserve old-format compatibility.
 */
public enum StateInspectTypeKind {
    SCALAR,
    ROW,
    LIST,
    TUPLE,
    UNKNOWN,
    MAP
}
