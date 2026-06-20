package io.cobble.flink.common.inspect;

/** Shape of a decoded value used by state inspect metadata. */
public enum StateInspectTypeKind {
    SCALAR,
    ROW,
    LIST,
    TUPLE,
    UNKNOWN
}
