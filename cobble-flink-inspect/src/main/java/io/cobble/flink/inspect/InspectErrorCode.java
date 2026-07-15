package io.cobble.flink.inspect;

/** Stable categories for SDK failures. */
public enum InspectErrorCode {
    INVALID_INPUT,
    NOT_FOUND,
    UNREADABLE,
    UNSUPPORTED,
    INTERNAL,
    CLOSED
}
