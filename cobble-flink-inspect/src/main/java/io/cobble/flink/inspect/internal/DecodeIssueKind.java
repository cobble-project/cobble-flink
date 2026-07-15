package io.cobble.flink.inspect.internal;

/**
 * Structured classification of a decode failure, recorded in the {@code decode_issues} array on
 * each inspected row. The kind is determined at the throw site (not by parsing exception messages)
 * so that the UI can render precise, actionable guidance.
 *
 * <ul>
 *   <li>{@link #CLASSLESS_UNSUPPORTED} - the descriptor's wire format cannot be decoded without
 *       user classes (e.g. a non-registered POJO subclass). A trusted {@code --user-jar} may enable
 *       the row.
 *   <li>{@link #SERIALIZER_RESTORE_FAILED} - the live serializer could not be restored or a {@link
 *       NoClassDefFoundError} identified a missing serializer dependency. The {@code --user-jar}
 *       needs to include the serializer class and its dependencies.
 *   <li>{@link #MALFORMED_BYTES} - the serialized bytes are truncated, have an invalid flag, or
 *       contain trailing data. This is a data-level error, not a classpath issue.
 *   <li>{@link #UNKNOWN} - an unexpected exception that does not match the above categories.
 * </ul>
 */
public enum DecodeIssueKind {
    CLASSLESS_UNSUPPORTED,
    SERIALIZER_RESTORE_FAILED,
    MALFORMED_BYTES,
    UNKNOWN
}
