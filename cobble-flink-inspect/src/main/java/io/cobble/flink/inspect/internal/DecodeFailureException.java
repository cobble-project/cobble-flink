package io.cobble.flink.inspect.internal;

import java.io.IOException;

/**
 * An {@link IOException} carrying a structured {@link DecodeIssueKind}, thrown at the source of a
 * decode failure so that catch sites in {@code StateInspectDecoder} can record a typed {@code
 * decode_issues} entry without classifying exception messages.
 *
 * <p>Callers that need to wrap a child failure with context (e.g. "POJO field 'x': ...") should use
 * {@link #wrap} so that the original kind is preserved.
 */
final class DecodeFailureException extends IOException {

    private static final long serialVersionUID = 1L;

    private final DecodeIssueKind kind;

    DecodeFailureException(DecodeIssueKind kind, String message) {
        super(message);
        this.kind = kind;
    }

    DecodeFailureException(DecodeIssueKind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    DecodeIssueKind kind() {
        return kind;
    }

    static DecodeFailureException malformed(String message) {
        return new DecodeFailureException(DecodeIssueKind.MALFORMED_BYTES, message);
    }

    static DecodeFailureException malformed(String message, Throwable cause) {
        return new DecodeFailureException(DecodeIssueKind.MALFORMED_BYTES, message, cause);
    }

    /**
     * Wraps {@code cause} with a prefixed message, preserving the original {@link DecodeIssueKind}
     * when {@code cause} is itself a {@link DecodeFailureException}. For a plain {@link
     * IOException} the result defaults to {@link DecodeIssueKind#MALFORMED_BYTES}, matching the
     * contract that untyped wire-format failures are treated as malformed input.
     */
    static DecodeFailureException wrap(String prefix, IOException cause) {
        if (cause instanceof DecodeFailureException) {
            DecodeFailureException typed = (DecodeFailureException) cause;
            return new DecodeFailureException(
                    typed.kind, prefix + ": " + typed.getMessage(), typed);
        }
        return new DecodeFailureException(
                DecodeIssueKind.MALFORMED_BYTES, prefix + ": " + cause.getMessage(), cause);
    }
}
