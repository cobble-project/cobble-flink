package io.cobble.flink.common.inspect.decode;

import java.io.IOException;

/** IOException with a stable category for classless wire-format failures. */
public final class ClasslessDecodeFailureException extends IOException {

    private static final long serialVersionUID = 1L;

    private final ClasslessDecodeIssueKind kind;

    public ClasslessDecodeFailureException(ClasslessDecodeIssueKind kind, String message) {
        super(message);
        this.kind = kind;
    }

    public ClasslessDecodeFailureException(
            ClasslessDecodeIssueKind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public ClasslessDecodeIssueKind kind() {
        return kind;
    }

    public static ClasslessDecodeFailureException malformed(String message) {
        return new ClasslessDecodeFailureException(
                ClasslessDecodeIssueKind.MALFORMED_BYTES, message);
    }

    public static ClasslessDecodeFailureException malformed(String message, Throwable cause) {
        return new ClasslessDecodeFailureException(
                ClasslessDecodeIssueKind.MALFORMED_BYTES, message, cause);
    }

    public static ClasslessDecodeFailureException wrap(String prefix, IOException cause) {
        if (cause instanceof ClasslessDecodeFailureException) {
            ClasslessDecodeFailureException typed = (ClasslessDecodeFailureException) cause;
            return new ClasslessDecodeFailureException(
                    typed.kind, prefix + ": " + typed.getMessage(), typed);
        }
        return new ClasslessDecodeFailureException(
                ClasslessDecodeIssueKind.MALFORMED_BYTES,
                prefix + ": " + cause.getMessage(),
                cause);
    }
}
