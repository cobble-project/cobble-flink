package io.cobble.flink.inspect;

import java.util.Objects;

/** A read-only inspection failure with a machine-readable category. */
public final class InspectException extends RuntimeException {
    private final InspectErrorCode errorCode;

    public InspectException(InspectErrorCode errorCode, String message) {
        super(message);
        this.errorCode = Objects.requireNonNull(errorCode, "errorCode");
    }

    public InspectException(InspectErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = Objects.requireNonNull(errorCode, "errorCode");
    }

    public InspectErrorCode errorCode() {
        return errorCode;
    }
}
