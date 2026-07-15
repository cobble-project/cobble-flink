package io.cobble.flink.inspect;

import java.util.Objects;

/** A row-local decode warning; raw bytes remain available when it is present. */
public final class DecodeIssue {
    public enum Kind {
        DECODE,
        SERIALIZER,
        SCHEMA,
        UNKNOWN
    }

    private final Kind kind;
    private final String part;
    private final String message;

    public DecodeIssue(String message) {
        this(Kind.UNKNOWN, null, message);
    }

    public DecodeIssue(Kind kind, String part, String message) {
        this.kind = kind == null ? Kind.UNKNOWN : kind;
        this.part = part;
        this.message = Objects.requireNonNull(message, "message");
    }

    public Kind kind() {
        return kind;
    }

    public String part() {
        return part;
    }

    public String message() {
        return message;
    }
}
