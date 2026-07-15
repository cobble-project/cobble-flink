package io.cobble.flink.inspect;

import java.util.Base64;
import java.util.Objects;

/** Scalar typed input used by schema-aware scan filters and exact lookup keys. */
public final class TypedValue {
    public enum Kind {
        STRING,
        BOOLEAN,
        INTEGER,
        FLOAT,
        DECIMAL,
        BYTES,
        DATE,
        TIME,
        TIMESTAMP
    }

    private final Kind kind;
    private final String text;

    private TypedValue(Kind kind, String text) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.text = Objects.requireNonNull(text, "text");
    }

    public static TypedValue string(String value) {
        return new TypedValue(Kind.STRING, value);
    }

    public static TypedValue bool(boolean value) {
        return new TypedValue(Kind.BOOLEAN, String.valueOf(value));
    }

    public static TypedValue integer(long value) {
        return new TypedValue(Kind.INTEGER, String.valueOf(value));
    }

    public static TypedValue decimal(String value) {
        return new TypedValue(Kind.DECIMAL, value);
    }

    public static TypedValue floating(double value) {
        return new TypedValue(Kind.FLOAT, String.valueOf(value));
    }

    public static TypedValue bytes(byte[] value) {
        return new TypedValue(
                Kind.BYTES,
                Base64.getEncoder().encodeToString(Objects.requireNonNull(value, "value")));
    }

    public static TypedValue date(String isoDate) {
        return new TypedValue(Kind.DATE, isoDate);
    }

    public static TypedValue time(String isoTime) {
        return new TypedValue(Kind.TIME, isoTime);
    }

    public static TypedValue timestamp(String isoTimestamp) {
        return new TypedValue(Kind.TIMESTAMP, isoTimestamp);
    }

    public Kind kind() {
        return kind;
    }

    public String text() {
        return text;
    }
}
