package io.cobble.flink.inspect;

/** One named value in an ordered semantic key group. */
public final class FieldValue {
    private final String name;
    private final TypedValue value;

    public FieldValue(String name, TypedValue value) {
        this.name = name;
        this.value = value;
    }

    public String name() {
        return name;
    }

    public TypedValue value() {
        return value;
    }
}
