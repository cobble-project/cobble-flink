package io.cobble.flink.common.inspect;

import java.util.Objects;

/** A named child of a structured {@link StateInspectType}. */
public final class StateInspectField {

    private final String name;
    private final StateInspectType type;

    public StateInspectField(String name, StateInspectType type) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("State inspect field name must not be empty");
        }
        this.name = name;
        this.type = Objects.requireNonNull(type, "type");
    }

    public String name() {
        return name;
    }

    public StateInspectType type() {
        return type;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StateInspectField)) {
            return false;
        }
        StateInspectField that = (StateInspectField) other;
        return Objects.equals(name, that.name) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }
}
