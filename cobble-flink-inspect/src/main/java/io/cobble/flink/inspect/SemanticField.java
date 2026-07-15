package io.cobble.flink.inspect;

import java.util.Objects;

/** One ordered field in a semantic row or tuple. */
public final class SemanticField {
    private final String name;
    private final SemanticType type;

    public SemanticField(String name, SemanticType type) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type, "type");
    }

    public String name() {
        return name;
    }

    public SemanticType type() {
        return type;
    }
}
