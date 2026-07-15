package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Recursive inspect-schema type, including logical annotations and nested structure. */
public final class SemanticType {
    public enum Kind {
        BOOLEAN,
        INTEGER,
        FLOAT,
        STRING,
        BYTES,
        ROW,
        TUPLE,
        LIST,
        MAP,
        UNKNOWN
    }

    private final Kind kind;
    private final String logicalType;
    private final List<SemanticField> fields;
    private final SemanticType elementType;
    private final SemanticType keyType;
    private final SemanticType valueType;

    public SemanticType(
            Kind kind,
            String logicalType,
            List<SemanticField> fields,
            SemanticType elementType,
            SemanticType keyType,
            SemanticType valueType) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.logicalType = logicalType;
        this.fields =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                fields == null ? Collections.<SemanticField>emptyList() : fields));
        this.elementType = elementType;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public Kind kind() {
        return kind;
    }

    public String logicalType() {
        return logicalType;
    }

    public List<SemanticField> fields() {
        return fields;
    }

    public SemanticType elementType() {
        return elementType;
    }

    public SemanticType keyType() {
        return keyType;
    }

    public SemanticType valueType() {
        return valueType;
    }
}
