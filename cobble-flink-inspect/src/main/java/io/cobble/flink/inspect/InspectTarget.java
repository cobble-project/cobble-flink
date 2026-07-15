package io.cobble.flink.inspect;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** An inspectable state, timer, sink, or raw column family. */
public final class InspectTarget {
    private final String id;
    private final InspectTargetKind kind;
    private final String columnFamily;
    private final String name;
    private final String stateKind;
    private final boolean allowsColumns;
    private final Map<String, String> serializerDiagnostics;
    private final Map<String, SemanticType> semanticSchema;
    private final List<SemanticField> sinkKeyFields;
    private final List<SemanticField> sinkValueFields;
    private final boolean exactLookupSupported;
    private final String exactLookupDiagnostic;

    public InspectTarget(String id, InspectTargetKind kind, String columnFamily) {
        this(
                id,
                id,
                kind,
                columnFamily,
                false,
                null,
                Collections.<String, String>emptyMap(),
                Collections.<String, SemanticType>emptyMap(),
                Collections.<SemanticField>emptyList(),
                Collections.<SemanticField>emptyList(),
                false,
                null);
    }

    public InspectTarget(
            String id,
            String name,
            InspectTargetKind kind,
            String columnFamily,
            boolean allowsColumns,
            String stateKind,
            Map<String, String> serializerDiagnostics,
            Map<String, SemanticType> semanticSchema,
            List<SemanticField> sinkKeyFields,
            List<SemanticField> sinkValueFields,
            boolean exactLookupSupported,
            String exactLookupDiagnostic) {
        this.id = Objects.requireNonNull(id, "id");
        this.name = Objects.requireNonNull(name, "name");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.columnFamily = columnFamily;
        this.allowsColumns = allowsColumns;
        this.stateKind = stateKind;
        this.serializerDiagnostics =
                Collections.unmodifiableMap(
                        new LinkedHashMap<String, String>(
                                serializerDiagnostics == null
                                        ? Collections.<String, String>emptyMap()
                                        : serializerDiagnostics));
        this.semanticSchema =
                Collections.unmodifiableMap(
                        new LinkedHashMap<String, SemanticType>(
                                semanticSchema == null
                                        ? Collections.<String, SemanticType>emptyMap()
                                        : semanticSchema));
        this.sinkKeyFields = immutableFields(sinkKeyFields);
        this.sinkValueFields = immutableFields(sinkValueFields);
        this.exactLookupSupported = exactLookupSupported;
        this.exactLookupDiagnostic = exactLookupDiagnostic;
    }

    public String id() {
        return id;
    }

    public InspectTargetKind kind() {
        return kind;
    }

    public String columnFamily() {
        return columnFamily;
    }

    public String name() {
        return name;
    }

    public String stateKind() {
        return stateKind;
    }

    public boolean allowsColumns() {
        return allowsColumns;
    }

    public Map<String, String> serializerDiagnostics() {
        return serializerDiagnostics;
    }

    public Map<String, SemanticType> semanticSchema() {
        return semanticSchema;
    }

    public List<SemanticField> sinkKeyFields() {
        return sinkKeyFields;
    }

    public List<SemanticField> sinkValueFields() {
        return sinkValueFields;
    }

    public boolean exactLookupSupported() {
        return exactLookupSupported;
    }

    public String exactLookupDiagnostic() {
        return exactLookupDiagnostic;
    }

    private static List<SemanticField> immutableFields(List<SemanticField> fields) {
        return Collections.unmodifiableList(
                new java.util.ArrayList<>(
                        fields == null ? Collections.<SemanticField>emptyList() : fields));
    }
}
