package io.cobble.flink.inspect.internal;

import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.StateInspectExactLookupSupport;
import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.StateInspectTypeKind;
import io.cobble.flink.inspect.DecodeIssue;
import io.cobble.flink.inspect.DecodedValue;
import io.cobble.flink.inspect.InspectTargetKind;
import io.cobble.flink.inspect.RawBytes;
import io.cobble.flink.inspect.SemanticField;
import io.cobble.flink.inspect.SemanticType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Converts the monitor's wire-oriented decoder structures into the typed public SDK model. */
final class PublicInspectModels {
    private PublicInspectModels() {}

    static io.cobble.flink.inspect.InspectTarget target(InspectTarget source) {
        Map<String, SemanticType> semantic = new LinkedHashMap<>();
        if (source.semanticSchema != null) {
            put(semantic, "state_key", source.semanticSchema.stateKey());
            put(semantic, "namespace", source.semanticSchema.namespace());
            put(semantic, "value", source.semanticSchema.value());
            put(semantic, "list_element", source.semanticSchema.listElement());
            put(semantic, "map_key", source.semanticSchema.mapUserKey());
            put(semantic, "map_value", source.semanticSchema.mapUserValue());
        }
        List<SemanticField> keyFields = new ArrayList<>();
        List<SemanticField> valueFields = new ArrayList<>();
        if (source.sinkSchema != null) {
            for (SinkInspectField field : source.sinkSchema.keyFields()) {
                keyFields.add(new SemanticField(field.name(), scalar(field.logicalType())));
            }
            for (SinkInspectField field : source.sinkSchema.valueFields()) {
                valueFields.add(new SemanticField(field.name(), scalar(field.logicalType())));
            }
        }
        boolean lookupSupported = false;
        String lookupDiagnostic = null;
        if (source.schema != null && source.semanticSchema != null) {
            StateInspectExactLookupSupport.Result result =
                    StateInspectExactLookupSupport.evaluate(source.schema, source.semanticSchema);
            lookupSupported = result.supported();
            lookupDiagnostic = result.reason();
        }
        return new io.cobble.flink.inspect.InspectTarget(
                source.id,
                source.name,
                targetKind(source.kind, source.schema == null && source.sinkSchema == null),
                source.columnFamily,
                source.allowsColumns,
                source.stateKind,
                source.serializerClasses,
                semantic,
                keyFields,
                valueFields,
                lookupSupported,
                lookupDiagnostic);
    }

    static DecodedValue decoded(Object value) {
        if (value == null) {
            return scalarValue(null, null);
        }
        if (value instanceof Map) {
            return decodedMap(castMap(value));
        }
        if (value instanceof List) {
            List<DecodedValue> elements = new ArrayList<>();
            for (Object item : (List<?>) value) {
                elements.add(decoded(item));
            }
            return new DecodedValue(
                    DecodedValue.Kind.LIST,
                    null,
                    null,
                    null,
                    Collections.<DecodedValue.DecodedField>emptyList(),
                    elements,
                    Collections.<DecodedValue.MapEntry>emptyList());
        }
        if (value instanceof byte[]) {
            return raw((byte[]) value);
        }
        if (value instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) value).duplicate();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return raw(bytes);
        }
        if (value instanceof DisplayLong) {
            return scalarValue(null, Long.valueOf(value.toString()));
        }
        return scalarValue(null, value);
    }

    static List<DecodedValue> sinkFields(List<Map<String, Object>> fields) {
        if (fields == null) {
            return Collections.emptyList();
        }
        List<DecodedValue> output = new ArrayList<>(fields.size());
        for (Map<String, Object> field : fields) {
            String name = string(field.get("name"));
            String logicalType = string(field.get("logical_type"));
            DecodedValue value =
                    field.get("value") == null
                            ? scalarValue(logicalType, null)
                            : withLogicalType(decoded(field.get("value")), logicalType);
            output.add(
                    new DecodedValue(
                            DecodedValue.Kind.ROW,
                            null,
                            null,
                            null,
                            Collections.singletonList(new DecodedValue.DecodedField(name, value)),
                            Collections.<DecodedValue>emptyList(),
                            Collections.<DecodedValue.MapEntry>emptyList()));
        }
        return output;
    }

    static DecodedValue sinkRow(List<Map<String, Object>> fields) {
        List<DecodedValue.DecodedField> output = new ArrayList<>();
        if (fields != null) {
            for (Map<String, Object> field : fields) {
                String logicalType = string(field.get("logical_type"));
                DecodedValue value =
                        field.get("value") == null
                                ? scalarValue(logicalType, null)
                                : withLogicalType(decoded(field.get("value")), logicalType);
                output.add(new DecodedValue.DecodedField(string(field.get("name")), value));
            }
        }
        return new DecodedValue(
                DecodedValue.Kind.ROW,
                null,
                null,
                null,
                output,
                Collections.<DecodedValue>emptyList(),
                Collections.<DecodedValue.MapEntry>emptyList());
    }

    static List<DecodeIssue> issues(StateInspectDecoder.DecodedRow row) {
        List<DecodeIssue> output = new ArrayList<>();
        for (StateInspectDecoder.DecodeIssue issue : row.decodeIssues) {
            output.add(new DecodeIssue(issueKind(issue.kind), issue.part, issue.message));
        }
        if (row.decodeError != null && output.isEmpty()) {
            output.add(new DecodeIssue(DecodeIssue.Kind.DECODE, "row", row.decodeError));
        }
        return output;
    }

    static List<DecodeIssue> issues(String error) {
        return error == null
                ? Collections.<DecodeIssue>emptyList()
                : Collections.singletonList(new DecodeIssue(DecodeIssue.Kind.DECODE, "row", error));
    }

    static Map<String, DecodedValue> parts(Map<String, Object> parts) {
        if (parts == null || parts.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, DecodedValue> output = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : parts.entrySet()) {
            output.put(entry.getKey(), decoded(entry.getValue()));
        }
        return output;
    }

    private static DecodedValue decodedMap(Map<String, Object> value) {
        String kind = string(value.get("kind"));
        String logicalType = string(value.get("logical_type"));
        if (value.containsKey("b64") && kind == null) {
            try {
                return raw(Base64.getDecoder().decode(string(value.get("b64"))));
            } catch (IllegalArgumentException ignored) {
                return scalarValue(logicalType, value.get("b64"));
            }
        }
        if ("ROW".equals(kind) || "TUPLE".equals(kind) || value.containsKey("fields")) {
            List<DecodedValue.DecodedField> fields = new ArrayList<>();
            Object rawFields = value.get("fields");
            if (rawFields instanceof List) {
                for (Object rawField : (List<?>) rawFields) {
                    Map<String, Object> field = castMap(rawField);
                    fields.add(
                            new DecodedValue.DecodedField(
                                    string(field.get("name")), decoded(field.get("value"))));
                }
            } else {
                for (Map.Entry<String, Object> entry : value.entrySet()) {
                    if (!"kind".equals(entry.getKey()) && !"logical_type".equals(entry.getKey())) {
                        fields.add(
                                new DecodedValue.DecodedField(
                                        entry.getKey(), decoded(entry.getValue())));
                    }
                }
            }
            return new DecodedValue(
                    DecodedValue.Kind.ROW,
                    logicalType,
                    null,
                    null,
                    fields,
                    Collections.<DecodedValue>emptyList(),
                    Collections.<DecodedValue.MapEntry>emptyList());
        }
        if ("LIST".equals(kind) || value.containsKey("elements")) {
            List<DecodedValue> elements = new ArrayList<>();
            Object rawElements = value.get("elements");
            if (rawElements instanceof List) {
                for (Object element : (List<?>) rawElements) {
                    elements.add(decoded(element));
                }
            }
            return new DecodedValue(
                    DecodedValue.Kind.LIST,
                    logicalType,
                    null,
                    null,
                    Collections.<DecodedValue.DecodedField>emptyList(),
                    elements,
                    Collections.<DecodedValue.MapEntry>emptyList());
        }
        if ("MAP".equals(kind) || value.containsKey("entries")) {
            List<DecodedValue.MapEntry> entries = new ArrayList<>();
            Object rawEntries = value.get("entries");
            if (rawEntries instanceof List) {
                for (Object rawEntry : (List<?>) rawEntries) {
                    Map<String, Object> entry = castMap(rawEntry);
                    entries.add(
                            new DecodedValue.MapEntry(
                                    decoded(entry.get("key")), decoded(entry.get("value"))));
                }
            }
            return new DecodedValue(
                    DecodedValue.Kind.MAP,
                    logicalType,
                    null,
                    null,
                    Collections.<DecodedValue.DecodedField>emptyList(),
                    Collections.<DecodedValue>emptyList(),
                    entries);
        }
        if (kind == null) {
            List<DecodedValue.DecodedField> fields = new ArrayList<>();
            for (Map.Entry<String, Object> entry : value.entrySet()) {
                fields.add(
                        new DecodedValue.DecodedField(entry.getKey(), decoded(entry.getValue())));
            }
            return new DecodedValue(
                    DecodedValue.Kind.ROW,
                    logicalType,
                    null,
                    null,
                    fields,
                    Collections.<DecodedValue>emptyList(),
                    Collections.<DecodedValue.MapEntry>emptyList());
        }
        return scalarValue(logicalType, value.containsKey("value") ? value.get("value") : value);
    }

    private static DecodedValue withLogicalType(DecodedValue value, String logicalType) {
        return new DecodedValue(
                value.kind(),
                logicalType == null ? value.logicalType() : logicalType,
                value.scalar(),
                value.raw(),
                value.fields(),
                value.elements(),
                value.entries());
    }

    private static DecodedValue raw(byte[] bytes) {
        return new DecodedValue(
                DecodedValue.Kind.RAW,
                "BYTES",
                null,
                new RawBytes(bytes),
                Collections.<DecodedValue.DecodedField>emptyList(),
                Collections.<DecodedValue>emptyList(),
                Collections.<DecodedValue.MapEntry>emptyList());
    }

    private static DecodedValue scalarValue(String logicalType, Object value) {
        return new DecodedValue(
                DecodedValue.Kind.SCALAR,
                logicalType,
                value,
                null,
                Collections.<DecodedValue.DecodedField>emptyList(),
                Collections.<DecodedValue>emptyList(),
                Collections.<DecodedValue.MapEntry>emptyList());
    }

    private static void put(Map<String, SemanticType> output, String name, StateInspectType type) {
        if (type != null) {
            output.put(name, type(type));
        }
    }

    private static SemanticType type(StateInspectType source) {
        List<SemanticField> fields = new ArrayList<>();
        for (StateInspectField field : source.fields()) {
            fields.add(new SemanticField(field.name(), type(field.type())));
        }
        SemanticType.Kind kind;
        if (source.kind() == StateInspectTypeKind.ROW) {
            kind = SemanticType.Kind.ROW;
        } else if (source.kind() == StateInspectTypeKind.TUPLE) {
            kind = SemanticType.Kind.TUPLE;
        } else if (source.kind() == StateInspectTypeKind.LIST) {
            kind = SemanticType.Kind.LIST;
        } else if (source.kind() == StateInspectTypeKind.MAP) {
            kind = SemanticType.Kind.MAP;
        } else if (source.kind() == StateInspectTypeKind.SCALAR) {
            return scalar(source.logicalType());
        } else {
            kind = SemanticType.Kind.UNKNOWN;
        }
        return new SemanticType(
                kind,
                source.logicalType(),
                fields,
                source.elementType() == null ? null : type(source.elementType()),
                source.keyType() == null ? null : type(source.keyType()),
                source.valueType() == null ? null : type(source.valueType()));
    }

    private static SemanticType scalar(String logicalType) {
        String normalized =
                logicalType == null ? "" : logicalType.toUpperCase(java.util.Locale.ROOT);
        SemanticType.Kind kind = SemanticType.Kind.UNKNOWN;
        if (normalized.contains("CHAR") || normalized.contains("STRING")) {
            kind = SemanticType.Kind.STRING;
        } else if (normalized.contains("BINARY") || normalized.contains("BYTES")) {
            kind = SemanticType.Kind.BYTES;
        } else if (normalized.contains("BOOLEAN")) {
            kind = SemanticType.Kind.BOOLEAN;
        } else if (normalized.contains("INT") || normalized.contains("DECIMAL")) {
            kind = SemanticType.Kind.INTEGER;
        } else if (normalized.contains("FLOAT") || normalized.contains("DOUBLE")) {
            kind = SemanticType.Kind.FLOAT;
        }
        return new SemanticType(
                kind, logicalType, Collections.<SemanticField>emptyList(), null, null, null);
    }

    private static InspectTargetKind targetKind(String kind, boolean raw) {
        if ("timer".equals(kind)) {
            return InspectTargetKind.TIMER;
        }
        if ("state".equals(kind)) {
            return InspectTargetKind.STATE;
        }
        return raw ? InspectTargetKind.RAW : InspectTargetKind.SINK;
    }

    private static DecodeIssue.Kind issueKind(DecodeIssueKind kind) {
        if (kind == null) {
            return DecodeIssue.Kind.UNKNOWN;
        }
        String name = kind.name();
        if (name.contains("SERIALIZER") || name.contains("CLASS")) {
            return DecodeIssue.Kind.SERIALIZER;
        }
        if (name.contains("SCHEMA")) {
            return DecodeIssue.Kind.SCHEMA;
        }
        return DecodeIssue.Kind.DECODE;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Object value) {
        return value instanceof Map
                ? (Map<String, Object>) value
                : Collections.<String, Object>emptyMap();
    }

    private static String string(Object value) {
        return value == null ? null : String.valueOf(value);
    }
}
