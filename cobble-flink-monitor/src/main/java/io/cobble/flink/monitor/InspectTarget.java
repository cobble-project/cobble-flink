package io.cobble.flink.monitor;

import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;
import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.StateKind;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class InspectTarget {
    final String id;
    final String name;
    final String kind;
    final String columnFamily;
    final boolean allowsColumns;
    final String stateKind;
    final Map<String, String> serializerClasses;
    final StateInspectSchema schema;
    final StateInspectSemanticSchema semanticSchema;
    final SinkInspectSchema sinkSchema;

    InspectTarget(
            String id,
            String name,
            String kind,
            String columnFamily,
            boolean allowsColumns,
            String stateKind,
            Map<String, String> serializerClasses,
            StateInspectSchema schema,
            SinkInspectSchema sinkSchema) {
        this(
                id,
                name,
                kind,
                columnFamily,
                allowsColumns,
                stateKind,
                serializerClasses,
                schema,
                null,
                sinkSchema);
    }

    InspectTarget(
            String id,
            String name,
            String kind,
            String columnFamily,
            boolean allowsColumns,
            String stateKind,
            Map<String, String> serializerClasses,
            StateInspectSchema schema,
            StateInspectSemanticSchema semanticSchema,
            SinkInspectSchema sinkSchema) {
        this.id = id;
        this.name = name;
        this.kind = kind;
        this.columnFamily = columnFamily;
        this.allowsColumns = allowsColumns;
        this.stateKind = stateKind;
        this.serializerClasses = serializerClasses;
        this.schema = schema;
        this.semanticSchema = semanticSchema;
        this.sinkSchema = sinkSchema;
    }

    static InspectTarget sink(String name) {
        return sink(name, null);
    }

    static InspectTarget sink(String name, SinkInspectSchema schema) {
        return new InspectTarget("sink", name, "sink", null, true, null, null, null, schema);
    }

    static InspectTarget state(String stateName, String columnFamily) {
        return new InspectTarget(
                stateName, stateName, "state", columnFamily, false, null, null, null, null);
    }

    static InspectTarget timer(String stateName, String columnFamily) {
        return new InspectTarget(
                "timer:" + stateName,
                stateName,
                "timer",
                columnFamily,
                false,
                null,
                null,
                null,
                null);
    }

    Map<String, Object> toJson() {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("id", id);
        output.put("name", name);
        output.put("kind", kind);
        output.put("allows_columns", allowsColumns);
        if (stateKind != null) {
            output.put("state_kind", stateKind);
            // AggregatingState persists the accumulator (ACC), not the output (OUT). Surface a
            // human-friendly label so the UI can show the "value" group as "Accumulator" without
            // changing the wire field name (decoded_value stays the same).
            if (StateKind.AGGREGATING.name().equals(stateKind)) {
                output.put("value_part_label", "Accumulator");
            }
        }
        if (serializerClasses != null && !serializerClasses.isEmpty()) {
            output.put("serializer_classes", serializerClasses);
        }
        if (semanticSchema != null && !semanticSchema.isEmpty()) {
            output.put("semantic_parts", semanticPartsToJson(semanticSchema));
        }
        if (sinkSchema != null) {
            output.put("key_fields", fieldsToJson(sinkSchema.keyFields()));
            output.put("value_fields", fieldsToJson(sinkSchema.valueFields()));
        }
        return output;
    }

    private static List<Map<String, Object>> fieldsToJson(List<SinkInspectField> fields) {
        List<Map<String, Object>> output = new ArrayList<>(fields.size());
        for (SinkInspectField field : fields) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("name", field.name());
            item.put("logical_type", field.logicalType());
            item.put("row_index", field.rowIndex());
            item.put("structured_column_index", field.structuredColumnIndex());
            item.put("role", field.role().name());
            output.add(item);
        }
        return output;
    }

    private static Map<String, Object> semanticPartsToJson(
            StateInspectSemanticSchema semanticSchema) {
        Map<String, Object> output = new LinkedHashMap<>();
        putType(output, "state_key", semanticSchema.stateKey());
        putType(output, "namespace", semanticSchema.namespace());
        putType(output, "value", semanticSchema.value());
        putType(output, "list_element", semanticSchema.listElement());
        putType(output, "map_key", semanticSchema.mapUserKey());
        putType(output, "map_value", semanticSchema.mapUserValue());
        return output;
    }

    private static void putType(
            Map<String, Object> output, String name, StateInspectType inspectType) {
        if (inspectType != null) {
            output.put(name, typeToJson(inspectType));
        }
    }

    private static Map<String, Object> typeToJson(StateInspectType inspectType) {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("kind", inspectType.kind().name());
        if (inspectType.logicalType() != null) {
            output.put("logical_type", inspectType.logicalType());
        }
        if (!inspectType.fields().isEmpty()) {
            List<Map<String, Object>> fields = new ArrayList<>(inspectType.fields().size());
            for (StateInspectField field : inspectType.fields()) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("name", field.name());
                item.put("type", typeToJson(field.type()));
                fields.add(item);
            }
            output.put("fields", fields);
        }
        if (inspectType.elementType() != null) {
            output.put("element_type", typeToJson(inspectType.elementType()));
        }
        return output;
    }
}
