package io.cobble.flink.monitor;

import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchema;

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
        this.id = id;
        this.name = name;
        this.kind = kind;
        this.columnFamily = columnFamily;
        this.allowsColumns = allowsColumns;
        this.stateKind = stateKind;
        this.serializerClasses = serializerClasses;
        this.schema = schema;
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
        }
        if (serializerClasses != null && !serializerClasses.isEmpty()) {
            output.put("serializer_classes", serializerClasses);
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
}
