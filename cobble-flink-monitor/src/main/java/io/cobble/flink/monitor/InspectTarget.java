package io.cobble.flink.monitor;

import java.util.LinkedHashMap;
import java.util.Map;

final class InspectTarget {
    final String id;
    final String name;
    final String kind;
    final String columnFamily;
    final boolean allowsColumns;
    final String stateKind;
    final Map<String, String> serializerClasses;

    InspectTarget(
            String id,
            String name,
            String kind,
            String columnFamily,
            boolean allowsColumns,
            String stateKind,
            Map<String, String> serializerClasses) {
        this.id = id;
        this.name = name;
        this.kind = kind;
        this.columnFamily = columnFamily;
        this.allowsColumns = allowsColumns;
        this.stateKind = stateKind;
        this.serializerClasses = serializerClasses;
    }

    static InspectTarget sink(String name) {
        return new InspectTarget("sink", name, "sink", null, true, null, null);
    }

    static InspectTarget state(String stateName, String columnFamily) {
        return new InspectTarget(stateName, stateName, "state", columnFamily, false, null, null);
    }

    static InspectTarget timer(String stateName, String columnFamily) {
        return new InspectTarget(
                "timer:" + stateName, stateName, "timer", columnFamily, false, null, null);
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
        return output;
    }
}
