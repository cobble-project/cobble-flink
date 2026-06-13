package io.cobble.flink.monitor;

import java.util.LinkedHashMap;
import java.util.Map;

final class InspectTarget {
    final String id;
    final String name;
    final String kind;
    final String columnFamily;
    final boolean allowsColumns;

    private InspectTarget(
            String id, String name, String kind, String columnFamily, boolean allowsColumns) {
        this.id = id;
        this.name = name;
        this.kind = kind;
        this.columnFamily = columnFamily;
        this.allowsColumns = allowsColumns;
    }

    static InspectTarget sink(String name) {
        return new InspectTarget("sink", name, "sink", null, true);
    }

    static InspectTarget state(String stateName, String columnFamily) {
        return new InspectTarget(stateName, stateName, "state", columnFamily, false);
    }

    static InspectTarget timer(String stateName, String columnFamily) {
        return new InspectTarget("timer:" + stateName, stateName, "timer", columnFamily, false);
    }

    Map<String, Object> toJson() {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("id", id);
        output.put("name", name);
        output.put("kind", kind);
        output.put("allows_columns", allowsColumns);
        return output;
    }
}
