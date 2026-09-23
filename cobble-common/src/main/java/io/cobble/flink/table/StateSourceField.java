package io.cobble.flink.table;

import java.io.Serializable;
import java.util.Objects;

/** One logical output column derived from persisted keyed-state inspect metadata. */
public final class StateSourceField implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Group {
        STATE_KEY,
        NAMESPACE,
        VALUE,
        LIST_ELEMENT,
        MAP_KEY,
        MAP_VALUE,
        TIMER_TIMESTAMP
    }

    private final String name;
    private final String logicalType;
    private final Group group;
    private final int groupFieldIndex;

    public StateSourceField(String name, String logicalType, Group group, int groupFieldIndex) {
        this.name = name;
        this.logicalType = logicalType;
        this.group = group;
        this.groupFieldIndex = groupFieldIndex;
    }

    public String name() {
        return name;
    }

    public String logicalType() {
        return logicalType;
    }

    public Group group() {
        return group;
    }

    public int groupFieldIndex() {
        return groupFieldIndex;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof StateSourceField)) return false;
        StateSourceField that = (StateSourceField) other;
        return groupFieldIndex == that.groupFieldIndex
                && Objects.equals(name, that.name)
                && Objects.equals(logicalType, that.logicalType)
                && group == that.group;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, logicalType, group, groupFieldIndex);
    }

    @Override
    public String toString() {
        return name + " " + logicalType + " (" + group + "#" + groupFieldIndex + ")";
    }
}
