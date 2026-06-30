package io.cobble.flink.table;

import java.io.Serializable;
import java.util.Objects;

/**
 * One physical SQL output column derived from a Cobble state inspect semantic schema.
 *
 * <p>Each field records which logical part of the state row it comes from ({@link Group}) and its
 * position within that part's flattened scalar fields, so the Step 3 runtime can map a decoded
 * state row onto the SQL output row without re-deriving the schema.
 */
final class StateSourceField implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Which logical part of a keyed-state row a column is sourced from. */
    enum Group {
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

    StateSourceField(String name, String logicalType, Group group, int groupFieldIndex) {
        this.name = name;
        this.logicalType = logicalType;
        this.group = group;
        this.groupFieldIndex = groupFieldIndex;
    }

    /** SQL column name. */
    String name() {
        return name;
    }

    /** {@code LogicalType.asSerializableString()} form of the column type. */
    String logicalType() {
        return logicalType;
    }

    /** The logical state part this column is decoded from. */
    Group group() {
        return group;
    }

    /** Zero-based index of this column within its group's flattened scalar fields. */
    int groupFieldIndex() {
        return groupFieldIndex;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StateSourceField)) {
            return false;
        }
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
