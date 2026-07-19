package io.cobble.flink.common;

import org.apache.flink.api.common.state.StateDescriptor;

import java.util.Objects;

/** Physical Cobble row format used by one runtime keyed state. */
public final class CobbleStateDescriptor {

    /** Runtime state kind whose rows use this descriptor. */
    public enum StateKind {
        VALUE,
        LIST,
        MAP,
        REDUCING,
        AGGREGATING,
        TIMER
    }

    /** Physical encoding used by the Cobble row key. */
    public enum RowKeyEncoding {
        KEY_NAMESPACE(1),
        KEY_NAMESPACE_MAP_KEY(1),
        TIMER_ELEMENT(1);

        private final int currentVersion;

        RowKeyEncoding(int currentVersion) {
            this.currentVersion = currentVersion;
        }

        public int currentVersion() {
            return currentVersion;
        }
    }

    /** Physical encoding used by column zero of the Cobble row. */
    public enum RowValueEncoding {
        SERIALIZED_VALUE(1),
        DELIMITED_LIST(1),
        NULLABLE_MAP_VALUE(1),
        NONE(1);

        private final int currentVersion;

        RowValueEncoding(int currentVersion) {
            this.currentVersion = currentVersion;
        }

        public int currentVersion() {
            return currentVersion;
        }
    }

    private final String stateName;
    private final String columnFamily;
    private final StateKind stateKind;
    private final RowKeyEncoding rowKeyEncoding;
    private final int rowKeyFormatVersion;
    private final RowValueEncoding rowValueEncoding;
    private final int rowValueFormatVersion;

    CobbleStateDescriptor(
            String stateName,
            String columnFamily,
            StateKind stateKind,
            RowKeyEncoding rowKeyEncoding,
            int rowKeyFormatVersion,
            RowValueEncoding rowValueEncoding,
            int rowValueFormatVersion) {
        this.stateName = Objects.requireNonNull(stateName, "stateName");
        this.columnFamily = Objects.requireNonNull(columnFamily, "columnFamily");
        this.stateKind = Objects.requireNonNull(stateKind, "stateKind");
        this.rowKeyEncoding = Objects.requireNonNull(rowKeyEncoding, "rowKeyEncoding");
        this.rowKeyFormatVersion = rowKeyFormatVersion;
        this.rowValueEncoding = Objects.requireNonNull(rowValueEncoding, "rowValueEncoding");
        this.rowValueFormatVersion = rowValueFormatVersion;
    }

    /** Creates the current physical descriptor for a Flink keyed state. */
    public static CobbleStateDescriptor forKeyValue(
            String stateName, String columnFamily, StateDescriptor.Type stateType) {
        switch (stateType) {
            case VALUE:
                return current(
                        stateName,
                        columnFamily,
                        StateKind.VALUE,
                        RowKeyEncoding.KEY_NAMESPACE,
                        RowValueEncoding.SERIALIZED_VALUE);
            case LIST:
                return current(
                        stateName,
                        columnFamily,
                        StateKind.LIST,
                        RowKeyEncoding.KEY_NAMESPACE,
                        RowValueEncoding.DELIMITED_LIST);
            case MAP:
                return current(
                        stateName,
                        columnFamily,
                        StateKind.MAP,
                        RowKeyEncoding.KEY_NAMESPACE_MAP_KEY,
                        RowValueEncoding.NULLABLE_MAP_VALUE);
            case REDUCING:
                return current(
                        stateName,
                        columnFamily,
                        StateKind.REDUCING,
                        RowKeyEncoding.KEY_NAMESPACE,
                        RowValueEncoding.SERIALIZED_VALUE);
            case AGGREGATING:
                return current(
                        stateName,
                        columnFamily,
                        StateKind.AGGREGATING,
                        RowKeyEncoding.KEY_NAMESPACE,
                        RowValueEncoding.SERIALIZED_VALUE);
            default:
                throw new IllegalArgumentException(
                        "Unsupported Cobble keyed state kind: " + stateType);
        }
    }

    /** Creates the current physical descriptor for a Cobble timer queue. */
    public static CobbleStateDescriptor forTimer(String stateName, String columnFamily) {
        return current(
                stateName,
                columnFamily,
                StateKind.TIMER,
                RowKeyEncoding.TIMER_ELEMENT,
                RowValueEncoding.NONE);
    }

    private static CobbleStateDescriptor current(
            String stateName,
            String columnFamily,
            StateKind stateKind,
            RowKeyEncoding rowKeyEncoding,
            RowValueEncoding rowValueEncoding) {
        return new CobbleStateDescriptor(
                stateName,
                columnFamily,
                stateKind,
                rowKeyEncoding,
                rowKeyEncoding.currentVersion(),
                rowValueEncoding,
                rowValueEncoding.currentVersion());
    }

    public String stateName() {
        return stateName;
    }

    public String columnFamily() {
        return columnFamily;
    }

    public StateKind stateKind() {
        return stateKind;
    }

    /** Stable identity used to distinguish keyed states from timer queues with the same name. */
    public String stateIdentity() {
        return (stateKind == StateKind.TIMER ? "timer:" : "kv:") + stateName;
    }

    public RowKeyEncoding rowKeyEncoding() {
        return rowKeyEncoding;
    }

    public int rowKeyFormatVersion() {
        return rowKeyFormatVersion;
    }

    public RowValueEncoding rowValueEncoding() {
        return rowValueEncoding;
    }

    public int rowValueFormatVersion() {
        return rowValueFormatVersion;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof CobbleStateDescriptor)) {
            return false;
        }
        CobbleStateDescriptor that = (CobbleStateDescriptor) other;
        return rowKeyFormatVersion == that.rowKeyFormatVersion
                && rowValueFormatVersion == that.rowValueFormatVersion
                && stateName.equals(that.stateName)
                && columnFamily.equals(that.columnFamily)
                && stateKind == that.stateKind
                && rowKeyEncoding == that.rowKeyEncoding
                && rowValueEncoding == that.rowValueEncoding;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                stateName,
                columnFamily,
                stateKind,
                rowKeyEncoding,
                rowKeyFormatVersion,
                rowValueEncoding,
                rowValueFormatVersion);
    }

    @Override
    public String toString() {
        return "CobbleStateDescriptor{"
                + "stateName='"
                + stateName
                + '\''
                + ", columnFamily='"
                + columnFamily
                + '\''
                + ", stateKind="
                + stateKind
                + ", rowKeyEncoding="
                + rowKeyEncoding
                + ", rowKeyFormatVersion="
                + rowKeyFormatVersion
                + ", rowValueEncoding="
                + rowValueEncoding
                + ", rowValueFormatVersion="
                + rowValueFormatVersion
                + '}';
    }
}
