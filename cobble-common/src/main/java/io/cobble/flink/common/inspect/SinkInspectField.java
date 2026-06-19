package io.cobble.flink.common.inspect;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/** Persisted inspect metadata for one physical field in a Cobble SQL sink table. */
public final class SinkInspectField {

    private final String name;
    private final String logicalType;
    private final int rowIndex;
    private final int structuredColumnIndex;
    private final SinkInspectFieldRole role;

    public SinkInspectField(
            String name,
            String logicalType,
            int rowIndex,
            int structuredColumnIndex,
            SinkInspectFieldRole role) {
        this.name = requireText(name, "name");
        this.logicalType = requireText(logicalType, "logicalType");
        this.rowIndex = rowIndex;
        this.structuredColumnIndex = structuredColumnIndex;
        this.role = Objects.requireNonNull(role, "role");
    }

    public static SinkInspectField key(
            String name, String logicalType, int rowIndex, int structuredColumnIndex) {
        return new SinkInspectField(
                name, logicalType, rowIndex, structuredColumnIndex, SinkInspectFieldRole.KEY);
    }

    public static SinkInspectField value(
            String name, String logicalType, int rowIndex, int structuredColumnIndex) {
        return new SinkInspectField(
                name, logicalType, rowIndex, structuredColumnIndex, SinkInspectFieldRole.VALUE);
    }

    public String name() {
        return name;
    }

    public String logicalType() {
        return logicalType;
    }

    public int rowIndex() {
        return rowIndex;
    }

    public int structuredColumnIndex() {
        return structuredColumnIndex;
    }

    public SinkInspectFieldRole role() {
        return role;
    }

    void write(DataOutputView output) throws IOException {
        output.writeUTF(name);
        output.writeUTF(logicalType);
        output.writeInt(rowIndex);
        output.writeInt(structuredColumnIndex);
        output.writeInt(role.ordinal());
    }

    static SinkInspectField read(DataInputView input) throws IOException {
        String name = input.readUTF();
        String logicalType = input.readUTF();
        int rowIndex = input.readInt();
        int structuredColumnIndex = input.readInt();
        SinkInspectFieldRole role = SinkInspectFieldRole.fromOrdinal(input.readInt());
        return new SinkInspectField(name, logicalType, rowIndex, structuredColumnIndex, role);
    }

    private static String requireText(String value, String label) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(label + " must not be empty");
        }
        return value;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SinkInspectField)) {
            return false;
        }
        SinkInspectField that = (SinkInspectField) other;
        return rowIndex == that.rowIndex
                && structuredColumnIndex == that.structuredColumnIndex
                && Objects.equals(name, that.name)
                && Objects.equals(logicalType, that.logicalType)
                && role == that.role;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, logicalType, rowIndex, structuredColumnIndex, role);
    }
}
