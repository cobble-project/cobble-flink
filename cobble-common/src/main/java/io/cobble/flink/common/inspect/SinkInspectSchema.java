package io.cobble.flink.common.inspect;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Persisted inspect metadata for one Cobble SQL sink table. */
public final class SinkInspectSchema {

    private final List<SinkInspectField> keyFields;
    private final List<SinkInspectField> valueFields;

    public SinkInspectSchema(List<SinkInspectField> keyFields, List<SinkInspectField> valueFields) {
        this.keyFields = immutableCopy(keyFields, "keyFields");
        this.valueFields = immutableCopy(valueFields, "valueFields");
        if (this.keyFields.isEmpty()) {
            throw new IllegalArgumentException("Sink inspect schema requires key fields");
        }
        if (this.valueFields.isEmpty()) {
            throw new IllegalArgumentException("Sink inspect schema requires value fields");
        }
        validateRoles(this.keyFields, SinkInspectFieldRole.KEY, "keyFields");
        validateRoles(this.valueFields, SinkInspectFieldRole.VALUE, "valueFields");
    }

    public List<SinkInspectField> keyFields() {
        return keyFields;
    }

    public List<SinkInspectField> valueFields() {
        return valueFields;
    }

    void write(DataOutputView output) throws IOException {
        output.writeInt(keyFields.size());
        for (SinkInspectField field : keyFields) {
            field.write(output);
        }
        output.writeInt(valueFields.size());
        for (SinkInspectField field : valueFields) {
            field.write(output);
        }
    }

    static SinkInspectSchema read(DataInputView input) throws IOException {
        int keyCount = input.readInt();
        List<SinkInspectField> keyFields = readFields(input, keyCount);
        int valueCount = input.readInt();
        List<SinkInspectField> valueFields = readFields(input, valueCount);
        return new SinkInspectSchema(keyFields, valueFields);
    }

    private static List<SinkInspectField> readFields(DataInputView input, int count)
            throws IOException {
        if (count < 0) {
            throw new IOException("Negative sink inspect field count: " + count);
        }
        List<SinkInspectField> fields = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            fields.add(SinkInspectField.read(input));
        }
        return fields;
    }

    private static List<SinkInspectField> immutableCopy(
            List<SinkInspectField> fields, String label) {
        if (fields == null) {
            throw new IllegalArgumentException(label + " must not be null");
        }
        return Collections.unmodifiableList(new ArrayList<>(fields));
    }

    private static void validateRoles(
            List<SinkInspectField> fields, SinkInspectFieldRole role, String label) {
        for (SinkInspectField field : fields) {
            if (field.role() != role) {
                throw new IllegalArgumentException(label + " contains " + field.role());
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SinkInspectSchema)) {
            return false;
        }
        SinkInspectSchema that = (SinkInspectSchema) other;
        return Objects.equals(keyFields, that.keyFields)
                && Objects.equals(valueFields, that.valueFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyFields, valueFields);
    }
}
