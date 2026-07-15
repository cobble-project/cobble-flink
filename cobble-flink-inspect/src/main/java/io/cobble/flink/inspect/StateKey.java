package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Ordered state-key, namespace, and map-key values. */
public final class StateKey {
    private final List<FieldValue> stateKeyFields;
    private final List<FieldValue> namespaceFields;
    private final List<FieldValue> mapKeyFields;

    public StateKey(
            List<FieldValue> stateKeyFields,
            List<FieldValue> namespaceFields,
            List<FieldValue> mapKeyFields) {
        this.stateKeyFields = immutable(stateKeyFields);
        this.namespaceFields = immutable(namespaceFields);
        this.mapKeyFields = immutable(mapKeyFields);
    }

    public List<FieldValue> stateKeyFields() {
        return stateKeyFields;
    }

    public List<FieldValue> namespaceFields() {
        return namespaceFields;
    }

    public List<FieldValue> mapKeyFields() {
        return mapKeyFields;
    }

    private static List<FieldValue> immutable(List<FieldValue> values) {
        return Collections.unmodifiableList(
                new ArrayList<FieldValue>(
                        values == null ? Collections.<FieldValue>emptyList() : values));
    }
}
