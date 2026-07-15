package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Complete schema-aware exact lookup key for a sink or keyed state target. */
public final class TypedLookupKey {
    public enum Kind {
        SINK,
        STATE
    }

    private final Kind kind;
    private final List<FieldValue> sinkKeyFields;
    private final StateKey stateKey;

    private TypedLookupKey(Kind kind, List<FieldValue> sinkKeyFields, StateKey stateKey) {
        this.kind = kind;
        this.sinkKeyFields =
                Collections.unmodifiableList(
                        new ArrayList<FieldValue>(
                                sinkKeyFields == null
                                        ? Collections.<FieldValue>emptyList()
                                        : sinkKeyFields));
        this.stateKey = stateKey;
    }

    public static TypedLookupKey sink(List<FieldValue> keyFields) {
        if (keyFields == null || keyFields.isEmpty()) {
            throw new IllegalArgumentException("sink key fields must not be empty");
        }
        return new TypedLookupKey(Kind.SINK, keyFields, null);
    }

    public static TypedLookupKey state(StateKey stateKey) {
        return new TypedLookupKey(
                Kind.STATE,
                Collections.<FieldValue>emptyList(),
                Objects.requireNonNull(stateKey, "stateKey"));
    }

    public Kind kind() {
        return kind;
    }

    public List<FieldValue> sinkKeyFields() {
        return sinkKeyFields;
    }

    public StateKey stateKey() {
        return stateKey;
    }
}
