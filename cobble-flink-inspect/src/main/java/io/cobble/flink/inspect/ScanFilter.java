package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Optional schema-aware scan filter. Field lists are ordered leading-key prefixes. */
public final class ScanFilter {
    private final List<FieldValue> sinkKeyFields;
    private final StateKey stateKey;
    private final boolean autoKeyGroup;

    private ScanFilter(List<FieldValue> sinkKeyFields, StateKey stateKey, boolean autoKeyGroup) {
        this.sinkKeyFields =
                Collections.unmodifiableList(
                        new ArrayList<FieldValue>(
                                sinkKeyFields == null
                                        ? Collections.<FieldValue>emptyList()
                                        : sinkKeyFields));
        this.stateKey = stateKey;
        this.autoKeyGroup = autoKeyGroup;
    }

    public static ScanFilter sink(List<FieldValue> leadingKeyFields) {
        return new ScanFilter(leadingKeyFields, null, false);
    }

    public static ScanFilter state(StateKey stateKey, boolean autoKeyGroup) {
        return new ScanFilter(
                Collections.<FieldValue>emptyList(),
                Objects.requireNonNull(stateKey, "stateKey"),
                autoKeyGroup);
    }

    public List<FieldValue> sinkKeyFields() {
        return sinkKeyFields;
    }

    public StateKey stateKey() {
        return stateKey;
    }

    public boolean autoKeyGroup() {
        return autoKeyGroup;
    }
}
