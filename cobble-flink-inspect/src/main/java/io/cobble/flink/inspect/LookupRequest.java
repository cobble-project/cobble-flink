package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** A batch of raw point lookups. */
public final class LookupRequest {
    private final String targetId;
    private final List<LookupKey> keys;
    private final int[] columns;

    public LookupRequest(String targetId, List<LookupKey> keys) {
        this(targetId, keys, null);
    }

    public LookupRequest(String targetId, List<LookupKey> keys, int[] columns) {
        this.targetId = targetId;
        this.keys =
                Collections.unmodifiableList(
                        new ArrayList<LookupKey>(
                                keys == null ? Collections.<LookupKey>emptyList() : keys));
        this.columns = columns == null ? null : Arrays.copyOf(columns, columns.length);
    }

    public String targetId() {
        return targetId;
    }

    public List<LookupKey> keys() {
        return keys;
    }

    public int[] columns() {
        return columns == null ? null : Arrays.copyOf(columns, columns.length);
    }
}
