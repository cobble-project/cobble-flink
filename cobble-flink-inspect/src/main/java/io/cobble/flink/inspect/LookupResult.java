package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Ordered raw point-lookup results, including misses represented by null rows. */
public final class LookupResult {
    private final List<InspectRow> rows;

    public LookupResult(List<InspectRow> rows) {
        this.rows =
                Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(rows, "rows")));
    }

    public List<InspectRow> rows() {
        return rows;
    }
}
