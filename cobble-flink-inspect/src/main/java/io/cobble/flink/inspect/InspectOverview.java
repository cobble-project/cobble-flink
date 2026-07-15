package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable Overview model shared by the Java SDK and monitor adapter. */
public final class InspectOverview {
    private final long checkpointId;
    private final String operatorId;
    private final List<InspectOverviewItem> items;

    public InspectOverview(long checkpointId, String operatorId, List<InspectOverviewItem> items) {
        this.checkpointId = checkpointId;
        this.operatorId = operatorId;
        this.items =
                Collections.unmodifiableList(
                        new ArrayList<InspectOverviewItem>(
                                items == null
                                        ? Collections.<InspectOverviewItem>emptyList()
                                        : items));
    }

    public long checkpointId() {
        return checkpointId;
    }

    public String operatorId() {
        return operatorId;
    }

    public List<InspectOverviewItem> items() {
        return items;
    }
}
