package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** One immutable scan page. */
public final class InspectPage {
    private final List<InspectRow> rows;
    private final PageToken nextPageToken;

    public InspectPage(List<InspectRow> rows, PageToken nextPageToken) {
        this.rows =
                Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(rows, "rows")));
        this.nextPageToken = nextPageToken;
    }

    public List<InspectRow> rows() {
        return rows;
    }

    public PageToken nextPageToken() {
        return nextPageToken;
    }
}
