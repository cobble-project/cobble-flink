package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Reproducible Flink SQL source example and its scan/lookup capabilities. */
public final class SourceSqlExample {
    private final String ddl;
    private final boolean batchScanSupported;
    private final boolean exactLookupSupported;
    private final List<String> requiredLookupFields;
    private final String unavailableReason;
    private final String note;

    public SourceSqlExample(
            String ddl,
            boolean batchScanSupported,
            boolean exactLookupSupported,
            List<String> requiredLookupFields,
            String unavailableReason,
            String note) {
        this.ddl = ddl;
        this.batchScanSupported = batchScanSupported;
        this.exactLookupSupported = exactLookupSupported;
        this.requiredLookupFields =
                Collections.unmodifiableList(
                        new ArrayList<String>(
                                requiredLookupFields == null
                                        ? Collections.<String>emptyList()
                                        : requiredLookupFields));
        this.unavailableReason = unavailableReason;
        this.note = note;
    }

    public String ddl() {
        return ddl;
    }

    public boolean available() {
        return ddl != null;
    }

    public boolean batchScanSupported() {
        return batchScanSupported;
    }

    public boolean exactLookupSupported() {
        return exactLookupSupported;
    }

    public List<String> requiredLookupFields() {
        return requiredLookupFields;
    }

    public String unavailableReason() {
        return unavailableReason;
    }

    public String note() {
        return note;
    }
}
