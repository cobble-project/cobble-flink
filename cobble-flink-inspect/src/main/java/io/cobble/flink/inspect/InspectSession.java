package io.cobble.flink.inspect;

import java.util.List;

/** A pinned inspection session. Closing it releases every SDK-owned resource. */
public interface InspectSession extends AutoCloseable {
    InspectCatalog catalog();

    InspectSessionInfo info();

    /** Targets resolved for the session's pinned checkpoint and operator. */
    List<InspectTarget> targets();

    InspectPage scan(ScanRequest request);

    LookupResult lookup(LookupRequest request);

    @Override
    void close();
}
