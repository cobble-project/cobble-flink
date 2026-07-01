package io.cobble.flink.table;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;

/**
 * Cobble state source whose DDL schema has been resolved and validated at planning time.
 *
 * <p>Batch scans use the state checkpoint runtime. Lookup is intentionally still rejected until the
 * state lookup source path is implemented.
 */
final class CobbleStateDynamicTableSource implements ScanTableSource, LookupTableSource {

    private static final String STREAMING_NOT_SUPPORTED =
            "Cobble state source currently supports only scan.mode='batch'.";
    private static final String LOOKUP_NOT_IMPLEMENTED =
            "Cobble state source lookup runtime is not implemented yet.";

    private final StateSourceConfig config;
    private final String summary;

    CobbleStateDynamicTableSource(StateSourceConfig config, String summary) {
        this.config = config;
        this.summary = summary;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        if (!"batch".equals(config.scanMode())) {
            throw new UnsupportedOperationException(STREAMING_NOT_SUPPORTED);
        }
        return SourceProvider.of(new CobbleStateSource(config));
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        throw new UnsupportedOperationException(LOOKUP_NOT_IMPLEMENTED);
    }

    @Override
    public DynamicTableSource copy() {
        return new CobbleStateDynamicTableSource(config, summary);
    }

    @Override
    public String asSummaryString() {
        return "CobbleStateTableSource{"
                + summary
                + ", state="
                + config.stateName()
                + ", kind="
                + config.stateKind()
                + "}";
    }
}
