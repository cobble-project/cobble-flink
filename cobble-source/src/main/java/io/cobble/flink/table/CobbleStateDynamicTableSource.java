package io.cobble.flink.table;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;

/**
 * Cobble state source whose DDL schema has been resolved and validated at planning time, but whose
 * data-reading runtime is not implemented yet.
 *
 * <p>Planning succeeds (so valid state DDL can be created and explained), but requesting a scan or
 * lookup runtime provider fails clearly. This keeps the state path separate from the sink source so
 * sink key/value codecs are never instantiated for state.
 */
final class CobbleStateDynamicTableSource implements ScanTableSource, LookupTableSource {

    private static final String RUNTIME_NOT_IMPLEMENTED =
            "Cobble state source schema is resolved, but state source runtime is not implemented"
                    + " yet.";

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
        throw new UnsupportedOperationException(RUNTIME_NOT_IMPLEMENTED);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        throw new UnsupportedOperationException(RUNTIME_NOT_IMPLEMENTED);
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
