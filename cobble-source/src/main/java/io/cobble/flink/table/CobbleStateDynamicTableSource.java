package io.cobble.flink.table;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;

/**
 * Cobble state source whose DDL schema has been resolved and validated at planning time.
 *
 * <p>Batch scans use the state checkpoint runtime. Lookup is an exact full-key contract: an
 * optional DDL {@code PRIMARY KEY} declares the full logical lookup key. The lookup
 * <em>runtime</em> is still not implemented, so after validating the contract and planner lookup
 * keys the provider fails with a clear not-implemented boundary.
 */
final class CobbleStateDynamicTableSource implements ScanTableSource, LookupTableSource {

    private static final String STREAMING_NOT_SUPPORTED =
            "Cobble state source currently supports only scan.mode='batch'.";
    private static final String LOOKUP_NOT_IMPLEMENTED =
            "Cobble state exact lookup runtime is not implemented yet.";
    private static final String LIST_LOOKUP_UNSUPPORTED =
            "Cobble state source list lookup is not supported yet.";
    private static final String TIMER_LOOKUP_UNSUPPORTED =
            "Cobble state source timer lookup is not supported.";

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
        StateSourceLookupKeyContract contract = config.lookupKeyContract();
        if (!contract.isPresent()) {
            throw new ValidationException(
                    "Cobble state source lookup for state '"
                            + config.stateName()
                            + "' requires a DDL PRIMARY KEY matching the full lookup key. Declare"
                            + " PRIMARY KEY (...) NOT ENFORCED on the state source table.");
        }
        if ("timer".equals(config.stateKind())) {
            throw new ValidationException(TIMER_LOOKUP_UNSUPPORTED);
        }
        if ("list".equals(config.stateKind())) {
            throw new ValidationException(LIST_LOOKUP_UNSUPPORTED);
        }
        validateLookupKeys(context, contract);
        // The exact-key contract and planner lookup keys are now fully validated. The lookup
        // runtime
        // itself is implemented in a later step; fail here so the planner never silently requests a
        // partial/prefix lookup.
        throw new UnsupportedOperationException(LOOKUP_NOT_IMPLEMENTED);
    }

    private static void validateLookupKeys(
            LookupContext context, StateSourceLookupKeyContract contract) {
        int[][] keys = context.getKeys();
        int[] requiredPositions = contract.requiredPhysicalPositions();
        if (keys.length != requiredPositions.length) {
            throw new ValidationException(
                    "Cobble state source lookup requires equality conditions for all "
                            + requiredPositions.length
                            + " PRIMARY KEY column(s), but the lookup provided "
                            + keys.length
                            + ".");
        }
        for (int i = 0; i < keys.length; i++) {
            int[] lookupKey = keys[i];
            if (lookupKey.length != 1) {
                throw new ValidationException(
                        "Cobble state source lookup supports only top-level PRIMARY KEY columns.");
            }
            if (lookupKey[0] != requiredPositions[i]) {
                throw new ValidationException(
                        "Cobble state source lookup key at position "
                                + i
                                + " targets physical column "
                                + lookupKey[0]
                                + " but the lookup contract requires physical column "
                                + requiredPositions[i]
                                + ".");
            }
        }
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
