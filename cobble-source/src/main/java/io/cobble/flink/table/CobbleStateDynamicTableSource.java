package io.cobble.flink.table;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;

/**
 * Cobble state source whose DDL schema has been resolved and validated at planning time.
 *
 * <p>Batch scans use the state checkpoint runtime. Lookup is an exact full-key contract: an
 * optional DDL {@code PRIMARY KEY} declares the full logical lookup key. Value-like states
 * (value/reducing/aggregating) and map states support exact lookup via {@link
 * CobbleStateLookupFunction}; list/timer are rejected with a clear message.
 */
final class CobbleStateDynamicTableSource implements ScanTableSource, LookupTableSource {

    private static final String STREAMING_NOT_SUPPORTED =
            "Cobble state source currently supports only scan.mode='batch'.";
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
        int[] lookupKeyPositions = resolveLookupKeyPositions(context, contract);
        return LookupFunctionProvider.of(new CobbleStateLookupFunction(config, lookupKeyPositions));
    }

    /**
     * Extracts the lookup-row field positions from the planner {@link LookupContext}, aligned with
     * the required-field order in the {@link StateSourceLookupKeyContract}.
     *
     * <p>Current validation requires planner lookup keys in the same order as required fields, so
     * this returns {@code [0, 1, ...]}. Kept as a separate method so a future order-independent
     * planner mapping can replace it without changing the lookup function.
     */
    private static int[] resolveLookupKeyPositions(
            LookupContext context, StateSourceLookupKeyContract contract) {
        int[][] keys = context.getKeys();
        int[] positions = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            positions[i] = i;
        }
        return positions;
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
