package io.cobble.flink.table;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        int[] lookupKeyPositions = resolveLookupKeyPositions(context, contract);
        return LookupFunctionProvider.of(new CobbleStateLookupFunction(config, lookupKeyPositions));
    }

    /**
     * Resolves the lookup-row field positions from the planner {@link LookupContext}, mapped to the
     * required-field order in the {@link StateSourceLookupKeyContract}.
     *
     * <p>This is order-independent: the planner may pass {@code context.getKeys()} in any order.
     * For each required field (identified by its physical column position), the resolver scans the
     * planner keys to find the matching lookup-row position. The returned array satisfies:
     *
     * <pre>{@code
     * lookupKeyPositionsByRequiredField[requiredFieldIndex] = lookupRowPosition
     * }</pre>
     *
     * <p>Validation rules enforced:
     *
     * <ol>
     *   <li>Key count must match the required-field count.
     *   <li>Each planner key must be a top-level column (path length 1).
     *   <li>Planner key physical positions must not be duplicated.
     *   <li>Every required physical position must be present in the planner keys.
     *   <li>Every planner key physical position must be part of the required set.
     * </ol>
     */
    private int[] resolveLookupKeyPositions(
            LookupContext context, StateSourceLookupKeyContract contract) {
        int[][] keys = context.getKeys();
        int[] requiredPositions = contract.requiredPhysicalPositions();
        List<StateSourceField> requiredFields = contract.requiredFields();

        if (keys.length != requiredPositions.length) {
            throw new ValidationException(
                    "Cobble state source lookup for state '"
                            + config.stateName()
                            + "' requires equality conditions for all "
                            + requiredPositions.length
                            + " PRIMARY KEY column(s), but the lookup provided "
                            + keys.length
                            + ".");
        }

        // Validate that all planner keys are top-level (no nested paths) and detect duplicates.
        Set<Integer> plannerPhysicalPositions = new HashSet<>();
        for (int i = 0; i < keys.length; i++) {
            int[] lookupKey = keys[i];
            if (lookupKey.length != 1) {
                throw new ValidationException(
                        "Cobble state source lookup for state '"
                                + config.stateName()
                                + "' supports only top-level PRIMARY KEY columns.");
            }
            int physicalPos = lookupKey[0];
            if (!plannerPhysicalPositions.add(physicalPos)) {
                throw new ValidationException(
                        "Cobble state source lookup for state '"
                                + config.stateName()
                                + "' has a duplicate lookup key for physical column "
                                + physicalPos
                                + ".");
            }
        }

        // Build a lookup from physical position → planner key index for fast reverse lookup.
        Map<Integer, Integer> plannerIndexByPhysicalPos = new HashMap<>();
        for (int i = 0; i < keys.length; i++) {
            plannerIndexByPhysicalPos.put(keys[i][0], i);
        }

        // For each required field, find the matching planner key by physical position.
        int[] result = new int[requiredPositions.length];
        Set<Integer> requiredPosSet = new HashSet<>();
        for (int pos : requiredPositions) {
            requiredPosSet.add(pos);
        }
        for (int requiredIndex = 0; requiredIndex < requiredPositions.length; requiredIndex++) {
            int targetPos = requiredPositions[requiredIndex];
            Integer plannerIndex = plannerIndexByPhysicalPos.get(targetPos);
            if (plannerIndex == null) {
                throw new ValidationException(
                        "Cobble state source lookup for state '"
                                + config.stateName()
                                + "' is missing a lookup key for column '"
                                + requiredFields.get(requiredIndex).name()
                                + "' (physical position "
                                + targetPos
                                + ").");
            }
            result[requiredIndex] = plannerIndex;
        }

        // Check for extra/unrelated planner keys targeting columns not in the required set.
        for (int i = 0; i < keys.length; i++) {
            int physicalPos = keys[i][0];
            if (!requiredPosSet.contains(physicalPos)) {
                throw new ValidationException(
                        "Cobble state source lookup for state '"
                                + config.stateName()
                                + "' has a lookup key at position "
                                + i
                                + " targeting physical column "
                                + physicalPos
                                + " which is not part of the PRIMARY KEY.");
            }
        }

        return result;
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
