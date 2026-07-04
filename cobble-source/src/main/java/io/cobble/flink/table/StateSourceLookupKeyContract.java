package io.cobble.flink.table;

import io.cobble.flink.common.inspect.StateKind;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The exact full-key lookup contract derived from a state source DDL {@code PRIMARY KEY}.
 *
 * <p>v1 lookup is exact only: the declared primary key must contain every required logical key
 * column for the state kind, in order, and nothing else. No prefix lookup, no partial lookup.
 *
 * <p>{@link #ABSENT} means the DDL has no primary key: scan is allowed and lookup must fail
 * clearly. {@link #PRESENT} means the DDL primary key exactly matches the derived required full
 * lookup key.
 *
 * <p>This is a planning-time contract only. It does not encode row-key bytes or runtime state; the
 * lookup runtime consumes it later to validate planner keys and encode the Cobble row key.
 */
final class StateSourceLookupKeyContract implements Serializable {

    private static final long serialVersionUID = 1L;

    enum Status {
        ABSENT,
        PRESENT
    }

    /** Whether the DDL declares a valid exact lookup key for this state source. */
    private final Status status;

    private final List<StateSourceField> requiredFields;
    private final int[] requiredPhysicalPositions;

    private StateSourceLookupKeyContract(
            Status status, List<StateSourceField> requiredFields, int[] requiredPhysicalPositions) {
        this.status = status;
        this.requiredFields = requiredFields;
        this.requiredPhysicalPositions = requiredPhysicalPositions;
    }

    static StateSourceLookupKeyContract absent() {
        return new StateSourceLookupKeyContract(Status.ABSENT, Collections.emptyList(), new int[0]);
    }

    static StateSourceLookupKeyContract present(
            List<StateSourceField> requiredFields, int[] requiredPhysicalPositions) {
        return new StateSourceLookupKeyContract(
                Status.PRESENT,
                Collections.unmodifiableList(new ArrayList<>(requiredFields)),
                requiredPhysicalPositions.clone());
    }

    Status status() {
        return status;
    }

    boolean isPresent() {
        return status == Status.PRESENT;
    }

    /** Required lookup fields in key order (state key, then namespace, then map key). */
    List<StateSourceField> requiredFields() {
        return requiredFields;
    }

    /** Physical column position of each required field, aligned with {@link #requiredFields()}. */
    int[] requiredPhysicalPositions() {
        return requiredPhysicalPositions.clone();
    }

    /**
     * Derives the lookup contract from the resolved state shape and the DDL.
     *
     * @param stateName resolved state name
     * @param stateKind resolved state kind (wire name)
     * @param outputFields resolved scan output fields (semantic order)
     * @param ddlSchema the original DDL, including any primary key
     */
    static StateSourceLookupKeyContract derive(
            String stateName,
            StateKind stateKind,
            List<StateSourceField> outputFields,
            ResolvedSchema ddlSchema) {
        List<StateSourceField> required = requiredFields(stateName, stateKind, outputFields);
        int[] positions = physicalPositions(outputFields, required);

        if (!ddlSchema.getPrimaryKey().isPresent()) {
            // No PK declared: scan is allowed; lookup is not contracted yet. The required fields
            // are
            // not stored because there is no contract to enforce at runtime.
            return absent();
        }

        if (stateKind == StateKind.TIMER) {
            throw new ValidationException(
                    "Cobble timer state source does not support lookup PRIMARY KEY. Remove the"
                            + " PRIMARY KEY or use scan for state '"
                            + stateName
                            + "'.");
        }

        UniqueConstraint primaryKey = ddlSchema.getPrimaryKey().get();
        List<String> pkColumns = primaryKey.getColumns();
        if (pkColumns.size() != required.size()) {
            throw new ValidationException(
                    lookupContractMessage(
                            stateName,
                            stateKind,
                            required,
                            pkColumns,
                            "requires PRIMARY KEY ("
                                    + describeNames(required)
                                    + ") but got PRIMARY KEY ("
                                    + describePk(pkColumns)
                                    + ")."));
        }
        for (int i = 0; i < required.size(); i++) {
            if (!pkColumns.get(i).equals(required.get(i).name())) {
                throw new ValidationException(
                        lookupContractMessage(
                                stateName,
                                stateKind,
                                required,
                                pkColumns,
                                "requires PRIMARY KEY ("
                                        + describeNames(required)
                                        + ") but got PRIMARY KEY ("
                                        + describePk(pkColumns)
                                        + ")."));
            }
        }

        // Reject a PK that references value/list-element/map-value/timer-timestamp columns. The
        // count/name match above already implies this when required fields are correct, but verify
        // explicitly so a future required-field bug fails loudly instead of silently.
        for (String pkColumn : pkColumns) {
            if (!isRequiredColumnName(required, pkColumn)) {
                throw new ValidationException(
                        "Cobble state source lookup for state '"
                                + stateName
                                + "' primary key column '"
                                + pkColumn
                                + "' is not part of the required lookup key ("
                                + describeNames(required)
                                + ").");
            }
        }

        return present(required, positions);
    }

    private static List<StateSourceField> requiredFields(
            String stateName, StateKind stateKind, List<StateSourceField> outputFields) {
        List<StateSourceField> required = new ArrayList<>();
        for (StateSourceField field : outputFields) {
            if (isRequiredGroup(field.group(), stateKind)) {
                required.add(field);
            }
        }
        if (required.isEmpty() && stateKind != StateKind.TIMER) {
            throw new ValidationException(
                    "Cobble state source could not derive a lookup key for state '"
                            + stateName
                            + "' (kind "
                            + stateKind.wireName()
                            + "). The resolved output fields have no key columns.");
        }
        return required;
    }

    private static boolean isRequiredGroup(StateSourceField.Group group, StateKind stateKind) {
        switch (stateKind) {
            case VALUE:
            case REDUCING:
            case AGGREGATING:
            case LIST:
                return group == StateSourceField.Group.STATE_KEY
                        || group == StateSourceField.Group.NAMESPACE;
            case MAP:
                return group == StateSourceField.Group.STATE_KEY
                        || group == StateSourceField.Group.NAMESPACE
                        || group == StateSourceField.Group.MAP_KEY;
            case TIMER:
            default:
                return false;
        }
    }

    private static int[] physicalPositions(
            List<StateSourceField> outputFields, List<StateSourceField> required) {
        Map<String, Integer> positionByName = new LinkedHashMap<>();
        for (int i = 0; i < outputFields.size(); i++) {
            positionByName.put(outputFields.get(i).name(), i);
        }
        int[] positions = new int[required.size()];
        for (int i = 0; i < required.size(); i++) {
            Integer pos = positionByName.get(required.get(i).name());
            if (pos == null) {
                // Should be unreachable because required fields come from outputFields.
                throw new IllegalStateException(
                        "Required lookup field '"
                                + required.get(i).name()
                                + "' is not present in output fields.");
            }
            positions[i] = pos;
        }
        return positions;
    }

    private static boolean isRequiredColumnName(
            List<StateSourceField> required, String columnName) {
        for (StateSourceField field : required) {
            if (field.name().equals(columnName)) {
                return true;
            }
        }
        return false;
    }

    private static String lookupContractMessage(
            String stateName,
            StateKind stateKind,
            List<StateSourceField> required,
            List<String> pkColumns,
            String detail) {
        if (stateKind == StateKind.MAP) {
            return "Cobble state source lookup for map state '"
                    + stateName
                    + "' requires the full map-entry key PRIMARY KEY ("
                    + describeNames(required)
                    + ").";
        }
        return "Cobble state source lookup for state '" + stateName + "' " + detail;
    }

    private static String describeNames(List<StateSourceField> fields) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append("`").append(fields.get(i).name()).append("`");
        }
        return builder.toString();
    }

    private static String describePk(List<String> pkColumns) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < pkColumns.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append("`").append(pkColumns.get(i)).append("`");
        }
        return builder.toString();
    }
}
