package io.cobble.flink.table;

import io.cobble.flink.common.inspect.StateKind;

import java.util.Collections;
import java.util.List;

/**
 * Result of resolving a Cobble state source against the inspect-schema registry at planning time.
 *
 * <p>Carries the selected operator, state, kind, the schema-registry checkpoint id the schema was
 * read from, and the expected SQL output columns. The factory turns this into a serializable {@link
 * StateSourceConfig} for the (future) runtime.
 */
final class StateSourceResolvedSchema {

    private final String operatorId;
    private final String stateName;
    private final StateKind stateKind;
    private final long schemaCheckpointId;
    private final List<StateSourceField> outputFields;

    StateSourceResolvedSchema(
            String operatorId,
            String stateName,
            StateKind stateKind,
            long schemaCheckpointId,
            List<StateSourceField> outputFields) {
        this.operatorId = operatorId;
        this.stateName = stateName;
        this.stateKind = stateKind;
        this.schemaCheckpointId = schemaCheckpointId;
        this.outputFields = Collections.unmodifiableList(outputFields);
    }

    String operatorId() {
        return operatorId;
    }

    String stateName() {
        return stateName;
    }

    StateKind stateKind() {
        return stateKind;
    }

    /** Checkpoint id of the schema-registry event the schema was read from. */
    long schemaCheckpointId() {
        return schemaCheckpointId;
    }

    List<StateSourceField> outputFields() {
        return outputFields;
    }
}
