package io.cobble.flink.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Configuration for the Cobble state source.
 *
 * <p>Detection (Step 1) produces a placeholder carrying only the detected {@code pathUri} and
 * {@link Layout}. Planning (Step 2) produces a fully-resolved, serializable config that the Step 3
 * runtime will consume: it adds the selected operator, state name/kind, the schema-registry
 * checkpoint id, the requested {@code scan.checkpoint-id}, the bucket count, and the resolved SQL
 * output columns.
 *
 * <p>It deliberately does <em>not</em> hold sink key/value field mappings: state decoding uses the
 * state inspect schema registry instead.
 */
final class StateSourceConfig implements Serializable {

    private static final long serialVersionUID = 2L;

    /** Shape of the detected state path. */
    enum Layout {
        /** A Flink checkpoint root: a directory containing {@code chk-*} subdirectories. */
        CHECKPOINT_ROOT,

        /**
         * A single state operator inspect-schema root. It lacks checkpoint root / shared-volume
         * context, so reads must be pointed at the enclosing checkpoint root instead.
         */
        OPERATOR_ROOT,

        /** Layout could not be confirmed (only reachable via an explicit {@code source.kind}). */
        UNKNOWN
    }

    private final String pathUri;
    private final Layout layout;
    // Fully-resolved fields (null on a detection-only placeholder).
    private final String operatorId;
    private final String stateName;
    private final String stateKind;
    private final String scanCheckpointId;
    private final long schemaCheckpointId;
    private final int bucketCount;
    private final List<StateSourceField> outputFields;

    /** Detection-only placeholder: layout is known, schema is not yet resolved. */
    StateSourceConfig(String pathUri, Layout layout) {
        this.pathUri = pathUri;
        this.layout = layout;
        this.operatorId = null;
        this.stateName = null;
        this.stateKind = null;
        this.scanCheckpointId = null;
        this.schemaCheckpointId = -1L;
        this.bucketCount = -1;
        this.outputFields = Collections.emptyList();
    }

    /** Fully-resolved config ready for the Step 3 runtime. */
    StateSourceConfig(
            String pathUri,
            Layout layout,
            String operatorId,
            String stateName,
            String stateKind,
            String scanCheckpointId,
            long schemaCheckpointId,
            int bucketCount,
            List<StateSourceField> outputFields) {
        this.pathUri = pathUri;
        this.layout = layout;
        this.operatorId = operatorId;
        this.stateName = stateName;
        this.stateKind = stateKind;
        this.scanCheckpointId = scanCheckpointId;
        this.schemaCheckpointId = schemaCheckpointId;
        this.bucketCount = bucketCount;
        this.outputFields = Collections.unmodifiableList(new ArrayList<>(outputFields));
    }

    String pathUri() {
        return pathUri;
    }

    Layout layout() {
        return layout;
    }

    String operatorId() {
        return operatorId;
    }

    String stateName() {
        return stateName;
    }

    /** Resolved state kind as a lowercase wire name (for example {@code value}, {@code map}). */
    String stateKind() {
        return stateKind;
    }

    String scanCheckpointId() {
        return scanCheckpointId;
    }

    /** Checkpoint id of the schema-registry event the schema was read from. */
    long schemaCheckpointId() {
        return schemaCheckpointId;
    }

    int bucketCount() {
        return bucketCount;
    }

    List<StateSourceField> outputFields() {
        return outputFields;
    }
}
