package io.cobble.flink.table;

import io.cobble.flink.common.CobbleFlinkStorageConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Configuration for the Cobble state source.
 *
 * <p>Detection produces a placeholder carrying only the detected {@code pathUri} and {@link
 * Layout}. Planning produces a fully-resolved, serializable config consumed by scan and lookup
 * runtimes: it adds the selected operator, state name/kind, the schema-registry checkpoint id, the
 * requested {@code scan.checkpoint-id}, the bucket count, the resolved SQL output columns, and the
 * optional exact-lookup key contract.
 *
 * <p>It deliberately does <em>not</em> hold sink key/value field mappings: state decoding uses the
 * state inspect schema registry instead.
 */
final class StateSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Shape of the detected state path. */
    enum Layout {
        /** A Flink checkpoint root: a directory containing {@code chk-*} subdirectories. */
        CHECKPOINT_ROOT,

        /** A Flink checkpoint/savepoint reachable through embedded {@code _metadata} payloads. */
        EMBEDDED_CHECKPOINT,

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
    private final String scanMode;
    private final long schemaCheckpointId;
    private final int bucketCount;
    private final long sourceBlockCacheMemoryBytes;
    private final List<StateSourceField> outputFields;
    private final int[] outputProjection;
    private final StateSourceLookupKeyContract lookupKeyContract;
    private final CobbleFlinkStorageConfig storageConfig;

    /** Detection-only placeholder: layout is known, schema is not yet resolved. */
    StateSourceConfig(String pathUri, Layout layout) {
        this.pathUri = pathUri;
        this.layout = layout;
        this.operatorId = null;
        this.stateName = null;
        this.stateKind = null;
        this.scanCheckpointId = null;
        this.scanMode = null;
        this.schemaCheckpointId = -1L;
        this.bucketCount = -1;
        this.sourceBlockCacheMemoryBytes = 0L;
        this.outputFields = Collections.emptyList();
        this.outputProjection = new int[0];
        this.lookupKeyContract = StateSourceLookupKeyContract.absent();
        this.storageConfig = CobbleFlinkStorageConfig.empty();
    }

    /** Fully-resolved config ready for state scan and lookup runtimes. */
    StateSourceConfig(
            String pathUri,
            Layout layout,
            String operatorId,
            String stateName,
            String stateKind,
            String scanCheckpointId,
            String scanMode,
            long schemaCheckpointId,
            int bucketCount,
            long sourceBlockCacheMemoryBytes,
            List<StateSourceField> outputFields) {
        this(
                pathUri,
                layout,
                operatorId,
                stateName,
                stateKind,
                scanCheckpointId,
                scanMode,
                schemaCheckpointId,
                bucketCount,
                sourceBlockCacheMemoryBytes,
                outputFields,
                StateSourceLookupKeyContract.absent());
    }

    /** Fully-resolved config carrying an optional exact-key lookup contract. */
    StateSourceConfig(
            String pathUri,
            Layout layout,
            String operatorId,
            String stateName,
            String stateKind,
            String scanCheckpointId,
            String scanMode,
            long schemaCheckpointId,
            int bucketCount,
            long sourceBlockCacheMemoryBytes,
            List<StateSourceField> outputFields,
            StateSourceLookupKeyContract lookupKeyContract) {
        this(
                pathUri,
                layout,
                operatorId,
                stateName,
                stateKind,
                scanCheckpointId,
                scanMode,
                schemaCheckpointId,
                bucketCount,
                sourceBlockCacheMemoryBytes,
                outputFields,
                lookupKeyContract,
                SourceProjection.all(outputFields.size()),
                CobbleFlinkStorageConfig.empty());
    }

    private StateSourceConfig(
            String pathUri,
            Layout layout,
            String operatorId,
            String stateName,
            String stateKind,
            String scanCheckpointId,
            String scanMode,
            long schemaCheckpointId,
            int bucketCount,
            long sourceBlockCacheMemoryBytes,
            List<StateSourceField> outputFields,
            StateSourceLookupKeyContract lookupKeyContract,
            int[] outputProjection,
            CobbleFlinkStorageConfig storageConfig) {
        this.pathUri = pathUri;
        this.layout = layout;
        this.operatorId = operatorId;
        this.stateName = stateName;
        this.stateKind = stateKind;
        this.scanCheckpointId = scanCheckpointId;
        this.scanMode = scanMode;
        this.schemaCheckpointId = schemaCheckpointId;
        this.bucketCount = bucketCount;
        this.sourceBlockCacheMemoryBytes = sourceBlockCacheMemoryBytes;
        this.outputFields = Collections.unmodifiableList(new ArrayList<>(outputFields));
        this.outputProjection = outputProjection.clone();
        this.storageConfig = storageConfig;
        this.lookupKeyContract =
                lookupKeyContract == null
                        ? StateSourceLookupKeyContract.absent()
                        : lookupKeyContract;
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

    String scanMode() {
        return scanMode;
    }

    /** Checkpoint id of the schema-registry event the schema was read from. */
    long schemaCheckpointId() {
        return schemaCheckpointId;
    }

    int bucketCount() {
        return bucketCount;
    }

    long sourceBlockCacheMemoryBytes() {
        return sourceBlockCacheMemoryBytes;
    }

    List<StateSourceField> outputFields() {
        return outputFields;
    }

    List<StateSourceField> projectedFields() {
        List<StateSourceField> fields = new ArrayList<>(outputProjection.length);
        for (int index : outputProjection) fields.add(outputFields.get(index));
        return fields;
    }

    int[] outputProjection() {
        return outputProjection.clone();
    }

    StateSourceConfig withProjection(int[] projection) {
        return new StateSourceConfig(
                pathUri,
                layout,
                operatorId,
                stateName,
                stateKind,
                scanCheckpointId,
                scanMode,
                schemaCheckpointId,
                bucketCount,
                sourceBlockCacheMemoryBytes,
                outputFields,
                lookupKeyContract,
                projection,
                storageConfig);
    }

    StateSourceConfig withStorageConfig(CobbleFlinkStorageConfig options) {
        return new StateSourceConfig(
                pathUri,
                layout,
                operatorId,
                stateName,
                stateKind,
                scanCheckpointId,
                scanMode,
                schemaCheckpointId,
                bucketCount,
                sourceBlockCacheMemoryBytes,
                outputFields,
                lookupKeyContract,
                outputProjection,
                options);
    }

    CobbleFlinkStorageConfig storageConfig() {
        return storageConfig;
    }

    /** The exact-key lookup contract derived from the DDL primary key (absent when no PK). */
    StateSourceLookupKeyContract lookupKeyContract() {
        return lookupKeyContract;
    }
}
