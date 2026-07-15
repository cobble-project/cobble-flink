package io.cobble.flink.common;

import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Parsed Cobble state embedded in a Flink NATIVE savepoint's {@code _metadata} file. */
public final class CobbleNativeSavepoint {

    private final long checkpointId;
    private final Map<String, OperatorSnapshot> operators;

    private CobbleNativeSavepoint(long checkpointId, Map<String, OperatorSnapshot> operators) {
        this.checkpointId = checkpointId;
        this.operators = Collections.unmodifiableMap(new LinkedHashMap<>(operators));
    }

    public long checkpointId() {
        return checkpointId;
    }

    public Map<String, OperatorSnapshot> operators() {
        return operators;
    }

    public OperatorSnapshot operator(String operatorId) {
        return operators.get(operatorId);
    }

    /** Loads only Cobble's NATIVE keyed-state handles and ignores every other operator/handle. */
    public static CobbleNativeSavepoint load(Path savepointDirectory) throws IOException {
        Path metadataPath = new Path(savepointDirectory, "_metadata");
        CheckpointMetadata metadata;
        try (FSDataInputStream input = metadataPath.getFileSystem().open(metadataPath)) {
            metadata =
                    Checkpoints.loadCheckpointMetadata(
                            new DataInputStream(input),
                            Thread.currentThread().getContextClassLoader(),
                            savepointDirectory.toUri().toString());
        }
        Map<String, OperatorSnapshot> operators = new LinkedHashMap<>();
        for (OperatorState operatorState : metadata.getOperatorStates()) {
            List<ShardSnapshot> shards = new ArrayList<>();
            List<StateInspectSchemaStore> stores = new ArrayList<>();
            for (OperatorSubtaskState subtaskState : operatorState.getStates()) {
                collectCobbleHandles(subtaskState.getManagedKeyedState(), shards, stores);
                collectCobbleHandles(subtaskState.getRawKeyedState(), shards, stores);
            }
            if (!shards.isEmpty()) {
                String operatorId = operatorState.getOperatorID().toHexString();
                operators.put(
                        operatorId,
                        new OperatorSnapshot(
                                operatorId,
                                operatorState.getMaxParallelism(),
                                shards,
                                mergeSchemaStores(stores)));
            }
        }
        if (operators.isEmpty()) {
            throw new IOException(
                    "No Cobble NATIVE keyed-state handles were found in Flink savepoint "
                            + savepointDirectory
                            + ". Canonical savepoints and non-Cobble operators are not readable "
                            + "as native Cobble state.");
        }
        return new CobbleNativeSavepoint(metadata.getCheckpointId(), operators);
    }

    private static void collectCobbleHandles(
            Collection<KeyedStateHandle> handles,
            List<ShardSnapshot> shards,
            List<StateInspectSchemaStore> stores)
            throws IOException {
        for (KeyedStateHandle handle : handles) {
            if (!(handle instanceof IncrementalRemoteKeyedStateHandle)) {
                continue;
            }
            IncrementalRemoteKeyedStateHandle incremental =
                    (IncrementalRemoteKeyedStateHandle) handle;
            if (incremental.getMetaStateHandle() == null) {
                continue;
            }
            CobbleSnapshotMetadataPayload payload;
            try (FSDataInputStream input = incremental.getMetaStateHandle().openInputStream()) {
                payload =
                        CobbleSnapshotMetadataCodec.readIfPresent(
                                new DataInputViewStreamWrapper(input));
            }
            if (payload != null && payload.shardSnapshot() != null) {
                shards.add(payload.shardSnapshot());
                stores.add(payload.schemaStore());
            }
        }
    }

    private static StateInspectSchemaStore mergeSchemaStores(List<StateInspectSchemaStore> stores) {
        Map<String, StateInspectSchema> schemas = new LinkedHashMap<>();
        Map<String, StateInspectSemanticSchema> semanticSchemas = new LinkedHashMap<>();
        for (StateInspectSchemaStore store : stores) {
            for (StateInspectSchema schema : store.schemas()) {
                schemas.putIfAbsent(schema.stateKind().name() + ':' + schema.stateName(), schema);
            }
            for (Map.Entry<String, StateInspectSemanticSchema> entry :
                    store.semanticSchemas().entrySet()) {
                semanticSchemas.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
        return schemas.isEmpty()
                ? StateInspectSchemaStore.empty()
                : new StateInspectSchemaStore(new ArrayList<>(schemas.values()), semanticSchemas);
    }

    /** Cobble snapshots and merged inspect schema for one Flink operator. */
    public static final class OperatorSnapshot {
        private final String operatorId;
        private final int maxParallelism;
        private final List<ShardSnapshot> shards;
        private final StateInspectSchemaStore schemaStore;

        private OperatorSnapshot(
                String operatorId,
                int maxParallelism,
                List<ShardSnapshot> shards,
                StateInspectSchemaStore schemaStore) {
            this.operatorId = operatorId;
            this.maxParallelism = maxParallelism;
            this.shards = Collections.unmodifiableList(new ArrayList<>(shards));
            this.schemaStore = schemaStore;
        }

        public String operatorId() {
            return operatorId;
        }

        public int maxParallelism() {
            return maxParallelism;
        }

        public List<ShardSnapshot> shards() {
            return shards;
        }

        public StateInspectSchemaStore schemaStore() {
            return schemaStore;
        }
    }
}
