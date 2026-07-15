package io.cobble.flink.inspect.internal;

import io.cobble.flink.common.inspect.StateInspectSchemaStore;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Result of resolving the inspect-schema registry for a given checkpoint and operator.
 *
 * <p>Callers should inspect {@link #status} rather than assuming a non-empty store; every
 * non-{@code available} resolution degrades to an empty schema store.
 */
public final class SchemaResolveResult {

    public static final String STATUS_AVAILABLE = "available";
    public static final String STATUS_MISSING = "missing";
    public static final String STATUS_UNAVAILABLE = "unavailable";
    public static final String STATUS_INVALID = "invalid";
    public static final String STATUS_UNSUPPORTED = "unsupported";

    public final StateInspectSchemaStore store;
    public final String status;
    public final String eventPath;
    public final String blobPath;
    public final String schemaHash;
    public final Long schemaCheckpointId;
    public final String warning;

    private SchemaResolveResult(
            StateInspectSchemaStore store,
            String status,
            String eventPath,
            String blobPath,
            String schemaHash,
            Long schemaCheckpointId,
            String warning) {
        this.store = store;
        this.status = status;
        this.eventPath = eventPath;
        this.blobPath = blobPath;
        this.schemaHash = schemaHash;
        this.schemaCheckpointId = schemaCheckpointId;
        this.warning = warning;
    }

    public static SchemaResolveResult available(
            StateInspectSchemaStore store,
            String eventPath,
            String blobPath,
            String schemaHash,
            long schemaCheckpointId) {
        return available(store, eventPath, blobPath, schemaHash, schemaCheckpointId, null);
    }

    public static SchemaResolveResult available(
            StateInspectSchemaStore store,
            String eventPath,
            String blobPath,
            String schemaHash,
            long schemaCheckpointId,
            String warning) {
        return new SchemaResolveResult(
                store,
                STATUS_AVAILABLE,
                eventPath,
                blobPath,
                schemaHash,
                schemaCheckpointId,
                warning);
    }

    public static SchemaResolveResult missing(String warning) {
        return new SchemaResolveResult(
                StateInspectSchemaStore.empty(), STATUS_MISSING, null, null, null, null, warning);
    }

    public static SchemaResolveResult unavailable(String warning) {
        return new SchemaResolveResult(
                StateInspectSchemaStore.empty(),
                STATUS_UNAVAILABLE,
                null,
                null,
                null,
                null,
                warning);
    }

    public static SchemaResolveResult invalid(String warning) {
        return new SchemaResolveResult(
                StateInspectSchemaStore.empty(), STATUS_INVALID, null, null, null, null, warning);
    }

    public static SchemaResolveResult unsupported(String warning) {
        return new SchemaResolveResult(
                StateInspectSchemaStore.empty(),
                STATUS_UNSUPPORTED,
                null,
                null,
                null,
                null,
                warning);
    }

    boolean hasSchema() {
        return STATUS_AVAILABLE.equals(status);
    }

    public Map<String, Object> toJson() {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("status", status);
        output.put("schema_checkpoint_id", schemaCheckpointId);
        output.put("schema_hash", schemaHash);
        output.put("event_path", eventPath);
        output.put("blob_path", blobPath);
        output.put("state_count", store.isEmpty() ? 0 : store.schemas().size());
        output.put("warning", warning);
        return output;
    }
}
