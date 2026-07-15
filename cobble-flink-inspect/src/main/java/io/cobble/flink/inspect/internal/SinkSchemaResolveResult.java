package io.cobble.flink.inspect.internal;

import io.cobble.flink.common.inspect.SinkInspectSchemaStore;

import java.util.LinkedHashMap;
import java.util.Map;

/** Result of resolving a Cobble sink inspect-schema registry for one selected snapshot. */
public final class SinkSchemaResolveResult {

    public static final String STATUS_AVAILABLE = "available";
    public static final String STATUS_MISSING = "missing";
    public static final String STATUS_UNAVAILABLE = "unavailable";
    public static final String STATUS_INVALID = "invalid";
    public static final String STATUS_UNSUPPORTED = "unsupported";

    public final SinkInspectSchemaStore store;
    public final String status;
    public final String eventPath;
    public final String blobPath;
    public final String schemaHash;
    public final Long schemaSnapshotId;
    public final String warning;

    private SinkSchemaResolveResult(
            SinkInspectSchemaStore store,
            String status,
            String eventPath,
            String blobPath,
            String schemaHash,
            Long schemaSnapshotId,
            String warning) {
        this.store = store;
        this.status = status;
        this.eventPath = eventPath;
        this.blobPath = blobPath;
        this.schemaHash = schemaHash;
        this.schemaSnapshotId = schemaSnapshotId;
        this.warning = warning;
    }

    public static SinkSchemaResolveResult available(
            SinkInspectSchemaStore store,
            String eventPath,
            String blobPath,
            String schemaHash,
            long schemaSnapshotId) {
        return new SinkSchemaResolveResult(
                store, STATUS_AVAILABLE, eventPath, blobPath, schemaHash, schemaSnapshotId, null);
    }

    public static SinkSchemaResolveResult missing(String warning) {
        return new SinkSchemaResolveResult(
                new SinkInspectSchemaStore(null), STATUS_MISSING, null, null, null, null, warning);
    }

    public static SinkSchemaResolveResult unavailable(String warning) {
        return new SinkSchemaResolveResult(
                new SinkInspectSchemaStore(null),
                STATUS_UNAVAILABLE,
                null,
                null,
                null,
                null,
                warning);
    }

    public static SinkSchemaResolveResult invalid(String warning) {
        return new SinkSchemaResolveResult(
                new SinkInspectSchemaStore(null), STATUS_INVALID, null, null, null, null, warning);
    }

    public static SinkSchemaResolveResult unsupported(String warning) {
        return new SinkSchemaResolveResult(
                new SinkInspectSchemaStore(null),
                STATUS_UNSUPPORTED,
                null,
                null,
                null,
                null,
                warning);
    }

    boolean hasSchema() {
        return STATUS_AVAILABLE.equals(status) && !store.isEmpty();
    }

    public Map<String, Object> toJson() {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("status", status);
        output.put("schema_snapshot_id", schemaSnapshotId);
        output.put("schema_hash", schemaHash);
        output.put("event_path", eventPath);
        output.put("blob_path", blobPath);
        output.put("available", hasSchema());
        output.put("warning", warning);
        return output;
    }
}
