package io.cobble.flink.monitor;

import io.cobble.flink.common.inspect.SinkInspectSchemaStore;

import java.util.LinkedHashMap;
import java.util.Map;

/** Result of resolving a Cobble sink inspect-schema registry for one selected snapshot. */
final class SinkSchemaResolveResult {

    static final String STATUS_AVAILABLE = "available";
    static final String STATUS_MISSING = "missing";
    static final String STATUS_UNAVAILABLE = "unavailable";
    static final String STATUS_INVALID = "invalid";
    static final String STATUS_UNSUPPORTED = "unsupported";

    final SinkInspectSchemaStore store;
    final String status;
    final String eventPath;
    final String blobPath;
    final String schemaHash;
    final Long schemaSnapshotId;
    final String warning;

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

    static SinkSchemaResolveResult available(
            SinkInspectSchemaStore store,
            String eventPath,
            String blobPath,
            String schemaHash,
            long schemaSnapshotId) {
        return new SinkSchemaResolveResult(
                store, STATUS_AVAILABLE, eventPath, blobPath, schemaHash, schemaSnapshotId, null);
    }

    static SinkSchemaResolveResult missing(String warning) {
        return new SinkSchemaResolveResult(
                new SinkInspectSchemaStore(null), STATUS_MISSING, null, null, null, null, warning);
    }

    static SinkSchemaResolveResult unavailable(String warning) {
        return new SinkSchemaResolveResult(
                new SinkInspectSchemaStore(null),
                STATUS_UNAVAILABLE,
                null,
                null,
                null,
                null,
                warning);
    }

    static SinkSchemaResolveResult invalid(String warning) {
        return new SinkSchemaResolveResult(
                new SinkInspectSchemaStore(null), STATUS_INVALID, null, null, null, null, warning);
    }

    static SinkSchemaResolveResult unsupported(String warning) {
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

    Map<String, Object> toJson() {
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
