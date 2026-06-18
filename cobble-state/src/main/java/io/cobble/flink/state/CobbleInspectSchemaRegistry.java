package io.cobble.flink.state;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Content-addressed inspect-schema registry for a single operator.
 *
 * <p>Schemas are stored as SHA-256-addressed blobs under {@code
 * <checkpoint-root>/cobble/<operatorId>/inspect-schema/blobs/<hash>.csch}. Checkpoint events
 * reference the effective schema hash so the monitor can resolve the correct schema for any
 * checkpoint without scanning checkpoint directories:
 *
 * <pre>{@code
 * <checkpoint-root>/cobble/<operatorId>/inspect-schema/
 *   blobs/
 *     <schemaSha256>.csch
 *   events/
 *     SCHEMA-00000000000000000100-<schemaSha256>.ref
 * }</pre>
 *
 * <p>When the schema does not change across checkpoints, only one blob and one event are written. A
 * later checkpoint with a new schema hash writes a fresh blob (if absent) and a fresh event.
 *
 * <p>Event filename parsing and hash validation are delegated to {@link
 * InspectSchemaRegistryLayout} so state and monitor share a single definition of the on-disk
 * protocol.
 */
final class CobbleInspectSchemaRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(CobbleInspectSchemaRegistry.class);

    private final String checkpointExternalPointer;
    private final String operatorIdHex;

    private String latestHash;
    private long latestCheckpointId = -1;
    private boolean recovered;

    CobbleInspectSchemaRegistry(String checkpointExternalPointer, String operatorIdHex) {
        this.checkpointExternalPointer = checkpointExternalPointer;
        this.operatorIdHex = operatorIdHex;
    }

    // ------------------------------------------------------------------------------------------
    //  Public API
    // ------------------------------------------------------------------------------------------

    /**
     * Writes a schema blob and checkpoint event only when the schema hash has changed since the
     * last registered checkpoint. Failures are propagated so the caller can warn and continue.
     */
    void writeIfChanged(long checkpointId, StateInspectSchemaStore store) throws Exception {
        if (store == null || store.isEmpty()) {
            return;
        }
        String hash = InspectSchemaRegistryLayout.sha256(store.toBytes());
        ensureRecovered();

        if (hash.equals(latestHash)) {
            LOG.debug(
                    "Inspect schema unchanged for operator {} at checkpoint {}, skipping write.",
                    operatorIdHex,
                    checkpointId);
            return;
        }

        writeBlobIfAbsent(hash, store.toBytes());
        writeEvent(checkpointId, hash);
        latestHash = hash;
        latestCheckpointId = checkpointId;
        LOG.debug(
                "Inspect schema registered for operator {} at checkpoint {}: hash={}.",
                operatorIdHex,
                checkpointId,
                hash);
    }

    /**
     * Resolves the effective {@link StateInspectSchemaStore} for a given checkpoint by selecting
     * the latest event whose checkpoint id is ≤ the requested id. Returns an empty store when no
     * matching event or blob exists.
     */
    StateInspectSchemaStore resolveSchemaForCheckpoint(long checkpointId) throws Exception {
        ensureRecovered();
        List<InspectSchemaRegistryLayout.SchemaEvent> events = listEvents();
        if (events.isEmpty()) {
            return StateInspectSchemaStore.empty();
        }

        InspectSchemaRegistryLayout.SchemaEvent best = null;
        for (InspectSchemaRegistryLayout.SchemaEvent event : events) {
            if (event.checkpointId() <= checkpointId) {
                if (best == null || event.checkpointId() > best.checkpointId()) {
                    best = event;
                }
            }
        }

        if (best == null) {
            LOG.debug(
                    "No inspect schema event found for operator {} with checkpointId <= {}.",
                    operatorIdHex,
                    checkpointId);
            return StateInspectSchemaStore.empty();
        }

        return readBlob(best.hash());
    }

    // ------------------------------------------------------------------------------------------
    //  Recovery
    // ------------------------------------------------------------------------------------------

    private void ensureRecovered() throws Exception {
        if (recovered) {
            return;
        }
        recovered = true;
        InspectSchemaRegistryLayout.SchemaEvent latest = loadLatestEvent();
        if (latest != null) {
            latestHash = latest.hash();
            latestCheckpointId = latest.checkpointId();
            LOG.debug(
                    "Recovered inspect schema for operator {}: checkpoint {}, hash {}.",
                    operatorIdHex,
                    latestCheckpointId,
                    latestHash);
        }
    }

    private InspectSchemaRegistryLayout.SchemaEvent loadLatestEvent() throws Exception {
        List<InspectSchemaRegistryLayout.SchemaEvent> events = listEvents();
        if (events.isEmpty()) {
            return null;
        }
        return Collections.max(events, Comparator.comparingLong(e -> e.checkpointId()));
    }

    // ------------------------------------------------------------------------------------------
    //  Event listing & file-name parsing
    // ------------------------------------------------------------------------------------------

    private List<InspectSchemaRegistryLayout.SchemaEvent> listEvents() throws Exception {
        String eventDir =
                CobblePathUtils.cobbleInspectSchemaEventDirectory(
                        checkpointExternalPointer, operatorIdHex);
        Path eventDirPath = new Path(eventDir);
        FileSystem fs = eventDirPath.getFileSystem();
        if (!fs.exists(eventDirPath)) {
            return Collections.emptyList();
        }

        FileStatus[] statuses = fs.listStatus(eventDirPath);
        if (statuses == null || statuses.length == 0) {
            return Collections.emptyList();
        }

        List<InspectSchemaRegistryLayout.SchemaEvent> events = new ArrayList<>();
        for (FileStatus status : statuses) {
            String name = status.getPath().getName();
            if (!name.startsWith(InspectSchemaRegistryLayout.EVENT_PREFIX)
                    || !name.endsWith(InspectSchemaRegistryLayout.EVENT_SUFFIX)) {
                continue;
            }
            InspectSchemaRegistryLayout.SchemaEvent event =
                    InspectSchemaRegistryLayout.parseEventFileName(name);
            if (event != null) {
                events.add(event);
            }
        }
        return events;
    }

    // ------------------------------------------------------------------------------------------
    //  File I/O
    // ------------------------------------------------------------------------------------------

    private void writeBlobIfAbsent(String hash, byte[] schemaBytes) throws Exception {
        String blobPath =
                CobblePathUtils.cobbleInspectSchemaBlobPath(
                        checkpointExternalPointer, operatorIdHex, hash);
        Path target = new Path(blobPath);
        FileSystem fs = target.getFileSystem();
        if (fs.exists(target)) {
            return;
        }
        fs.mkdirs(target.getParent());
        try (FSDataOutputStream out = fs.create(target, FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write(schemaBytes);
        }
    }

    private void writeEvent(long checkpointId, String hash) throws Exception {
        String eventPath =
                CobblePathUtils.cobbleInspectSchemaEventPath(
                        checkpointExternalPointer, operatorIdHex, checkpointId, hash);
        Path target = new Path(eventPath);
        FileSystem fs = target.getFileSystem();
        fs.mkdirs(target.getParent());
        try (FSDataOutputStream out = fs.create(target, FileSystem.WriteMode.NO_OVERWRITE);
                Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
            writer.write("version=1\n");
            writer.write("checkpoint_id=" + checkpointId + "\n");
            writer.write("operator_id=" + operatorIdHex + "\n");
            writer.write("schema_sha256=" + hash + "\n");
        }
    }

    private StateInspectSchemaStore readBlob(String hash) throws Exception {
        String blobPath =
                CobblePathUtils.cobbleInspectSchemaBlobPath(
                        checkpointExternalPointer, operatorIdHex, hash);
        Path target = new Path(blobPath);
        FileSystem fs = target.getFileSystem();
        if (!fs.exists(target)) {
            LOG.warn(
                    "Inspect schema blob missing for operator {} hash {}: {}.",
                    operatorIdHex,
                    hash,
                    blobPath);
            return StateInspectSchemaStore.empty();
        }
        try (FSDataInputStream input = fs.open(target)) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[8 * 1024];
            int n;
            while ((n = input.read(chunk)) >= 0) {
                buffer.write(chunk, 0, n);
            }
            return StateInspectSchemaStore.fromBytes(buffer.toByteArray());
        } catch (Exception e) {
            LOG.warn(
                    "Failed to read inspect schema blob for operator {} hash {}: {}.",
                    operatorIdHex,
                    hash,
                    e.getMessage());
            return StateInspectSchemaStore.empty();
        }
    }
}
