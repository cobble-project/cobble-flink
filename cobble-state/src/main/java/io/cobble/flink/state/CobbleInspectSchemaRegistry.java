package io.cobble.flink.state;

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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
        String hash = sha256(store.toBytes());
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
        List<SchemaEvent> events = listEvents();
        if (events.isEmpty()) {
            return StateInspectSchemaStore.empty();
        }

        SchemaEvent best = null;
        for (SchemaEvent event : events) {
            if (event.checkpointId <= checkpointId) {
                if (best == null || event.checkpointId > best.checkpointId) {
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

        return readBlob(best.hash);
    }

    // ------------------------------------------------------------------------------------------
    //  Recovery
    // ------------------------------------------------------------------------------------------

    private void ensureRecovered() throws Exception {
        if (recovered) {
            return;
        }
        recovered = true;
        SchemaEvent latest = loadLatestEvent();
        if (latest != null) {
            latestHash = latest.hash;
            latestCheckpointId = latest.checkpointId;
            LOG.debug(
                    "Recovered inspect schema for operator {}: checkpoint {}, hash {}.",
                    operatorIdHex,
                    latestCheckpointId,
                    latestHash);
        }
    }

    private SchemaEvent loadLatestEvent() throws Exception {
        List<SchemaEvent> events = listEvents();
        if (events.isEmpty()) {
            return null;
        }
        return Collections.max(events, Comparator.comparingLong(e -> e.checkpointId));
    }

    // ------------------------------------------------------------------------------------------
    //  Event listing & file-name parsing
    // ------------------------------------------------------------------------------------------

    private List<SchemaEvent> listEvents() throws Exception {
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

        List<SchemaEvent> events = new ArrayList<>();
        for (FileStatus status : statuses) {
            String name = status.getPath().getName();
            if (!name.startsWith("SCHEMA-") || !name.endsWith(".ref")) {
                continue;
            }
            SchemaEvent event = parseEventFileName(name);
            if (event != null) {
                events.add(event);
            }
        }
        return events;
    }

    // VisibleForTesting
    static SchemaEvent parseEventFileName(String fileName) {
        // Expected format: SCHEMA-<20-digit checkpointId>-<64-char-hex-hash>.ref
        if (fileName == null || fileName.isEmpty() || !fileName.endsWith(".ref")) {
            return null;
        }
        String stripped = fileName.substring(0, fileName.length() - 4);
        if (!stripped.startsWith("SCHEMA-")) {
            return null;
        }
        stripped = stripped.substring("SCHEMA-".length());

        // After "SCHEMA-" we expect 20 digits, a '-', then the hash.
        if (stripped.length() < 21) {
            return null;
        }
        String checkpointIdStr = stripped.substring(0, 20);
        long checkpointId;
        try {
            checkpointId = Long.parseLong(checkpointIdStr);
        } catch (NumberFormatException e) {
            return null;
        }

        if (stripped.charAt(20) != '-') {
            return null;
        }
        String hash = stripped.substring(21);
        if (!isValidHash(hash)) {
            return null;
        }

        return new SchemaEvent(checkpointId, hash);
    }

    private static boolean isValidHash(String hash) {
        if (hash.length() != 64) {
            return false;
        }
        for (int i = 0; i < 64; i++) {
            char c = hash.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return false;
            }
        }
        return true;
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

    // ------------------------------------------------------------------------------------------
    //  Hashing
    // ------------------------------------------------------------------------------------------

    static String sha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available.", e);
        }
    }

    // ------------------------------------------------------------------------------------------
    //  Inner types
    // ------------------------------------------------------------------------------------------

    static final class SchemaEvent {
        final long checkpointId;
        final String hash;

        SchemaEvent(long checkpointId, String hash) {
            this.checkpointId = checkpointId;
            this.hash = hash;
        }
    }
}
