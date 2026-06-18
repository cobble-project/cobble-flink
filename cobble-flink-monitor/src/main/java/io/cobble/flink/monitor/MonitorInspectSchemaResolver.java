package io.cobble.flink.monitor;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Resolves the effective {@link StateInspectSchemaStore} for a selected checkpoint and operator
 * from the content-addressed schema registry.
 *
 * <p>The resolver:
 *
 * <ol>
 *   <li>Derives the checkpoint root from {@code checkpoint.directory} (parent of chk-N).
 *   <li>Lists {@code <root>/cobble/<operatorId>/inspect-schema/events/}.
 *   <li>Parses event filenames via {@link InspectSchemaRegistryLayout}.
 *   <li>Picks the event with the largest {@code checkpointId <= selectedCheckpointId}.
 *   <li>Reads {@code <root>/cobble/<operatorId>/inspect-schema/blobs/<hash>.csch}.
 *   <li>Parses via {@link StateInspectSchemaStore#read(java.io.InputStream)}.
 * </ol>
 *
 * <p>Every failure path produces a {@link SchemaResolveResult} with an empty store and a
 * descriptive status — resolvers never throw and callers never need try-catch around resolution.
 */
final class MonitorInspectSchemaResolver {

    private static final Logger LOG = LoggerFactory.getLogger(MonitorInspectSchemaResolver.class);

    private static final String INSPECT_SCHEMA = "inspect-schema";
    private static final String EVENTS = "events";
    private static final String BLOBS = "blobs";

    private MonitorInspectSchemaResolver() {}

    /**
     * Resolves the schema for {@code checkpoint} / {@code operator} from the content-addressed
     * registry.
     */
    static SchemaResolveResult resolve(CheckpointEntry checkpoint, OperatorEntry operator) {
        if (!operator.globalSnapshotLayout) {
            return SchemaResolveResult.unsupported(
                    "Schema registry resolution is not available for non-global-snapshot operators.");
        }

        Path checkpointRoot;
        try {
            checkpointRoot = checkpointRoot(checkpoint);
        } catch (Exception e) {
            LOG.debug(
                    "Failed to determine checkpoint root for schema registry: {}", e.getMessage());
            return SchemaResolveResult.unavailable(
                    "Failed to determine checkpoint root: " + e.getMessage());
        }

        Path eventsDir =
                new Path(
                        new Path(
                                new Path(new Path(checkpointRoot, "cobble"), operator.operatorId),
                                INSPECT_SCHEMA),
                        EVENTS);
        FileSystem fs;
        try {
            fs = eventsDir.getFileSystem();
        } catch (Exception e) {
            LOG.debug("Failed to open filesystem for schema events: {}", e.getMessage());
            return SchemaResolveResult.unavailable(
                    "Failed to open filesystem for " + eventsDir + ": " + e.getMessage());
        }

        List<InspectSchemaRegistryLayout.SchemaEvent> events = listEvents(fs, eventsDir);
        if (events.isEmpty()) {
            return SchemaResolveResult.missing(
                    "No schema events found at " + pathToStorageString(eventsDir));
        }

        InspectSchemaRegistryLayout.SchemaEvent best = null;
        for (InspectSchemaRegistryLayout.SchemaEvent event : events) {
            if (event.checkpointId() <= checkpoint.id) {
                if (best == null || event.checkpointId() > best.checkpointId()) {
                    best = event;
                }
            }
        }

        if (best == null) {
            return SchemaResolveResult.missing(
                    "No schema event with checkpointId <= "
                            + checkpoint.id
                            + " (events at "
                            + pathToStorageString(eventsDir)
                            + ")");
        }

        Path blobsDir =
                new Path(
                        new Path(
                                new Path(new Path(checkpointRoot, "cobble"), operator.operatorId),
                                INSPECT_SCHEMA),
                        BLOBS);
        Path blobPath = new Path(blobsDir, InspectSchemaRegistryLayout.blobFileName(best.hash()));

        return readBlob(fs, blobPath, best, eventsDir);
    }

    private static List<InspectSchemaRegistryLayout.SchemaEvent> listEvents(
            FileSystem fs, Path eventsDir) {
        try {
            if (!fs.exists(eventsDir)) {
                return java.util.Collections.emptyList();
            }
            FileStatus[] statuses = fs.listStatus(eventsDir);
            if (statuses == null || statuses.length == 0) {
                return java.util.Collections.emptyList();
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
                } else {
                    LOG.debug("Skipping malformed schema event file: {}", name);
                }
            }
            return events;
        } catch (Exception e) {
            LOG.debug("Failed to list schema events at {}: {}", eventsDir, e.getMessage());
            return java.util.Collections.emptyList();
        }
    }

    private static SchemaResolveResult readBlob(
            FileSystem fs,
            Path blobPath,
            InspectSchemaRegistryLayout.SchemaEvent event,
            Path eventsDir) {
        try {
            if (!fs.exists(blobPath)) {
                return SchemaResolveResult.invalid(
                        "Schema blob missing for hash "
                                + event.hash()
                                + " (expected at "
                                + pathToStorageString(blobPath)
                                + ")");
            }
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            try (FSDataInputStream input = fs.open(blobPath)) {
                byte[] chunk = new byte[8 * 1024];
                int n;
                while ((n = input.read(chunk)) >= 0) {
                    buffer.write(chunk, 0, n);
                }
            }
            StateInspectSchemaStore store = StateInspectSchemaStore.fromBytes(buffer.toByteArray());
            if (store.isEmpty()) {
                return SchemaResolveResult.invalid(
                        "Schema blob parsed as empty store "
                                + pathToStorageString(blobPath)
                                + "; falling back to raw inspect");
            }
            return SchemaResolveResult.available(
                    store,
                    pathToStorageString(eventsDir),
                    pathToStorageString(blobPath),
                    event.hash(),
                    event.checkpointId());
        } catch (Exception e) {
            LOG.debug("Failed to read schema blob {}: {}", blobPath.getName(), e.getMessage());
            return SchemaResolveResult.invalid(
                    "Failed to read or parse schema blob "
                            + pathToStorageString(blobPath)
                            + ": "
                            + e.getMessage());
        }
    }

    /** Returns the checkpoint root directory: the parent of {@code chk-N}. */
    static Path checkpointRoot(CheckpointEntry checkpoint) {
        Path checkpointDir =
                new Path(MonitorPathUtils.normalizeStorageDirectory(checkpoint.directory));
        Path parent = checkpointDir.getParent();
        if (parent == null) {
            throw new IllegalArgumentException(
                    "Checkpoint directory has no parent: " + checkpoint.directory);
        }
        return parent;
    }

    private static String pathToStorageString(Path path) {
        return MonitorPathUtils.pathToStorageString(path);
    }
}
