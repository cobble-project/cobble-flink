package io.cobble.flink.monitor;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/** Resolves the effective Cobble sink inspect schema for a selected sink snapshot. */
final class SinkInspectSchemaResolver {

    private static final Logger LOG = LoggerFactory.getLogger(SinkInspectSchemaResolver.class);

    private static final String INSPECT_SCHEMA = "inspect-schema";
    private static final String EVENTS = "events";
    private static final String BLOBS = "blobs";

    private SinkInspectSchemaResolver() {}

    static SinkSchemaResolveResult resolve(String sinkRoot, long snapshotId) {
        Path schemaRoot =
                new Path(
                        new Path(MonitorPathUtils.normalizeStorageDirectory(sinkRoot)),
                        INSPECT_SCHEMA);
        Path eventsDir = new Path(schemaRoot, EVENTS);
        FileSystem fs;
        try {
            fs = eventsDir.getFileSystem();
        } catch (Exception e) {
            LOG.debug("Failed to open sink schema filesystem: {}", e.getMessage());
            return SinkSchemaResolveResult.unavailable(
                    "Failed to open filesystem for " + eventsDir + ": " + e.getMessage());
        }

        List<InspectSchemaRegistryLayout.SchemaEvent> events = listEvents(fs, eventsDir);
        if (events.isEmpty()) {
            return SinkSchemaResolveResult.missing(
                    "No sink schema events found at " + pathToStorageString(eventsDir));
        }

        InspectSchemaRegistryLayout.SchemaEvent best = null;
        for (InspectSchemaRegistryLayout.SchemaEvent event : events) {
            if (event.checkpointId() <= snapshotId) {
                if (best == null || event.checkpointId() > best.checkpointId()) {
                    best = event;
                }
            }
        }
        if (best == null) {
            return SinkSchemaResolveResult.missing(
                    "No sink schema event with snapshotId <= "
                            + snapshotId
                            + " (events at "
                            + pathToStorageString(eventsDir)
                            + ")");
        }

        Path blobPath =
                new Path(
                        new Path(schemaRoot, BLOBS),
                        InspectSchemaRegistryLayout.blobFileName(best.hash()));
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
                    LOG.debug("Skipping malformed sink schema event file: {}", name);
                }
            }
            return events;
        } catch (Exception e) {
            LOG.debug("Failed to list sink schema events at {}: {}", eventsDir, e.getMessage());
            return java.util.Collections.emptyList();
        }
    }

    private static SinkSchemaResolveResult readBlob(
            FileSystem fs,
            Path blobPath,
            InspectSchemaRegistryLayout.SchemaEvent event,
            Path eventsDir) {
        try {
            if (!fs.exists(blobPath)) {
                return SinkSchemaResolveResult.invalid(
                        "Sink schema blob missing for hash "
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
            SinkInspectSchemaStore store = SinkInspectSchemaStore.fromBytes(buffer.toByteArray());
            if (store.isEmpty()) {
                return SinkSchemaResolveResult.invalid(
                        "Sink schema blob parsed as empty store "
                                + pathToStorageString(blobPath)
                                + "; falling back to raw inspect");
            }
            return SinkSchemaResolveResult.available(
                    store,
                    pathToStorageString(eventsDir),
                    pathToStorageString(blobPath),
                    event.hash(),
                    event.checkpointId());
        } catch (Exception e) {
            LOG.debug("Failed to read sink schema blob {}: {}", blobPath.getName(), e.getMessage());
            return SinkSchemaResolveResult.invalid(
                    "Failed to read or parse sink schema blob "
                            + pathToStorageString(blobPath)
                            + ": "
                            + e.getMessage());
        }
    }

    private static String pathToStorageString(Path path) {
        return MonitorPathUtils.pathToStorageString(path);
    }
}
