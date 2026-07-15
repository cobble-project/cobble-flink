package io.cobble.flink.inspect.internal;

import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleMetadataFileIO;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Resolves the effective Cobble sink inspect schema for a selected sink snapshot. */
public final class SinkInspectSchemaResolver {

    private static final Logger LOG = LoggerFactory.getLogger(SinkInspectSchemaResolver.class);

    private static final String INSPECT_SCHEMA = "inspect-schema";
    private static final String EVENTS = "events";
    private static final String BLOBS = "blobs";

    private SinkInspectSchemaResolver() {}

    public static SinkSchemaResolveResult resolve(String sinkRoot, long snapshotId) {
        return resolve(sinkRoot, snapshotId, CobbleConnectorStorageOptions.empty());
    }

    public static SinkSchemaResolveResult resolve(
            String sinkRoot, long snapshotId, CobbleConnectorStorageOptions storageOptions) {
        String normalizedRoot = InspectPathUtils.normalizeStorageDirectory(sinkRoot);
        String eventsDir = INSPECT_SCHEMA + "/" + EVENTS;
        String blobsDir = INSPECT_SCHEMA + "/" + BLOBS;
        try {
            CobbleMetadataFileIO fileIO = CobbleMetadataFileIO.open(normalizedRoot, storageOptions);
            List<InspectSchemaRegistryLayout.SchemaEvent> events = listEvents(fileIO, eventsDir);
            if (events.isEmpty()) {
                return SinkSchemaResolveResult.missing(
                        "No sink schema events found at " + storagePath(normalizedRoot, eventsDir));
            }

            InspectSchemaRegistryLayout.SchemaEvent best = null;
            for (InspectSchemaRegistryLayout.SchemaEvent event : events) {
                if (event.checkpointId() <= snapshotId
                        && (best == null || event.checkpointId() > best.checkpointId())) {
                    best = event;
                }
            }
            if (best == null) {
                return SinkSchemaResolveResult.missing(
                        "No sink schema event with snapshotId <= "
                                + snapshotId
                                + " (events at "
                                + storagePath(normalizedRoot, eventsDir)
                                + ")");
            }

            String blobPath =
                    blobsDir + "/" + InspectSchemaRegistryLayout.blobFileName(best.hash());
            return readBlob(fileIO, normalizedRoot, blobPath, best, eventsDir);
        } catch (Exception e) {
            LOG.debug("Failed to open sink schema metadata: {}", e.getMessage());
            return SinkSchemaResolveResult.unavailable(
                    "Failed to open connector-scoped sink schema metadata: " + e.getMessage());
        }
    }

    private static List<InspectSchemaRegistryLayout.SchemaEvent> listEvents(
            CobbleMetadataFileIO fileIO, String eventsDir) {
        try {
            List<InspectSchemaRegistryLayout.SchemaEvent> events = new ArrayList<>();
            for (String name : fileIO.list(eventsDir)) {
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
            LOG.debug("Failed to list sink schema events: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private static SinkSchemaResolveResult readBlob(
            CobbleMetadataFileIO fileIO,
            String normalizedRoot,
            String blobPath,
            InspectSchemaRegistryLayout.SchemaEvent event,
            String eventsDir) {
        String displayBlobPath = storagePath(normalizedRoot, blobPath);
        try {
            if (!fileIO.exists(blobPath)) {
                return SinkSchemaResolveResult.invalid(
                        "Sink schema blob missing for hash "
                                + event.hash()
                                + " (expected at "
                                + displayBlobPath
                                + ")");
            }
            SinkInspectSchemaStore store = SinkInspectSchemaStore.fromBytes(fileIO.read(blobPath));
            if (store.isEmpty()) {
                return SinkSchemaResolveResult.invalid(
                        "Sink schema blob parsed as empty store "
                                + displayBlobPath
                                + "; falling back to raw inspect");
            }
            return SinkSchemaResolveResult.available(
                    store,
                    storagePath(normalizedRoot, eventsDir),
                    displayBlobPath,
                    event.hash(),
                    event.checkpointId());
        } catch (Exception e) {
            LOG.debug("Failed to read sink schema blob: {}", e.getMessage());
            return SinkSchemaResolveResult.invalid(
                    "Failed to read or parse sink schema blob "
                            + displayBlobPath
                            + ": "
                            + e.getMessage());
        }
    }

    private static String storagePath(String root, String relativePath) {
        String normalizedRoot = root.endsWith("/") ? root.substring(0, root.length() - 1) : root;
        return normalizedRoot + "/" + relativePath;
    }
}
