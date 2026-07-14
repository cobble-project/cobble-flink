package io.cobble.flink.monitor;

import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleMetadataFileIO;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** Connector-scoped discovery for normal Cobble datasource roots. */
final class CobbleDataSourceDiscovery {

    private static final String SNAPSHOT = "snapshot";
    private static final int MANIFEST_PREFIX_BYTES = 8192;

    private CobbleDataSourceDiscovery() {}

    static List<CheckpointEntry> discover(
            String sourceRoot, CobbleConnectorStorageOptions storageOptions) {
        String normalizedRoot = MonitorPathUtils.normalizeStorageDirectory(sourceRoot);
        try {
            CobbleMetadataFileIO fileIO = CobbleMetadataFileIO.open(normalizedRoot, storageOptions);
            if (!fileIO.exists("")) {
                throw new InputException("Data source path does not exist: " + sourceRoot);
            }
            List<CheckpointEntry> snapshots = new ArrayList<>();
            for (String name : fileIO.list(SNAPSHOT)) {
                Long snapshotId = snapshotManifestId(name);
                String relativeManifest = SNAPSHOT + "/" + name;
                if (snapshotId == null
                        || !snapshotManifestLooksGlobal(
                                fileIO,
                                relativeManifest,
                                storagePath(normalizedRoot, relativeManifest))) {
                    continue;
                }
                String manifestPath = storagePath(normalizedRoot, relativeManifest);
                OperatorEntry operator =
                        new OperatorEntry(
                                "sink",
                                manifestPath,
                                normalizedRoot,
                                Collections.singletonList(normalizedRoot),
                                true);
                snapshots.add(
                        new CheckpointEntry(
                                snapshotId, normalizedRoot, Collections.singletonList(operator)));
            }
            snapshots.sort(Comparator.comparingLong((CheckpointEntry item) -> item.id).reversed());
            if (snapshots.isEmpty()) {
                throw new InputException(
                        "No Cobble data source snapshots found under "
                                + storagePath(normalizedRoot, SNAPSHOT));
            }
            return snapshots;
        } catch (InputException e) {
            throw e;
        } catch (IOException e) {
            throw new InputException(
                    "Failed to inspect Cobble data source metadata: " + e.getMessage());
        }
    }

    private static Long snapshotManifestId(String name) {
        String prefix = "SNAPSHOT-";
        if (name == null || !name.startsWith(prefix)) {
            return null;
        }
        try {
            return Long.valueOf(name.substring(prefix.length()));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static boolean snapshotManifestLooksGlobal(
            CobbleMetadataFileIO fileIO, String relativeManifest, String displayPath) {
        byte[] bytes;
        try {
            bytes = fileIO.read(relativeManifest);
        } catch (IOException e) {
            throw new InputException(
                    "Failed to inspect Cobble snapshot manifest "
                            + displayPath
                            + ": "
                            + e.getMessage());
        }
        if (bytes.length == 0) {
            return false;
        }
        int prefixLength = Math.min(bytes.length, MANIFEST_PREFIX_BYTES);
        String prefix = new String(bytes, 0, prefixLength, StandardCharsets.UTF_8);
        return prefix.contains("\"total_buckets\"") || prefix.contains("\"totalBuckets\"");
    }

    private static String storagePath(String root, String relativePath) {
        String normalizedRoot = root.endsWith("/") ? root.substring(0, root.length() - 1) : root;
        return normalizedRoot + "/" + relativePath;
    }
}
