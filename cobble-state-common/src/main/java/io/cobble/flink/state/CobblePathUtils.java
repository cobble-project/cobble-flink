package io.cobble.flink.state;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/** Shared Cobble path helpers that keep local file URIs stable. */
final class CobblePathUtils {

    private CobblePathUtils() {}

    static String normalizeStorageDirectory(String directory) {
        if (directory == null || directory.trim().isEmpty()) {
            return null;
        }

        Path path = new Path(directory);
        URI uri = path.toUri();
        String scheme = uri.getScheme();
        if (scheme == null || scheme.trim().isEmpty()) {
            return normalizeLocalPath(new File(directory));
        }

        String normalizedScheme = normalizeCheckpointScheme(scheme);
        if ("file".equals(normalizedScheme)) {
            return normalizeLocalPath(new File(uri));
        }

        if (normalizedScheme.equals(scheme.toLowerCase(Locale.ROOT))) {
            return path.toString();
        }

        try {
            return new URI(
                            normalizedScheme,
                            uri.getAuthority(),
                            uri.getPath(),
                            uri.getQuery(),
                            uri.getFragment())
                    .toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Failed to normalize Cobble storage directory '" + directory + "'.", e);
        }
    }

    static String appendPath(String normalizedBaseDir, String... segments) {
        if (normalizedBaseDir.startsWith("file://")) {
            File path = new File(URI.create(normalizedBaseDir));
            for (String segment : segments) {
                path = new File(path, segment);
            }
            return CobbleKeyedStateBackendBuilder.normalizeLocalPath(path);
        }

        Path path = new Path(normalizedBaseDir);
        for (String segment : segments) {
            path = new Path(path, segment);
        }
        return path.toString();
    }

    static String normalizeLocalPath(File localPath) {
        try {
            String normalizedPath = localPath.getAbsoluteFile().toPath().normalize().toString();
            normalizedPath = normalizedPath.replace(File.separatorChar, '/');
            if (!normalizedPath.startsWith("/")) {
                normalizedPath = "/" + normalizedPath;
            }
            return new URI("file", "", normalizedPath, null, null).toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Failed to normalize Cobble local path '" + localPath + "'.", e);
        }
    }

    static String toFileCompatibleName(String identifier) {
        return identifier.replaceAll("[^a-zA-Z0-9\\-]", "_");
    }

    static String checkpointSharedStatePath(String normalizedCheckpointDirectory) {
        return appendPath(
                normalizedCheckpointDirectory,
                AbstractFsCheckpointStorageAccess.CHECKPOINT_SHARED_STATE_DIR);
    }

    static String checkpointOperatorSharedStatePath(
            String normalizedCheckpointDirectory, String checkpointScopeDirectoryName) {
        return appendPath(
                checkpointSharedStatePath(normalizedCheckpointDirectory),
                checkpointScopeDirectoryName);
    }

    static String cobbleGlobalSnapshotDirectory(String checkpointExternalPointer) {
        return appendPath(checkpointParentDirectory(checkpointExternalPointer), "cobble");
    }

    static String cobbleOperatorSnapshotDirectory(
            String checkpointExternalPointer, String operatorIdHex) {
        return appendPath(cobbleGlobalSnapshotDirectory(checkpointExternalPointer), operatorIdHex);
    }

    static String cobbleGlobalSnapshotManifestPath(
            String checkpointExternalPointer, String operatorIdHex, long checkpointId) {
        return appendPath(
                cobbleOperatorSnapshotDirectory(checkpointExternalPointer, operatorIdHex),
                "snapshot",
                snapshotManifestName(checkpointId));
    }

    static String cobbleGlobalSnapshotCurrentPointerPath(
            String checkpointExternalPointer, String operatorIdHex) {
        return appendPath(
                cobbleOperatorSnapshotDirectory(checkpointExternalPointer, operatorIdHex),
                "snapshot",
                "CURRENT");
    }

    static String checkpointGlobalSnapshotManifestCopyPath(
            String checkpointExternalPointer, String operatorIdHex) {
        return appendPath(
                checkpointExternalPointer,
                String.format(Locale.ROOT, "COBBLE-SNAPSHOT-%s-MANIFEST", operatorIdHex));
    }

    /**
     * Returns the root directory of the content-addressed inspect-schema registry for an operator.
     * Schema blobs and checkpoint events live under this directory rather than inside individual
     * checkpoint directories, so unchanged schemas are deduplicated across checkpoints.
     */
    static String cobbleOperatorInspectSchemaDirectory(
            String checkpointExternalPointer, String operatorIdHex) {
        return appendPath(
                cobbleOperatorSnapshotDirectory(checkpointExternalPointer, operatorIdHex),
                "inspect-schema");
    }

    static String cobbleInspectSchemaBlobDirectory(
            String checkpointExternalPointer, String operatorIdHex) {
        return appendPath(
                cobbleOperatorInspectSchemaDirectory(checkpointExternalPointer, operatorIdHex),
                "blobs");
    }

    static String cobbleInspectSchemaBlobPath(
            String checkpointExternalPointer, String operatorIdHex, String schemaHash) {
        return appendPath(
                cobbleInspectSchemaBlobDirectory(checkpointExternalPointer, operatorIdHex),
                schemaHash + ".csch");
    }

    static String cobbleInspectSchemaEventDirectory(
            String checkpointExternalPointer, String operatorIdHex) {
        return appendPath(
                cobbleOperatorInspectSchemaDirectory(checkpointExternalPointer, operatorIdHex),
                "events");
    }

    static String cobbleInspectSchemaEventPath(
            String checkpointExternalPointer,
            String operatorIdHex,
            long checkpointId,
            String schemaHash) {
        return appendPath(
                cobbleInspectSchemaEventDirectory(checkpointExternalPointer, operatorIdHex),
                String.format(Locale.ROOT, "SCHEMA-%020d-%s.ref", checkpointId, schemaHash));
    }

    /**
     * Returns the volume root that contains a shard snapshot manifest.
     *
     * <p>A shard manifest lives at {@code <volume>/<db-id>/snapshot/SNAPSHOT-*}. During rescale the
     * new backend writes into a fresh operator volume, but restore still needs read access to the
     * source volume referenced by checkpoint metadata.
     */
    static String snapshotVolumeDirectory(String snapshotManifestPath, String dbId) {
        Path manifestPath = new Path(normalizeStorageDirectory(snapshotManifestPath));
        Path snapshotDirectory = requireParent(manifestPath, "snapshot manifest");
        Path dbDirectory = requireParent(snapshotDirectory, "snapshot directory");
        if (!dbId.equals(dbDirectory.getName())) {
            throw new IllegalArgumentException(
                    "Cobble snapshot manifest DB directory '"
                            + dbDirectory.getName()
                            + "' does not match DB id '"
                            + dbId
                            + "'.");
        }
        return normalizeStorageDirectory(
                requireParent(dbDirectory, "snapshot DB directory").toString());
    }

    static boolean deletePathQuietly(String normalizedPath) throws Exception {
        Path path = new Path(normalizedPath);
        FileSystem fileSystem = path.getFileSystem();
        return !fileSystem.exists(path) || fileSystem.delete(path, true);
    }

    static String checkpointParentDirectory(String checkpointExternalPointer) {
        String normalizedCheckpointPointer = normalizeStorageDirectory(checkpointExternalPointer);
        if (normalizedCheckpointPointer.startsWith("file://")) {
            File parent = new File(URI.create(normalizedCheckpointPointer)).getParentFile();
            if (parent == null) {
                throw new IllegalArgumentException(
                        "Checkpoint external pointer has no parent directory: "
                                + checkpointExternalPointer);
            }
            return normalizeLocalPath(parent);
        }

        Path parent = new Path(normalizedCheckpointPointer).getParent();
        if (parent == null) {
            throw new IllegalArgumentException(
                    "Checkpoint external pointer has no parent directory: "
                            + checkpointExternalPointer);
        }
        return parent.toString();
    }

    private static Path requireParent(Path path, String description) {
        Path parent = path.getParent();
        if (parent == null) {
            throw new IllegalArgumentException(
                    "Cobble " + description + " has no parent directory: " + path);
        }
        return parent;
    }

    private static String normalizeCheckpointScheme(String scheme) {
        String normalizedScheme = scheme.toLowerCase(Locale.ROOT);
        if ("s3a".equals(normalizedScheme) || "s3p".equals(normalizedScheme)) {
            return "s3";
        }
        return normalizedScheme;
    }

    private static String snapshotManifestName(long checkpointId) {
        return String.format(Locale.ROOT, "SNAPSHOT-%d", checkpointId);
    }
}
