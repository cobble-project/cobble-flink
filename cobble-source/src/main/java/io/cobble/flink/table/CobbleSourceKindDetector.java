package io.cobble.flink.table;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.ValidationException;

import java.io.IOException;

/**
 * Resolves a Cobble source path to a concrete {@link CobbleSourceKind} from its on-disk layout.
 *
 * <p>The detector performs a small, bounded probe using Flink {@link FileSystem} APIs (so remote
 * paths keep working later). It never scans recursively. Detection signals, in priority order:
 *
 * <ol>
 *   <li>A Flink checkpoint root: a direct child {@code chk-*} directory that contains a Flink
 *       {@code _metadata} file and a {@code COBBLE-SNAPSHOT-<operatorId>-MANIFEST} file → STATE.
 *   <li>{@code <path>/inspect-schema/blobs/*.csch} whose blob begins with {@link
 *       SinkInspectSchemaStore#MAGIC} (CSNK) → SINK.
 *   <li>{@code <path>/inspect-schema/blobs/*.csch} whose blob begins with {@link
 *       StateInspectSchemaStore#MAGIC} (CSCH) → STATE operator root.
 *   <li>{@code <path>/snapshot/CURRENT} or {@code <path>/writer-paths.properties} present but no
 *       recognized schema/checkpoint layout → ambiguous.
 * </ol>
 *
 * <p>An explicit {@code source.kind} wins, but it must not contradict strong on-disk signals: a
 * {@code sink} request over a state path and a {@code state} request over a sink path both fail
 * loudly rather than silently falling back.
 *
 * <p>To preserve existing sink behavior, {@code auto} over an ambiguous path (a weak sink signal,
 * no strong schema/checkpoint signal) falls back to {@link CobbleSourceKind#SINK} when the DDL is
 * <em>sink-shaped</em> (declares a primary key and at least one non-key column). This keeps sink
 * tables written without an inspect-schema sidecar working without forcing {@code source.kind}.
 * Strong state signals are never overridden by a sink-shaped DDL, and truly unknown or missing
 * paths still fail.
 */
final class CobbleSourceKindDetector {

    private static final String INSPECT_SCHEMA = "inspect-schema";
    private static final String BLOBS = "blobs";
    private static final String SNAPSHOT = "snapshot";
    private static final String CURRENT = "CURRENT";
    private static final String WRITER_PATHS = "writer-paths.properties";
    private static final String FLINK_METADATA = "_metadata";
    private static final String CHECKPOINT_PREFIX = "chk-";
    private static final String COBBLE_MANIFEST_PREFIX = "COBBLE-SNAPSHOT-";
    private static final String COBBLE_MANIFEST_SUFFIX = "-MANIFEST";

    /** Raw layout signal observed at a path, before applying the requested kind. */
    private enum Probe {
        SINK,
        STATE_CHECKPOINT,
        STATE_OPERATOR,
        AMBIGUOUS,
        UNKNOWN
    }

    private CobbleSourceKindDetector() {}

    /**
     * Detects the source kind for {@code pathUri}.
     *
     * @param pathUri normalized, scheme-qualified path URI string
     * @param requestedKind parsed {@code source.kind} option ({@link CobbleSourceKind#AUTO} when
     *     not set)
     * @param sinkShapedSchema whether the DDL is sink-shaped (a primary key plus at least one
     *     non-key column); used only to let {@code auto} fall back to sink on an ambiguous path
     * @throws ValidationException when the path is missing, when an explicit kind contradicts the
     *     layout, or when auto-detection cannot determine the kind
     */
    static CobbleResolvedSource detect(
            String pathUri, CobbleSourceKind requestedKind, boolean sinkShapedSchema) {
        Path root = new Path(pathUri);
        FileSystem fs;
        try {
            fs = root.getFileSystem();
        } catch (IOException e) {
            throw new ValidationException(
                    "Failed to open filesystem for Cobble source path "
                            + pathUri
                            + ": "
                            + e.getMessage(),
                    e);
        }

        boolean exists;
        try {
            exists = fs.exists(root);
        } catch (IOException e) {
            throw new ValidationException(
                    "Failed to access Cobble source path " + pathUri + ": " + e.getMessage(), e);
        }
        if (!exists) {
            throw new ValidationException("Cobble source path does not exist: " + pathUri);
        }

        Probe probe = probe(fs, root, pathUri);

        switch (requestedKind) {
            case SINK:
                if (probe == Probe.STATE_CHECKPOINT || probe == Probe.STATE_OPERATOR) {
                    throw new ValidationException(
                            "source.kind='sink' was requested, but path appears to be a Cobble"
                                    + " state checkpoint.");
                }
                return CobbleResolvedSource.sink(sinkDiagnostics(pathUri));
            case STATE:
                if (probe == Probe.SINK) {
                    throw new ValidationException(
                            "source.kind='state' was requested, but path appears to be a Cobble"
                                    + " sink table.");
                }
                return resolveState(pathUri, layoutFor(probe));
            case AUTO:
                return resolveAuto(pathUri, probe, sinkShapedSchema);
            case RAW_RESERVED:
            default:
                throw new IllegalStateException("Unexpected source kind: " + requestedKind);
        }
    }

    private static CobbleResolvedSource resolveAuto(
            String pathUri, Probe probe, boolean sinkShapedSchema) {
        switch (probe) {
            case SINK:
                return CobbleResolvedSource.sink(sinkDiagnostics(pathUri));
            case STATE_CHECKPOINT:
                return resolveState(pathUri, StateSourceConfig.Layout.CHECKPOINT_ROOT);
            case STATE_OPERATOR:
                return resolveState(pathUri, StateSourceConfig.Layout.OPERATOR_ROOT);
            case AMBIGUOUS:
                if (sinkShapedSchema) {
                    // A weak sink signal plus a sink-shaped DDL: keep existing sink behavior for
                    // tables (including externally written ones) that lack an inspect-schema
                    // sidecar. Strong state signals are handled above and are never reached here.
                    return CobbleResolvedSource.sink(sinkFallbackDiagnostics(pathUri));
                }
                throw ambiguousFailure(pathUri);
            case UNKNOWN:
            default:
                throw ambiguousFailure(pathUri);
        }
    }

    private static ValidationException ambiguousFailure(String pathUri) {
        return new ValidationException(
                "Unable to auto-detect Cobble source kind for path "
                        + pathUri
                        + ". Set source.kind='sink' or source.kind='state'. Raw Cobble"
                        + " source is reserved for future support.");
    }

    private static CobbleResolvedSource resolveState(
            String pathUri, StateSourceConfig.Layout layout) {
        return CobbleResolvedSource.state(
                new StateSourceConfig(pathUri, layout), stateDiagnostics(pathUri, layout));
    }

    private static StateSourceConfig.Layout layoutFor(Probe probe) {
        switch (probe) {
            case STATE_CHECKPOINT:
                return StateSourceConfig.Layout.CHECKPOINT_ROOT;
            case STATE_OPERATOR:
                return StateSourceConfig.Layout.OPERATOR_ROOT;
            default:
                return StateSourceConfig.Layout.UNKNOWN;
        }
    }

    private static String sinkDiagnostics(String pathUri) {
        return "Detected Cobble sink table root at " + pathUri + ".";
    }

    private static String sinkFallbackDiagnostics(String pathUri) {
        return "Resolved Cobble sink source for "
                + pathUri
                + " from the sink-shaped table schema; no inspect-schema sidecar was found.";
    }

    private static String stateDiagnostics(String pathUri, StateSourceConfig.Layout layout) {
        switch (layout) {
            case CHECKPOINT_ROOT:
                return "Detected Cobble state checkpoint root at "
                        + pathUri
                        + " (chk-* with _metadata and Cobble manifest).";
            case OPERATOR_ROOT:
                return "Detected Cobble state operator root at "
                        + pathUri
                        + ". This path lacks checkpoint root / shared-volume context; point the"
                        + " source at the enclosing checkpoint root (the parent directory that"
                        + " contains chk-*) for state reads.";
            case UNKNOWN:
            default:
                return "source.kind='state' was requested for "
                        + pathUri
                        + ", but the on-disk state layout could not be confirmed.";
        }
    }

    // ------------------------------------------------------------------------------------------
    //  Bounded filesystem probing
    // ------------------------------------------------------------------------------------------

    private static Probe probe(FileSystem fs, Path root, String pathUri) {
        if (hasCheckpointLayout(fs, root, pathUri)) {
            return Probe.STATE_CHECKPOINT;
        }
        int magic = inspectSchemaBlobMagic(fs, root, pathUri);
        if (magic == SinkInspectSchemaStore.MAGIC) {
            return Probe.SINK;
        }
        if (magic == StateInspectSchemaStore.MAGIC) {
            return Probe.STATE_OPERATOR;
        }
        if (exists(fs, new Path(new Path(root, SNAPSHOT), CURRENT), pathUri)
                || exists(fs, new Path(root, WRITER_PATHS), pathUri)) {
            return Probe.AMBIGUOUS;
        }
        return Probe.UNKNOWN;
    }

    /**
     * True when {@code root} has a {@code chk-*} child holding {@code _metadata} and a manifest.
     */
    private static boolean hasCheckpointLayout(FileSystem fs, Path root, String pathUri) {
        FileStatus[] children = listStatus(fs, root, pathUri);
        if (children == null) {
            return false;
        }
        for (FileStatus child : children) {
            if (!child.isDir()) {
                continue;
            }
            if (!child.getPath().getName().startsWith(CHECKPOINT_PREFIX)) {
                continue;
            }
            Path chkDir = child.getPath();
            if (!exists(fs, new Path(chkDir, FLINK_METADATA), pathUri)) {
                continue;
            }
            if (hasCobbleManifest(fs, chkDir, pathUri)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasCobbleManifest(FileSystem fs, Path chkDir, String pathUri) {
        FileStatus[] entries = listStatus(fs, chkDir, pathUri);
        if (entries == null) {
            return false;
        }
        for (FileStatus entry : entries) {
            String name = entry.getPath().getName();
            if (name.startsWith(COBBLE_MANIFEST_PREFIX)
                    && name.endsWith(COBBLE_MANIFEST_SUFFIX)
                    && name.length()
                            > COBBLE_MANIFEST_PREFIX.length() + COBBLE_MANIFEST_SUFFIX.length()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reads the 4-byte magic of the first inspect-schema blob under {@code
     * <root>/inspect-schema/blobs}, or {@code 0} when no blob is present.
     */
    private static int inspectSchemaBlobMagic(FileSystem fs, Path root, String pathUri) {
        Path blobsDir = new Path(new Path(root, INSPECT_SCHEMA), BLOBS);
        FileStatus[] blobs = listStatus(fs, blobsDir, pathUri);
        if (blobs == null) {
            return 0;
        }
        for (FileStatus blob : blobs) {
            if (blob.isDir()) {
                continue;
            }
            if (!blob.getPath().getName().endsWith(InspectSchemaRegistryLayout.BLOB_SUFFIX)) {
                continue;
            }
            return readMagic(fs, blob.getPath(), pathUri);
        }
        return 0;
    }

    private static int readMagic(FileSystem fs, Path file, String pathUri) {
        try (FSDataInputStream input = fs.open(file)) {
            byte[] magic = new byte[4];
            int total = 0;
            while (total < magic.length) {
                int n = input.read(magic, total, magic.length - total);
                if (n < 0) {
                    break;
                }
                total += n;
            }
            if (total < 4) {
                return 0;
            }
            return ((magic[0] & 0xFF) << 24)
                    | ((magic[1] & 0xFF) << 16)
                    | ((magic[2] & 0xFF) << 8)
                    | (magic[3] & 0xFF);
        } catch (IOException e) {
            throw new ValidationException(
                    "Failed to read Cobble inspect schema blob "
                            + file
                            + " under "
                            + pathUri
                            + ": "
                            + e.getMessage(),
                    e);
        }
    }

    private static boolean exists(FileSystem fs, Path path, String pathUri) {
        try {
            return fs.exists(path);
        } catch (IOException e) {
            throw new ValidationException(
                    "Failed to access "
                            + path
                            + " under Cobble source path "
                            + pathUri
                            + ": "
                            + e.getMessage(),
                    e);
        }
    }

    /** Lists {@code dir}, returning {@code null} when it does not exist or is not a directory. */
    private static FileStatus[] listStatus(FileSystem fs, Path dir, String pathUri) {
        try {
            if (!fs.exists(dir)) {
                return null;
            }
            return fs.listStatus(dir);
        } catch (IOException e) {
            throw new ValidationException(
                    "Failed to list "
                            + dir
                            + " under Cobble source path "
                            + pathUri
                            + ": "
                            + e.getMessage(),
                    e);
        }
    }
}
