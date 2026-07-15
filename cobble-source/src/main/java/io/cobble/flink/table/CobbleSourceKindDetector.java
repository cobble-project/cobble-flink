package io.cobble.flink.table;

import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleMetadataFileIO;
import io.cobble.flink.common.CobbleNativeSavepoint;
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
 * <p>The detector performs small, bounded probes. Table roots use connector-scoped metadata IO;
 * state checkpoint roots use Flink {@link FileSystem}. It never scans recursively. Detection
 * signals, in priority order:
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
    enum Probe {
        SINK,
        STATE_CHECKPOINT,
        NATIVE_SAVEPOINT,
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
        return detect(
                pathUri, requestedKind, sinkShapedSchema, CobbleConnectorStorageOptions.empty());
    }

    static CobbleResolvedSource detect(
            String pathUri,
            CobbleSourceKind requestedKind,
            boolean sinkShapedSchema,
            CobbleConnectorStorageOptions storageOptions) {
        if (requestedKind == CobbleSourceKind.STATE) {
            return resolveExplicitState(pathUri, probeStatePath(pathUri, true));
        }

        Probe probe;
        try {
            probe = probeTableRoot(pathUri, storageOptions);
        } catch (ValidationException tableFailure) {
            if (requestedKind == CobbleSourceKind.AUTO) {
                Probe stateProbe = probeStatePath(pathUri, false);
                if (stateProbe == Probe.NATIVE_SAVEPOINT) {
                    return resolveState(pathUri, StateSourceConfig.Layout.NATIVE_SAVEPOINT);
                }
                if (stateProbe == Probe.STATE_CHECKPOINT) {
                    return resolveState(pathUri, StateSourceConfig.Layout.CHECKPOINT_ROOT);
                }
            }
            throw tableFailure;
        }
        if (probe == Probe.UNKNOWN) {
            Probe stateProbe = probeStatePath(pathUri, false);
            if (isStateCheckpointProbe(stateProbe) || stateProbe == Probe.STATE_OPERATOR) {
                probe = stateProbe;
            }
        }

        switch (requestedKind) {
            case SINK:
                if (isStateCheckpointProbe(probe) || probe == Probe.STATE_OPERATOR) {
                    throw new ValidationException(
                            "source.kind='sink' was requested, but path appears to be a Cobble"
                                    + " state checkpoint.");
                }
                return CobbleResolvedSource.sink(sinkDiagnostics(pathUri));
            case STATE:
                throw new IllegalStateException("State source should have been resolved earlier.");
            case RAW:
                if (isStateCheckpointProbe(probe) || probe == Probe.STATE_OPERATOR) {
                    throw new ValidationException(
                            "source.kind='raw' expects a Cobble table root. For Cobble Flink keyed"
                                    + " state, use source.kind='state'.");
                }
                if (probe == Probe.UNKNOWN) {
                    throw new ValidationException(
                            "source.kind='raw' expects a Cobble table root, but path "
                                    + pathUri
                                    + " does not appear to be a Cobble table root"
                                    + " (no snapshot/CURRENT or writer-paths.properties found).");
                }
                return CobbleResolvedSource.raw(rawDiagnostics(pathUri, probe));
            case AUTO:
                return resolveAuto(pathUri, probe, sinkShapedSchema);
            default:
                throw new IllegalStateException("Unexpected source kind: " + requestedKind);
        }
    }

    private static Probe probeTableRoot(
            String pathUri, CobbleConnectorStorageOptions storageOptions) {
        try {
            CobbleMetadataFileIO fileIO = CobbleMetadataFileIO.open(pathUri, storageOptions);
            if (!fileIO.exists("")) {
                throw new ValidationException("Cobble source path does not exist: " + pathUri);
            }
            int magic = inspectSchemaBlobMagic(fileIO);
            if (magic == SinkInspectSchemaStore.MAGIC) {
                return Probe.SINK;
            }
            if (magic == StateInspectSchemaStore.MAGIC) {
                return Probe.STATE_OPERATOR;
            }
            if (fileIO.exists(SNAPSHOT + "/" + CURRENT) || fileIO.exists(WRITER_PATHS)) {
                return Probe.AMBIGUOUS;
            }
            return Probe.UNKNOWN;
        } catch (ValidationException e) {
            throw e;
        } catch (IOException e) {
            throw new ValidationException("Failed to inspect Cobble table metadata.", e);
        }
    }

    private static Probe probeStatePath(String pathUri, boolean strict) {
        try {
            Path root = new Path(pathUri);
            FileSystem fileSystem = root.getFileSystem();
            return probeStatePath(fileSystem, root, pathUri);
        } catch (IOException | RuntimeException | LinkageError e) {
            if (strict) {
                throw new ValidationException(
                        "Failed to access Flink filesystem for Cobble state checkpoint source.", e);
            }
            return Probe.UNKNOWN;
        }
    }

    static Probe probeStatePath(FileSystem fileSystem, Path root, String pathUri) {
        if (!exists(fileSystem, root, pathUri)) {
            return Probe.UNKNOWN;
        }
        if (hasNativeSavepointLayout(fileSystem, root)) {
            return Probe.NATIVE_SAVEPOINT;
        }
        if (hasCheckpointLayout(fileSystem, root, pathUri)) {
            return Probe.STATE_CHECKPOINT;
        }
        int magic = inspectSchemaBlobMagic(fileSystem, root, pathUri);
        if (magic == StateInspectSchemaStore.MAGIC) {
            return Probe.STATE_OPERATOR;
        }
        if (magic == SinkInspectSchemaStore.MAGIC) {
            return Probe.SINK;
        }
        return Probe.UNKNOWN;
    }

    static CobbleResolvedSource resolveExplicitState(String pathUri, Probe stateProbe) {
        if (stateProbe == Probe.NATIVE_SAVEPOINT) {
            return resolveState(pathUri, StateSourceConfig.Layout.NATIVE_SAVEPOINT);
        }
        if (stateProbe == Probe.STATE_CHECKPOINT) {
            return resolveState(pathUri, StateSourceConfig.Layout.CHECKPOINT_ROOT);
        }
        if (stateProbe == Probe.SINK) {
            throw new ValidationException(
                    "source.kind='state' was requested, but path appears to be a Cobble sink"
                            + " table.");
        }
        if (stateProbe == Probe.STATE_OPERATOR) {
            return resolveState(pathUri, StateSourceConfig.Layout.OPERATOR_ROOT);
        }
        return resolveState(pathUri, StateSourceConfig.Layout.UNKNOWN);
    }

    private static int inspectSchemaBlobMagic(CobbleMetadataFileIO fileIO) throws IOException {
        String blobsDir = INSPECT_SCHEMA + "/" + BLOBS;
        for (String name : fileIO.list(blobsDir)) {
            if (!name.endsWith(InspectSchemaRegistryLayout.BLOB_SUFFIX)) {
                continue;
            }
            byte[] bytes = fileIO.read(blobsDir + "/" + name);
            if (bytes.length < 4) {
                return 0;
            }
            return ((bytes[0] & 0xFF) << 24)
                    | ((bytes[1] & 0xFF) << 16)
                    | ((bytes[2] & 0xFF) << 8)
                    | (bytes[3] & 0xFF);
        }
        return 0;
    }

    private static int inspectSchemaBlobMagic(FileSystem fileSystem, Path root, String pathUri) {
        Path blobsDir = new Path(new Path(root, INSPECT_SCHEMA), BLOBS);
        FileStatus[] entries = listStatus(fileSystem, blobsDir, pathUri);
        if (entries == null) {
            return 0;
        }
        for (FileStatus entry : entries) {
            if (entry.isDir()
                    || !entry.getPath()
                            .getName()
                            .endsWith(InspectSchemaRegistryLayout.BLOB_SUFFIX)) {
                continue;
            }
            byte[] bytes = new byte[4];
            try (FSDataInputStream input = fileSystem.open(entry.getPath())) {
                int offset = 0;
                while (offset < bytes.length) {
                    int read = input.read(bytes, offset, bytes.length - offset);
                    if (read < 0) {
                        return 0;
                    }
                    offset += read;
                }
            } catch (IOException e) {
                throw new ValidationException(
                        "Failed to read Cobble state inspect schema under " + pathUri + '.', e);
            }
            return ((bytes[0] & 0xFF) << 24)
                    | ((bytes[1] & 0xFF) << 16)
                    | ((bytes[2] & 0xFF) << 8)
                    | (bytes[3] & 0xFF);
        }
        return 0;
    }

    private static CobbleResolvedSource resolveAuto(
            String pathUri, Probe probe, boolean sinkShapedSchema) {
        switch (probe) {
            case SINK:
                return CobbleResolvedSource.sink(sinkDiagnostics(pathUri));
            case STATE_CHECKPOINT:
                return resolveState(pathUri, StateSourceConfig.Layout.CHECKPOINT_ROOT);
            case NATIVE_SAVEPOINT:
                return resolveState(pathUri, StateSourceConfig.Layout.NATIVE_SAVEPOINT);
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
                        + ". Set source.kind='sink', source.kind='state', or source.kind='raw'.");
    }

    private static CobbleResolvedSource resolveState(
            String pathUri, StateSourceConfig.Layout layout) {
        return CobbleResolvedSource.state(
                new StateSourceConfig(pathUri, layout), stateDiagnostics(pathUri, layout));
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
            case NATIVE_SAVEPOINT:
                return "Detected Flink NATIVE savepoint at "
                        + pathUri
                        + " (Cobble keyed-state metadata embedded in _metadata).";
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

    private static String rawDiagnostics(String pathUri, Probe probe) {
        String signal;
        switch (probe) {
            case SINK:
                signal = "inspect-schema sidecar (CSNK)";
                break;
            case AMBIGUOUS:
                signal = "snapshot/CURRENT or writer-paths.properties";
                break;
            default:
                signal = "Cobble table root";
                break;
        }
        return "Resolved Cobble raw source for "
                + pathUri
                + " from "
                + signal
                + "; reading raw bytes without typed schema.";
    }

    // ------------------------------------------------------------------------------------------
    //  Bounded filesystem probing
    // ------------------------------------------------------------------------------------------

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

    /** A savepoint is native only when its Flink metadata contains Cobble's keyed-state payload. */
    private static boolean hasNativeSavepointLayout(FileSystem fs, Path root) {
        if (!exists(fs, new Path(root, FLINK_METADATA), root.toString())) {
            return false;
        }
        try {
            CobbleNativeSavepoint.load(root);
            return true;
        } catch (IOException | RuntimeException ignored) {
            return false;
        }
    }

    private static boolean isStateCheckpointProbe(Probe probe) {
        return probe == Probe.STATE_CHECKPOINT || probe == Probe.NATIVE_SAVEPOINT;
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

    private static boolean exists(FileSystem fs, Path path, String pathUri) {
        try {
            return fs.exists(path);
        } catch (IOException e) {
            throw new ValidationException(
                    "Failed to access Cobble state checkpoint metadata under " + pathUri + '.', e);
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
