package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.flink.common.CobbleEmbeddedCheckpoint;
import io.cobble.flink.common.CobbleFlinkStorageConfig;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** Runtime helpers for checkpoint-root Cobble state source scans. */
final class CobbleStateSourceRuntime {

    private static final String CHECKPOINT_PREFIX = "chk-";
    private static final String FLINK_METADATA = "_metadata";
    private static final String COBBLE_MANIFEST_PREFIX = "COBBLE-SNAPSHOT-";
    private static final String COBBLE_MANIFEST_SUFFIX = "-MANIFEST";
    private static final int SCAN_BLOCK_CACHE_BYTES = 8 * 1024 * 1024;

    private CobbleStateSourceRuntime() {}

    static long resolveCheckpointId(StateSourceConfig config) throws IOException {
        if (config.layout() == StateSourceConfig.Layout.EMBEDDED_CHECKPOINT) {
            return CobbleEmbeddedCheckpoint.select(
                            new Path(config.pathUri()), config.scanCheckpointId())
                    .checkpoint()
                    .checkpointId();
        }
        if (!"latest".equals(config.scanCheckpointId())) {
            long checkpointId = Long.parseLong(config.scanCheckpointId());
            Path checkpointDir = checkpointDir(config.pathUri(), checkpointId);
            if (!exists(checkpointDir) || !exists(new Path(checkpointDir, FLINK_METADATA))) {
                throw new IOException(
                        "Cobble state source checkpoint "
                                + checkpointId
                                + " was not found under "
                                + config.pathUri()
                                + ".");
            }
            Path manifest = manifestCopyPath(checkpointDir, config.operatorId());
            if (!exists(manifest)) {
                throw new IOException(
                        "Cobble state source checkpoint "
                                + checkpointId
                                + " is missing manifest "
                                + manifest
                                + ".");
            }
            return checkpointId;
        }

        Path root = new Path(config.pathUri());
        FileStatus[] children = listStatus(root);
        long latest = -1L;
        if (children != null) {
            for (FileStatus child : children) {
                if (!child.isDir()) {
                    continue;
                }
                Long checkpointId = parseCheckpointDirName(child.getPath().getName());
                if (checkpointId == null) {
                    continue;
                }
                if (!exists(new Path(child.getPath(), FLINK_METADATA))) {
                    continue;
                }
                if (!exists(manifestCopyPath(child.getPath(), config.operatorId()))) {
                    continue;
                }
                if (checkpointId.longValue() > latest) {
                    latest = checkpointId.longValue();
                }
            }
        }
        if (latest < 0L) {
            throw new IOException(
                    "Cobble state source could not find a readable chk-* checkpoint for operator '"
                            + config.operatorId()
                            + "' under "
                            + config.pathUri()
                            + ".");
        }
        return latest;
    }

    static List<CobbleStateSourceSplit> createStateSourceSplits(StateSourceConfig config)
            throws IOException {
        try {
            return CobbleStateTableReadProvider.createSourceSplits(config);
        } catch (IOException error) {
            throw error;
        } catch (Exception error) {
            throw new IOException("Unable to create fixed state source splits", error);
        }
    }

    private static Config baseConfig(StateSourceConfig config, int totalKeyGroups) {
        Config cobbleConfig = new Config().numColumns(1);
        if (totalKeyGroups > 0) {
            cobbleConfig.totalBuckets(totalKeyGroups);
        }
        cobbleConfig.snapshotRetention = null;
        cobbleConfig.governanceMode = Config.GovernanceMode.NOOP;
        cobbleConfig.logConsole = false;
        cobbleConfig.memtableCapacity = 1;
        cobbleConfig.memtableBufferCount = 1;
        cobbleConfig.blockCacheSize = SCAN_BLOCK_CACHE_BYTES;
        cobbleConfig.blockCacheHybridEnabled = false;
        cobbleConfig.blockCacheHybridDiskSize = 0;
        Config.ReaderConfigEntry readerOptions = new Config.ReaderConfigEntry();
        readerOptions.blockCacheSize = nonNegativeInt(config.sourceBlockCacheMemoryBytes());
        readerOptions.reloadToleranceSeconds = 0L;
        cobbleConfig.reader = readerOptions;
        return cobbleConfig;
    }

    /** Base reader configuration shared with the fixed public state table reader. */
    static Config tableReadConfig(StateSourceConfig config) {
        Config reader = baseConfig(config, config.bucketCount());
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = CobbleFlinkStorageConfig.nativePath(config.pathUri());
        volume.kinds = Collections.singletonList(Config.VolumeUsageKind.READONLY);
        config.storageConfig().register(reader, volume);
        return reader;
    }

    private static Path checkpointDir(String checkpointRootUri, long checkpointId) {
        return new Path(new Path(checkpointRootUri), CHECKPOINT_PREFIX + checkpointId);
    }

    private static Path manifestCopyPath(Path checkpointDir, String operatorId) {
        return new Path(
                checkpointDir, COBBLE_MANIFEST_PREFIX + operatorId + COBBLE_MANIFEST_SUFFIX);
    }

    private static Long parseCheckpointDirName(String name) {
        if (name == null || !name.startsWith(CHECKPOINT_PREFIX)) {
            return null;
        }
        try {
            long value = Long.parseLong(name.substring(CHECKPOINT_PREFIX.length()));
            return value > 0L ? value : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static boolean exists(Path path) throws IOException {
        return path.getFileSystem().exists(path);
    }

    private static FileStatus[] listStatus(Path path) throws IOException {
        FileSystem fs = path.getFileSystem();
        if (!fs.exists(path)) {
            return null;
        }
        return fs.listStatus(path);
    }

    private static int nonNegativeInt(long value) {
        if (value < 0L || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    CobbleSourceTableOptions.SOURCE_BLOCK_CACHE_MEMORY.key()
                            + " must be in [0, "
                            + Integer.MAX_VALUE
                            + "].");
        }
        return (int) value;
    }
}
