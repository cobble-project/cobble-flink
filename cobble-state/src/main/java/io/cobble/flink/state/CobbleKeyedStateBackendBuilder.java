package io.cobble.flink.state;

import io.cobble.Config;
import io.cobble.Db;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

/** Builder that prepares Cobble resources for the current keyed backend shell. */
final class CobbleKeyedStateBackendBuilder<K> {

    private static final String COBBLE_CONFIG_FILE_PREFIX = "cobble-config-";
    private static final String COBBLE_DB_DIR_NAME = "cobble-db";
    private static final String COBBLE_LOG_FILE_NAME = "cobble.log";
    private final Environment env;
    private final TaskKvStateRegistry kvStateRegistry;
    private final TypeSerializer<K> keySerializer;
    private final int numberOfKeyGroups;
    private final KeyGroupRange keyGroupRange;
    private final TtlTimeProvider ttlTimeProvider;
    private final LatencyTrackingStateConfig latencyTrackingStateConfig;
    private final CloseableRegistry cancelStreamRegistry;
    private final Collection<KeyedStateHandle> restoreStateHandles;
    private final File instanceBasePath;
    private final String checkpointScopeDirectoryName;
    private final CobbleMemoryConfiguration memoryConfiguration;
    private final String checkpointDirectory;
    private final boolean localDirPrimaryHighPriority;
    private final double managedMemoryFraction;
    private final boolean manualTtlTimeProviderForTests;
    private final Configuration flinkConfig;

    CobbleKeyedStateBackendBuilder(
            Environment env,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            Collection<KeyedStateHandle> restoreStateHandles,
            File instanceBasePath,
            String checkpointScopeDirectoryName,
            CobbleMemoryConfiguration memoryConfiguration,
            String checkpointDirectory,
            boolean localDirPrimaryHighPriority,
            double managedMemoryFraction,
            boolean manualTtlTimeProviderForTests,
            Configuration flinkConfig) {
        this.env = env;
        this.kvStateRegistry = kvStateRegistry;
        this.keySerializer = keySerializer;
        this.numberOfKeyGroups = numberOfKeyGroups;
        this.keyGroupRange = keyGroupRange;
        this.ttlTimeProvider = ttlTimeProvider;
        this.latencyTrackingStateConfig = latencyTrackingStateConfig;
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.restoreStateHandles = restoreStateHandles;
        this.instanceBasePath = instanceBasePath;
        this.checkpointScopeDirectoryName = checkpointScopeDirectoryName;
        this.memoryConfiguration = memoryConfiguration;
        this.checkpointDirectory = checkpointDirectory;
        this.localDirPrimaryHighPriority = localDirPrimaryHighPriority;
        this.managedMemoryFraction = managedMemoryFraction;
        this.manualTtlTimeProviderForTests = manualTtlTimeProviderForTests;
        this.flinkConfig = flinkConfig;
    }

    /** Builds the minimal keyed-backend shell after Cobble resources are prepared. */
    CobbleKeyedStateBackend<K> build() throws IOException {
        CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();
        CobbleBackendResources resources = prepareCobbleResources();
        boolean success = false;
        try {
            CobbleKeyedStateBackend<K> backend =
                    new CobbleKeyedStateBackend<>(
                            kvStateRegistry,
                            keySerializer,
                            env.getUserCodeClassLoader().asClassLoader(),
                            env.getExecutionConfig(),
                            ttlTimeProvider,
                            latencyTrackingStateConfig,
                            cancelStreamRegistryForBackend,
                            new InternalKeyContextImpl<>(keyGroupRange, numberOfKeyGroups),
                            resources.instanceBasePath,
                            resources.volumePath,
                            resources.configPath,
                            resources.config,
                            resources.db,
                            manualTtlTimeProviderForTests);
            success = true;
            return backend;
        } finally {
            if (!success) {
                IOUtils.closeQuietly(cancelStreamRegistryForBackend);
                resources.close();
            }
        }
    }

    /** Normalizes checkpoint URIs into the set of schemes Cobble currently understands. */
    static String normalizeCheckpointDirectory(String checkpointDirectory) {
        return CobblePathUtils.normalizeStorageDirectory(checkpointDirectory);
    }

    /** Creates the Cobble volume descriptors that mirror the resolved local/remote layout. */
    static List<Config.VolumeDescriptor> createVolumeDescriptors(
            File instanceBasePath,
            String checkpointScopeDirectoryName,
            String checkpointDirectory,
            boolean localDirPrimaryHighPriority) {
        return createVolumeDescriptors(
                instanceBasePath,
                checkpointScopeDirectoryName,
                checkpointDirectory,
                localDirPrimaryHighPriority,
                new Configuration());
    }

    /** Creates the Cobble volume descriptors that mirror the resolved local/remote layout. */
    static List<Config.VolumeDescriptor> createVolumeDescriptors(
            File instanceBasePath,
            String checkpointScopeDirectoryName,
            String checkpointDirectory,
            boolean localDirPrimaryHighPriority,
            Configuration flinkConfig) {
        File localVolumePath = new File(instanceBasePath, COBBLE_DB_DIR_NAME);
        String normalizedLocalVolumePath = normalizeLocalPath(localVolumePath);
        String normalizedCheckpointDirectory = normalizeCheckpointDirectory(checkpointDirectory);

        if (normalizedCheckpointDirectory == null) {
            return Collections.singletonList(
                    Config.VolumeDescriptor.singleVolume(normalizedLocalVolumePath));
        }

        Config.VolumeDescriptor checkpointVolume = new Config.VolumeDescriptor();
        checkpointVolume.baseDir =
                createCheckpointVolumeBaseDir(
                        normalizedCheckpointDirectory, checkpointScopeDirectoryName);
        checkpointVolume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT);
        CobbleFlinkConfigMapper.applyCheckpointVolumeOptions(
                checkpointVolume, normalizedCheckpointDirectory, flinkConfig);

        Config.VolumeDescriptor localVolume = new Config.VolumeDescriptor();
        localVolume.baseDir = normalizedLocalVolumePath;
        localVolume.kinds =
                Collections.singletonList(
                        localDirPrimaryHighPriority
                                ? Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH
                                : Config.VolumeUsageKind.CACHE);

        return Arrays.asList(checkpointVolume, localVolume);
    }

    /** Creates the local working directory, writes config JSON, and opens the Cobble DB. */
    private CobbleBackendResources prepareCobbleResources() throws IOException {
        Files.createDirectories(instanceBasePath.toPath());

        VolumeLayout volumeLayout = createVolumeLayout();
        Files.createDirectories(volumeLayout.localVolumePath.toPath());

        Path configPath =
                new File(instanceBasePath, COBBLE_CONFIG_FILE_PREFIX + UUID.randomUUID() + ".json")
                        .toPath();
        Config config = createCobbleConfig(volumeLayout);
        Files.write(configPath, config.toJson().getBytes(StandardCharsets.UTF_8));

        Db db = null;
        boolean success = false;
        try {
            db = openDb(configPath);
            success = true;
            return new CobbleBackendResources(
                    instanceBasePath, volumeLayout.localVolumePath, configPath, config, db);
        } finally {
            if (!success) {
                if (db != null) {
                    db.close();
                }
                org.apache.flink.util.FileUtils.deleteDirectory(instanceBasePath);
            }
        }
    }

    /** Computes the local volume root and the full Cobble volume list for this backend instance. */
    private VolumeLayout createVolumeLayout() throws IOException {
        File localVolumePath = new File(instanceBasePath, COBBLE_DB_DIR_NAME);
        List<Config.VolumeDescriptor> volumes =
                new ArrayList<>(
                        createVolumeDescriptors(
                                instanceBasePath,
                                checkpointScopeDirectoryName,
                                checkpointDirectory,
                                localDirPrimaryHighPriority,
                                flinkConfig));
        addRestoreSourceVolumes(volumes);
        return new VolumeLayout(localVolumePath, volumes);
    }

    /**
     * Keeps source checkpoint volumes available while a restored backend opens its writable DB.
     *
     * <p>Snapshot manifests contain absolute paths into the source operator's shared-state volume.
     * A new Flink job receives a fresh shared-state directory, so native restore cannot resolve
     * those paths unless the source volume is also present in the new config. A same-range restore
     * claims its single source DB and resumes it in place, so that source volume remains writable.
     * Rescale creates a new DB from one or more source manifests instead, so its source volumes
     * stay READONLY.
     */
    private void addRestoreSourceVolumes(List<Config.VolumeDescriptor> volumes) throws IOException {
        if (restoreStateHandles == null || restoreStateHandles.isEmpty()) {
            return;
        }

        List<RestoreSource> restoreSources = readRestoreSources();
        boolean resumeSingleSource = canResumeSingleSource(restoreSources);
        List<Config.VolumeDescriptor> claimedSourceVolumes = new ArrayList<>();
        Set<String> configuredBaseDirectories = new LinkedHashSet<>();
        for (Config.VolumeDescriptor volume : volumes) {
            configuredBaseDirectories.add(volume.baseDir);
        }
        for (RestoreSource restoreSource : restoreSources) {
            String sourceVolumeDirectory =
                    CobblePathUtils.snapshotVolumeDirectory(
                            restoreSource.metadata.shardSnapshot().manifestPath,
                            restoreSource.metadata.shardSnapshot().dbId);
            if (!configuredBaseDirectories.add(sourceVolumeDirectory)) {
                continue;
            }

            Config.VolumeDescriptor sourceVolume = new Config.VolumeDescriptor();
            sourceVolume.baseDir = sourceVolumeDirectory;
            sourceVolume.kinds =
                    resumeSingleSource
                            ? Arrays.asList(
                                    Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                                    Config.VolumeUsageKind.META,
                                    Config.VolumeUsageKind.SNAPSHOT)
                            : Collections.singletonList(Config.VolumeUsageKind.READONLY);
            CobbleFlinkConfigMapper.applyCheckpointVolumeOptions(
                    sourceVolume, sourceVolumeDirectory, flinkConfig);
            if (resumeSingleSource) {
                claimedSourceVolumes.add(sourceVolume);
            } else {
                volumes.add(sourceVolume);
            }
        }
        // Db.resume() loads manifests from the first snapshot-persistable volume. Put the claimed
        // source first so resume opens the old DB before it starts writing snapshots for the new
        // job.
        volumes.addAll(0, claimedSourceVolumes);
    }

    /** Fills the Cobble config object with volume, bucket, and memory settings. */
    private Config createCobbleConfig(VolumeLayout volumeLayout) {
        Config config = new Config().numColumns(1).totalBuckets(numberOfKeyGroups);
        for (Config.VolumeDescriptor volume : volumeLayout.volumes) {
            config.addVolume(volume);
        }
        config.ttlEnabled = Boolean.TRUE;
        config.timeProvider =
                manualTtlTimeProviderForTests
                        ? Config.TimeProviderKind.MANUAL
                        : Config.TimeProviderKind.SYSTEM;
        config.logPath = new File(instanceBasePath, COBBLE_LOG_FILE_NAME).getAbsolutePath();
        config.logConsole = Boolean.FALSE;

        OptionalLong totalMemoryBytes =
                memoryConfiguration.resolveTotalMemoryBytes(env, managedMemoryFraction);
        config.memtableBufferCount = memoryConfiguration.getMemtableBufferCount();

        if (totalMemoryBytes.isPresent()) {
            long totalMemoryBudget = totalMemoryBytes.getAsLong();
            int memtableBufferCount = memoryConfiguration.getMemtableBufferCount();
            long minimumWriteBudget = Math.min(totalMemoryBudget, memtableBufferCount);
            long desiredWriteBudget =
                    Math.round(totalMemoryBudget * memoryConfiguration.getWriteBufferRatio());
            long writeBufferBudget =
                    Math.max(minimumWriteBudget, Math.min(totalMemoryBudget, desiredWriteBudget));
            long perMemtableCapacity = Math.max(1L, writeBufferBudget / memtableBufferCount);
            long blockCacheBudget = Math.max(0L, totalMemoryBudget - writeBufferBudget);

            config.memtableCapacity = toIntBytes("memtable capacity", perMemtableCapacity);
            config.blockCacheSize = toIntBytes("block cache size", blockCacheBudget);

            Config.ReaderConfigEntry reader = new Config.ReaderConfigEntry();
            reader.blockCacheSize = config.blockCacheSize;
            config.reader = reader;
        }

        CobbleFlinkConfigMapper.applyExposedOptions(config, flinkConfig);
        return config;
    }

    private Db openDb(Path configPath) throws IOException {
        if (restoreStateHandles == null || restoreStateHandles.isEmpty()) {
            return Db.open(
                    configPath.toString(),
                    keyGroupRange.getStartKeyGroup(),
                    keyGroupRange.getEndKeyGroup());
        }

        List<RestoreSource> restoreSources = readRestoreSources();
        if (restoreSources.isEmpty()) {
            throw new IOException(
                    "Cobble restore did not receive any readable checkpoint handles.");
        }
        if (canResumeSingleSource(restoreSources)) {
            return Db.resume(
                    configPath.toString(), restoreSources.get(0).metadata.shardSnapshot().dbId);
        }
        return restoreRescaledDb(configPath, restoreSources);
    }

    private boolean canResumeSingleSource(List<RestoreSource> restoreSources) {
        return restoreSources.size() == 1
                && keyGroupRange.equals(restoreSources.get(0).keyGroupRange);
    }

    private List<RestoreSource> readRestoreSources() throws IOException {
        List<RestoreSource> restoreSources = new ArrayList<>(restoreStateHandles.size());
        for (KeyedStateHandle restoreHandle : restoreStateHandles) {
            Preconditions.checkNotNull(
                    restoreHandle, "Cobble restore received a null keyed state handle.");
            if (!(restoreHandle instanceof IncrementalRemoteKeyedStateHandle)) {
                throw new UnsupportedOperationException(
                        "Cobble restore currently supports only IncrementalRemoteKeyedStateHandle, but found "
                                + restoreHandle.getClass().getName()
                                + '.');
            }
            IncrementalRemoteKeyedStateHandle incrementalHandle =
                    (IncrementalRemoteKeyedStateHandle) restoreHandle;
            if (incrementalHandle.getMetaStateHandle() == null) {
                throw new IOException("Cobble restore handle did not include checkpoint metadata.");
            }
            CobbleSnapshotMetadata metadata;
            try (org.apache.flink.core.fs.FSDataInputStream inputStream =
                    incrementalHandle.getMetaStateHandle().openInputStream()) {
                metadata = CobbleSnapshotMetadata.read(new DataInputViewStreamWrapper(inputStream));
            }
            restoreSources.add(new RestoreSource(incrementalHandle.getKeyGroupRange(), metadata));
        }
        restoreSources.sort(
                Comparator.comparingInt(source -> source.keyGroupRange.getStartKeyGroup()));
        return restoreSources;
    }

    private Db restoreRescaledDb(Path configPath, List<RestoreSource> restoreSources)
            throws IOException {
        List<RestoreSource> relevantSources = new ArrayList<>(restoreSources.size());
        for (RestoreSource source : restoreSources) {
            if (hasIntersection(source.keyGroupRange, keyGroupRange)) {
                relevantSources.add(source);
            }
        }
        if (relevantSources.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Cobble restore could not find any checkpoint shard covering backend range "
                            + keyGroupRange
                            + '.');
        }
        ensureRestoreCoverage(relevantSources);

        RestoreSource baseSource = selectBaseSource(relevantSources);
        CobbleSnapshotMetadata baseMetadata = baseSource.metadata;
        Db db =
                Db.restoreWithManifest(
                        configPath.toString(), baseMetadata.shardSnapshot().manifestPath);
        boolean success = false;
        try {
            shrinkBaseSourceToTargetRange(db, baseSource.keyGroupRange);
            for (RestoreSource source : relevantSources) {
                if (source == baseSource) {
                    continue;
                }
                int[] intersectionStarts = new int[] {intersectionStart(source.keyGroupRange)};
                int[] intersectionEnds = new int[] {intersectionEnd(source.keyGroupRange)};
                db.expandBucket(
                        source.metadata.shardSnapshot().dbId,
                        source.metadata.shardSnapshot().snapshotId,
                        intersectionStarts,
                        intersectionEnds);
            }
            success = true;
            return db;
        } finally {
            if (!success) {
                db.close();
            }
        }
    }

    private RestoreSource selectBaseSource(List<RestoreSource> restoreSources) {
        RestoreSource baseSource = null;
        int bestOverlap = -1;
        for (RestoreSource source : restoreSources) {
            int overlapSize = overlapSize(source.keyGroupRange);
            if (overlapSize > bestOverlap) {
                bestOverlap = overlapSize;
                baseSource = source;
            }
        }
        return Preconditions.checkNotNull(baseSource);
    }

    private void shrinkBaseSourceToTargetRange(Db db, KeyGroupRange sourceRange) {
        List<Integer> rangeStarts = new ArrayList<>(2);
        List<Integer> rangeEnds = new ArrayList<>(2);
        if (sourceRange.getStartKeyGroup() < keyGroupRange.getStartKeyGroup()) {
            rangeStarts.add(sourceRange.getStartKeyGroup());
            rangeEnds.add(keyGroupRange.getStartKeyGroup() - 1);
        }
        if (sourceRange.getEndKeyGroup() > keyGroupRange.getEndKeyGroup()) {
            rangeStarts.add(keyGroupRange.getEndKeyGroup() + 1);
            rangeEnds.add(sourceRange.getEndKeyGroup());
        }
        if (!rangeStarts.isEmpty()) {
            db.shrinkBucket(
                    rangeStarts.stream().mapToInt(Integer::intValue).toArray(),
                    rangeEnds.stream().mapToInt(Integer::intValue).toArray());
        }
    }

    private void ensureRestoreCoverage(List<RestoreSource> restoreSources) {
        int nextExpected = keyGroupRange.getStartKeyGroup();
        for (RestoreSource source : restoreSources) {
            int intersectionStart = intersectionStart(source.keyGroupRange);
            int intersectionEnd = intersectionEnd(source.keyGroupRange);
            if (intersectionStart > nextExpected) {
                throw new UnsupportedOperationException(
                        "Cobble restore is missing checkpoint shards for key-group range "
                                + KeyGroupRange.of(nextExpected, intersectionStart - 1)
                                + '.');
            }
            nextExpected = Math.max(nextExpected, intersectionEnd + 1);
            if (nextExpected > keyGroupRange.getEndKeyGroup()) {
                return;
            }
        }
        throw new UnsupportedOperationException(
                "Cobble restore is missing checkpoint shards for key-group range "
                        + KeyGroupRange.of(nextExpected, keyGroupRange.getEndKeyGroup())
                        + '.');
    }

    private boolean hasIntersection(KeyGroupRange sourceRange, KeyGroupRange targetRange) {
        return sourceRange.getStartKeyGroup() <= targetRange.getEndKeyGroup()
                && targetRange.getStartKeyGroup() <= sourceRange.getEndKeyGroup();
    }

    private int intersectionStart(KeyGroupRange sourceRange) {
        return Math.max(sourceRange.getStartKeyGroup(), keyGroupRange.getStartKeyGroup());
    }

    private int intersectionEnd(KeyGroupRange sourceRange) {
        return Math.min(sourceRange.getEndKeyGroup(), keyGroupRange.getEndKeyGroup());
    }

    private int overlapSize(KeyGroupRange sourceRange) {
        return intersectionEnd(sourceRange) - intersectionStart(sourceRange) + 1;
    }

    /** Maps the remote checkpoint location to Flink's shared-state base directory. */
    private static String createCheckpointVolumeBaseDir(
            String normalizedCheckpointDirectory, String checkpointScopeDirectoryName) {
        return CobblePathUtils.checkpointOperatorSharedStatePath(
                normalizedCheckpointDirectory, checkpointScopeDirectoryName);
    }

    /** Converts any local filesystem path into a stable file:// URI for Cobble config. */
    static String normalizeLocalPath(File localPath) {
        return CobblePathUtils.normalizeLocalPath(localPath);
    }

    /** Converts long-sized memory values into the int-sized fields expected by Cobble config. */
    private static int toIntBytes(String fieldName, long value) {
        Preconditions.checkArgument(
                value >= 0L, "Cobble %s must be >= 0, but was %s.", fieldName, value);
        try {
            return Math.toIntExact(value);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                    "Cobble " + fieldName + " exceeds Integer.MAX_VALUE bytes: " + value + '.', e);
        }
    }

    private static final class CobbleBackendResources implements AutoCloseable {
        private final File instanceBasePath;
        private final File volumePath;
        private final Path configPath;
        private final Config config;
        private final Db db;

        private CobbleBackendResources(
                File instanceBasePath, File volumePath, Path configPath, Config config, Db db) {
            this.instanceBasePath = instanceBasePath;
            this.volumePath = volumePath;
            this.configPath = configPath;
            this.config = config;
            this.db = db;
        }

        /** Releases the native DB and removes the temporary local working directory. */
        @Override
        public void close() throws IOException {
            db.close();
            org.apache.flink.util.FileUtils.deleteDirectory(instanceBasePath);
        }
    }

    private static final class VolumeLayout {
        private final File localVolumePath;
        private final List<Config.VolumeDescriptor> volumes;

        private VolumeLayout(File localVolumePath, List<Config.VolumeDescriptor> volumes) {
            this.localVolumePath = localVolumePath;
            this.volumes = volumes;
        }
    }

    private static final class RestoreSource {
        private final KeyGroupRange keyGroupRange;
        private final CobbleSnapshotMetadata metadata;

        private RestoreSource(KeyGroupRange keyGroupRange, CobbleSnapshotMetadata metadata) {
            this.keyGroupRange = keyGroupRange;
            this.metadata = metadata;
        }
    }
}
