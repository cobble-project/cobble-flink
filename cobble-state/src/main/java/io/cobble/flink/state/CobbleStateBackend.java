package io.cobble.flink.state;

import io.cobble.NativeLoader;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractManagedMemoryStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Cobble-backed state backend bootstrap for Flink 1.17.
 *
 * <p>The current version derives Cobble memory and storage directories from Flink
 * configuration/runtime and opens a live Cobble DB while the keyed-state API remains intentionally
 * unimplemented.
 */
public class CobbleStateBackend extends AbstractManagedMemoryStateBackend
        implements ConfigurableStateBackend {

    private static final long serialVersionUID = 1L;

    private transient File[] initializedDbBasePaths;
    private transient JobID jobId;
    private transient int nextDirectory;
    private transient boolean isInitialized;

    private File[] localDbDirectories;
    private final CobbleMemoryConfiguration memoryConfiguration;
    private final String checkpointDirectory;
    private final boolean localDirPrimaryHighPriority;
    private final boolean manualTtlTimeProviderForTests;
    private final int directIoBufferSizeBytes;
    private final int directIoBufferPoolMaxSize;

    public CobbleStateBackend() {
        this(
                null,
                new CobbleMemoryConfiguration(),
                null,
                false,
                false,
                toDirectIoBufferSizeBytes(CobbleOptions.DIRECT_IO_BUFFER_SIZE.defaultValue()),
                CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE.defaultValue());
        ensureCobbleLoaded();
    }

    CobbleStateBackend(boolean manualTtlTimeProviderForTests) {
        this(
                null,
                new CobbleMemoryConfiguration(),
                null,
                false,
                manualTtlTimeProviderForTests,
                toDirectIoBufferSizeBytes(CobbleOptions.DIRECT_IO_BUFFER_SIZE.defaultValue()),
                CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE.defaultValue());
        ensureCobbleLoaded();
    }

    private CobbleStateBackend(
            File[] localDbDirectories,
            CobbleMemoryConfiguration memoryConfiguration,
            String checkpointDirectory,
            boolean localDirPrimaryHighPriority,
            boolean manualTtlTimeProviderForTests,
            int directIoBufferSizeBytes,
            int directIoBufferPoolMaxSize) {
        this.localDbDirectories = localDbDirectories;
        this.memoryConfiguration = memoryConfiguration;
        this.checkpointDirectory = checkpointDirectory;
        this.localDirPrimaryHighPriority = localDirPrimaryHighPriority;
        this.manualTtlTimeProviderForTests = manualTtlTimeProviderForTests;
        this.directIoBufferSizeBytes = directIoBufferSizeBytes;
        this.directIoBufferPoolMaxSize = directIoBufferPoolMaxSize;
    }

    private CobbleStateBackend(CobbleStateBackend original, ReadableConfig config) {
        this.localDbDirectories =
                original.localDbDirectories != null
                        ? original.localDbDirectories
                        : parseDbStoragePaths(config.get(CobbleOptions.LOCAL_DIRECTORIES));
        this.memoryConfiguration =
                CobbleMemoryConfiguration.fromOtherAndConfiguration(
                        original.memoryConfiguration, config);
        this.memoryConfiguration.validate();
        this.checkpointDirectory =
                original.checkpointDirectory != null
                        ? original.checkpointDirectory
                        : config.get(CheckpointingOptions.CHECKPOINTS_DIRECTORY);
        this.localDirPrimaryHighPriority =
                original.localDirPrimaryHighPriority
                        || config.get(CobbleOptions.LOCAL_DIR_PRIMARY_HIGH_PRIORITY);
        this.manualTtlTimeProviderForTests = original.manualTtlTimeProviderForTests;
        this.directIoBufferSizeBytes =
                toDirectIoBufferSizeBytes(config.get(CobbleOptions.DIRECT_IO_BUFFER_SIZE));
        this.directIoBufferPoolMaxSize =
                toDirectIoBufferPoolMaxSize(
                        config.get(CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE));
        this.latencyTrackingConfigBuilder = original.latencyTrackingConfigBuilder.configure(config);
    }

    /** Ensures the Cobble JNI library is loaded before any backend work starts. */
    static void ensureCobbleLoaded() {
        NativeLoader.load();
    }

    /** Rebuilds the backend with Flink-side configuration applied to Cobble options. */
    @Override
    public CobbleStateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        return new CobbleStateBackend(this, config);
    }

    /** The backend only manages local/native resources, so no-claim restore is fine. */
    @Override
    public boolean supportsNoClaimRestoreMode() {
        return true;
    }

    /** Savepoint format is delegated to Flink's default operator-state handling for now. */
    @Override
    public boolean supportsSavepointFormat(SavepointFormatType formatType) {
        return true;
    }

    /** Uses the default managed-memory fraction when Flink calls the legacy entrypoint. */
    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws IOException {
        try {
            return createKeyedStateBackend(
                    env,
                    jobID,
                    operatorIdentifier,
                    keySerializer,
                    numberOfKeyGroups,
                    keyGroupRange,
                    kvStateRegistry,
                    ttlTimeProvider,
                    metricGroup,
                    stateHandles,
                    cancelStreamRegistry,
                    1.0d);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to create Cobble keyed state backend.", e);
        }
    }

    /** Creates the Cobble-backed keyed backend shell and wires in resolved runtime settings. */
    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry,
            double managedMemoryFraction)
            throws Exception {
        ensureCobbleLoaded();

        String fileCompatibleIdentifier = CobblePathUtils.toFileCompatibleName(operatorIdentifier);
        lazyInitializeForJob(env);
        String subtaskDirectoryName =
                String.format(
                        "subtask_%d_attempt_%d",
                        env.getTaskInfo().getIndexOfThisSubtask(),
                        env.getTaskInfo().getAttemptNumber());
        String checkpointScopeDirectoryName = "op_" + env.getJobVertexId().toHexString();

        File instanceBasePath =
                new File(
                        new File(
                                getNextStoragePath(),
                                "job_" + jobId + "_op_" + fileCompatibleIdentifier),
                        subtaskDirectoryName);

        LatencyTrackingStateConfig latencyTrackingStateConfig =
                latencyTrackingConfigBuilder.setMetricGroup(metricGroup).build();

        return new CobbleKeyedStateBackendBuilder<>(
                        env,
                        kvStateRegistry,
                        keySerializer,
                        numberOfKeyGroups,
                        keyGroupRange,
                        ttlTimeProvider,
                        latencyTrackingStateConfig,
                        cancelStreamRegistry,
                        stateHandles,
                        instanceBasePath,
                        checkpointScopeDirectoryName,
                        memoryConfiguration,
                        checkpointDirectory,
                        localDirPrimaryHighPriority,
                        managedMemoryFraction,
                        manualTtlTimeProviderForTests,
                        directIoBufferSizeBytes,
                        directIoBufferPoolMaxSize)
                .build();
    }

    private static int toDirectIoBufferSizeBytes(org.apache.flink.configuration.MemorySize size) {
        long bytes = size.getBytes();
        if (bytes <= 0 || bytes > Integer.MAX_VALUE) {
            throw new IllegalConfigurationException(
                    "state.backend.cobble.direct-io.buffer-size must be in (0, "
                            + Integer.MAX_VALUE
                            + "] bytes");
        }
        return (int) bytes;
    }

    private static int toDirectIoBufferPoolMaxSize(int poolMaxSize) {
        if (poolMaxSize <= 0) {
            throw new IllegalConfigurationException(
                    "state.backend.cobble.direct-io.pool-max-size must be > 0");
        }
        return poolMaxSize;
    }

    /**
     * Keeps operator state on Flink's default implementation while Cobble focuses on keyed state.
     */
    @Override
    public OperatorStateBackend createOperatorStateBackend(
            Environment env,
            String operatorIdentifier,
            @Nonnull Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {
        return new DefaultOperatorStateBackendBuilder(
                        env.getUserCodeClassLoader().asClassLoader(),
                        env.getExecutionConfig(),
                        true,
                        stateHandles,
                        cancelStreamRegistry)
                .build();
    }

    /** Resolves and validates the candidate local base directories once per job execution. */
    private synchronized void lazyInitializeForJob(Environment env) throws IOException {
        if (isInitialized) {
            return;
        }

        this.jobId = env.getJobID();

        if (localDbDirectories == null) {
            initializedDbBasePaths = new File[] {env.getTaskManagerInfo().getTmpWorkingDirectory()};
        } else {
            List<File> directories = new ArrayList<>(localDbDirectories.length);
            StringBuilder errorMessage = new StringBuilder();

            for (File directory : localDbDirectories) {
                File testDir = new File(directory, UUID.randomUUID().toString());
                if (!testDir.mkdirs()) {
                    String message =
                            "Cobble local state directory '"
                                    + directory
                                    + "' does not exist and cannot be created. ";
                    errorMessage.append(message);
                } else {
                    directories.add(directory);
                }
                testDir.delete();
            }

            if (directories.isEmpty()) {
                throw new IOException(
                        "No Cobble local storage directories available. " + errorMessage);
            }
            initializedDbBasePaths = directories.toArray(new File[0]);
        }

        nextDirectory = new Random().nextInt(initializedDbBasePaths.length);
        isInitialized = true;
    }

    /** Picks the next local base directory in round-robin order. */
    private synchronized File getNextStoragePath() {
        int nextIndex = nextDirectory + 1;
        nextIndex = nextIndex >= initializedDbBasePaths.length ? 0 : nextIndex;
        nextDirectory = nextIndex;
        return initializedDbBasePaths[nextIndex];
    }

    /** Parses the configured local directory list from Flink config into File handles. */
    private static File[] parseDbStoragePaths(String configuredPaths) {
        if (configuredPaths == null) {
            return null;
        }

        String[] directories = configuredPaths.split(",|" + File.pathSeparator);
        List<File> files = new ArrayList<>(directories.length);
        for (String directory : directories) {
            String trimmed = directory.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalArgumentException(
                        "Invalid configuration for Cobble local storage directories: empty path segment.");
            }
            files.add(new File(trimmed));
        }
        return files.toArray(new File[0]);
    }
}
