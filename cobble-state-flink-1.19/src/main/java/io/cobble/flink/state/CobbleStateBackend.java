package io.cobble.flink.state;

import static org.apache.flink.configuration.description.TextElement.text;

import io.cobble.flink.common.CobbleLoader;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractManagedMemoryStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Cobble-backed state backend bootstrap for Flink 1.17.
 *
 * <p>The backend derives Cobble memory, storage directories, and timer-queue implementation from
 * Flink configuration and opens one live structured Cobble DB per keyed backend.
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
    private final Configuration flinkConfig;
    private PriorityQueueStateType priorityQueueStateType;

    public CobbleStateBackend() {
        this(null, new CobbleMemoryConfiguration(), null, false, false, new Configuration(), null);
        ensureCobbleLoaded();
    }

    CobbleStateBackend(boolean manualTtlTimeProviderForTests) {
        this(
                null,
                new CobbleMemoryConfiguration(),
                null,
                false,
                manualTtlTimeProviderForTests,
                new Configuration(),
                null);
        ensureCobbleLoaded();
    }

    private CobbleStateBackend(
            File[] localDbDirectories,
            CobbleMemoryConfiguration memoryConfiguration,
            String checkpointDirectory,
            boolean localDirPrimaryHighPriority,
            boolean manualTtlTimeProviderForTests,
            Configuration flinkConfig,
            PriorityQueueStateType priorityQueueStateType) {
        this.localDbDirectories = localDbDirectories;
        this.memoryConfiguration = memoryConfiguration;
        this.checkpointDirectory = checkpointDirectory;
        this.localDirPrimaryHighPriority = localDirPrimaryHighPriority;
        this.manualTtlTimeProviderForTests = manualTtlTimeProviderForTests;
        this.flinkConfig = flinkConfig;
        this.priorityQueueStateType = priorityQueueStateType;
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
        this.flinkConfig = mergedConfiguration(original.flinkConfig, config);
        this.priorityQueueStateType =
                original.priorityQueueStateType != null
                        ? original.priorityQueueStateType
                        : config.get(CobbleOptions.TIMER_SERVICE_FACTORY);
        this.latencyTrackingConfigBuilder = original.latencyTrackingConfigBuilder.configure(config);
    }

    /** Ensures the Cobble JNI library is loaded before any backend work starts. */
    static void ensureCobbleLoaded() {
        CobbleLoader.ensureCobbleLoaded();
    }

    /** Rebuilds the backend with Flink-side configuration applied to Cobble options. */
    @Override
    public CobbleStateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        return new CobbleStateBackend(this, config);
    }

    /** Returns the configured timer-service priority queue implementation. */
    public PriorityQueueStateType getPriorityQueueStateType() {
        return priorityQueueStateType != null
                ? priorityQueueStateType
                : CobbleOptions.TIMER_SERVICE_FACTORY.defaultValue();
    }

    /** Selects the timer-service priority queue implementation. */
    public void setPriorityQueueStateType(PriorityQueueStateType priorityQueueStateType) {
        this.priorityQueueStateType =
                Preconditions.checkNotNull(
                        priorityQueueStateType, "priorityQueueStateType must not be null");
    }

    /** Cobble restore currently takes ownership of its source snapshot volumes. */
    @Override
    public boolean supportsNoClaimRestoreMode() {
        return false;
    }

    /** Savepoint format is delegated to Flink's default operator-state handling for now. */
    @Override
    public boolean supportsSavepointFormat(SavepointFormatType formatType) {
        return true;
    }

    /** Creates the Cobble-backed keyed backend shell and wires in resolved runtime settings. */
    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            StateBackend.KeyedStateBackendParameters<K> parameters) throws IOException {
        ensureCobbleLoaded();

        Environment env = parameters.getEnv();
        String operatorIdentifier = parameters.getOperatorIdentifier();
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
                                "job_" + parameters.getJobID() + "_op_" + fileCompatibleIdentifier),
                        subtaskDirectoryName);

        LatencyTrackingStateConfig latencyTrackingStateConfig =
                latencyTrackingConfigBuilder.setMetricGroup(parameters.getMetricGroup()).build();

        try {
            return new CobbleKeyedStateBackendBuilder<>(
                            env,
                            parameters.getKvStateRegistry(),
                            parameters.getKeySerializer(),
                            parameters.getNumberOfKeyGroups(),
                            parameters.getKeyGroupRange(),
                            parameters.getTtlTimeProvider(),
                            latencyTrackingStateConfig,
                            parameters.getCancelStreamRegistry(),
                            parameters.getStateHandles(),
                            instanceBasePath,
                            checkpointScopeDirectoryName,
                            memoryConfiguration,
                            checkpointDirectory,
                            localDirPrimaryHighPriority,
                            parameters.getManagedMemoryFraction(),
                            manualTtlTimeProviderForTests,
                            flinkConfig,
                            getPriorityQueueStateType())
                    .build();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to create Cobble keyed state backend.", e);
        }
    }

    private static Configuration mergedConfiguration(
            Configuration baseConfiguration, ReadableConfig additionalConfig) {
        Configuration merged = baseConfiguration.clone();
        if (additionalConfig instanceof Configuration) {
            merged.addAll((Configuration) additionalConfig);
        }
        return merged;
    }

    /**
     * Keeps operator state on Flink's default implementation while Cobble focuses on keyed state.
     */
    @Override
    public OperatorStateBackend createOperatorStateBackend(
            StateBackend.OperatorStateBackendParameters parameters) throws Exception {
        Environment env = parameters.getEnv();
        return new DefaultOperatorStateBackendBuilder(
                        env.getUserCodeClassLoader().asClassLoader(),
                        env.getExecutionConfig(),
                        true,
                        parameters.getStateHandles(),
                        parameters.getCancelStreamRegistry())
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

    /** Timer-service priority queue implementations supported by the Cobble state backend. */
    public enum PriorityQueueStateType implements DescribedEnum {
        HEAP(text("Heap-based")),
        COBBLE(text("Implementation based on Cobble"));

        private final InlineElement description;

        PriorityQueueStateType(InlineElement description) {
            this.description = description;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }
}
