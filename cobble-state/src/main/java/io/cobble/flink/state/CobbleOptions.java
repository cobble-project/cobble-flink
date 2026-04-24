package io.cobble.flink.state;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

/** Configuration options for the Cobble state backend. */
public final class CobbleOptions {

    /** The local directory on the TaskManager where Cobble stores its files. */
    public static final ConfigOption<String> LOCAL_DIRECTORIES =
            ConfigOptions.key("state.backend.cobble.localdir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The local directory on the TaskManager where Cobble stores its files. "
                                    + "By default this uses the TaskManager working directory derived from "
                                    + ClusterOptions.TASK_MANAGER_PROCESS_WORKING_DIR_BASE.key()
                                    + ".");

    /** Whether Cobble should use the managed memory budget of the slot. */
    public static final ConfigOption<Boolean> USE_MANAGED_MEMORY =
            ConfigOptions.key("state.backend.cobble.memory.managed")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If enabled, the Cobble state backend derives its memory budget from Flink managed memory.");

    /** Fixed total memory for Cobble per slot. Overrides managed-memory derived sizing. */
    public static final ConfigOption<MemorySize> FIX_PER_SLOT_MEMORY_SIZE =
            ConfigOptions.key("state.backend.cobble.memory.fixed-per-slot")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The fixed total amount of memory shared by all Cobble instances per slot.");

    /** Fraction of the total Cobble memory budget reserved for memtables/write buffers. */
    public static final ConfigOption<Double> WRITE_BUFFER_RATIO =
            ConfigOptions.key("state.backend.cobble.memory.write-buffer-ratio")
                    .doubleType()
                    .defaultValue(0.5d)
                    .withDescription(
                            "The fraction of the total Cobble memory budget reserved for memtables/write buffers.");

    /** Number of memtable buffers kept in memory. */
    public static final ConfigOption<Integer> MEMTABLE_BUFFER_COUNT =
            ConfigOptions.key("state.backend.cobble.memory.memtable-buffer-count")
                    .intType()
                    .defaultValue(2)
                    .withDescription("The number of memtable buffers kept in memory.");

    /**
     * Whether the local Cobble working directory should remain a high-priority primary volume when
     * a Flink checkpoint directory is configured.
     */
    public static final ConfigOption<Boolean> LOCAL_DIR_PRIMARY_HIGH_PRIORITY =
            ConfigOptions.key("state.backend.cobble.localdir.primary-high-priority")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If enabled and a Flink checkpoint directory is configured, the local Cobble working "
                                    + "directory is registered as a high-priority primary volume instead of a cache volume.");

    private CobbleOptions() {}
}
