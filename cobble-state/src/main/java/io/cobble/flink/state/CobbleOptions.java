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

    /** Memtable implementation type used by Cobble (hash, skiplist, vec). */
    public static final ConfigOption<String> MEMTABLE_TYPE =
            ConfigOptions.key("state.backend.cobble.memtable.type")
                    .stringType()
                    .defaultValue("hash")
                    .withDescription(
                            "The memtable implementation used by Cobble. Supported values: hash, skiplist, vec.");

    /** Compaction policy used by Cobble (round_robin, min_overlap, score_priority). */
    public static final ConfigOption<String> COMPACTION_POLICY =
            ConfigOptions.key("state.backend.cobble.compaction.policy")
                    .stringType()
                    .defaultValue("round_robin")
                    .withDescription(
                            "The compaction policy used by Cobble. Supported values: round_robin, min_overlap, score_priority.");

    /** Whether Cobble should enable SST bloom filters. */
    public static final ConfigOption<Boolean> SST_BLOOM_FILTER_ENABLED =
            ConfigOptions.key("state.backend.cobble.sst.bloom-filter.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether Cobble should enable bloom filters for SST files.");

    /** Bloom filter bits per key when SST bloom filters are enabled. */
    public static final ConfigOption<Integer> SST_BLOOM_FILTER_BITS_PER_KEY =
            ConfigOptions.key("state.backend.cobble.sst.bloom-filter.bits-per-key")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "The bloom filter bits-per-key value used when SST bloom filters are enabled.");

    /** Whether Cobble should enable partitioned SST index/filter blocks. */
    public static final ConfigOption<Boolean> SST_PARTITIONED_INDEX_ENABLED =
            ConfigOptions.key("state.backend.cobble.sst.partitioned-index.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether Cobble should enable partitioned index/filter blocks for SST files.");

    /** Threshold above which values are separated into the value log. */
    public static final ConfigOption<MemorySize> VALUE_SEPARATION_THRESHOLD =
            ConfigOptions.key("state.backend.cobble.value-separation.threshold")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1kb"))
                    .withDescription(
                            "Values larger than this threshold are separated into Cobble's value log. "
                                    + "Default: 1kb.");

    /** Byte size of each pooled direct ByteBuffer used by Cobble direct reads. */
    public static final ConfigOption<MemorySize> DIRECT_IO_BUFFER_SIZE =
            ConfigOptions.key("state.backend.cobble.direct-io.buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("2kb"))
                    .withDescription(
                            "The size of each pooled direct ByteBuffer used by Cobble direct reads.");

    /** Maximum number of pooled direct ByteBuffers kept for Cobble direct reads. */
    public static final ConfigOption<Integer> DIRECT_IO_BUFFER_POOL_MAX_SIZE =
            ConfigOptions.key("state.backend.cobble.direct-io.pool-max-size")
                    .intType()
                    .defaultValue(64)
                    .withDescription(
                            "The maximum number of pooled direct ByteBuffers kept for Cobble direct reads.");

    /** Maximum size of the active Cobble log file before rolling. */
    public static final ConfigOption<MemorySize> LOG_MAX_FILE_SIZE =
            ConfigOptions.key("state.backend.cobble.log.max-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("10mb"))
                    .withDescription(
                            "The maximum size of the active Cobble log file before it is rolled.");

    /** Total number of Cobble log files to keep, including the active file. */
    public static final ConfigOption<Integer> LOG_KEEP_FILES =
            ConfigOptions.key("state.backend.cobble.log.keep-files")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The total number of Cobble log files to keep, including the active file.");

    /** Log level used by Cobble native logging. */
    public static final ConfigOption<String> LOG_LEVEL =
            ConfigOptions.key("state.backend.cobble.log.level")
                    .stringType()
                    .defaultValue("info")
                    .withDescription(
                            "The log level used by Cobble native logging. Supported values: trace, debug, info, warn, error, off.");

    /** Automatically expire older Cobble snapshots after this many newer snapshots. */
    public static final ConfigOption<Integer> SNAPSHOT_RETENTION =
            ConfigOptions.key("state.backend.cobble.snapshot.retention")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Automatically expire older Cobble snapshots after this many newer snapshots have completed.");

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
