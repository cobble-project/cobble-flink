package io.cobble.flink.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

/** Connector options for the initial Cobble SQL sink. */
final class CobbleTableOptions {

    static final ConfigOption<String> PATH =
            ConfigOptions.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Shared Cobble table root path used for primary data, metadata, and snapshots.");

    static final ConfigOption<Integer> BUCKET =
            ConfigOptions.key("bucket")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Total bucket count for HASH_FIXED key routing.");

    static final ConfigOption<Integer> SNAPSHOT_RETENTION =
            ConfigOptions.key("snapshot.retention")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "How many committed Cobble snapshots to retain automatically.");

    static final ConfigOption<Boolean> SINK_USE_MANAGED_MEMORY_ALLOCATOR =
            ConfigOptions.key("sink.use-managed-memory-allocator")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, the sink writer requests Flink managed memory for its"
                                    + " write buffer.");

    static final ConfigOption<MemorySize> SINK_WRITER_BUFFER_MEMORY =
            ConfigOptions.key("sink.writer-buffer-memory")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(256))
                    .withDescription(
                            "Write-buffer budget for one sink writer. This is always used as"
                                    + " Cobble write-buffer capacity and, when managed memory is"
                                    + " enabled, also used as the managed-memory declaration size.");

    private CobbleTableOptions() {}
}
