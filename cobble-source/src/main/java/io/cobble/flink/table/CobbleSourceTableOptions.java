package io.cobble.flink.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

/** Connector options for the initial Cobble SQL source. */
final class CobbleSourceTableOptions {

    static final ConfigOption<String> PATH =
            ConfigOptions.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Shared Cobble table root path used for reading snapshots.");

    static final ConfigOption<Integer> BUCKET =
            ConfigOptions.key("bucket")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Total bucket count for the Cobble table.");

    static final ConfigOption<String> SOURCE_KIND =
            ConfigOptions.key("source.kind")
                    .stringType()
                    .defaultValue("auto")
                    .withDescription(
                            "Kind of Cobble path to read. Supported values are 'auto', 'sink' and"
                                    + " 'state'. 'auto' detects sink table roots and Flink state"
                                    + " checkpoint roots from on-disk layout.");

    static final ConfigOption<String> SCAN_CHECKPOINT_ID =
            ConfigOptions.key("scan.checkpoint-id")
                    .stringType()
                    .defaultValue("latest")
                    .withDescription(
                            "Checkpoint to read. Use a numeric global snapshot id or 'latest'.");

    static final ConfigOption<String> SCAN_MODE =
            ConfigOptions.key("scan.mode")
                    .stringType()
                    .defaultValue("batch")
                    .withDescription("Read mode. Supported values are 'batch' and 'streaming'.");

    static final ConfigOption<Long> SCAN_POLL_INTERVAL_MS =
            ConfigOptions.key("scan.poll-interval-ms")
                    .longType()
                    .defaultValue(3000L)
                    .withDescription(
                            "Polling interval in milliseconds for 'latest' + 'streaming' mode.");

    static final ConfigOption<MemorySize> SOURCE_BLOCK_CACHE_MEMORY =
            ConfigOptions.key("source.block-cache-memory")
                    .memoryType()
                    .defaultValue(MemorySize.ZERO)
                    .withDescription(
                            "Cobble source lookup block-cache size. Scan path keeps a fixed small"
                                    + " block cache budget and does not use this option.");

    static final ConfigOption<String> STATE_NAME =
            ConfigOptions.key("state.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Logical Flink state name to read. Required when source.kind='state';"
                                    + " rejected for sink sources.");

    static final ConfigOption<String> STATE_OPERATOR_ID =
            ConfigOptions.key("state.operator-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Cobble operator id under <path>/cobble to read state from. Optional"
                                    + " when the checkpoint contains exactly one Cobble operator;"
                                    + " required when several operators are present.");

    static final ConfigOption<String> STATE_KIND =
            ConfigOptions.key("state.kind")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional state-kind validation hint. One of 'value', 'list', 'map',"
                                    + " 'reducing', 'aggregating', 'timer'. When set it must match"
                                    + " the resolved inspect-schema state kind.");

    private CobbleSourceTableOptions() {}
}
