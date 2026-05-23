package io.cobble.flink.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

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

    private CobbleSourceTableOptions() {}
}
