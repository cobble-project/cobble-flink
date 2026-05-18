package io.cobble.flink.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

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

    private CobbleTableOptions() {}
}
