package io.cobble.flink.state;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Cobble-specific HA wrapper options. */
final class CobbleHighAvailabilityOptions {

    static final ConfigOption<String> DELEGATE_HA_TYPE =
            ConfigOptions.key("cobble.ha.delegate.type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The inner Flink HA mode or HighAvailabilityServicesFactory class that "
                                    + "the Cobble HA wrapper should delegate to.");

    static final ConfigOption<String> DELEGATE_NONE_MODE =
            ConfigOptions.key("cobble.ha.delegate.none-mode")
                    .stringType()
                    .defaultValue("auto")
                    .withDescription(
                            "How the Cobble HA wrapper should interpret cobble.ha.delegate.type=NONE. "
                                    + "Use 'standalone' for multi-process Flink clusters, 'embedded' "
                                    + "for MiniCluster-style in-JVM execution, or 'auto' to let Cobble "
                                    + "inspect the current call stack for MiniCluster frames and choose "
                                    + "between the two automatically.");

    private CobbleHighAvailabilityOptions() {}
}
