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

    private CobbleHighAvailabilityOptions() {}
}
