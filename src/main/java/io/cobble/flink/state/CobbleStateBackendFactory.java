package io.cobble.flink.state;

import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackendFactory;

import java.io.IOException;

/** Factory for constructing the Cobble state backend from Flink configuration. */
public class CobbleStateBackendFactory implements StateBackendFactory<CobbleStateBackend> {

    @Override
    public CobbleStateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException, IOException {
        return new CobbleStateBackend().configure(config, classLoader);
    }
}
