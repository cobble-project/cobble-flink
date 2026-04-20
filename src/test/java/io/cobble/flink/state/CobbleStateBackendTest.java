package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

/** Tests for {@link CobbleStateBackend}. */
class CobbleStateBackendTest {

    @Test
    void createsBackendAndLoadsNativeLibrary() {
        assertDoesNotThrow(CobbleStateBackend::new);
    }

    @Test
    void factoryCreatesCobbleStateBackend() throws Exception {
        CobbleStateBackendFactory factory = new CobbleStateBackendFactory();

        assertInstanceOf(
                CobbleStateBackend.class,
                factory.createFromConfig(new Configuration(), getClass().getClassLoader()));
    }
}
