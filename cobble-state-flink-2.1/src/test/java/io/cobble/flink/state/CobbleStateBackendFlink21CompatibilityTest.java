package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.metrics.SizeTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.junit.jupiter.api.Test;

/**
 * Locks the Flink 2.1 keyed-backend constructor contract used by this compatibility bridge.
 *
 * <p>Flink 2.1 added {@link SizeTrackingStateConfig} to this constructor. A bridge compiled against
 * the older signature fails during task initialization on Flink 2.1 and later.
 */
class CobbleStateBackendFlink21CompatibilityTest {

    @Test
    void flinkKeyedBackendContractIncludesSizeTrackingConfig() throws Exception {
        assertNotNull(
                AbstractKeyedStateBackend.class.getConstructor(
                        TaskKvStateRegistry.class,
                        TypeSerializer.class,
                        ClassLoader.class,
                        ExecutionConfig.class,
                        TtlTimeProvider.class,
                        LatencyTrackingStateConfig.class,
                        SizeTrackingStateConfig.class,
                        CloseableRegistry.class,
                        InternalKeyContext.class));
    }
}
