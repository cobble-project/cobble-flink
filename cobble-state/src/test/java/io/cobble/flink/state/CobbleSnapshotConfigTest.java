package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.cobble.flink.common.CobbleFlinkStorageConfig;

import org.junit.jupiter.api.Test;

class CobbleSnapshotConfigTest {
    @Test
    void preservesResolvedStorageAndTimeout() {
        CobbleFlinkStorageConfig storage = CobbleFlinkStorageConfig.empty();
        for (long timeout : new long[] {0L, 1234L, Long.MAX_VALUE}) {
            CobbleSnapshotConfig config = new CobbleSnapshotConfig(storage, timeout);
            assertSame(storage, config.storage);
            assertEquals(timeout, config.exportTimeoutMillis);
        }
    }
}
