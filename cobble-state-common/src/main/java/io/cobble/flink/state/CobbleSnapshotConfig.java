package io.cobble.flink.state;

import io.cobble.flink.common.CobbleFlinkStorageConfig;

/** Snapshot export settings resolved by the backend builder. */
final class CobbleSnapshotConfig {
    final CobbleFlinkStorageConfig storage;
    final long exportTimeoutMillis;

    CobbleSnapshotConfig(CobbleFlinkStorageConfig storage, long exportTimeoutMillis) {
        this.storage = storage;
        this.exportTimeoutMillis = exportTimeoutMillis;
    }
}
