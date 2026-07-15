package io.cobble.flink.inspect.internal;

import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.inspect.InspectCatalog;
import io.cobble.flink.inspect.InspectSelection;
import io.cobble.flink.inspect.InspectSession;

/** Internal factory that keeps the concrete session implementation out of the public API. */
public final class InspectSessions {
    private InspectSessions() {}

    public static InspectSession open(
            InspectCatalog catalog,
            InspectSelection selection,
            MonitorReaderSession readerSession,
            String sourceKind,
            String sourceRoot,
            CheckpointEntry checkpoint,
            OperatorEntry operator,
            CobbleConnectorStorageOptions storageOptions,
            ClassLoader userClassLoader,
            int configuredTotalBuckets) {
        return new InspectSessionImpl(
                catalog,
                selection,
                readerSession,
                sourceKind,
                sourceRoot,
                checkpoint,
                operator,
                storageOptions,
                userClassLoader,
                configuredTotalBuckets);
    }
}
