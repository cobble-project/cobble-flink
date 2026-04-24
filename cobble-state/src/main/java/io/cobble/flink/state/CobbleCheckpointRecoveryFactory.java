package io.cobble.flink.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;

import java.util.concurrent.Executor;

/** Wraps Flink checkpoint recovery so completed checkpoints also drive Cobble global snapshots. */
final class CobbleCheckpointRecoveryFactory implements CheckpointRecoveryFactory {

    private final CheckpointRecoveryFactory delegate;

    CobbleCheckpointRecoveryFactory(CheckpointRecoveryFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
            JobID jobId,
            int maxNumberOfCheckpointsToRetain,
            SharedStateRegistryFactory sharedStateRegistryFactory,
            Executor ioExecutor,
            RestoreMode restoreMode)
            throws Exception {
        return new CobbleCompletedCheckpointStore(
                delegate.createRecoveredCompletedCheckpointStore(
                        jobId,
                        maxNumberOfCheckpointsToRetain,
                        sharedStateRegistryFactory,
                        ioExecutor,
                        restoreMode));
    }

    @Override
    public CheckpointIDCounter createCheckpointIDCounter(JobID jobId) throws Exception {
        return delegate.createCheckpointIDCounter(jobId);
    }
}
