package io.cobble.flink.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Delegating HA services that only wraps checkpoint recovery with Cobble global snapshot logic. */
final class CobbleHighAvailabilityServices implements HighAvailabilityServices {

    private final HighAvailabilityServices delegate;

    CobbleHighAvailabilityServices(HighAvailabilityServices delegate) {
        this.delegate = delegate;
    }

    @Override
    public LeaderRetrievalService getResourceManagerLeaderRetriever() {
        return delegate.getResourceManagerLeaderRetriever();
    }

    @Override
    public LeaderRetrievalService getDispatcherLeaderRetriever() {
        return delegate.getDispatcherLeaderRetriever();
    }

    @Deprecated
    @Override
    public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
        return delegate.getJobManagerLeaderRetriever(jobID);
    }

    @Override
    public LeaderRetrievalService getJobManagerLeaderRetriever(
            JobID jobID, String defaultJobManagerAddress) {
        return delegate.getJobManagerLeaderRetriever(jobID, defaultJobManagerAddress);
    }

    @Override
    public LeaderElectionService getResourceManagerLeaderElectionService() {
        return delegate.getResourceManagerLeaderElectionService();
    }

    @Override
    public LeaderElectionService getDispatcherLeaderElectionService() {
        return delegate.getDispatcherLeaderElectionService();
    }

    @Override
    public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
        return delegate.getJobManagerLeaderElectionService(jobID);
    }

    @Override
    public CheckpointRecoveryFactory getCheckpointRecoveryFactory() throws Exception {
        return new CobbleCheckpointRecoveryFactory(delegate.getCheckpointRecoveryFactory());
    }

    @Override
    public JobGraphStore getJobGraphStore() throws Exception {
        return delegate.getJobGraphStore();
    }

    @Override
    public JobResultStore getJobResultStore() throws Exception {
        return delegate.getJobResultStore();
    }

    @Override
    public BlobStore createBlobStore() throws IOException {
        return delegate.createBlobStore();
    }

    @Override
    public LeaderElectionService getClusterRestEndpointLeaderElectionService() {
        return delegate.getClusterRestEndpointLeaderElectionService();
    }

    @Override
    public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
        return delegate.getClusterRestEndpointLeaderRetriever();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    @Override
    public void closeAndCleanupAllData() throws Exception {
        delegate.closeAndCleanupAllData();
    }

    @Override
    public CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor executor) {
        return delegate.globalCleanupAsync(jobId, executor);
    }
}
