package io.cobble.flink.state;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Executes Flink 2.0 state request batches outside the task thread. */
final class CobbleStateExecutor implements StateExecutor {

    private final ExecutorService coordinator;
    private final ExecutorService reads;
    private final ExecutorService writes;
    private final int readParallelism;
    private final int loadLimit;
    private final AtomicInteger ongoingRequests;
    private final AtomicBoolean shutdown;
    private volatile Throwable executionFailure;

    CobbleStateExecutor(int readParallelism, int writeParallelism) {
        Preconditions.checkArgument(readParallelism > 0, "readParallelism must be positive");
        Preconditions.checkArgument(writeParallelism > 0, "writeParallelism must be positive");
        this.coordinator =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("Cobble-StateExecutor-Coordinator"));
        this.reads =
                Executors.newFixedThreadPool(
                        readParallelism, new ExecutorThreadFactory("Cobble-StateExecutor-Read"));
        this.writes =
                Executors.newFixedThreadPool(
                        writeParallelism, new ExecutorThreadFactory("Cobble-StateExecutor-Write"));
        this.readParallelism = readParallelism;
        this.loadLimit = Math.max(1, (readParallelism + writeParallelism) * 2);
        this.ongoingRequests = new AtomicInteger();
        this.shutdown = new AtomicBoolean(false);
    }

    @Override
    public CompletableFuture<Void> executeBatchRequests(StateRequestContainer container) {
        checkRunning();
        Preconditions.checkArgument(container instanceof CobbleStateRequestContainer);
        List<StateRequest<?, ?, ?, ?>> requests =
                new ArrayList<>(((CobbleStateRequestContainer) container).requests());
        if (requests.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        List<RequestBatch> batches = createBatches(requests);
        ongoingRequests.addAndGet(batches.size());
        CompletableFuture<Void> batchFuture = new CompletableFuture<>();
        coordinator.execute(
                () -> {
                    List<CompletableFuture<Void>> operations = new ArrayList<>(batches.size());
                    for (RequestBatch batch : batches) {
                        ExecutorService executor = batch.read ? reads : writes;
                        operations.add(
                                CompletableFuture.runAsync(
                                        () -> executeAndComplete(batch), executor));
                    }
                    CompletableFuture.allOf(operations.toArray(new CompletableFuture<?>[0]))
                            .whenComplete(
                                    (ignored, error) -> {
                                        ongoingRequests.addAndGet(-batches.size());
                                        if (error == null) {
                                            batchFuture.complete(null);
                                        } else {
                                            executionFailure = unwrap(error);
                                            batchFuture.completeExceptionally(executionFailure);
                                        }
                                    });
                });
        return batchFuture;
    }

    @Override
    public StateRequestContainer createStateRequestContainer() {
        checkRunning();
        return new CobbleStateRequestContainer();
    }

    @Override
    public void executeRequestSync(StateRequest<?, ?, ?, ?> stateRequest) {
        checkRunning();
        executeAndComplete(stateRequest);
        checkRunning();
    }

    @Override
    public boolean fullyLoaded() {
        return ongoingRequests.get() >= loadLimit;
    }

    @Override
    public void shutdown() {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }
        shutdownAndAwait(coordinator);
        shutdownAndAwait(reads);
        shutdownAndAwait(writes);
    }

    private List<RequestBatch> createBatches(List<StateRequest<?, ?, ?, ?>> requests) {
        Map<CobbleAsyncRequestState, List<StateRequest<?, ?, ?, ?>>> readGroups =
                new IdentityHashMap<>();
        Map<CobbleAsyncRequestState, List<StateRequest<?, ?, ?, ?>>> writeGroups =
                new IdentityHashMap<>();
        for (StateRequest<?, ?, ?, ?> request : requests) {
            if (request.getRequestType() == StateRequestType.SYNC_POINT) {
                completeRequest(request, null);
                continue;
            }
            CobbleAsyncRequestState state = requestState(request);
            Map<CobbleAsyncRequestState, List<StateRequest<?, ?, ?, ?>>> groups =
                    state.isReadRequest(request) ? readGroups : writeGroups;
            groups.computeIfAbsent(state, ignored -> new ArrayList<>()).add(request);
        }

        List<RequestBatch> batches = new ArrayList<>();
        for (Map.Entry<CobbleAsyncRequestState, List<StateRequest<?, ?, ?, ?>>> entry :
                readGroups.entrySet()) {
            addReadBatches(batches, entry.getKey(), entry.getValue());
        }
        for (Map.Entry<CobbleAsyncRequestState, List<StateRequest<?, ?, ?, ?>>> entry :
                writeGroups.entrySet()) {
            batches.add(new RequestBatch(entry.getKey(), entry.getValue(), false));
        }
        return batches;
    }

    private void addReadBatches(
            List<RequestBatch> batches,
            CobbleAsyncRequestState state,
            List<StateRequest<?, ?, ?, ?>> requests) {
        int batchCount = Math.min(readParallelism, requests.size());
        int batchSize = (requests.size() + batchCount - 1) / batchCount;
        for (int start = 0; start < requests.size(); start += batchSize) {
            int end = Math.min(requests.size(), start + batchSize);
            batches.add(new RequestBatch(state, requests.subList(start, end), true));
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void executeAndComplete(StateRequest request) {
        try {
            Object result =
                    request.getRequestType() == StateRequestType.SYNC_POINT
                            ? null
                            : requestState(request).execute(request);
            request.getFuture().complete(result);
        } catch (Throwable error) {
            request.getFuture().completeExceptionally("Cobble state request failed", error);
            throw new CobbleStateRequestException(error);
        }
    }

    private void executeAndComplete(RequestBatch batch) {
        try {
            Object[] results = batch.state.executeBatch(batch.requests);
            if (results.length != batch.requests.size()) {
                throw new IllegalStateException(
                        "Cobble state batch returned "
                                + results.length
                                + " results for "
                                + batch.requests.size()
                                + " requests.");
            }
            for (int i = 0; i < batch.requests.size(); i++) {
                completeRequest(batch.requests.get(i), results[i]);
            }
        } catch (Throwable error) {
            for (StateRequest<?, ?, ?, ?> request : batch.requests) {
                request.getFuture().completeExceptionally("Cobble state request failed", error);
            }
            throw new CobbleStateRequestException(error);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void completeRequest(StateRequest request, Object result) {
        request.getFuture().complete(result);
    }

    private CobbleAsyncRequestState requestState(StateRequest<?, ?, ?, ?> request) {
        State state = request.getState();
        if (!(state instanceof CobbleAsyncRequestState)) {
            throw new IllegalArgumentException(
                    "State request does not target a Cobble async state: "
                            + (state == null ? "null" : state.getClass().getName()));
        }
        return (CobbleAsyncRequestState) state;
    }

    private void checkRunning() {
        if (shutdown.get()) {
            throw new IllegalStateException("Cobble StateExecutor is shut down.");
        }
        if (executionFailure != null) {
            throw new IllegalStateException(
                    "A previous Cobble state request failed.", executionFailure);
        }
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof java.util.concurrent.CompletionException && error.getCause() != null
                ? error.getCause()
                : error;
    }

    private static void shutdownAndAwait(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static final class CobbleStateRequestException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private CobbleStateRequestException(Throwable cause) {
            super(cause);
        }
    }

    private static final class RequestBatch {
        private final CobbleAsyncRequestState state;
        private final List<StateRequest<?, ?, ?, ?>> requests;
        private final boolean read;

        private RequestBatch(
                CobbleAsyncRequestState state,
                List<StateRequest<?, ?, ?, ?>> requests,
                boolean read) {
            this.state = state;
            this.requests = requests;
            this.read = read;
        }
    }
}
