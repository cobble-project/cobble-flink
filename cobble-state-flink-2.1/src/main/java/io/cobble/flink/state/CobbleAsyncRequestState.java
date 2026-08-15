package io.cobble.flink.state;

import org.apache.flink.runtime.asyncprocessing.StateRequest;

import java.util.List;

/** Backend-facing request execution contract implemented by every asynchronous state wrapper. */
interface CobbleAsyncRequestState {
    boolean isReadRequest(StateRequest<?, ?, ?, ?> request);

    Object execute(StateRequest<?, ?, ?, ?> request) throws Exception;

    default Object[] executeBatch(List<StateRequest<?, ?, ?, ?>> requests) throws Exception {
        Object[] results = new Object[requests.size()];
        for (int i = 0; i < requests.size(); i++) {
            results[i] = execute(requests.get(i));
        }
        return results;
    }

    void close();
}
