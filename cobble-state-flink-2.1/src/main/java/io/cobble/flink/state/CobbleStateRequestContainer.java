package io.cobble.flink.state;

import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequest;

import java.util.ArrayList;
import java.util.List;

/** Mutable request batch handed from Flink's async execution controller to Cobble. */
final class CobbleStateRequestContainer implements AsyncRequestContainer<StateRequest<?, ?, ?, ?>> {
    private final List<StateRequest<?, ?, ?, ?>> requests = new ArrayList<>();

    @Override
    public void offer(StateRequest<?, ?, ?, ?> stateRequest) {
        requests.add(stateRequest);
    }

    @Override
    public boolean isEmpty() {
        return requests.isEmpty();
    }

    List<StateRequest<?, ?, ?, ?>> requests() {
        return requests;
    }
}
