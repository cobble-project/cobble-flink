package io.cobble.flink.state;

import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestContainer;

import java.util.ArrayList;
import java.util.List;

/** Mutable request batch handed from Flink's async execution controller to Cobble. */
final class CobbleStateRequestContainer implements StateRequestContainer {
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
