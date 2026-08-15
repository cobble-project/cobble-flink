package io.cobble.flink.state;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.runtime.asyncprocessing.AbstractStateIterator;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import java.util.Collection;

/** Fully loaded iterator that supports both Flink v2's synchronous and asynchronous APIs. */
final class CobbleCompleteStateIterator<T> extends AbstractStateIterator<T> {

    CobbleCompleteStateIterator(
            State originalState,
            StateRequestType requestType,
            StateRequestHandler stateHandler,
            Collection<T> values) {
        super(originalState, requestType, stateHandler, values);
    }

    @Override
    public boolean hasNextLoading() {
        return false;
    }

    @Override
    protected Object nextPayloadForContinuousLoading() {
        return null;
    }
}
