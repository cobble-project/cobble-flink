package io.cobble.flink.state;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.runtime.asyncprocessing.AbstractStateIterator;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import java.util.Collection;

/** A partial MapState result that reopens the native scan after its last emitted row. */
final class CobbleMapStateIterator<T> extends AbstractStateIterator<T> {
    private final StateRequestType projection;
    private final byte[] lastEmittedRowKey;
    private final boolean hasMore;

    CobbleMapStateIterator(
            State originalState,
            StateRequestType projection,
            StateRequestHandler stateHandler,
            Collection<T> values,
            byte[] lastEmittedRowKey,
            boolean hasMore) {
        super(originalState, projection, stateHandler, values);
        this.projection = projection;
        this.lastEmittedRowKey = lastEmittedRowKey;
        this.hasMore = hasMore;
    }

    @Override
    public boolean hasNextLoading() {
        return hasMore;
    }

    @Override
    protected Object nextPayloadForContinuousLoading() {
        return new Continuation(projection, lastEmittedRowKey);
    }

    static final class Continuation {
        private final StateRequestType projection;
        private final byte[] lastEmittedRowKey;

        private Continuation(StateRequestType projection, byte[] lastEmittedRowKey) {
            this.projection = projection;
            this.lastEmittedRowKey = lastEmittedRowKey;
        }

        StateRequestType projection() {
            return projection;
        }

        byte[] lastEmittedRowKey() {
            return lastEmittedRowKey;
        }
    }
}
