package io.cobble.flink.state;

import io.cobble.flink.common.CobbleStateDescriptor;
import io.cobble.structured.Db;

import org.apache.flink.api.common.state.v2.AggregatingState;
import org.apache.flink.api.common.state.v2.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.AbstractAggregatingState;

/** Cobble-backed Flink 2.0 asynchronous aggregating state. */
final class CobbleAsyncAggregatingState<K, N, IN, ACC, OUT>
        extends AbstractAggregatingState<K, N, IN, ACC, OUT>
        implements AggregatingState<IN, OUT>, CobbleAsyncRequestState {

    private final AggregatingAccess<K, N, ACC> access;

    CobbleAsyncAggregatingState(
            StateRequestHandler requestHandler,
            Db db,
            CobbleStateDescriptor stateDescriptor,
            TypeSerializer<K> keySerializer,
            N defaultNamespace,
            TypeSerializer<N> namespaceSerializer,
            AggregatingStateDescriptor<IN, ACC, OUT> descriptor) {
        super(requestHandler, descriptor);
        this.access =
                new AggregatingAccess<>(
                        db,
                        stateDescriptor,
                        keySerializer,
                        defaultNamespace,
                        namespaceSerializer,
                        descriptor);
    }

    @Override
    public boolean isReadRequest(StateRequest<?, ?, ?, ?> request) {
        return access.isReadRequest(request);
    }

    @Override
    public Object execute(StateRequest<?, ?, ?, ?> request) throws Exception {
        return access.execute(request);
    }

    @Override
    public void close() {
        access.close();
    }

    private static final class AggregatingAccess<K, N, ACC> extends CobbleAsyncState<K, N, ACC> {
        private AggregatingAccess(
                Db db,
                CobbleStateDescriptor stateDescriptor,
                TypeSerializer<K> keySerializer,
                N defaultNamespace,
                TypeSerializer<N> namespaceSerializer,
                AggregatingStateDescriptor<?, ACC, ?> descriptor) {
            super(
                    db,
                    stateDescriptor,
                    keySerializer,
                    defaultNamespace,
                    namespaceSerializer,
                    descriptor.getSerializer(),
                    descriptor.getTtlConfig());
        }

        @Override
        boolean isReadRequest(StateRequest<?, ?, ?, ?> request) {
            return request.getRequestType() == StateRequestType.AGGREGATING_GET;
        }

        @Override
        Object execute(StateRequest<?, ?, ?, ?> request) throws Exception {
            int bucket = bucket(request);
            byte[] key = rowKey(request);
            switch (request.getRequestType()) {
                case AGGREGATING_GET:
                    byte[] stored = readBytes(bucket, key);
                    return stored == null ? null : deserializeValue(valueSerializer(), stored);
                case AGGREGATING_ADD:
                    if (request.getPayload() == null) {
                        delete(bucket, key);
                    } else {
                        putBytes(
                                bucket,
                                key,
                                serializeValue(valueSerializer(), request.getPayload()));
                    }
                    return null;
                case CLEAR:
                    delete(bucket, key);
                    return null;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported aggregating-state request: " + request.getRequestType());
            }
        }
    }
}
