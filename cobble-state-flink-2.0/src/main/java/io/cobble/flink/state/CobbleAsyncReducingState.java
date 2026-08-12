package io.cobble.flink.state;

import io.cobble.flink.common.CobbleStateDescriptor;
import io.cobble.structured.Db;

import org.apache.flink.api.common.state.v2.ReducingState;
import org.apache.flink.api.common.state.v2.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.AbstractReducingState;

/** Cobble-backed Flink 2.0 asynchronous reducing state. */
final class CobbleAsyncReducingState<K, N, V> extends AbstractReducingState<K, N, V>
        implements ReducingState<V>, CobbleAsyncRequestState {

    private final ReducingAccess<K, N, V> access;

    CobbleAsyncReducingState(
            StateRequestHandler requestHandler,
            Db db,
            CobbleStateDescriptor stateDescriptor,
            TypeSerializer<K> keySerializer,
            N defaultNamespace,
            TypeSerializer<N> namespaceSerializer,
            ReducingStateDescriptor<V> descriptor) {
        super(requestHandler, descriptor);
        this.access =
                new ReducingAccess<>(
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

    private static final class ReducingAccess<K, N, V> extends CobbleAsyncState<K, N, V> {
        private ReducingAccess(
                Db db,
                CobbleStateDescriptor stateDescriptor,
                TypeSerializer<K> keySerializer,
                N defaultNamespace,
                TypeSerializer<N> namespaceSerializer,
                ReducingStateDescriptor<V> descriptor) {
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
            return request.getRequestType() == StateRequestType.REDUCING_GET;
        }

        @Override
        Object execute(StateRequest<?, ?, ?, ?> request) throws Exception {
            int bucket = bucket(request);
            byte[] key = rowKey(request);
            switch (request.getRequestType()) {
                case REDUCING_GET:
                    byte[] stored = readBytes(bucket, key);
                    return stored == null ? null : deserializeValue(valueSerializer(), stored);
                case REDUCING_ADD:
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
                            "Unsupported reducing-state request: " + request.getRequestType());
            }
        }
    }
}
