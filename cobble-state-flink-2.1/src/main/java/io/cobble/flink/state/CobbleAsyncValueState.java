package io.cobble.flink.state;

import io.cobble.flink.common.CobbleStateDescriptor;
import io.cobble.structured.Db;

import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.AbstractValueState;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Cobble-backed Flink 2.1 asynchronous value state. */
final class CobbleAsyncValueState<K, N, V> extends AbstractValueState<K, N, V>
        implements ValueState<V>, CobbleAsyncRequestState {

    private final CobbleAsyncState<K, N, V> access;

    CobbleAsyncValueState(
            StateRequestHandler requestHandler,
            Db db,
            CobbleStateDescriptor stateDescriptor,
            TypeSerializer<K> keySerializer,
            N defaultNamespace,
            TypeSerializer<N> namespaceSerializer,
            ValueStateDescriptor<V> descriptor) {
        super(requestHandler, descriptor.getSerializer());
        this.access =
                new ValueAccess<>(
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
    public Object[] executeBatch(List<StateRequest<?, ?, ?, ?>> requests) throws Exception {
        return access.executeBatch(requests);
    }

    @Override
    public void close() {
        access.close();
    }

    private static final class ValueAccess<K, N, V> extends CobbleAsyncState<K, N, V> {
        private ValueAccess(
                Db db,
                CobbleStateDescriptor stateDescriptor,
                TypeSerializer<K> keySerializer,
                N defaultNamespace,
                TypeSerializer<N> namespaceSerializer,
                ValueStateDescriptor<V> descriptor) {
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
            return request.getRequestType() == StateRequestType.VALUE_GET;
        }

        @Override
        Object execute(StateRequest<?, ?, ?, ?> request) throws Exception {
            int bucket = bucket(request);
            byte[] key = rowKey(request);
            switch (request.getRequestType()) {
                case VALUE_GET:
                    byte[] stored = readBytes(bucket, key);
                    return stored == null ? null : deserializeValue(valueSerializer(), stored);
                case VALUE_UPDATE:
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
                            "Unsupported value-state request: " + request.getRequestType());
            }
        }

        @Override
        Object[] executeBatch(List<StateRequest<?, ?, ?, ?>> requests) throws Exception {
            if (requests.isEmpty()) {
                return new Object[0];
            }
            StateRequestType requestType = requests.get(0).getRequestType();
            if (requestType == StateRequestType.VALUE_GET) {
                return executeGets(requests);
            }
            if (allNonNullUpdates(requests)) {
                executeUpdates(requests);
                return new Object[requests.size()];
            }
            return super.executeBatch(requests);
        }

        private Object[] executeGets(List<StateRequest<?, ?, ?, ?>> requests) throws Exception {
            int[] buckets = new int[requests.size()];
            byte[][] keys = new byte[requests.size()][];
            for (int i = 0; i < requests.size(); i++) {
                StateRequest<?, ?, ?, ?> request = requests.get(i);
                if (request.getRequestType() != StateRequestType.VALUE_GET) {
                    return super.executeBatch(requests);
                }
                buckets[i] = bucket(request);
                keys[i] = rowKey(request);
            }

            return readValuesBatch(buckets, keys, valueSerializer());
        }

        private boolean allNonNullUpdates(List<StateRequest<?, ?, ?, ?>> requests) {
            for (StateRequest<?, ?, ?, ?> request : requests) {
                if (request.getRequestType() != StateRequestType.VALUE_UPDATE
                        || request.getPayload() == null) {
                    return false;
                }
            }
            return true;
        }

        private void executeUpdates(List<StateRequest<?, ?, ?, ?>> requests) throws Exception {
            Map<Integer, BucketWriteBatch<V>> batches = new LinkedHashMap<>();
            TypeSerializer<V> serializer = valueSerializer();
            for (StateRequest<?, ?, ?, ?> request : requests) {
                BucketWriteBatch<V> batch =
                        batches.computeIfAbsent(
                                bucket(request), ignored -> new BucketWriteBatch<>());
                batch.keys.add(rowKey(request));
                @SuppressWarnings("unchecked")
                V value = (V) request.getPayload();
                batch.values.add(value);
            }
            for (Map.Entry<Integer, BucketWriteBatch<V>> entry : batches.entrySet()) {
                BucketWriteBatch<V> batch = entry.getValue();
                putValuesBatch(entry.getKey(), batch.keys, batch.values, serializer);
            }
        }

        private static final class BucketWriteBatch<V> {
            private final List<byte[]> keys = new ArrayList<>();
            private final List<V> values = new ArrayList<>();
        }
    }
}
