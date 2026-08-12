package io.cobble.flink.state;

import io.cobble.flink.common.CobbleStateDescriptor;
import io.cobble.structured.Db;

import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.AbstractListState;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;

/** Cobble-backed Flink 2.0 asynchronous list state. */
final class CobbleAsyncListState<K, N, V> extends AbstractListState<K, N, V>
        implements ListState<V>, CobbleAsyncRequestState {

    private final ListAccess<K, N, V> access;

    CobbleAsyncListState(
            StateRequestHandler requestHandler,
            Db db,
            CobbleStateDescriptor stateDescriptor,
            TypeSerializer<K> keySerializer,
            N defaultNamespace,
            TypeSerializer<N> namespaceSerializer,
            ListStateDescriptor<V> descriptor) {
        super(requestHandler, descriptor);
        this.access =
                new ListAccess<>(
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
        Object result = access.execute(request);
        if (request.getRequestType() == StateRequestType.LIST_GET) {
            @SuppressWarnings("unchecked")
            List<V> values = (List<V>) result;
            return new CobbleCompleteStateIterator<>(
                    this, request.getRequestType(), getStateRequestHandler(), values);
        }
        return result;
    }

    @Override
    public void close() {
        access.close();
    }

    private static final class ListAccess<K, N, V> extends CobbleAsyncState<K, N, V> {
        private final ThreadLocal<DirectDelimitedListSerializer> listDecoders;

        private ListAccess(
                Db db,
                CobbleStateDescriptor stateDescriptor,
                TypeSerializer<K> keySerializer,
                N defaultNamespace,
                TypeSerializer<N> namespaceSerializer,
                ListStateDescriptor<V> descriptor) {
            super(
                    db,
                    stateDescriptor,
                    keySerializer,
                    defaultNamespace,
                    namespaceSerializer,
                    descriptor.getSerializer(),
                    descriptor.getTtlConfig());
            this.listDecoders =
                    ThreadLocal.withInitial(() -> new DirectDelimitedListSerializer(256));
        }

        @Override
        boolean isReadRequest(StateRequest<?, ?, ?, ?> request) {
            return request.getRequestType() == StateRequestType.LIST_GET;
        }

        @Override
        Object execute(StateRequest<?, ?, ?, ?> request) throws Exception {
            int bucket = bucket(request);
            byte[] key = rowKey(request);
            switch (request.getRequestType()) {
                case LIST_GET:
                    byte[] stored = readBytes(bucket, key);
                    List<V> values =
                            stored == null
                                    ? Collections.emptyList()
                                    : listDecoders
                                            .get()
                                            .decode(
                                                    valueSerializer(),
                                                    new ByteArrayInputStream(stored));
                    return values;
                case LIST_ADD:
                    Preconditions.checkNotNull(
                            request.getPayload(), "You cannot add null to a ListState.");
                    mergeBytes(
                            bucket,
                            key,
                            encodeValues(Collections.singletonList(request.getPayload())));
                    return null;
                case LIST_ADD_ALL:
                    @SuppressWarnings("unchecked")
                    List<V> additions = (List<V>) request.getPayload();
                    Preconditions.checkNotNull(
                            additions, "ListState addAll value must not be null.");
                    if (!additions.isEmpty()) {
                        mergeBytes(bucket, key, encodeValues(additions));
                    }
                    return null;
                case LIST_UPDATE:
                    @SuppressWarnings("unchecked")
                    List<V> replacement = (List<V>) request.getPayload();
                    if (replacement == null || replacement.isEmpty()) {
                        delete(bucket, key);
                    } else {
                        putBytes(bucket, key, encodeValues(replacement));
                    }
                    return null;
                case CLEAR:
                    delete(bucket, key);
                    return null;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported list-state request: " + request.getRequestType());
            }
        }

        private byte[] encodeValues(List<?> values) throws Exception {
            DataOutputSerializer output = new DataOutputSerializer(128);
            @SuppressWarnings("unchecked")
            TypeSerializer<Object> serializer = (TypeSerializer<Object>) valueSerializer();
            for (Object value : values) {
                Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
                serializer.serialize(value, output);
                output.writeByte(DirectDelimitedListSerializer.DELIMITER);
            }
            return output.getCopyOfBuffer();
        }
    }
}
