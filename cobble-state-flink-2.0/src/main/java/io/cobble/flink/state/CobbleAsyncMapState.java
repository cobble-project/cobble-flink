package io.cobble.flink.state;

import io.cobble.flink.common.CobbleStateDescriptor;
import io.cobble.structured.Db;
import io.cobble.structured.Row;
import io.cobble.structured.ScanCursor;

import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.AbstractMapState;
import org.apache.flink.util.Preconditions;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Cobble-backed Flink 2.0 asynchronous map state. */
final class CobbleAsyncMapState<K, N, UK, UV> extends AbstractMapState<K, N, UK, UV>
        implements MapState<UK, UV>, CobbleAsyncRequestState {

    private static final int ITERATION_BATCH_SIZE = 128;

    private final MapAccess<K, N, UK, UV> access;

    CobbleAsyncMapState(
            StateRequestHandler requestHandler,
            Db db,
            CobbleStateDescriptor stateDescriptor,
            TypeSerializer<K> keySerializer,
            N defaultNamespace,
            TypeSerializer<N> namespaceSerializer,
            MapStateDescriptor<UK, UV> descriptor) {
        super(requestHandler, descriptor);
        this.access =
                new MapAccess<>(
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
        MapScanResult result;
        switch (request.getRequestType()) {
            case MAP_ITER:
            case MAP_ITER_KEY:
            case MAP_ITER_VALUE:
                result = access.scan(request, request.getRequestType(), null);
                break;
            case ITERATOR_LOADING:
                CobbleMapStateIterator.Continuation continuation =
                        (CobbleMapStateIterator.Continuation) request.getPayload();
                result =
                        access.scan(
                                request,
                                continuation.projection(),
                                continuation.lastEmittedRowKey());
                break;
            default:
                return access.execute(request);
        }
        return new CobbleMapStateIterator<>(
                this,
                result.projection,
                getStateRequestHandler(),
                result.values,
                result.lastEmittedRowKey,
                result.hasMore);
    }

    @Override
    public void close() {
        access.close();
    }

    private static final class MapAccess<K, N, UK, UV> extends CobbleAsyncState<K, N, UV> {
        private final ThreadLocal<TypeSerializer<UK>> userKeySerializers;
        private final ThreadLocal<TypeSerializer<UV>> mapValueSerializers;
        private final int userKeyFixedLength;
        private final int trailerBytes;

        private MapAccess(
                Db db,
                CobbleStateDescriptor stateDescriptor,
                TypeSerializer<K> keySerializer,
                N defaultNamespace,
                TypeSerializer<N> namespaceSerializer,
                MapStateDescriptor<UK, UV> descriptor) {
            super(
                    db,
                    stateDescriptor,
                    keySerializer,
                    defaultNamespace,
                    namespaceSerializer,
                    descriptor.getSerializer(),
                    descriptor.getTtlConfig());
            TypeSerializer<UK> userKeySerializer = descriptor.getUserKeySerializer();
            TypeSerializer<UV> userValueSerializer = descriptor.getSerializer();
            this.userKeySerializers = ThreadLocal.withInitial(userKeySerializer::duplicate);
            this.mapValueSerializers =
                    ThreadLocal.withInitial(
                            () -> MapValueCodec.adapterFor(userValueSerializer.duplicate()));
            this.userKeyFixedLength = CobbleStateKeySerializer.maybeFixedLength(userKeySerializer);
            int keyLength = CobbleStateKeySerializer.maybeFixedLength(keySerializer);
            int namespaceLength = CobbleStateKeySerializer.maybeFixedLength(namespaceSerializer);
            boolean keyLengthStored =
                    CobbleStateKeySerializer.shouldStoreMapKeyLength(
                            keyLength, namespaceLength, userKeyFixedLength);
            boolean namespaceLengthStored =
                    CobbleStateKeySerializer.shouldStoreMapNamespaceLength(
                            keyLength, namespaceLength, userKeyFixedLength);
            this.trailerBytes =
                    (keyLengthStored ? Integer.BYTES : 0)
                            + (namespaceLengthStored ? Integer.BYTES : 0);
        }

        @Override
        boolean isReadRequest(StateRequest<?, ?, ?, ?> request) {
            switch (request.getRequestType()) {
                case MAP_GET:
                case MAP_CONTAINS:
                case MAP_IS_EMPTY:
                case MAP_ITER:
                case MAP_ITER_KEY:
                case MAP_ITER_VALUE:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        Object execute(StateRequest<?, ?, ?, ?> request) throws Exception {
            switch (request.getRequestType()) {
                case MAP_GET:
                    return get(request, userKey(request.getPayload()));
                case MAP_CONTAINS:
                    return containsRow(
                            bucket(request),
                            mapEntryRowKey(
                                    request, userKeySerializer(), userKey(request.getPayload())));
                case MAP_PUT:
                    @SuppressWarnings("unchecked")
                    Tuple2<UK, UV> entry = (Tuple2<UK, UV>) request.getPayload();
                    Preconditions.checkNotNull(entry, "MapState put payload must not be null.");
                    put(
                            request,
                            Preconditions.checkNotNull(
                                    entry.f0, "MapState user key must not be null."),
                            entry.f1);
                    return null;
                case MAP_PUT_ALL:
                    @SuppressWarnings("unchecked")
                    Map<UK, UV> entries = (Map<UK, UV>) request.getPayload();
                    Preconditions.checkNotNull(entries, "MapState putAll value must not be null.");
                    List<byte[]> keys = new ArrayList<>(entries.size());
                    List<UV> values = new ArrayList<>(entries.size());
                    for (Map.Entry<UK, UV> mapEntry : entries.entrySet()) {
                        UK userKey =
                                Preconditions.checkNotNull(
                                        mapEntry.getKey(), "MapState user key must not be null.");
                        keys.add(mapEntryRowKey(request, userKeySerializer(), userKey));
                        values.add(mapEntry.getValue());
                    }
                    putValuesBatch(bucket(request), keys, values, mapValueSerializer());
                    return null;
                case MAP_REMOVE:
                    delete(
                            bucket(request),
                            mapEntryRowKey(
                                    request, userKeySerializer(), userKey(request.getPayload())));
                    return null;
                case MAP_IS_EMPTY:
                    return !hasAnyEntry(request);
                case MAP_ITER:
                case MAP_ITER_KEY:
                case MAP_ITER_VALUE:
                case ITERATOR_LOADING:
                    throw new IllegalStateException(
                            "Map iteration requests must be handled by the state wrapper.");
                case CLEAR:
                    clearMap(request);
                    return null;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported map-state request: " + request.getRequestType());
            }
        }

        private UV get(StateRequest<?, ?, ?, ?> request, UK userKey) throws Exception {
            byte[] stored =
                    readBytes(
                            bucket(request), mapEntryRowKey(request, userKeySerializer(), userKey));
            return stored == null ? null : decodeMapValue(stored);
        }

        private void put(StateRequest<?, ?, ?, ?> request, UK userKey, UV value) throws Exception {
            putBytes(
                    bucket(request),
                    mapEntryRowKey(request, userKeySerializer(), userKey),
                    encodeMapValue(value));
        }

        private MapScanResult scan(
                StateRequest<?, ?, ?, ?> request,
                StateRequestType projection,
                byte[] lastEmittedRowKey)
                throws Exception {
            byte[] prefix = mapPrefix(request);
            byte[] prefixStart = append(prefix, (byte) 0x00);
            byte[] start = lastEmittedRowKey == null ? prefixStart : lastEmittedRowKey;
            byte[] end = append(prefix, (byte) 0x01);
            List<Object> result = new ArrayList<>();
            byte[] lastKey = null;
            boolean hasMore = false;
            try (ScanCursor cursor = db.scanWithOptions(bucket(request), start, end, scanOptions)) {
                for (Row row : cursor) {
                    byte[] rowKey = row.getKey();
                    if (!startsWith(rowKey, prefixStart)) {
                        break;
                    }
                    if (lastEmittedRowKey != null && Arrays.equals(rowKey, lastEmittedRowKey)) {
                        continue;
                    }
                    if (result.size() == ITERATION_BATCH_SIZE) {
                        hasMore = true;
                        break;
                    }
                    UK userKey = decodeUserKey(rowKey, prefix.length);
                    switch (projection) {
                        case MAP_ITER_KEY:
                            result.add(userKey);
                            break;
                        case MAP_ITER_VALUE:
                            result.add(decodeMapValue(row.getBytes(STATE_COLUMN_INDEX)));
                            break;
                        case MAP_ITER:
                            result.add(
                                    new AbstractMap.SimpleImmutableEntry<>(
                                            userKey,
                                            decodeMapValue(row.getBytes(STATE_COLUMN_INDEX))));
                            break;
                        default:
                            throw new IllegalArgumentException(
                                    "Unsupported map projection: " + projection);
                    }
                    lastKey = Arrays.copyOf(rowKey, rowKey.length);
                }
            }
            return new MapScanResult(projection, result, lastKey, hasMore);
        }

        private boolean hasAnyEntry(StateRequest<?, ?, ?, ?> request) throws Exception {
            byte[] prefix = mapPrefix(request);
            byte[] start = append(prefix, (byte) 0x00);
            byte[] end = append(prefix, (byte) 0x01);
            try (ScanCursor cursor = db.scanWithOptions(bucket(request), start, end, scanOptions)) {
                for (Row row : cursor) {
                    return startsWith(row.getKey(), start);
                }
            }
            return false;
        }

        private void clearMap(StateRequest<?, ?, ?, ?> request) throws Exception {
            byte[] prefix = mapPrefix(request);
            byte[] start = append(prefix, (byte) 0x00);
            byte[] end = append(prefix, (byte) 0x01);
            List<byte[]> keys = new ArrayList<>();
            try (ScanCursor cursor = db.scanWithOptions(bucket(request), start, end, scanOptions)) {
                for (Row row : cursor) {
                    if (startsWith(row.getKey(), start)) {
                        keys.add(Arrays.copyOf(row.getKey(), row.getKey().length));
                    }
                }
            }
            for (byte[] key : keys) {
                delete(bucket(request), key);
            }
        }

        private byte[] encodeMapValue(UV value) throws Exception {
            return serializeValue(mapValueSerializer(), value);
        }

        private UV decodeMapValue(byte[] bytes) throws Exception {
            if (bytes == null || bytes.length == 0) {
                throw new java.io.IOException("MapState row value is empty.");
            }
            return deserializeValue(mapValueSerializer(), bytes);
        }

        private UK decodeUserKey(byte[] rowKey, int prefixLength) throws Exception {
            int userKeyLength = rowKey.length - prefixLength - 1 - trailerBytes;
            if (userKeyLength < 0
                    || (userKeyFixedLength >= 0 && userKeyLength != userKeyFixedLength)) {
                throw new java.io.IOException("Corrupted Cobble MapState row key.");
            }
            DataInputDeserializer input = new DataInputDeserializer();
            input.setBuffer(rowKey, prefixLength + 1, userKeyLength);
            return userKeySerializer().deserialize(input);
        }

        @Override
        public void close() {
            userKeySerializers.remove();
            mapValueSerializers.remove();
            super.close();
        }

        private TypeSerializer<UK> userKeySerializer() {
            return userKeySerializers.get();
        }

        private TypeSerializer<UV> mapValueSerializer() {
            return mapValueSerializers.get();
        }

        @SuppressWarnings("unchecked")
        private UK userKey(Object value) {
            return Preconditions.checkNotNull((UK) value, "MapState user key must not be null.");
        }

        private static byte[] append(byte[] bytes, byte suffix) {
            byte[] result = Arrays.copyOf(bytes, bytes.length + 1);
            result[bytes.length] = suffix;
            return result;
        }

        private static boolean startsWith(byte[] bytes, byte[] prefix) {
            if (bytes.length < prefix.length) {
                return false;
            }
            for (int i = 0; i < prefix.length; i++) {
                if (bytes[i] != prefix[i]) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final class MapScanResult {
        private final StateRequestType projection;
        private final List<Object> values;
        private final byte[] lastEmittedRowKey;
        private final boolean hasMore;

        private MapScanResult(
                StateRequestType projection,
                List<Object> values,
                byte[] lastEmittedRowKey,
                boolean hasMore) {
            this.projection = projection;
            this.values = values;
            this.lastEmittedRowKey = lastEmittedRowKey;
            this.hasMore = hasMore;
        }
    }
}
