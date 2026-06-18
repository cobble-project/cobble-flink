package io.cobble.flink.common.inspect;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Persisted inspect metadata for one Flink keyed state, describing enough of the row-key and value
 * layout for the monitor to decode raw Cobble bytes back into Flink concepts.
 *
 * <p>Row-key layout rules mirror {@code CobbleStateKeySerializer} (kept in {@code cobble-state}):
 *
 * <ul>
 *   <li>For ValueState/ListState the row key is {@code [key][namespace][keyLength?]} where the
 *       trailing {@code keyLength} int is stored only when both key and namespace are
 *       variable-sized.
 *   <li>For MapState the row key is {@code [key][namespace][0x00][mapUserKey][keyLength?]
 *       [namespaceLength?]} where the trailer ints are stored based on how many of the three parts
 *       are variable-sized (only {@code variableParts - 1} lengths are persisted).
 * </ul>
 *
 * <p>These layout decisions are computed once at capture time and persisted as booleans so the
 * monitor can split row keys without re-implementing the serializer-length heuristics.
 *
 * <p>Binary layout written by {@link #write(DataOutputView)}:
 *
 * <ol>
 *   <li>UTF state name
 *   <li>state kind ordinal (see {@link StateKind})
 *   <li>UTF column family
 *   <li>boolean ttlEnabled
 *   <li>key+namespace layout booleans: keyFixedLength, namespaceFixedLength, keyLengthStored
 *   <li>map layout booleans: mapUserKeyFixedLength, mapKeyLengthStored, mapNamespaceLengthStored
 *   <li>serializer schemas: key, namespace, value (always); list element (LIST only); map user key
 *       and map user value (MAP only). Unwanted serializers are written as a {@code null} sentinel
 *       so the reader can reconstruct the right shape per kind.
 * </ol>
 */
public final class StateInspectSchema {

    private final String stateName;
    private final StateKind stateKind;
    private final String columnFamily;
    private final boolean ttlEnabled;
    // key + namespace layout (Value/List/Map)
    private final boolean keyFixedLength;
    private final boolean namespaceFixedLength;
    private final boolean keyLengthStored;
    // map-only layout
    private final boolean mapUserKeyFixedLength;
    private final boolean mapKeyLengthStored;
    private final boolean mapNamespaceLengthStored;
    // serializer schemas
    private final SerializerInspectSchema keySerializer;
    private final SerializerInspectSchema namespaceSerializer;
    private final SerializerInspectSchema valueSerializer;
    private final SerializerInspectSchema listElementSerializer;
    private final SerializerInspectSchema mapUserKeySerializer;
    private final SerializerInspectSchema mapUserValueSerializer;

    private StateInspectSchema(
            String stateName,
            StateKind stateKind,
            String columnFamily,
            boolean ttlEnabled,
            boolean keyFixedLength,
            boolean namespaceFixedLength,
            boolean keyLengthStored,
            boolean mapUserKeyFixedLength,
            boolean mapKeyLengthStored,
            boolean mapNamespaceLengthStored,
            SerializerInspectSchema keySerializer,
            SerializerInspectSchema namespaceSerializer,
            SerializerInspectSchema valueSerializer,
            SerializerInspectSchema listElementSerializer,
            SerializerInspectSchema mapUserKeySerializer,
            SerializerInspectSchema mapUserValueSerializer) {
        this.stateName = stateName;
        this.stateKind = stateKind;
        this.columnFamily = columnFamily;
        this.ttlEnabled = ttlEnabled;
        this.keyFixedLength = keyFixedLength;
        this.namespaceFixedLength = namespaceFixedLength;
        this.keyLengthStored = keyLengthStored;
        this.mapUserKeyFixedLength = mapUserKeyFixedLength;
        this.mapKeyLengthStored = mapKeyLengthStored;
        this.mapNamespaceLengthStored = mapNamespaceLengthStored;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
        this.listElementSerializer = listElementSerializer;
        this.mapUserKeySerializer = mapUserKeySerializer;
        this.mapUserValueSerializer = mapUserValueSerializer;
    }

    /** Captures a ValueState schema. */
    public static <K, N, V> StateInspectSchema forValue(
            String stateName,
            String columnFamily,
            boolean ttlEnabled,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer) {
        return new StateInspectSchema(
                stateName,
                StateKind.VALUE,
                columnFamily,
                ttlEnabled,
                keySerializer,
                namespaceSerializer,
                valueSerializer,
                null,
                null,
                null);
    }

    /** Captures a ListState schema. */
    public static <K, N, E> StateInspectSchema forList(
            String stateName,
            String columnFamily,
            boolean ttlEnabled,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<E> elementSerializer) {
        return new StateInspectSchema(
                stateName,
                StateKind.LIST,
                columnFamily,
                ttlEnabled,
                keySerializer,
                namespaceSerializer,
                null, // primaryValueSerializer (VALUE only)
                elementSerializer, // listElementSerializer
                null, // mapUserKeySerializer
                null); // mapUserValueSerializer
    }

    /** Captures a MapState schema. */
    public static <K, N, UK, UV> StateInspectSchema forMap(
            String stateName,
            String columnFamily,
            boolean ttlEnabled,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<UK> mapUserKeySerializer,
            TypeSerializer<UV> mapUserValueSerializer) {
        return new StateInspectSchema(
                stateName,
                StateKind.MAP,
                columnFamily,
                ttlEnabled,
                keySerializer,
                namespaceSerializer,
                null, // primaryValueSerializer (VALUE only)
                null, // listElementSerializer (LIST only)
                mapUserKeySerializer,
                mapUserValueSerializer);
    }

    private StateInspectSchema(
            String stateName,
            StateKind stateKind,
            String columnFamily,
            boolean ttlEnabled,
            TypeSerializer<?> keySerializer,
            TypeSerializer<?> namespaceSerializer,
            TypeSerializer<?> primaryValueSerializer,
            TypeSerializer<?> listElementSerializer,
            TypeSerializer<?> mapUserKeySerializer,
            TypeSerializer<?> mapUserValueSerializer) {
        this.stateName = stateName;
        this.stateKind = stateKind;
        this.columnFamily = columnFamily;
        this.ttlEnabled = ttlEnabled;

        int keyLength = safeLength(keySerializer);
        int namespaceLength = safeLength(namespaceSerializer);
        this.keyFixedLength = keyLength >= 0;
        this.namespaceFixedLength = namespaceLength >= 0;
        this.keyLengthStored =
                StateRowKeyLayout.shouldStoreKeyLengthForKeyNamespace(keyLength, namespaceLength);

        if (stateKind == StateKind.MAP && mapUserKeySerializer != null) {
            int userKeyLength = safeLength(mapUserKeySerializer);
            this.mapUserKeyFixedLength = userKeyLength >= 0;
            this.mapKeyLengthStored =
                    StateRowKeyLayout.shouldStoreMapKeyLength(
                            keyLength, namespaceLength, userKeyLength);
            this.mapNamespaceLengthStored =
                    StateRowKeyLayout.shouldStoreMapNamespaceLength(
                            keyLength, namespaceLength, userKeyLength);
        } else {
            this.mapUserKeyFixedLength = false;
            this.mapKeyLengthStored = false;
            this.mapNamespaceLengthStored = false;
        }

        this.keySerializer = SerializerInspectSchema.fromSerializer(keySerializer);
        this.namespaceSerializer = SerializerInspectSchema.fromSerializer(namespaceSerializer);
        if (stateKind == StateKind.VALUE) {
            this.valueSerializer = SerializerInspectSchema.fromSerializer(primaryValueSerializer);
            this.listElementSerializer = null;
            this.mapUserKeySerializer = null;
            this.mapUserValueSerializer = null;
        } else if (stateKind == StateKind.LIST) {
            this.valueSerializer = null;
            this.listElementSerializer =
                    SerializerInspectSchema.fromSerializer(listElementSerializer);
            this.mapUserKeySerializer = null;
            this.mapUserValueSerializer = null;
        } else {
            this.valueSerializer = null;
            this.listElementSerializer = null;
            this.mapUserKeySerializer =
                    SerializerInspectSchema.fromSerializer(mapUserKeySerializer);
            this.mapUserValueSerializer =
                    SerializerInspectSchema.fromSerializer(mapUserValueSerializer);
        }
    }

    public String stateName() {
        return stateName;
    }

    public StateKind stateKind() {
        return stateKind;
    }

    public String columnFamily() {
        return columnFamily;
    }

    public boolean ttlEnabled() {
        return ttlEnabled;
    }

    public boolean keyFixedLength() {
        return keyFixedLength;
    }

    public boolean namespaceFixedLength() {
        return namespaceFixedLength;
    }

    public boolean keyLengthStored() {
        return keyLengthStored;
    }

    public boolean mapUserKeyFixedLength() {
        return mapUserKeyFixedLength;
    }

    public boolean mapKeyLengthStored() {
        return mapKeyLengthStored;
    }

    public boolean mapNamespaceLengthStored() {
        return mapNamespaceLengthStored;
    }

    public SerializerInspectSchema keySerializer() {
        return keySerializer;
    }

    public SerializerInspectSchema namespaceSerializer() {
        return namespaceSerializer;
    }

    public SerializerInspectSchema valueSerializer() {
        return valueSerializer;
    }

    public SerializerInspectSchema listElementSerializer() {
        return listElementSerializer;
    }

    public SerializerInspectSchema mapUserKeySerializer() {
        return mapUserKeySerializer;
    }

    public SerializerInspectSchema mapUserValueSerializer() {
        return mapUserValueSerializer;
    }

    /** Serializer class-name summary keyed by role, for human-friendly monitor output. */
    public Map<String, String> serializerClassNames() {
        Map<String, String> names = new LinkedHashMap<>();
        names.put("key", className(keySerializer));
        names.put("namespace", className(namespaceSerializer));
        switch (stateKind) {
            case VALUE:
                names.put("value", className(valueSerializer));
                break;
            case LIST:
                names.put("element", className(listElementSerializer));
                break;
            case MAP:
                names.put("map_user_key", className(mapUserKeySerializer));
                names.put("map_user_value", className(mapUserValueSerializer));
                break;
        }
        return Collections.unmodifiableMap(names);
    }

    private static String className(SerializerInspectSchema schema) {
        return schema == null ? null : schema.serializerClassName();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StateInspectSchema)) {
            return false;
        }
        StateInspectSchema that = (StateInspectSchema) other;
        return Objects.equals(stateName, that.stateName)
                && stateKind == that.stateKind
                && Objects.equals(columnFamily, that.columnFamily)
                && ttlEnabled == that.ttlEnabled
                && keyFixedLength == that.keyFixedLength
                && namespaceFixedLength == that.namespaceFixedLength
                && keyLengthStored == that.keyLengthStored
                && mapUserKeyFixedLength == that.mapUserKeyFixedLength
                && mapKeyLengthStored == that.mapKeyLengthStored
                && mapNamespaceLengthStored == that.mapNamespaceLengthStored
                && Objects.equals(keySerializer, that.keySerializer)
                && Objects.equals(namespaceSerializer, that.namespaceSerializer)
                && Objects.equals(valueSerializer, that.valueSerializer)
                && Objects.equals(listElementSerializer, that.listElementSerializer)
                && Objects.equals(mapUserKeySerializer, that.mapUserKeySerializer)
                && Objects.equals(mapUserValueSerializer, that.mapUserValueSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                stateName,
                stateKind,
                columnFamily,
                ttlEnabled,
                keyFixedLength,
                namespaceFixedLength,
                keyLengthStored,
                mapUserKeyFixedLength,
                mapKeyLengthStored,
                mapNamespaceLengthStored);
    }

    void write(DataOutputView output) throws IOException {
        output.writeUTF(stateName);
        output.writeInt(stateKind.ordinal());
        output.writeUTF(columnFamily != null ? columnFamily : "");
        output.writeBoolean(ttlEnabled);
        output.writeBoolean(keyFixedLength);
        output.writeBoolean(namespaceFixedLength);
        output.writeBoolean(keyLengthStored);
        output.writeBoolean(mapUserKeyFixedLength);
        output.writeBoolean(mapKeyLengthStored);
        output.writeBoolean(mapNamespaceLengthStored);
        keySerializer.write(output);
        namespaceSerializer.write(output);
        switch (stateKind) {
            case VALUE:
                valueSerializer.write(output);
                break;
            case LIST:
                listElementSerializer.write(output);
                break;
            case MAP:
                mapUserKeySerializer.write(output);
                mapUserValueSerializer.write(output);
                break;
        }
    }

    static StateInspectSchema read(DataInputView input) throws IOException {
        String stateName = input.readUTF();
        StateKind kind = StateKind.fromOrdinal(input.readInt());
        String columnFamily = input.readUTF();
        boolean ttlEnabled = input.readBoolean();
        boolean keyFixedLength = input.readBoolean();
        boolean namespaceFixedLength = input.readBoolean();
        boolean keyLengthStored = input.readBoolean();
        boolean mapUserKeyFixedLength = input.readBoolean();
        boolean mapKeyLengthStored = input.readBoolean();
        boolean mapNamespaceLengthStored = input.readBoolean();
        SerializerInspectSchema keySerializer = SerializerInspectSchema.read(input);
        SerializerInspectSchema namespaceSerializer = SerializerInspectSchema.read(input);
        SerializerInspectSchema valueSerializer = null;
        SerializerInspectSchema listElementSerializer = null;
        SerializerInspectSchema mapUserKeySerializer = null;
        SerializerInspectSchema mapUserValueSerializer = null;
        switch (kind) {
            case VALUE:
                valueSerializer = SerializerInspectSchema.read(input);
                break;
            case LIST:
                listElementSerializer = SerializerInspectSchema.read(input);
                break;
            case MAP:
                mapUserKeySerializer = SerializerInspectSchema.read(input);
                mapUserValueSerializer = SerializerInspectSchema.read(input);
                break;
        }
        return new StateInspectSchema(
                stateName,
                kind,
                columnFamily.isEmpty() ? null : columnFamily,
                ttlEnabled,
                keyFixedLength,
                namespaceFixedLength,
                keyLengthStored,
                mapUserKeyFixedLength,
                mapKeyLengthStored,
                mapNamespaceLengthStored,
                keySerializer,
                namespaceSerializer,
                valueSerializer,
                listElementSerializer,
                mapUserKeySerializer,
                mapUserValueSerializer);
    }

    private static int safeLength(TypeSerializer<?> serializer) {
        try {
            return serializer.getLength();
        } catch (RuntimeException e) {
            return -1;
        }
    }
}
