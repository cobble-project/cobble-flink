package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.SerializerInspectSchema;
import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.MathUtils;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

class StateInspectDecoderTest {

    private static final String VOID_NAMESPACE_SERIALIZER_CLASS =
            "org.apache.flink.runtime.state.VoidNamespaceSerializer";

    @Test
    void decodesValueStateKeyNamespaceAndValue() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "value-state",
                        "cf-value",
                        false,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        InspectTarget target = target(schema);
        byte[] rowKey = keyAndNamespace("user-1", "window-a");
        byte[][] columns = new byte[][] {serialize(IntSerializer.INSTANCE, 42)};

        StateInspectDecoder.DecodedRow row = StateInspectDecoder.decode(target, rowKey, columns);

        assertNull(row.decodeError);
        assertEquals("user-1", row.decodedKey.get("key"));
        assertEquals("window-a", row.decodedKey.get("namespace"));
        assertEquals(42, row.decodedValue);
    }

    @Test
    void decodesListStateElements() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forList(
                        "list-state",
                        "cf-list",
                        false,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        InspectTarget target = target(schema);
        byte[] rowKey = keyAndNamespace("user-1", "ns");
        byte[][] columns = new byte[][] {listPayload("a", "b")};

        StateInspectDecoder.DecodedRow row = StateInspectDecoder.decode(target, rowKey, columns);

        assertNull(row.decodeError);
        assertEquals("user-1", row.decodedKey.get("key"));
        assertEquals(Arrays.asList("a", "b"), row.decodedValue);
    }

    @Test
    void decodesMapStateKeyNamespaceMapKeyAndValue() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "map-state",
                        "cf-map",
                        false,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        InspectTarget target = target(schema);
        byte[] rowKey = mapKey("user-1", "ns", "field-a");
        byte[][] columns = new byte[][] {serialize(StringSerializer.INSTANCE, "value-a")};

        StateInspectDecoder.DecodedRow row = StateInspectDecoder.decode(target, rowKey, columns);

        assertNull(row.decodeError);
        assertEquals("user-1", row.decodedKey.get("key"));
        assertEquals("ns", row.decodedKey.get("namespace"));
        assertEquals("field-a", row.decodedKey.get("map_key"));
        assertEquals("value-a", ((java.util.Map<?, ?>) row.decodedValue).get("map_value"));
    }

    @Test
    void decodesSemanticInnerJoinMapParts() throws Exception {
        RowDataSerializer stateKeySerializer = new RowDataSerializer(new BigIntType(false));
        RowDataSerializer recordSerializer =
                new RowDataSerializer(new BigIntType(false), VarCharType.STRING_TYPE);
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "left-records",
                        "left-records",
                        false,
                        stateKeySerializer,
                        VoidNamespaceSerializer.INSTANCE,
                        recordSerializer,
                        IntSerializer.INSTANCE);
        InspectTarget target =
                semanticTarget(
                        schema,
                        StateInspectSemanticSchema.forMap(
                                rowType("f0"),
                                StateInspectType.unknown(),
                                rowType("order_id", "region"),
                                StateInspectType.scalar("INT")));
        byte[] stateKey = serialize(stateKeySerializer, GenericRowData.of(7L));
        byte[] mapKey =
                serialize(recordSerializer, GenericRowData.of(100L, StringData.fromString("apac")));

        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(
                        target,
                        mapKeyWithVoidNamespace(schema, stateKey, mapKey),
                        new byte[][] {serialize(IntSerializer.INSTANCE, 3)});

        assertNull(row.decodeError);
        assertEquals(7L, semanticFieldValue(row.decodedParts, "state_key", 0));
        assertEquals(100L, semanticFieldValue(row.decodedParts, "map_key", 0));
        assertEquals("apac", semanticFieldValue(row.decodedParts, "map_key", 1));
        assertEquals(3, ((Map<?, ?>) row.decodedParts.get("map_value")).get("value"));
    }

    @Test
    void matchesSemanticStateAndMapKeyFieldPrefixes() throws Exception {
        RowDataSerializer stateKeySerializer = new RowDataSerializer(new BigIntType(false));
        RowDataSerializer recordSerializer =
                new RowDataSerializer(new BigIntType(false), VarCharType.STRING_TYPE);
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "left-records",
                        "left-records",
                        false,
                        stateKeySerializer,
                        VoidNamespaceSerializer.INSTANCE,
                        recordSerializer,
                        IntSerializer.INSTANCE);
        StateInspectType stateKeyType = rowType("f0");
        StateInspectType mapKeyType = rowType("order_id", "region");
        InspectTarget target =
                semanticTarget(
                        schema,
                        StateInspectSemanticSchema.forMap(
                                stateKeyType,
                                StateInspectType.unknown(),
                                mapKeyType,
                                StateInspectType.scalar("INT")));
        byte[] stateKey = serialize(stateKeySerializer, GenericRowData.of(7L));
        byte[] mapKey =
                serialize(recordSerializer, GenericRowData.of(100L, StringData.fromString("apac")));
        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(
                        target,
                        mapKeyWithVoidNamespace(schema, stateKey, mapKey),
                        new byte[][] {serialize(IntSerializer.INSTANCE, 3)});

        assertTrue(
                StateInspectDecoder.matchesSemanticPartFilter(
                        row.decodedParts, "state_key", stateKeyType, Arrays.asList("7")));
        assertTrue(
                StateInspectDecoder.matchesSemanticPartFilter(
                        row.decodedParts, "map_key", mapKeyType, Arrays.asList("100", "ap")));
        assertFalse(
                StateInspectDecoder.matchesSemanticPartFilter(
                        row.decodedParts, "map_key", mapKeyType, Arrays.asList("101")));
    }

    @Test
    void decodesSemanticPrimaryKeyJoinValue() throws Exception {
        RowDataSerializer stateKeySerializer = new RowDataSerializer(new BigIntType(false));
        RowDataSerializer recordSerializer =
                new RowDataSerializer(new BigIntType(false), VarCharType.STRING_TYPE);
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "right-records",
                        "right-records",
                        false,
                        stateKeySerializer,
                        VoidNamespaceSerializer.INSTANCE,
                        recordSerializer);
        InspectTarget target =
                semanticTarget(
                        schema,
                        StateInspectSemanticSchema.forValue(
                                rowType("f0"),
                                StateInspectType.unknown(),
                                rowType("user_id", "tier")));
        byte[] stateKey = serialize(stateKeySerializer, GenericRowData.of(7L));

        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(
                        target,
                        keyWithVoidNamespace(stateKey),
                        new byte[][] {
                            serialize(
                                    recordSerializer,
                                    GenericRowData.of(7L, StringData.fromString("gold")))
                        });

        assertNull(row.decodeError);
        assertEquals(7L, semanticFieldValue(row.decodedParts, "state_key", 0));
        assertEquals(7L, semanticFieldValue(row.decodedParts, "value", 0));
        assertEquals("gold", semanticFieldValue(row.decodedParts, "value", 1));
    }

    @Test
    void decodesSemanticOuterJoinTupleValue() throws Exception {
        RowDataSerializer stateKeySerializer = new RowDataSerializer(new BigIntType(false));
        RowDataSerializer recordSerializer =
                new RowDataSerializer(new BigIntType(false), VarCharType.STRING_TYPE);
        InternalTypeInfo<RowData> recordType =
                InternalTypeInfo.of(
                        RowType.of(
                                new LogicalType[] {new BigIntType(false), VarCharType.STRING_TYPE},
                                new String[] {"order_id", "region"}));
        @SuppressWarnings("unchecked")
        TypeSerializer<Tuple2<RowData, Integer>> tupleSerializer =
                (TypeSerializer<Tuple2<RowData, Integer>>)
                        (TypeSerializer<?>)
                                new TupleTypeInfo<>(recordType, Types.INT)
                                        .createSerializer(new ExecutionConfig());
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "left-records",
                        "left-records",
                        false,
                        stateKeySerializer,
                        VoidNamespaceSerializer.INSTANCE,
                        recordSerializer,
                        tupleSerializer);
        StateInspectType tupleType =
                StateInspectType.tuple(
                        Arrays.asList(
                                new StateInspectField("f0", rowType("order_id", "region")),
                                new StateInspectField("f1", StateInspectType.scalar("INT"))));
        InspectTarget target =
                semanticTarget(
                        schema,
                        StateInspectSemanticSchema.forMap(
                                rowType("f0"),
                                StateInspectType.unknown(),
                                rowType("order_id", "region"),
                                tupleType));
        byte[] stateKey = serialize(stateKeySerializer, GenericRowData.of(7L));
        byte[] mapKey =
                serialize(recordSerializer, GenericRowData.of(100L, StringData.fromString("apac")));

        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(
                        target,
                        mapKeyWithVoidNamespace(schema, stateKey, mapKey),
                        new byte[][] {
                            serialize(
                                    tupleSerializer,
                                    Tuple2.of(
                                            GenericRowData.of(100L, StringData.fromString("apac")),
                                            2))
                        });

        assertNull(row.decodeError);
        Map<?, ?> tuple = (Map<?, ?>) row.decodedParts.get("map_value");
        List<?> tupleFields = (List<?>) tuple.get("fields");
        Map<?, ?> recordField = (Map<?, ?>) tupleFields.get(0);
        List<?> recordFields = (List<?>) recordField.get("fields");
        assertEquals(100L, ((Map<?, ?>) recordFields.get(0)).get("value"));
        assertEquals("apac", ((Map<?, ?>) recordFields.get(1)).get("value"));
        assertEquals(2, ((Map<?, ?>) tupleFields.get(1)).get("value"));
    }

    @Test
    void decodesByteArrayValueAsRawBytesJson() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "value-state",
                        "cf-value",
                        false,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        BytePrimitiveArraySerializer.INSTANCE);
        InspectTarget target = target(schema);
        byte[] rowKey = keyAndNamespace("user-1", "window-a");
        byte[][] columns =
                new byte[][] {
                    serialize(BytePrimitiveArraySerializer.INSTANCE, new byte[] {1, 2, 3})
                };

        StateInspectDecoder.DecodedRow row = StateInspectDecoder.decode(target, rowKey, columns);

        assertNull(row.decodeError);
        assertEquals(CobbleFlinkMonitorServer.bytesJson(new byte[] {1, 2, 3}), row.decodedValue);
    }

    @Test
    void decodesTimerKeyParts() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forTimer(
                        "event-time-timers",
                        "__cobble_timer__event-time-timers",
                        StringSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        InspectTarget target =
                new InspectTarget(
                        "timer:event-time-timers",
                        "event-time-timers",
                        "timer",
                        "__cobble_timer__event-time-timers",
                        false,
                        schema.stateKind().name(),
                        java.util.Collections.emptyMap(),
                        schema,
                        null);
        byte[] rowKey = timerKey(1234L, "user-1", 7);

        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(target, rowKey, new byte[][] {new byte[0]});

        assertNull(row.decodeError);
        assertEquals(1234L, row.decodedKey.get("timestamp"));
        assertEquals("user-1", row.decodedKey.get("key"));
        assertEquals(7, row.decodedKey.get("namespace"));
        assertNull(row.decodedValue);
    }

    @Test
    void decodesTimerKeyWithVoidNamespace() throws Exception {
        StateInspectSchema schema =
                withVoidNamespace(
                        StateInspectSchema.forTimer(
                                "_timer_state/processing_user-timers",
                                "__cobble_timer___timer_state/processing_user-timers",
                                StringSerializer.INSTANCE,
                                StringSerializer.INSTANCE));
        InspectTarget target =
                new InspectTarget(
                        "timer:_timer_state/processing_user-timers",
                        "_timer_state/processing_user-timers",
                        "timer",
                        "__cobble_timer___timer_state/processing_user-timers",
                        false,
                        schema.stateKind().name(),
                        java.util.Collections.emptyMap(),
                        schema,
                        null);
        byte[] rowKey = timerKeyWithVoidNamespace(1234L, "user-1");

        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(target, rowKey, new byte[][] {new byte[0]});

        assertNull(row.decodeError);
        assertEquals(1234L, row.decodedKey.get("timestamp"));
        assertEquals("user-1", row.decodedKey.get("key"));
        assertEquals("VoidNamespace", row.decodedKey.get("namespace"));
        assertNull(row.decodedValue);
    }

    @Test
    void decodesVoidNamespaceWithoutRestoringSerializer() throws Exception {
        StateInspectSchema schema =
                withVoidNamespace(
                        StateInspectSchema.forValue(
                                "value-state",
                                "cf-value",
                                false,
                                StringSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                StringSerializer.INSTANCE));
        InspectTarget target = target(schema);
        byte[] rowKey = keyAndVoidNamespace("user-1");
        byte[][] columns = new byte[][] {serialize(StringSerializer.INSTANCE, "value-a")};

        StateInspectDecoder.DecodedRow row = StateInspectDecoder.decode(target, rowKey, columns);

        assertNull(row.decodeError);
        assertEquals("user-1", row.decodedKey.get("key"));
        assertEquals("VoidNamespace", row.decodedKey.get("namespace"));
        assertEquals("value-a", row.decodedValue);
    }

    @Test
    void decodesMapStateWithVoidNamespace() throws Exception {
        StateInspectSchema schema =
                withVoidNamespace(
                        StateInspectSchema.forMap(
                                "map-state",
                                "cf-map",
                                false,
                                StringSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                IntSerializer.INSTANCE));
        InspectTarget target = target(schema);
        byte[] rowKey = mapKeyWithVoidNamespace("user-1", "field-a");
        byte[][] columns = new byte[][] {serialize(IntSerializer.INSTANCE, 42)};

        StateInspectDecoder.DecodedRow row = StateInspectDecoder.decode(target, rowKey, columns);

        assertNull(row.decodeError);
        assertEquals("user-1", row.decodedKey.get("key"));
        assertEquals("VoidNamespace", row.decodedKey.get("namespace"));
        assertEquals("field-a", row.decodedKey.get("map_key"));
        assertEquals(42, ((java.util.Map<?, ?>) row.decodedValue).get("map_value"));
    }

    @Test
    void decodeFailureStaysRowLocal() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "value-state",
                        "cf-value",
                        false,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        InspectTarget target = target(schema);

        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(
                        target, new byte[] {1, 2}, new byte[][] {new byte[] {3}});

        assertTrue(row.decodeError != null && !row.decodeError.isEmpty());
        assertNull(row.decodedKey);
        assertNull(row.decodedValue);
    }

    @Test
    void encodesTypedValueStateKeyPrefix() throws Exception {
        StateInspectSchema schema =
                withVoidNamespace(
                        StateInspectSchema.forValue(
                                "value-state",
                                "cf-value",
                                false,
                                IntSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                LongSerializer.INSTANCE));
        InspectTarget target = target(schema);

        byte[] encoded =
                StateInspectDecoder.encodeStateKeyPrefix(
                        target, "42", null, null, null, null, null);

        DataOutputSerializer expected = new DataOutputSerializer(8);
        expected.write(serialize(IntSerializer.INSTANCE, 42));
        expected.writeByte(0);
        assertArrayEquals(expected.getCopyOfBuffer(), encoded);
        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(
                        target, encoded, new byte[][] {serialize(LongSerializer.INSTANCE, 123L)});
        assertNull(row.decodeError);
        assertEquals(42, row.decodedKey.get("key"));
        assertEquals(123L, row.decodedValue);
    }

    @Test
    void encodesMapStateKeyPrefixAndExactUserKey() throws Exception {
        StateInspectSchema schema =
                withVoidNamespace(
                        StateInspectSchema.forMap(
                                "map-state",
                                "cf-map",
                                false,
                                StringSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                IntSerializer.INSTANCE));
        InspectTarget target = target(schema);

        byte[] keyPrefix =
                StateInspectDecoder.encodeStateKeyPrefix(
                        target, "user-1", null, null, null, null, null);
        byte[] expectedPrefix = keyAndVoidNamespaceMapPrefix("user-1");
        assertArrayEquals(expectedPrefix, keyPrefix);

        byte[] exactKey =
                StateInspectDecoder.encodeStateKeyPrefix(
                        target, "user-1", null, null, null, "field-a", null);
        assertArrayEquals(mapKeyWithVoidNamespace("user-1", "field-a"), exactKey);
    }

    @Test
    void matchesRawMapKeyBytesPrefix() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "map-state",
                        "cf-map",
                        false,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        BytePrimitiveArraySerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        InspectTarget target = target(schema);
        byte[] mapKeyBytes = serialize(BytePrimitiveArraySerializer.INSTANCE, new byte[] {1, 2, 3});
        byte[] rowKey = mapKey("user-1", "ns", mapKeyBytes);

        assertTrue(
                StateInspectDecoder.mapKeyBytesStartsWith(
                        target, rowKey, Arrays.copyOf(mapKeyBytes, mapKeyBytes.length - 1)));

        StateInspectDecoder.DecodedRow row =
                StateInspectDecoder.decode(
                        target, rowKey, new byte[][] {serialize(IntSerializer.INSTANCE, 7)});
        assertNull(row.decodeError);
        assertEquals(
                CobbleFlinkMonitorServer.bytesJson(new byte[] {1, 2, 3}),
                ((Map<?, ?>) row.decodedKey).get("map_key"));
    }

    private static InspectTarget target(StateInspectSchema schema) {
        return new InspectTarget(
                schema.stateName(),
                schema.stateName(),
                "state",
                schema.columnFamily(),
                false,
                schema.stateKind().name(),
                java.util.Collections.emptyMap(),
                schema,
                null);
    }

    private static InspectTarget semanticTarget(
            StateInspectSchema schema, StateInspectSemanticSchema semanticSchema) {
        return new InspectTarget(
                schema.stateName(),
                schema.stateName(),
                "state",
                schema.columnFamily(),
                false,
                schema.stateKind().name(),
                java.util.Collections.emptyMap(),
                schema,
                semanticSchema,
                null);
    }

    private static StateInspectType rowType(String... fieldNames) {
        List<StateInspectField> fields = new ArrayList<>(fieldNames.length);
        for (String fieldName : fieldNames) {
            String logicalType =
                    "region".equals(fieldName) || "tier".equals(fieldName)
                            ? "VARCHAR(2147483647)"
                            : "BIGINT NOT NULL";
            fields.add(new StateInspectField(fieldName, StateInspectType.scalar(logicalType)));
        }
        return StateInspectType.row(fields);
    }

    private static Object semanticFieldValue(
            Map<String, Object> decodedParts, String partName, int fieldIndex) {
        Map<?, ?> part = (Map<?, ?>) decodedParts.get(partName);
        List<?> fields = (List<?>) part.get("fields");
        return ((Map<?, ?>) fields.get(fieldIndex)).get("value");
    }

    private static byte[] keyAndNamespace(String key, String namespace) throws Exception {
        byte[] keyBytes = serialize(StringSerializer.INSTANCE, key);
        byte[] namespaceBytes = serialize(StringSerializer.INSTANCE, namespace);
        DataOutputSerializer output = new DataOutputSerializer(64);
        output.write(keyBytes);
        output.write(namespaceBytes);
        output.writeInt(keyBytes.length);
        return output.getCopyOfBuffer();
    }

    private static byte[] timerKey(long timestamp, String key, int namespace) throws Exception {
        byte[] keyBytes = serialize(StringSerializer.INSTANCE, key);
        byte[] namespaceBytes = serialize(IntSerializer.INSTANCE, namespace);
        DataOutputSerializer output = new DataOutputSerializer(64);
        output.writeLong(MathUtils.flipSignBit(timestamp));
        output.write(keyBytes);
        output.write(namespaceBytes);
        return output.getCopyOfBuffer();
    }

    private static byte[] timerKeyWithVoidNamespace(long timestamp, String key) throws Exception {
        byte[] keyBytes = serialize(StringSerializer.INSTANCE, key);
        DataOutputSerializer output = new DataOutputSerializer(64);
        output.writeLong(MathUtils.flipSignBit(timestamp));
        output.write(keyBytes);
        output.writeByte(0);
        return output.getCopyOfBuffer();
    }

    private static byte[] keyAndVoidNamespace(String key) throws Exception {
        byte[] keyBytes = serialize(StringSerializer.INSTANCE, key);
        DataOutputSerializer output = new DataOutputSerializer(64);
        output.write(keyBytes);
        output.writeByte(0);
        return output.getCopyOfBuffer();
    }

    private static byte[] keyWithVoidNamespace(byte[] keyBytes) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(keyBytes.length + 1);
        output.write(keyBytes);
        output.writeByte(0);
        return output.getCopyOfBuffer();
    }

    private static byte[] mapKey(String key, String namespace, String mapKey) throws Exception {
        return mapKey(key, namespace, serialize(StringSerializer.INSTANCE, mapKey));
    }

    private static byte[] mapKey(String key, String namespace, byte[] mapKeyBytes)
            throws Exception {
        byte[] keyBytes = serialize(StringSerializer.INSTANCE, key);
        byte[] namespaceBytes = serialize(StringSerializer.INSTANCE, namespace);
        DataOutputSerializer output = new DataOutputSerializer(64);
        output.write(keyBytes);
        output.write(namespaceBytes);
        output.writeByte(0);
        output.write(mapKeyBytes);
        output.writeInt(keyBytes.length);
        output.writeInt(namespaceBytes.length);
        return output.getCopyOfBuffer();
    }

    private static byte[] mapKeyWithVoidNamespace(String key, String mapKey) throws Exception {
        byte[] keyBytes = serialize(StringSerializer.INSTANCE, key);
        byte[] mapKeyBytes = serialize(StringSerializer.INSTANCE, mapKey);
        DataOutputSerializer output = new DataOutputSerializer(64);
        output.write(keyBytes);
        output.writeByte(0);
        output.writeByte(0);
        output.write(mapKeyBytes);
        output.writeInt(keyBytes.length);
        return output.getCopyOfBuffer();
    }

    private static byte[] mapKeyWithVoidNamespace(
            StateInspectSchema schema, byte[] stateKeyBytes, byte[] mapKeyBytes) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(128);
        output.write(stateKeyBytes);
        output.writeByte(0);
        output.writeByte(0);
        output.write(mapKeyBytes);
        if (schema.mapKeyLengthStored()) {
            output.writeInt(stateKeyBytes.length);
        }
        if (schema.mapNamespaceLengthStored()) {
            output.writeInt(1);
        }
        return output.getCopyOfBuffer();
    }

    private static byte[] keyAndVoidNamespaceMapPrefix(String key) throws Exception {
        byte[] keyBytes = serialize(StringSerializer.INSTANCE, key);
        DataOutputSerializer output = new DataOutputSerializer(64);
        output.write(keyBytes);
        output.writeByte(0);
        output.writeByte(0);
        return output.getCopyOfBuffer();
    }

    private static byte[] listPayload(String... values) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (String value : values) {
            output.write(serialize(StringSerializer.INSTANCE, value));
            output.write(',');
        }
        return output.toByteArray();
    }

    private static <T> byte[] serialize(TypeSerializer<T> serializer, T value) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(32);
        serializer.serialize(value, output);
        return output.getCopyOfBuffer();
    }

    private static StateInspectSchema withVoidNamespace(StateInspectSchema schema)
            throws Exception {
        setField(
                schema,
                "namespaceSerializer",
                serializerSchema(VOID_NAMESPACE_SERIALIZER_CLASS, 1));
        setField(schema, "namespaceFixedLength", true);
        setField(schema, "keyLengthStored", false);
        if ("MAP".equals(schema.stateKind().name())) {
            setField(schema, "mapKeyLengthStored", true);
            setField(schema, "mapNamespaceLengthStored", false);
        }
        return schema;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static SerializerInspectSchema serializerSchema(String className, int lengthTag)
            throws Exception {
        Constructor<SerializerInspectSchema> constructor =
                SerializerInspectSchema.class.getDeclaredConstructor(
                        String.class, int.class, byte[].class, byte[].class);
        constructor.setAccessible(true);
        return constructor.newInstance(className, lengthTag, null, null);
    }
}
