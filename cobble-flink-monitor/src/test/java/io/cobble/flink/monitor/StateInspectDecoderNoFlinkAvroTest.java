package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Proves that {@link StateInspectDecoder#decode} produces correct semantic output for ValueState,
 * ListState, and MapState with Avro serializers when {@code flink-avro} is absent from the
 * classpath.
 *
 * <p>The test builds fixtures using the test-scoped {@link AvroSerializer}, then invokes {@code
 * StateInspectDecoder.decode()} with a thread context classloader that rejects {@code
 * org.apache.flink.formats.avro.*}. This matches the classloader used by the monitor when restoring
 * serializer snapshots.
 *
 * <p>For ValueState/ListState/MapState, the FULLY_CLASSLESS semantic path succeeds and no false
 * {@code decode_error} is produced from the legacy preview (which requires flink-avro to restore
 * the live serializer).
 *
 * <p>For timers, classless Avro key decode is not supported because the timer path must restore the
 * live serializer to split the key/namespace boundary. The timer test verifies that the timestamp
 * is still preserved in the semantic parts and that the {@code decode_error} clearly identifies
 * {@code state_key} as the failing part.
 */
class StateInspectDecoderNoFlinkAvroTest {

    @Test
    void valueStateAvroDecodesWithoutFlinkAvro() throws Exception {
        Schema avroSchema = Schema.createRecord("ValUser", null, null, false);
        avroSchema.setFields(
                Arrays.asList(
                        new Schema.Field("userId", Schema.create(Schema.Type.INT), null, null),
                        new Schema.Field(
                                "userName", Schema.create(Schema.Type.STRING), null, null)));
        AvroSerializer<GenericRecord> avroSerializer =
                new AvroSerializer<>(GenericRecord.class, avroSchema);

        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("userId", 55);
        record.put("userName", "carol");
        byte[] valueBytes = serialize(avroSerializer, record);

        StateInspectSchema stateSchema =
                StateInspectSchema.forValue(
                        "val-avro",
                        "cf-val-avro",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        avroSerializer);
        assertNull(stateSchema.valueSerializer().serializedSerializerBytes());

        StateInspectType rowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("userId", StateInspectType.scalar("INT")),
                                new StateInspectField(
                                        "userName", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), rowType);

        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row =
                decodeWithBlockedFlinkAvro(stateSchema, semanticSchema, rowKey, valueBytes);

        // Verify: no decode_error, value part decoded correctly.
        assertNull(row.decodeError);
        Map<?, ?> decodedParts = row.decodedParts;
        assertNotNull(decodedParts);
        Map<?, ?> valuePart = (Map<?, ?>) decodedParts.get("value");
        assertNotNull(valuePart);
        assertEquals("ROW", valuePart.get("kind"));
    }

    @Test
    void stateKeyAvroDecodesWithoutFlinkAvro() throws Exception {
        Schema keySchema = Schema.createRecord("StateKey", null, null, false);
        keySchema.setFields(
                Collections.singletonList(
                        new Schema.Field("accountId", Schema.create(Schema.Type.INT), null, null)));
        AvroSerializer<GenericRecord> keySerializer =
                new AvroSerializer<>(GenericRecord.class, keySchema);
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("accountId", 7);

        StateInspectSchema stateSchema =
                StateInspectSchema.forValue(
                        "key-avro",
                        "cf-key-avro",
                        false,
                        keySerializer,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        assertNull(stateSchema.keySerializer().serializedSerializerBytes());

        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.row(
                                Collections.singletonList(
                                        new StateInspectField(
                                                "accountId", StateInspectType.scalar("INT")))),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("INT"));
        byte[] rowKey = keyWithVoidNamespace(serialize(keySerializer, key));
        byte[] valueBytes = serialize(IntSerializer.INSTANCE, 99);

        StateInspectDecoder.DecodedRow row =
                decodeWithBlockedFlinkAvro(stateSchema, semanticSchema, rowKey, valueBytes);

        assertNull(row.decodeError);
        Map<?, ?> stateKeyPart = (Map<?, ?>) row.decodedParts.get("state_key");
        assertEquals("ROW", stateKeyPart.get("kind"));
        assertEquals(7, semanticFieldValue(stateKeyPart, 0));
        assertNotNull(row.decodedValue);
    }

    @Test
    void listStateAvroDecodesWithoutFlinkAvro() throws Exception {
        Schema avroSchema = Schema.createRecord("ListItem", null, null, false);
        avroSchema.setFields(
                Collections.singletonList(
                        new Schema.Field("sku", Schema.create(Schema.Type.INT), null, null)));
        AvroSerializer<GenericRecord> avroSerializer =
                new AvroSerializer<>(GenericRecord.class, avroSchema);

        GenericRecord r1 = new GenericData.Record(avroSchema);
        r1.put("sku", 10);
        GenericRecord r2 = new GenericData.Record(avroSchema);
        r2.put("sku", 20);

        StateInspectSchema stateSchema =
                StateInspectSchema.forList(
                        "list-avro",
                        "cf-list-avro",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        avroSerializer);
        assertNull(stateSchema.listElementSerializer().serializedSerializerBytes());

        StateInspectType elementType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField("sku", StateInspectType.scalar("INT"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forList(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), elementType);

        // Build list payload: element, delimiter, element, delimiter.
        java.io.ByteArrayOutputStream listOut = new java.io.ByteArrayOutputStream();
        listOut.write(serialize(avroSerializer, r1));
        listOut.write(',');
        listOut.write(serialize(avroSerializer, r2));
        listOut.write(',');
        byte[] listBytes = listOut.toByteArray();
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row =
                decodeWithBlockedFlinkAvro(stateSchema, semanticSchema, rowKey, listBytes);

        // Verify: no decode_error, list value decoded correctly with 2 elements.
        assertNull(row.decodeError);
        Map<?, ?> decodedParts = row.decodedParts;
        assertNotNull(decodedParts);
        Map<?, ?> valuePart = (Map<?, ?>) decodedParts.get("value");
        assertNotNull(valuePart);
        assertEquals("LIST", valuePart.get("kind"));
        List<?> values = (List<?>) valuePart.get("values");
        assertEquals(2, values.size());
    }

    @Test
    void mapStateAvroKeyAndValueDecodeWithoutFlinkAvro() throws Exception {
        Schema keySchema = Schema.createRecord("MapKey", null, null, false);
        keySchema.setFields(
                Collections.singletonList(
                        new Schema.Field("region", Schema.create(Schema.Type.STRING), null, null)));
        Schema valueSchema = Schema.createRecord("MapVal", null, null, false);
        valueSchema.setFields(
                Collections.singletonList(
                        new Schema.Field("count", Schema.create(Schema.Type.INT), null, null)));
        AvroSerializer<GenericRecord> keySerializer =
                new AvroSerializer<>(GenericRecord.class, keySchema);
        AvroSerializer<GenericRecord> valueSerializer =
                new AvroSerializer<>(GenericRecord.class, valueSchema);

        GenericRecord keyRecord = new GenericData.Record(keySchema);
        keyRecord.put("region", "us-west");
        GenericRecord valueRecord = new GenericData.Record(valueSchema);
        valueRecord.put("count", 99);

        StateInspectSchema stateSchema =
                StateInspectSchema.forMap(
                        "map-avro",
                        "cf-map-avro",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        keySerializer,
                        valueSerializer);
        assertNull(stateSchema.mapUserKeySerializer().serializedSerializerBytes());
        assertNull(stateSchema.mapUserValueSerializer().serializedSerializerBytes());

        StateInspectType keyType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField(
                                        "region", StateInspectType.scalar("VARCHAR"))));
        StateInspectType valueType =
                StateInspectType.row(
                        Collections.singletonList(
                                new StateInspectField("count", StateInspectType.scalar("INT"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forMap(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        keyType,
                        valueType);

        byte[] mapKeyBytes = serialize(keySerializer, keyRecord);
        byte[] rowKey =
                mapKeyWithVoidNamespace(
                        stateSchema, serialize(IntSerializer.INSTANCE, 1), mapKeyBytes);
        byte[] mapValueColumn = mapValueBytes(serialize(valueSerializer, valueRecord));

        StateInspectDecoder.DecodedRow row =
                decodeWithBlockedFlinkAvro(stateSchema, semanticSchema, rowKey, mapValueColumn);

        // Verify: no decode_error, both map_key and map_value decoded correctly.
        assertNull(row.decodeError);
        Map<?, ?> decodedParts = row.decodedParts;
        assertNotNull(decodedParts);
        Map<?, ?> keyPart = (Map<?, ?>) decodedParts.get("map_key");
        assertNotNull(keyPart);
        assertEquals("ROW", keyPart.get("kind"));
        Map<?, ?> valuePart = (Map<?, ?>) decodedParts.get("map_value");
        assertNotNull(valuePart);
        assertEquals("ROW", valuePart.get("kind"));
    }

    @Test
    void timerSemanticPartsIncludeTimestamp() throws Exception {
        // Timer key classless Avro decode is NOT supported: the timer path must restore the live
        // serializer to split the key/namespace boundary, and FULLY_CLASSLESS Avro serializers
        // cannot be restored without flink-avro. This test verifies that the timestamp is still
        // preserved in the semantic parts, and that the decode_error clearly identifies the
        // state_key boundary as the failing part (not the timestamp).
        Schema keySchema = Schema.createRecord("TimerKey", null, null, false);
        keySchema.setFields(
                Collections.singletonList(
                        new Schema.Field("accountId", Schema.create(Schema.Type.INT), null, null)));
        AvroSerializer<GenericRecord> keySerializer =
                new AvroSerializer<>(GenericRecord.class, keySchema);
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("accountId", 7);

        StateInspectSchema stateSchema =
                StateInspectSchema.forTimer(
                        "timer-ts",
                        "__cobble_timer__timer-ts",
                        keySerializer,
                        VoidNamespaceSerializer.INSTANCE);
        assertNull(stateSchema.keySerializer().serializedSerializerBytes());

        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.row(
                                Collections.singletonList(
                                        new StateInspectField(
                                                "accountId", StateInspectType.scalar("INT")))),
                        StateInspectType.unknown(),
                        StateInspectType.unknown());

        // Build timer row key: flipped-timestamp(8) + avro-key-bytes + void-ns-marker(1).
        long timestamp = 9999L;
        byte[] keyBytes = serialize(keySerializer, key);
        DataOutputSerializer out = new DataOutputSerializer(16 + keyBytes.length);
        out.writeLong(org.apache.flink.util.MathUtils.flipSignBit(timestamp));
        out.write(keyBytes);
        out.writeByte(0);
        byte[] rowKey = out.getCopyOfBuffer();

        StateInspectDecoder.DecodedRow row =
                decodeWithBlockedFlinkAvro(stateSchema, semanticSchema, rowKey, new byte[0]);

        // The timestamp must be in the semantic parts even when the key boundary fails.
        assertNotNull(row.decodedParts, "semantic parts should not be null");
        assertEquals(timestamp, row.decodedParts.get("timestamp"));

        // The state_key boundary cannot be determined without the restored Avro serializer.
        // The error must reference state_key (not timestamp).
        assertNotNull(row.decodeError, "expected a decode_error for the un-restorable key");
        assertTrue(
                row.decodeError.contains("state_key"),
                "decode_error should reference state_key: " + row.decodeError);
    }

    private static StateInspectDecoder.DecodedRow decodeWithBlockedFlinkAvro(
            StateInspectSchema schema,
            StateInspectSemanticSchema semanticSchema,
            byte[] rowKey,
            byte[] valueColumn) {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader blockingLoader =
                new ClassLoader(ClassLoader.getSystemClassLoader()) {
                    @Override
                    public Class<?> loadClass(String name, boolean resolve)
                            throws ClassNotFoundException {
                        if (name.startsWith("org.apache.flink.formats.avro.")) {
                            throw new ClassNotFoundException("Blocked: " + name);
                        }
                        return super.loadClass(name, resolve);
                    }
                };
        InspectTarget target =
                new InspectTarget(
                        schema.stateName(),
                        schema.stateName(),
                        "state",
                        schema.columnFamily(),
                        false,
                        schema.stateKind().name(),
                        Collections.emptyMap(),
                        schema,
                        semanticSchema,
                        null);

        Thread.currentThread().setContextClassLoader(blockingLoader);
        try {
            return StateInspectDecoder.decode(target, rowKey, new byte[][] {valueColumn});
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    // ---- Helpers: serialization ----

    private static <T> byte[] serialize(TypeSerializer<T> serializer, T value) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);
        return out.getCopyOfBuffer();
    }

    private static byte[] keyWithVoidNamespace(byte[] stateKeyBytes) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(stateKeyBytes.length + 1);
        output.write(stateKeyBytes);
        output.writeByte(0);
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

    private static byte[] mapValueBytes(byte[] payload) {
        byte[] row = new byte[payload.length + 1];
        row[0] = 0x00;
        System.arraycopy(payload, 0, row, 1, payload.length);
        return row;
    }

    private static Object semanticFieldValue(Map<?, ?> row, int index) {
        List<?> fields = (List<?>) row.get("fields");
        return ((Map<?, ?>) fields.get(index)).get("value");
    }
}
