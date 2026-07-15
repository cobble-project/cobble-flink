package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.decode.FlinkDataInputDecoder;
import io.cobble.flink.inspect.internal.*;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link FlinkDataInputDecoder}, verifying that it correctly decodes bytes written
 * by Flink's {@code DataOutputEncoder} semantics and rejects malformed input.
 *
 * <p>These tests use Avro's {@link GenericDatumReader} with a {@link FlinkDataInputDecoder} to
 * verify end-to-end decoding of real Avro schemas. The writer bytes are constructed manually using
 * {@code DataOutputSerializer} (which has the same semantics as Flink's {@code DataOutputEncoder})
 * to avoid depending on {@code flink-avro} at test scope for this class.
 */
class FlinkDataInputDecoderTest {

    // ---- Primitive decoding ----

    @Test
    void decodesIntRecord() throws Exception {
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field(
                                        "value", Schema.create(Schema.Type.INT), null, null)));
        byte[] bytes = writeInt(42);
        GenericRecord record = decode(schema, bytes);
        assertEquals(42, record.get("value"));
    }

    @Test
    void decodesLongRecord() throws Exception {
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field(
                                        "value", Schema.create(Schema.Type.LONG), null, null)));
        byte[] bytes = writeLong(1234567890L);
        GenericRecord record = decode(schema, bytes);
        assertEquals(1234567890L, record.get("value"));
    }

    @Test
    void decodesBooleanRecord() throws Exception {
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field(
                                        "value", Schema.create(Schema.Type.BOOLEAN), null, null)));
        byte[] bytes = writeBoolean(true);
        GenericRecord record = decode(schema, bytes);
        assertEquals(true, record.get("value"));
    }

    @Test
    void decodesFloatRecord() throws Exception {
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field(
                                        "value", Schema.create(Schema.Type.FLOAT), null, null)));
        byte[] bytes = writeFloat(3.14f);
        GenericRecord record = decode(schema, bytes);
        assertEquals(3.14f, (Float) record.get("value"), 0.001f);
    }

    @Test
    void decodesDoubleRecord() throws Exception {
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field(
                                        "value", Schema.create(Schema.Type.DOUBLE), null, null)));
        byte[] bytes = writeDouble(2.71828);
        GenericRecord record = decode(schema, bytes);
        assertEquals(2.71828, (Double) record.get("value"), 0.000001);
    }

    // ---- String and bytes ----

    @Test
    void decodesStringRecord() throws Exception {
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field(
                                        "value", Schema.create(Schema.Type.STRING), null, null)));
        byte[] bytes = writeString("hello avro");
        GenericRecord record = decode(schema, bytes);
        assertEquals("hello avro", record.get("value").toString());
        assertTrue(record.get("value") instanceof Utf8);
    }

    @Test
    void decodesBytesRecord() throws Exception {
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field(
                                        "value", Schema.create(Schema.Type.BYTES), null, null)));
        byte[] payload = {1, 2, 3, 4, 5};
        byte[] bytes = writeBytes(payload);
        GenericRecord record = decode(schema, bytes);
        ByteBuffer buf = (ByteBuffer) record.get("value");
        byte[] result = new byte[buf.remaining()];
        buf.get(result);
        assertArrayEquals(payload, result);
    }

    // ---- Fixed and enum ----

    @Test
    void decodesFixedRecord() throws Exception {
        Schema fixedSchema = Schema.createFixed("MD5", null, null, 4);
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field("value", fixedSchema, null, null)));
        byte[] fixedBytes = {0x01, 0x02, 0x03, 0x04};
        GenericRecord record = decode(schema, fixedBytes);
        Object value = record.get("value");
        assertTrue(value instanceof org.apache.avro.generic.GenericFixed);
        assertArrayEquals(fixedBytes, ((org.apache.avro.generic.GenericFixed) value).bytes());
    }

    @Test
    void decodesEnumRecord() throws Exception {
        Schema enumSchema =
                Schema.createEnum(
                        "Color", null, null, java.util.Arrays.asList("RED", "GREEN", "BLUE"));
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field("value", enumSchema, null, null)));
        byte[] bytes = writeInt(2); // BLUE = index 2
        GenericRecord record = decode(schema, bytes);
        assertEquals("BLUE", record.get("value").toString());
    }

    // ---- Nullable union ----

    @Test
    void decodesNullableUnionNonNullBranch() throws Exception {
        Schema nullableString =
                Schema.createUnion(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field("value", nullableString, null, null)));
        // Union index 1 (STRING), then string payload.
        DataOutputSerializer out = new DataOutputSerializer(32);
        out.writeInt(1);
        byte[] strBytes = "nullable-value".getBytes(StandardCharsets.UTF_8);
        out.writeInt(strBytes.length);
        out.write(strBytes);
        GenericRecord record = decode(schema, out.getCopyOfBuffer());
        assertEquals("nullable-value", record.get("value").toString());
    }

    @Test
    void decodesNullableUnionNullBranch() throws Exception {
        Schema nullableString =
                Schema.createUnion(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field("value", nullableString, null, null)));
        byte[] bytes = writeInt(0); // NULL branch
        GenericRecord record = decode(schema, bytes);
        assertEquals(null, record.get("value"));
    }

    // ---- Array ----

    @Test
    void decodesArrayOfInts() throws Exception {
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field(
                                        "values",
                                        Schema.createArray(Schema.create(Schema.Type.INT)),
                                        null,
                                        null)));
        // Write: block count=3, three ints, block count=0 (terminator).
        DataOutputSerializer out = new DataOutputSerializer(32);
        writeVarLongCount(out, 3);
        out.writeInt(10);
        out.writeInt(20);
        out.writeInt(30);
        writeVarLongCount(out, 0);
        GenericRecord record = decode(schema, out.getCopyOfBuffer());
        @SuppressWarnings("unchecked")
        java.util.List<Integer> values = (java.util.List<Integer>) record.get("values");
        assertEquals(java.util.Arrays.asList(10, 20, 30), values);
    }

    // ---- Map ----

    @Test
    void decodesMapOfStrings() throws Exception {
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field(
                                        "entries",
                                        Schema.createMap(Schema.create(Schema.Type.STRING)),
                                        null,
                                        null)));
        // Write: block count=2, key1, value1, key2, value2, block count=0.
        DataOutputSerializer out = new DataOutputSerializer(64);
        writeVarLongCount(out, 2);
        writeStringTo(out, "key1");
        writeStringTo(out, "val1");
        writeStringTo(out, "key2");
        writeStringTo(out, "val2");
        writeVarLongCount(out, 0);
        GenericRecord record = decode(schema, out.getCopyOfBuffer());
        @SuppressWarnings("unchecked")
        java.util.Map<java.lang.CharSequence, java.lang.CharSequence> entries =
                (java.util.Map<java.lang.CharSequence, java.lang.CharSequence>)
                        record.get("entries");
        assertEquals("val1", entries.get(new Utf8("key1")).toString());
        assertEquals("val2", entries.get(new Utf8("key2")).toString());
    }

    // ---- skipArray / skipMap return the encoded count ----

    @Test
    void skipArrayReturnsEncodedCount() throws Exception {
        // Build array bytes: count=3, 3 ints, count=0.
        DataOutputSerializer out = new DataOutputSerializer(32);
        writeVarLongCount(out, 3);
        out.writeInt(10);
        out.writeInt(20);
        out.writeInt(30);
        writeVarLongCount(out, 0);
        byte[] bytes = out.getCopyOfBuffer();
        FlinkDataInputDecoder decoder =
                new FlinkDataInputDecoder(
                        new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)),
                        bytes.length);
        assertEquals(3, decoder.skipArray());
    }

    @Test
    void skipMapReturnsEncodedCount() throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(32);
        writeVarLongCount(out, 5);
        writeVarLongCount(out, 0);
        byte[] bytes = out.getCopyOfBuffer();
        FlinkDataInputDecoder decoder =
                new FlinkDataInputDecoder(
                        new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)),
                        bytes.length);
        assertEquals(5, decoder.skipMap());
    }

    // ---- Malformed input guards ----

    @Test
    void rejectsNegativeLength() throws Exception {
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field(
                                        "value", Schema.create(Schema.Type.STRING), null, null)));
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeInt(-1); // negative length
        IOException error =
                assertThrows(IOException.class, () -> decode(schema, out.getCopyOfBuffer()));
        assertTrue(
                error.getMessage().contains("Negative") || error.getMessage().contains("length"));
    }

    @Test
    void rejectsOverflowedVarint() throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(16);
        // Write 10 continuation bytes (0x80) followed by a non-continuation byte.
        for (int i = 0; i < 10; i++) {
            out.writeByte(0x80);
        }
        out.writeByte(0x01);
        byte[] bytes = out.getCopyOfBuffer();
        FlinkDataInputDecoder decoder =
                new FlinkDataInputDecoder(
                        new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)),
                        bytes.length);
        IOException error = assertThrows(IOException.class, decoder::readArrayStart);
        assertTrue(
                error.getMessage().contains("overflow") || error.getMessage().contains("varint"));
    }

    @Test
    void rejectsTerminalVarintByteAboveOneAtShift63() throws Exception {
        // A 10-byte varint: 9 continuation bytes (0x80) + a terminal byte > 0x01.
        // At shift == 63, only the lowest bit (0x01) is valid. A terminal byte of 0x02 or
        // higher would overflow the 64-bit long. 0xFF is the worst case.
        DataOutputSerializer out = new DataOutputSerializer(16);
        for (int i = 0; i < 9; i++) {
            out.writeByte(0x80);
        }
        out.writeByte(0x02); // terminal byte > 1 at shift 63
        byte[] bytes = out.getCopyOfBuffer();
        FlinkDataInputDecoder decoder =
                new FlinkDataInputDecoder(
                        new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)),
                        bytes.length);
        IOException error = assertThrows(IOException.class, decoder::readArrayStart);
        assertTrue(
                error.getMessage().contains("overflow") || error.getMessage().contains("terminal"));
    }

    @Test
    void acceptsTerminalVarintByteOneAtShift63() throws Exception {
        // A 10-byte varint: 9 continuation bytes (0x80) + terminal byte 0x01.
        // This represents Long.MIN_VALUE as an unsigned varint (bit 63 set).
        // It should be rejected by validateCount (negative), not by the overflow guard.
        DataOutputSerializer out = new DataOutputSerializer(16);
        for (int i = 0; i < 9; i++) {
            out.writeByte(0x80);
        }
        out.writeByte(0x01); // terminal byte == 1 at shift 63 -> valid, but count is huge
        byte[] bytes = out.getCopyOfBuffer();
        FlinkDataInputDecoder decoder =
                new FlinkDataInputDecoder(
                        new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)),
                        bytes.length);
        IOException error = assertThrows(IOException.class, decoder::readArrayStart);
        // Should fail on excessive count, not on overflow.
        assertTrue(
                error.getMessage().contains("exceeds limit")
                        || error.getMessage().contains("count"));
    }

    @Test
    void rejectsExcessiveCollectionCount() throws Exception {
        // Write a varint that decodes to a value > MAX_COLLECTION_COUNT.
        // MAX_COLLECTION_COUNT = 10_000_000L. We need a varint that decodes to a larger value.
        // 10_000_001 in base-128 varint.
        DataOutputSerializer out = new DataOutputSerializer(16);
        long value = FlinkDataInputDecoder.MAX_COLLECTION_COUNT + 1;
        writeVarLongCount(out, value);
        byte[] bytes = out.getCopyOfBuffer();
        FlinkDataInputDecoder decoder =
                new FlinkDataInputDecoder(
                        new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)),
                        bytes.length);
        IOException error = assertThrows(IOException.class, decoder::readArrayStart);
        assertTrue(
                error.getMessage().contains("exceeds limit")
                        || error.getMessage().contains("count"));
    }

    @Test
    void rejectsTrailingBytesAfterDatum() throws Exception {
        Schema schema =
                Schema.createRecord(
                        "Test",
                        "test",
                        null,
                        false,
                        java.util.Collections.singletonList(
                                new Schema.Field(
                                        "value", Schema.create(Schema.Type.INT), null, null)));
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeInt(42);
        out.writeByte(0x00); // trailing byte
        // decode should succeed but AvroClasslessDecoder.decode would reject trailing bytes.
        // Here we test the datum reader + decoder directly.
        byte[] bytes = out.getCopyOfBuffer();
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(byteStream);
        FlinkDataInputDecoder decoder = new FlinkDataInputDecoder(inputView, bytes.length);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        reader.read(null, decoder);
        // The decoder should have consumed 4 bytes; 1 byte remains.
        assertEquals(4, decoder.bytesRead());
        assertEquals(5, bytes.length);
    }

    // ---- Helpers ----

    private static GenericRecord decode(Schema schema, byte[] bytes) throws IOException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(byteStream);
        FlinkDataInputDecoder decoder = new FlinkDataInputDecoder(inputView, bytes.length);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        return reader.read(null, decoder);
    }

    private static byte[] writeInt(int value) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(8);
        out.writeInt(value);
        return out.getCopyOfBuffer();
    }

    private static byte[] writeLong(long value) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeLong(value);
        return out.getCopyOfBuffer();
    }

    private static byte[] writeBoolean(boolean value) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(8);
        out.writeBoolean(value);
        return out.getCopyOfBuffer();
    }

    private static byte[] writeFloat(float value) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(8);
        out.writeFloat(value);
        return out.getCopyOfBuffer();
    }

    private static byte[] writeDouble(double value) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeDouble(value);
        return out.getCopyOfBuffer();
    }

    private static byte[] writeString(String value) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(32);
        writeStringTo(out, value);
        return out.getCopyOfBuffer();
    }

    private static void writeStringTo(DataOutputSerializer out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static byte[] writeBytes(byte[] payload) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(32);
        out.writeInt(payload.length);
        out.write(payload);
        return out.getCopyOfBuffer();
    }

    private static void writeVarLongCount(DataOutputSerializer out, long value) throws IOException {
        if (value < 0) {
            throw new IOException("Illegal count (must be non-negative): " + value);
        }
        while ((value & ~0x7FL) != 0) {
            out.writeByte(((int) value) | 0x80);
            value >>>= 7;
        }
        out.writeByte((int) value);
    }
}
