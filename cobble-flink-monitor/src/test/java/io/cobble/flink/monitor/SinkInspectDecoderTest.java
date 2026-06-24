package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

class SinkInspectDecoderTest {

    @Test
    void decodesCompositeKeyAndValueColumns() throws Exception {
        SinkInspectSchema schema =
                new SinkInspectSchema(
                        Arrays.asList(
                                SinkInspectField.key("id", "BIGINT", 0, -1),
                                SinkInspectField.key("tenant", "VARCHAR(2147483647)", 1, -1)),
                        Arrays.asList(
                                SinkInspectField.value("name", "VARCHAR(2147483647)", 2, 0),
                                SinkInspectField.value("score", "INT", 3, 1),
                                SinkInspectField.value("active", "BOOLEAN", 4, 2)));
        InspectTarget target = InspectTarget.sink("sink", schema);

        SinkInspectDecoder.DecodedRow row =
                SinkInspectDecoder.decode(
                        target,
                        key(
                                field("BIGINT", Long.valueOf(42L)),
                                field("VARCHAR(2147483647)", "acme")),
                        new byte[][] {
                            field("VARCHAR(2147483647)", "alice"),
                            field("INT", Integer.valueOf(99)),
                            field("BOOLEAN", Boolean.TRUE)
                        },
                        null);

        assertNull(row.decodeError);
        assertEquals(Long.valueOf(42L), row.decodedKey.get(0).get("value"));
        assertEquals("acme", row.decodedKey.get(1).get("value"));
        assertEquals("name", row.decodedColumns.get(0).get("name"));
        assertEquals("alice", row.decodedColumns.get(0).get("value"));
        assertEquals("score", row.decodedColumns.get(1).get("name"));
        assertEquals(Integer.valueOf(99), row.decodedColumns.get(1).get("value"));
        assertEquals(Boolean.TRUE, row.decodedColumns.get(2).get("value"));
    }

    @Test
    void preservesUnsafeBigintAcrossTheJsonBoundary() throws Exception {
        SinkInspectSchema schema =
                new SinkInspectSchema(
                        Collections.singletonList(SinkInspectField.key("id", "BIGINT", 0, -1)),
                        Collections.singletonList(SinkInspectField.value("value", "BIGINT", 1, 0)));
        InspectTarget target = InspectTarget.sink("sink", schema);

        SinkInspectDecoder.DecodedRow row =
                SinkInspectDecoder.decode(
                        target,
                        key(field("BIGINT", Long.valueOf(Long.MAX_VALUE))),
                        new byte[][] {field("BIGINT", Long.valueOf(Long.MIN_VALUE))},
                        null);

        assertNull(row.decodeError);
        Object decodedKey = row.decodedKey.get(0).get("value");
        Object decodedValue = row.decodedColumns.get(0).get("value");
        assertEquals(Long.toString(Long.MAX_VALUE), decodedKey.toString());
        assertEquals(Long.toString(Long.MIN_VALUE), decodedValue.toString());
        assertEquals(
                "[\"" + Long.MAX_VALUE + "\",\"" + Long.MIN_VALUE + "\"]",
                CobbleFlinkMonitorServer.toJson(Arrays.asList(decodedKey, decodedValue)));
    }

    @Test
    void decodesOnlyProjectedColumns() throws Exception {
        SinkInspectSchema schema =
                new SinkInspectSchema(
                        Collections.singletonList(SinkInspectField.key("id", "BIGINT", 0, -1)),
                        Arrays.asList(
                                SinkInspectField.value("name", "VARCHAR(2147483647)", 1, 0),
                                SinkInspectField.value("score", "INT", 2, 1)));
        InspectTarget target = InspectTarget.sink("sink", schema);

        SinkInspectDecoder.DecodedRow row =
                SinkInspectDecoder.decode(
                        target,
                        key(field("BIGINT", Long.valueOf(7L))),
                        new byte[][] {field("INT", Integer.valueOf(12))},
                        new int[] {1});

        assertNull(row.decodeError);
        assertEquals(1, row.decodedColumns.size());
        assertEquals("score", row.decodedColumns.get(0).get("name"));
        assertEquals(Integer.valueOf(12), row.decodedColumns.get(0).get("value"));
    }

    @Test
    void nullValueColumnDecodesAsNull() throws Exception {
        SinkInspectSchema schema =
                new SinkInspectSchema(
                        Collections.singletonList(SinkInspectField.key("id", "BIGINT", 0, -1)),
                        Collections.singletonList(
                                SinkInspectField.value("name", "VARCHAR(2147483647)", 1, 0)));
        InspectTarget target = InspectTarget.sink("sink", schema);

        SinkInspectDecoder.DecodedRow row =
                SinkInspectDecoder.decode(
                        target, key(field("BIGINT", Long.valueOf(7L))), new byte[][] {null}, null);

        assertNull(row.decodeError);
        assertNull(row.decodedColumns.get(0).get("value"));
    }

    @Test
    void badFieldBytesReturnDecodeErrorAndRawFallback() throws Exception {
        SinkInspectSchema schema =
                new SinkInspectSchema(
                        Collections.singletonList(SinkInspectField.key("id", "BIGINT", 0, -1)),
                        Collections.singletonList(SinkInspectField.value("score", "INT", 1, 0)));
        InspectTarget target = InspectTarget.sink("sink", schema);

        SinkInspectDecoder.DecodedRow row =
                SinkInspectDecoder.decode(
                        target, key(field("BIGINT", Long.valueOf(7L))), new byte[][] {{1}}, null);

        assertNotNull(row.decodeError);
        assertTrue(row.decodeError.contains("score"));
        assertEquals(
                CobbleFlinkMonitorServer.bytesJson(new byte[] {1}),
                row.decodedColumns.get(0).get("value"));
    }

    @Test
    void encodesLeadingCompositeKeyFieldsForPrefixScan() throws Exception {
        SinkInspectSchema schema =
                new SinkInspectSchema(
                        Arrays.asList(
                                SinkInspectField.key("id", "BIGINT", 0, -1),
                                SinkInspectField.key("tenant", "VARCHAR(2147483647)", 1, -1)),
                        Collections.singletonList(
                                SinkInspectField.value("value", "VARCHAR(2147483647)", 2, 0)));
        InspectTarget target = InspectTarget.sink("sink", schema);

        assertEquals(
                Base64.getEncoder().encodeToString(key(field("BIGINT", Long.valueOf(42L)))),
                Base64.getEncoder()
                        .encodeToString(
                                SinkInspectDecoder.encodeKeyPrefix(
                                        target, Collections.singletonList("42"))));
        assertEquals(
                Base64.getEncoder()
                        .encodeToString(
                                key(
                                        field("BIGINT", Long.valueOf(42L)),
                                        field("VARCHAR(2147483647)", "acme"))),
                Base64.getEncoder()
                        .encodeToString(
                                SinkInspectDecoder.encodeKeyPrefix(
                                        target, Arrays.asList("42", "acme"))));
    }

    private static byte[] key(byte[]... fields) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(128);
        for (byte[] field : fields) {
            output.writeInt(field.length);
            output.write(field);
        }
        return output.getCopyOfBuffer();
    }

    @SuppressWarnings("unchecked")
    private static byte[] field(String logicalTypeText, Object value) throws Exception {
        LogicalType logicalType = LogicalTypeParser.parse(logicalTypeText);
        TypeSerializer<Object> serializer =
                (TypeSerializer<Object>) InternalSerializers.create(logicalType);
        Object normalized = value;
        if (value instanceof String) {
            normalized = StringData.fromString((String) value);
        }
        DataOutputSerializer output = new DataOutputSerializer(64);
        serializer.serialize(normalized, output);
        return output.getCopyOfBuffer();
    }
}
