package io.cobble.flink.inspect.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.cobble.table.KeyCodec;
import io.cobble.table.LogicalTypes;
import io.cobble.table.Value;
import io.cobble.table.ValueCodec;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

class TableInspectDecoderTest {

    @Test
    void decodesNativeTableKeyAndProjectedValue() throws Exception {
        TableInspectSchema schema =
                new TableInspectSchema(CobbleTableInspectTestData.compositeSchema());
        InspectTarget target = InspectTarget.table("table", schema);
        byte[] key =
                KeyCodec.encode(
                        Arrays.asList(
                                LogicalTypes.string().notNull(), LogicalTypes.string().notNull()),
                        Arrays.asList(Value.string("us"), Value.string("42")));
        byte[] payload = ValueCodec.encode(LogicalTypes.string(), Value.string("hello"));

        TableInspectDecoder.DecodedRow row =
                TableInspectDecoder.decode(target, key, new byte[][] {payload}, new int[] {0});

        assertNull(row.decodeError);
        assertEquals("us", row.decodedKey.get(0).get("value"));
        assertEquals("42", row.decodedKey.get(1).get("value"));
        assertEquals("hello", row.decodedColumns.get(0).get("value"));
    }

    @Test
    void encodesTypedKeyPrefixInPrimaryKeyOrder() throws Exception {
        TableInspectSchema schema =
                new TableInspectSchema(CobbleTableInspectTestData.compositeSchema());
        InspectTarget target = InspectTarget.table("table", schema);

        byte[] encoded = TableInspectDecoder.encodeKeyPrefix(target, Arrays.asList("us", "42"));
        assertEquals(
                Arrays.asList(Value.string("us"), Value.string("42")),
                KeyCodec.decode(
                        Arrays.asList(
                                LogicalTypes.string().notNull(), LogicalTypes.string().notNull()),
                        java.nio.ByteBuffer.wrap(encoded)));
        assertEquals(
                0, TableInspectDecoder.encodeKeyPrefix(target, Collections.emptyList()).length);
    }
}
