package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cobble.table.KeyCodec;
import io.cobble.table.LogicalTypes;
import io.cobble.table.Value;
import io.cobble.table.ValueCodec;

import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class CobbleRowDataDecodersTest {

    @Test
    void projectionDoesNotDecodeUnusedKeyOrValue() throws Exception {
        CobbleDynamicTableSource.SerializableConfig config =
                new CobbleDynamicTableSource.SerializableConfig(
                        "file:///unused",
                        1,
                        "latest",
                        "batch",
                        1L,
                        0L,
                        Collections.singletonList(
                                new CobbleDynamicTableSource.SerializableField(
                                        "id", "BIGINT NOT NULL", 0, -1)),
                        Collections.singletonList(
                                new CobbleDynamicTableSource.SerializableField(
                                        "score", "INT", 1, 0)));
        byte[] key =
                KeyCodec.encode(
                        Collections.singletonList(LogicalTypes.int64()),
                        Collections.singletonList(Value.int64(7L)));

        CobbleRowDataDecoders.RuntimeRowDecoder keyOnly =
                new CobbleRowDataDecoders.RuntimeRowDecoder(config.withProjection(new int[] {0}));
        assertEquals(
                1,
                CobbleSinkTableReadProvider.schemaFor(config.withProjection(new int[] {0}))
                        .fields()
                        .size());
        assertEquals(
                "id",
                CobbleSinkTableReadProvider.schemaFor(config.withProjection(new int[] {0}))
                        .fields()
                        .get(0)
                        .name());
        RowData keyRow = keyOnly.decode(key, new byte[][] {{0x7f}});
        assertEquals(1, keyRow.getArity());
        assertEquals(7L, keyRow.getLong(0));

        CobbleRowDataDecoders.RuntimeRowDecoder valueOnly =
                new CobbleRowDataDecoders.RuntimeRowDecoder(config.withProjection(new int[] {1}));
        RowData valueRow =
                valueOnly.decode(
                        new byte[] {0x7f},
                        new byte[][] {
                            ValueCodec.encode(LogicalTypes.int32().nullable(), Value.int32(42))
                        });
        assertEquals(1, valueRow.getArity());
        assertEquals(
                "score",
                CobbleSinkTableReadProvider.schemaFor(config.withProjection(new int[] {1}))
                        .fields()
                        .get(0)
                        .name());
        assertEquals(42, valueRow.getInt(0));

        CobbleRowDataDecoders.RuntimeRowDecoder literalOnly =
                new CobbleRowDataDecoders.RuntimeRowDecoder(config.withProjection(new int[0]));
        assertEquals(
                0,
                CobbleSinkTableReadProvider.schemaFor(config.withProjection(new int[0]))
                        .fields()
                        .size());
        assertEquals(0, literalOnly.decode(new byte[] {0x7f}, new byte[][] {{0x7f}}).getArity());
    }
}
