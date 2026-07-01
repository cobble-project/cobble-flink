package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Unit coverage for state-row decoding independent of native Cobble readers. */
class CobbleStateRowDecoderTest {

    @Test
    void decodesValueStateScalarKeyAndValue() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "orders",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        CobbleStateRowDecoder decoder =
                new CobbleStateRowDecoder(
                        config(
                                "value",
                                fields(
                                        "key",
                                        "INT",
                                        StateSourceField.Group.STATE_KEY,
                                        "value",
                                        "INT",
                                        StateSourceField.Group.VALUE)),
                        runtimeSchema(
                                schema,
                                StateInspectSemanticSchema.forValue(
                                        StateInspectType.scalar("INT"),
                                        StateInspectType.unknown(),
                                        StateInspectType.scalar("INT"))));

        List<RowData> rows =
                decoder.decode(
                        concat(serialize(IntSerializer.INSTANCE, 12), namespaceBytes()),
                        new byte[][] {serialize(IntSerializer.INSTANCE, 34)},
                        "0:0:1",
                        0);

        assertEquals(1, rows.size());
        assertEquals(12, rows.get(0).getInt(0));
        assertEquals(34, rows.get(0).getInt(1));
    }

    @Test
    void decodesListStateOneRowPerElement() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forList(
                        "events",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        CobbleStateRowDecoder decoder =
                new CobbleStateRowDecoder(
                        config(
                                "list",
                                fields(
                                        "key",
                                        "INT",
                                        StateSourceField.Group.STATE_KEY,
                                        "value",
                                        "INT",
                                        StateSourceField.Group.LIST_ELEMENT)),
                        runtimeSchema(
                                schema,
                                StateInspectSemanticSchema.forList(
                                        StateInspectType.scalar("INT"),
                                        StateInspectType.unknown(),
                                        StateInspectType.scalar("INT"))));

        List<RowData> rows =
                decoder.decode(
                        concat(serialize(IntSerializer.INSTANCE, 1), namespaceBytes()),
                        new byte[][] {
                            concat(
                                    serialize(IntSerializer.INSTANCE, 10),
                                    new byte[] {','},
                                    serialize(IntSerializer.INSTANCE, 20))
                        },
                        "0:0:1",
                        0);

        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(10, rows.get(0).getInt(1));
        assertEquals(1, rows.get(1).getInt(0));
        assertEquals(20, rows.get(1).getInt(1));
    }

    @Test
    void decodesMapStatePresentNullAsNullValue() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "counters",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        CobbleStateRowDecoder decoder =
                new CobbleStateRowDecoder(
                        config(
                                "map",
                                fields(
                                        "key",
                                        "INT",
                                        StateSourceField.Group.STATE_KEY,
                                        "map_key",
                                        "INT",
                                        StateSourceField.Group.MAP_KEY,
                                        "map_value",
                                        "INT",
                                        StateSourceField.Group.MAP_VALUE)),
                        runtimeSchema(
                                schema,
                                StateInspectSemanticSchema.forMap(
                                        StateInspectType.scalar("INT"),
                                        StateInspectType.unknown(),
                                        StateInspectType.scalar("INT"),
                                        StateInspectType.scalar("INT"))));

        List<RowData> rows =
                decoder.decode(
                        concat(
                                serialize(IntSerializer.INSTANCE, 7),
                                namespaceBytes(),
                                new byte[] {0},
                                serialize(IntSerializer.INSTANCE, 8)),
                        new byte[][] {new byte[] {1}},
                        "0:0:1",
                        0);

        assertEquals(1, rows.size());
        assertEquals(7, rows.get(0).getInt(0));
        assertEquals(8, rows.get(0).getInt(1));
        assertTrue(rows.get(0).isNullAt(2));
    }

    @Test
    void decodesNonVoidNamespace() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "orders",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        CobbleStateRowDecoder decoder =
                new CobbleStateRowDecoder(
                        config(
                                "value",
                                fields(
                                        "key",
                                        "INT",
                                        StateSourceField.Group.STATE_KEY,
                                        "namespace",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.NAMESPACE,
                                        "value",
                                        "INT",
                                        StateSourceField.Group.VALUE)),
                        runtimeSchema(
                                schema,
                                StateInspectSemanticSchema.forValue(
                                        StateInspectType.scalar("INT"),
                                        StateInspectType.scalar("VARCHAR(2147483647)"),
                                        StateInspectType.scalar("INT"))));

        List<RowData> rows =
                decoder.decode(
                        concat(
                                serialize(IntSerializer.INSTANCE, 5),
                                serialize(StringSerializer.INSTANCE, "ns")),
                        new byte[][] {serialize(IntSerializer.INSTANCE, 9)},
                        "0:0:1",
                        0);

        assertEquals(1, rows.size());
        assertEquals(5, rows.get(0).getInt(0));
        assertEquals("ns", rows.get(0).getString(1).toString());
        assertEquals(9, rows.get(0).getInt(2));
    }

    private static CobbleStateSourceRuntime.RuntimeSchema runtimeSchema(
            StateInspectSchema schema, StateInspectSemanticSchema semanticSchema) {
        return new CobbleStateSourceRuntime.RuntimeSchema(schema, semanticSchema);
    }

    private static StateSourceConfig config(String kind, List<StateSourceField> fields) {
        return new StateSourceConfig(
                "file:///tmp/checkpoints",
                StateSourceConfig.Layout.CHECKPOINT_ROOT,
                "operator-1",
                "orders",
                kind,
                "7",
                "batch",
                7L,
                -1,
                0L,
                fields);
    }

    private static List<StateSourceField> fields(Object... values) {
        java.util.ArrayList<StateSourceField> fields = new java.util.ArrayList<>();
        int[] indexByGroup = new int[StateSourceField.Group.values().length];
        for (int i = 0; i < values.length; i += 3) {
            StateSourceField.Group group = (StateSourceField.Group) values[i + 2];
            fields.add(
                    new StateSourceField(
                            (String) values[i],
                            (String) values[i + 1],
                            group,
                            indexByGroup[group.ordinal()]++));
        }
        return fields;
    }

    private static byte[] namespaceBytes() throws Exception {
        return serialize(VoidNamespaceSerializer.INSTANCE, VoidNamespace.INSTANCE);
    }

    private static <T> byte[] serialize(TypeSerializer<T> serializer, T value) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(32);
        serializer.serialize(value, out);
        return out.getCopyOfBuffer();
    }

    private static byte[] concat(byte[]... chunks) {
        int length = 0;
        for (byte[] chunk : chunks) {
            length += chunk.length;
        }
        byte[] out = new byte[length];
        int offset = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, out, offset, chunk.length);
            offset += chunk.length;
        }
        return out;
    }
}
