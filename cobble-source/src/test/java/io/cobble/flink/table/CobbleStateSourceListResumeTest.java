package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit coverage for checkpointed ListState positions. The shared table cursor owns intra-entry row
 * skipping when a physical LIST entry decodes to multiple rows.
 */
class CobbleStateSourceListResumeTest {

    /**
     * A completed entry persists its offset too, so restoring it re-reads and skips the entry
     * before continuing at the next physical row.
     */
    @Test
    void completedEntryRetainsConsumedOffset() throws Exception {
        CobbleStateRowDecoder decoder = listDecoder();
        byte[] rowKey = concat(serialize(IntSerializer.INSTANCE, 1), namespaceBytes());
        byte[][] columns =
                new byte[][] {
                    concat(
                            serialize(IntSerializer.INSTANCE, 10),
                            new byte[] {','},
                            serialize(IntSerializer.INSTANCE, 20))
                };

        // Full decode + emit all rows.
        List<RowData> initial = decoder.decode(rowKey, columns, "0:0:4", 0);
        assertEquals(2, initial.size());

        // Checkpoint after full consumption: boundary = entry key, no partial state.
        CobbleStateSourceSplit checkpointed =
                new CobbleStateSourceSplit(
                        "0:3:4", 7L, 4, 0, 3, "operator-1", "events", "list", 0, rowKey, 2);

        assertEquals(0, checkpointed.resumeBucket);
        assertEquals(2, checkpointed.resumeIntraEntryOffset);

        // The cursor skips this completed boundary entry and decodes the next one normally.
        List<RowData> nextEntry =
                decoder.decode(
                        concat(serialize(IntSerializer.INSTANCE, 2), namespaceBytes()),
                        new byte[][] {serialize(IntSerializer.INSTANCE, 99)},
                        "0:3:4",
                        0);
        assertEquals(1, nextEntry.size());
        assertEquals(99, nextEntry.get(0).getInt(1));
    }

    /** Repeated checkpoints within the same physical LIST entry retain the accumulated offset. */
    @Test
    void repeatedCheckpointsRetainAccumulatedIntraEntryOffset() throws Exception {
        byte[] entryKey = concat(serialize(IntSerializer.INSTANCE, 1), namespaceBytes());
        CobbleStateSourceSplit split =
                new CobbleStateSourceSplit(
                        "0:3:4", 7L, 4, 0, 3, "operator-1", "events", "list", 0, entryKey, 1);

        CobbleStateSourceSplit.Serializer serializer = new CobbleStateSourceSplit.Serializer();
        byte[] bytes = serializer.serialize(split);
        CobbleStateSourceSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(7L, restored.checkpointId);
        assertEquals(0, restored.resumeBucket);
        assertArrayEquals(entryKey, restored.resumePhysicalKey);
        assertEquals(1, restored.resumeIntraEntryOffset);

        CobbleStateSourceSplit secondCheckpoint =
                new CobbleStateSourceSplit(
                        restored.splitId,
                        restored.checkpointId,
                        restored.totalKeyGroups,
                        restored.keyGroupStart,
                        restored.keyGroupEnd,
                        restored.operatorId,
                        restored.stateName,
                        restored.stateKind,
                        restored.resumeBucket,
                        restored.resumePhysicalKey,
                        restored.resumeIntraEntryOffset + 1);
        CobbleStateSourceSplit secondRestore =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(secondCheckpoint));
        assertEquals(2, secondRestore.resumeIntraEntryOffset);
    }

    private static CobbleStateRowDecoder listDecoder() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forList(
                        "events",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        List<StateSourceField> fields =
                fields(
                        "key",
                        "INT",
                        StateSourceField.Group.STATE_KEY,
                        "value",
                        "INT",
                        StateSourceField.Group.LIST_ELEMENT);
        return new CobbleStateRowDecoder(
                schema,
                StateInspectSemanticSchema.forList(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("INT")),
                fields,
                "test");
    }

    private static List<StateSourceField> fields(Object... values) {
        List<StateSourceField> fields = new ArrayList<>();
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
