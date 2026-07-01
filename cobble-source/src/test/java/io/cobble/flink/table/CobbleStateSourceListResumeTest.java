package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.Arrays;
import java.util.List;

/**
 * Unit coverage for the intra-entry resume mechanism that prevents ListState data loss when a
 * checkpoint fires mid-entry. A single native LIST entry decodes to multiple rows; the reader must
 * be able to snapshot after emitting a subset and resume without losing or duplicating rows.
 */
class CobbleStateSourceListResumeTest {

    /**
     * Simulates the core resume scenario: a list entry with 3 elements is decoded, 1 row is
     * emitted, a checkpoint saves the partial-entry state, and the reader resumes by re-decoding
     * with a skip. The remaining rows must be exactly elements 2 and 3 — no loss, no duplication.
     */
    @Test
    void listEntryResumeSkipsAlreadyEmittedRows() throws Exception {
        CobbleStateRowDecoder decoder = listDecoder();
        byte[] rowKey = concat(serialize(IntSerializer.INSTANCE, 1), namespaceBytes());
        byte[][] columns =
                new byte[][] {
                    concat(
                            serialize(IntSerializer.INSTANCE, 10),
                            new byte[] {','},
                            serialize(IntSerializer.INSTANCE, 20),
                            new byte[] {','},
                            serialize(IntSerializer.INSTANCE, 30))
                };

        // Initial decode: entry produces 3 rows [10, 20, 30].
        List<RowData> initial = decoder.decode(rowKey, columns, "0:0:4", 0);
        assertEquals(3, initial.size());
        assertEquals(10, initial.get(0).getInt(1));
        assertEquals(20, initial.get(1).getInt(1));
        assertEquals(30, initial.get(2).getInt(1));

        // Simulate: row 0 (value=10) emitted, checkpoint fires with partialEmittedCount=1.
        int emittedBeforeCheckpoint = 1;
        CobbleStateSourceSplit checkpointed =
                new CobbleStateSourceSplit(
                        "0:3:4",
                        7L,
                        4,
                        0,
                        3,
                        "operator-1",
                        "events",
                        "list",
                        0,
                        null,
                        rowKey,
                        emittedBeforeCheckpoint);

        // On resume, the reader re-reads the same entry and decodes with skip=1.
        List<RowData> resumed =
                decoder.decode(
                        checkpointed.partialEntryKey,
                        columns,
                        "0:3:4",
                        0,
                        checkpointed.partialEmittedCount);

        // Exactly the remaining 2 rows, in order, no duplication of row 0.
        assertEquals(2, resumed.size());
        assertEquals(20, resumed.get(0).getInt(1));
        assertEquals(30, resumed.get(1).getInt(1));
    }

    /**
     * Edge case: checkpoint fires after all rows from the entry are emitted. partialEntryKey is
     * null and partialEmittedCount is 0. Resume produces the next entry normally (no skip).
     */
    @Test
    void fullyConsumedEntryHasNoPartialState() throws Exception {
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
                        "0:3:4", 7L, 4, 0, 3, "operator-1", "events", "list", 0, rowKey, null, 0);

        assertTrue(checkpointed.partialEntryKey == null);
        assertEquals(0, checkpointed.partialEmittedCount);

        // On resume, the cursor starts from the boundary (inclusive), skips the boundary entry
        // (already consumed), and returns the next entry. No skip applied to the next entry.
        List<RowData> nextEntry =
                decoder.decode(
                        concat(serialize(IntSerializer.INSTANCE, 2), namespaceBytes()),
                        new byte[][] {serialize(IntSerializer.INSTANCE, 99)},
                        "0:3:4",
                        0,
                        0);
        assertEquals(1, nextEntry.size());
        assertEquals(99, nextEntry.get(0).getInt(1));
    }

    /**
     * The partial-entry state must survive serializer round-trip so a checkpoint barrier can
     * serialize it and the restored reader can pick up exactly where it left off.
     */
    @Test
    void partialEntryStateSurvivesSerializerRoundTrip() throws Exception {
        byte[] entryKey = concat(serialize(IntSerializer.INSTANCE, 1), namespaceBytes());
        CobbleStateSourceSplit split =
                new CobbleStateSourceSplit(
                        "0:3:4", 7L, 4, 0, 3, "operator-1", "events", "list", 0, null, entryKey, 2);

        CobbleStateSourceSplit.Serializer serializer = new CobbleStateSourceSplit.Serializer();
        byte[] bytes = serializer.serialize(split);
        CobbleStateSourceSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(7L, restored.checkpointId);
        assertEquals(0, restored.startKeyGroup);
        assertTrue(restored.startKeyExclusive == null);
        assertTrue(
                Arrays.equals(entryKey, restored.partialEntryKey),
                "partialEntryKey must survive round-trip");
        assertEquals(2, restored.partialEmittedCount);
    }

    /**
     * Skip count equal to the total number of decoded rows yields an empty result (all rows already
     * emitted). The reader treats this as "entry fully consumed" and advances to the next entry.
     */
    @Test
    void skipAllRowsYieldsEmpty() throws Exception {
        CobbleStateRowDecoder decoder = listDecoder();
        byte[] rowKey = concat(serialize(IntSerializer.INSTANCE, 1), namespaceBytes());
        byte[][] columns =
                new byte[][] {
                    concat(
                            serialize(IntSerializer.INSTANCE, 10),
                            new byte[] {','},
                            serialize(IntSerializer.INSTANCE, 20))
                };

        List<RowData> skipped = decoder.decode(rowKey, columns, "0:0:4", 0, 2);
        assertTrue(skipped.isEmpty());
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
        return new CobbleStateRowDecoder(
                config(
                        fields(
                                "key",
                                "INT",
                                StateSourceField.Group.STATE_KEY,
                                "value",
                                "INT",
                                StateSourceField.Group.LIST_ELEMENT)),
                new CobbleStateSourceRuntime.RuntimeSchema(
                        schema,
                        StateInspectSemanticSchema.forList(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT"))));
    }

    private static StateSourceConfig config(List<StateSourceField> fields) {
        return new StateSourceConfig(
                "file:///tmp/checkpoints",
                StateSourceConfig.Layout.CHECKPOINT_ROOT,
                "operator-1",
                "events",
                "list",
                "7",
                "batch",
                7L,
                -1,
                0L,
                fields);
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
