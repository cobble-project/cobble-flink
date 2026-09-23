package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.util.Collections;

/** Serializer coverage for checkpointed state source split metadata. */
class CobbleStateSourceSplitSerializerTest {

    @Test
    void roundTripsSplitState() throws Exception {
        CobbleStateSourceSplit.Serializer serializer = new CobbleStateSourceSplit.Serializer();
        CobbleStateSourceSplit split =
                new CobbleStateSourceSplit(
                        "2:3:8",
                        7L,
                        8,
                        2,
                        3,
                        "operator-1",
                        "orders",
                        "value",
                        3,
                        new byte[] {4, 5},
                        0);

        byte[] bytes = serializer.serialize(split);
        CobbleStateSourceSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals("2:3:8", restored.splitId());
        assertEquals(7L, restored.checkpointId);
        assertEquals(8, restored.totalKeyGroups);
        assertEquals(2, restored.keyGroupStart);
        assertEquals(3, restored.keyGroupEnd);
        assertEquals("operator-1", restored.operatorId);
        assertEquals("orders", restored.stateName);
        assertEquals("value", restored.stateKind);
        assertEquals(3, restored.resumeBucket);
        org.junit.jupiter.api.Assertions.assertArrayEquals(
                new byte[] {4, 5}, restored.resumePhysicalKey);
        assertEquals(0, restored.resumeIntraEntryOffset);
    }

    @Test
    void roundTripsPartialEntryResumeState() throws Exception {
        CobbleStateSourceSplit.Serializer serializer = new CobbleStateSourceSplit.Serializer();
        CobbleStateSourceSplit split =
                new CobbleStateSourceSplit(
                        "2:3:8",
                        7L,
                        8,
                        2,
                        3,
                        "operator-1",
                        "events",
                        "list",
                        3,
                        new byte[] {4, 5},
                        2);

        byte[] bytes = serializer.serialize(split);
        CobbleStateSourceSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals("2:3:8", restored.splitId());
        assertEquals(7L, restored.checkpointId);
        assertEquals("list", restored.stateKind);
        assertEquals(3, restored.resumeBucket);
        org.junit.jupiter.api.Assertions.assertArrayEquals(
                new byte[] {4, 5}, restored.resumePhysicalKey);
        assertEquals(2, restored.resumeIntraEntryOffset);
    }

    @Test
    void roundTripsEnumeratorState() throws Exception {
        CobbleStateSourceSplit split =
                CobbleStateSourceSplit.forRange(7L, 8, 2, 3, "operator-1", "orders", "value");
        CobbleStateSourceEnumeratorState state =
                new CobbleStateSourceEnumeratorState(7L, Collections.singletonList(split));
        CobbleStateSourceEnumeratorState.Serializer serializer =
                new CobbleStateSourceEnumeratorState.Serializer();

        byte[] bytes = serializer.serialize(state);
        CobbleStateSourceEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(7L, restored.checkpointId);
        assertEquals(1, restored.pendingSplits.size());
        assertEquals("2:3:8", restored.pendingSplits.get(0).splitId());
    }
}
