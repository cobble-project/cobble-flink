package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** Serializer coverage for checkpointed enumerator metadata. */
class CobbleSourceEnumeratorStateSerializerTest {

    @Test
    void roundTripsCurrentSnapshotState() throws Exception {
        CobbleSourceEnumeratorState.Serializer serializer =
                new CobbleSourceEnumeratorState.Serializer();
        CobbleSourceEnumeratorState state =
                new CobbleSourceEnumeratorState(
                        20L,
                        21L,
                        java.util.Collections.singletonList(
                                CobbleSourceSplit.forSnapshot(2, 3, 8, 20L)));

        byte[] bytes = serializer.serialize(state);
        CobbleSourceEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(20L, restored.currentSnapshotId);
        assertEquals(21L, restored.nextSnapshotId);
        assertEquals(1, restored.pendingSplits.size());
        assertEquals("2:3:8", restored.pendingSplits.get(0).splitId());
        assertEquals(2, restored.pendingSplits.get(0).rangeStartBucket);
        assertEquals(3, restored.pendingSplits.get(0).rangeEndBucket);
        assertEquals(8, restored.pendingSplits.get(0).totalBuckets);
        assertEquals(-1, restored.pendingSplits.get(0).startBucket);
        assertNull(restored.pendingSplits.get(0).startKeyExclusive);
        assertEquals(CobbleSourceSplit.ScanState.ACTIVE, restored.pendingSplits.get(0).scanState);
    }
}
