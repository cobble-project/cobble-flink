package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/** Serializer coverage for checkpointed source split metadata. */
class CobbleSourceSplitSerializerTest {

    @Test
    void roundTripsCurrentSplitState() throws Exception {
        CobbleSourceSplit.Serializer serializer = new CobbleSourceSplit.Serializer();
        CobbleSourceSplit split =
                new CobbleSourceSplit(2, 3, 8, 9L, CobbleSourceSplit.ScanState.IDLE);

        byte[] bytes = serializer.serialize(split);
        CobbleSourceSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals("2:3:8", restored.splitId());
        assertEquals(2, restored.rangeStartBucket);
        assertEquals(3, restored.rangeEndBucket);
        assertEquals(8, restored.totalBuckets);
        assertEquals(9L, restored.snapshotId);
        assertEquals(CobbleSourceSplit.ScanState.IDLE, restored.scanState);
    }

    @Test
    void javaSerializesReplaceSplitEventPayload() throws Exception {
        CobbleSourceSplit split =
                new CobbleSourceSplit(2, 3, 8, 9L, CobbleSourceSplit.ScanState.IDLE);
        CobbleSourceEvents.ReplaceSplitEvent event =
                new CobbleSourceEvents.ReplaceSplitEvent(split);

        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        java.io.ObjectOutputStream objectOut = new java.io.ObjectOutputStream(out);
        objectOut.writeObject(event);
        objectOut.flush();

        java.io.ObjectInputStream objectIn =
                new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(out.toByteArray()));
        CobbleSourceEvents.ReplaceSplitEvent restored =
                (CobbleSourceEvents.ReplaceSplitEvent) objectIn.readObject();

        assertEquals("2:3:8", restored.split.splitId());
        assertEquals(2, restored.split.rangeStartBucket);
        assertEquals(3, restored.split.rangeEndBucket);
        assertEquals(8, restored.split.totalBuckets);
        assertEquals(9L, restored.split.snapshotId);
        assertEquals(CobbleSourceSplit.ScanState.IDLE, restored.split.scanState);
    }
}
