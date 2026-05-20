package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

class CobbleSourceEnumeratorStateSerializerTest {

    @Test
    void deserializesVersionOneStateWithInferredNextSnapshotId() throws Exception {
        CobbleSourceEnumeratorState.Serializer serializer =
                new CobbleSourceEnumeratorState.Serializer();

        ByteArrayOutputStream splitOut = new ByteArrayOutputStream();
        DataOutputStream splitData = new DataOutputStream(splitOut);
        splitData.writeInt(4);
        splitData.writeLong(12L);
        splitData.writeBoolean(true);
        byte[] manifestBytes =
                "file:///tmp/state-v1".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        splitData.writeInt(manifestBytes.length);
        splitData.write(manifestBytes);
        splitData.writeInt(-1);
        splitData.writeInt(-1);
        splitData.writeInt(-1);
        splitData.writeInt(CobbleSourceSplit.ScanPhase.FORWARD.ordinal());
        splitData.flush();
        byte[] v1Split = splitOut.toByteArray();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        dataOut.writeLong(12L);
        dataOut.writeInt(1);
        dataOut.writeInt(v1Split.length);
        dataOut.write(v1Split);
        dataOut.flush();

        CobbleSourceEnumeratorState restored = serializer.deserialize(1, out.toByteArray());

        assertEquals(12L, restored.currentSnapshotId);
        assertEquals(13L, restored.nextSnapshotId);
        assertEquals(1, restored.pendingSplits.size());
        assertEquals(4, restored.pendingSplits.get(0).splitId);
        assertEquals(4, restored.pendingSplits.get(0).bucketId);
    }

    @Test
    void roundTripsNextSnapshotId() throws Exception {
        CobbleSourceEnumeratorState.Serializer serializer =
                new CobbleSourceEnumeratorState.Serializer();
        CobbleSourceEnumeratorState state =
                new CobbleSourceEnumeratorState(
                        20L,
                        21L,
                        java.util.Collections.singletonList(
                                CobbleSourceSplit.forSnapshot(17, 3, 20L, "file:///tmp/manifest")));

        byte[] bytes = serializer.serialize(state);
        CobbleSourceEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(20L, restored.currentSnapshotId);
        assertEquals(21L, restored.nextSnapshotId);
        assertEquals(1, restored.pendingSplits.size());
        assertEquals(17, restored.pendingSplits.get(0).splitId);
        assertEquals(3, restored.pendingSplits.get(0).bucketId);
    }
}
