package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

class CobbleSourceSplitSerializerTest {

    @Test
    void roundTripsSplitIdIndependentlyFromBucketId() throws Exception {
        CobbleSourceSplit.Serializer serializer = new CobbleSourceSplit.Serializer();
        CobbleSourceSplit split =
                new CobbleSourceSplit(
                        17,
                        3,
                        9L,
                        "file:///tmp/manifest",
                        new byte[] {1, 2},
                        new byte[] {3},
                        new byte[] {4, 5},
                        CobbleSourceSplit.ScanPhase.WRAPPED);

        byte[] bytes = serializer.serialize(split);
        CobbleSourceSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(17, restored.splitId);
        assertEquals(3, restored.bucketId);
        assertEquals(9L, restored.snapshotId);
        assertEquals("file:///tmp/manifest", restored.manifestPath);
        assertArrayEquals(new byte[] {1, 2}, restored.lastConsumedKey);
        assertArrayEquals(new byte[] {3}, restored.anchorKey);
        assertArrayEquals(new byte[] {4, 5}, restored.resumeKey);
        assertEquals(CobbleSourceSplit.ScanPhase.WRAPPED, restored.phase);
    }

    @Test
    void deserializesVersionOneStateWithBucketAsSplitId() throws Exception {
        CobbleSourceSplit.Serializer serializer = new CobbleSourceSplit.Serializer();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        dataOut.writeInt(5);
        dataOut.writeLong(11L);
        writeString(dataOut, "file:///tmp/manifest-v1");
        writeBytes(dataOut, new byte[] {7});
        writeBytes(dataOut, null);
        writeBytes(dataOut, new byte[] {8, 9});
        dataOut.writeInt(CobbleSourceSplit.ScanPhase.FORWARD.ordinal());
        dataOut.flush();

        CobbleSourceSplit restored = serializer.deserialize(1, out.toByteArray());

        assertEquals(5, restored.splitId);
        assertEquals(5, restored.bucketId);
        assertEquals(11L, restored.snapshotId);
        assertEquals("file:///tmp/manifest-v1", restored.manifestPath);
        assertArrayEquals(new byte[] {7}, restored.lastConsumedKey);
        assertArrayEquals(new byte[] {8, 9}, restored.resumeKey);
        assertEquals(CobbleSourceSplit.ScanPhase.FORWARD, restored.phase);
    }

    private static void writeString(DataOutputStream out, String value) throws Exception {
        out.writeBoolean(true);
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static void writeBytes(DataOutputStream out, byte[] bytes) throws Exception {
        if (bytes == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }
}
