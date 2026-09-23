package io.cobble.flink.table;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/** Checkpointed key-group assignment and its one canonical logical-row resume position. */
final class CobbleStateSourceSplit implements SourceSplit, Serializable {
    private static final long serialVersionUID = 1L;

    final String splitId;
    final long checkpointId;
    final int totalKeyGroups;
    final int keyGroupStart;
    final int keyGroupEnd;
    final String operatorId;
    final String stateName;
    final String stateKind;
    final int resumeBucket;
    final byte[] resumePhysicalKey;
    final int resumeIntraEntryOffset;

    CobbleStateSourceSplit(
            String splitId,
            long checkpointId,
            int totalKeyGroups,
            int keyGroupStart,
            int keyGroupEnd,
            String operatorId,
            String stateName,
            String stateKind,
            int resumeBucket,
            byte[] resumePhysicalKey,
            int resumeIntraEntryOffset) {
        this.splitId = splitId;
        this.checkpointId = checkpointId;
        this.totalKeyGroups = totalKeyGroups;
        this.keyGroupStart = keyGroupStart;
        this.keyGroupEnd = keyGroupEnd;
        this.operatorId = operatorId;
        this.stateName = stateName;
        this.stateKind = stateKind;
        this.resumeBucket = resumeBucket;
        this.resumePhysicalKey = copyOrNull(resumePhysicalKey);
        this.resumeIntraEntryOffset = resumeIntraEntryOffset;
    }

    static CobbleStateSourceSplit forRange(
            long checkpointId,
            int totalKeyGroups,
            int keyGroupStart,
            int keyGroupEnd,
            String operatorId,
            String stateName,
            String stateKind) {
        return new CobbleStateSourceSplit(
                splitIdForRange(keyGroupStart, keyGroupEnd, totalKeyGroups),
                checkpointId,
                totalKeyGroups,
                keyGroupStart,
                keyGroupEnd,
                operatorId,
                stateName,
                stateKind,
                -1,
                null,
                0);
    }

    @Override
    public String splitId() {
        return splitId;
    }

    static String splitIdForRange(int keyGroupStart, int keyGroupEnd, int totalKeyGroups) {
        return keyGroupStart + ":" + keyGroupEnd + ":" + totalKeyGroups;
    }

    static final class Serializer implements SimpleVersionedSerializer<CobbleStateSourceSplit> {
        private static final int VERSION = 2;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(CobbleStateSourceSplit split) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(out);
            dataOut.writeUTF(split.splitId);
            dataOut.writeLong(split.checkpointId);
            dataOut.writeInt(split.totalKeyGroups);
            dataOut.writeInt(split.keyGroupStart);
            dataOut.writeInt(split.keyGroupEnd);
            dataOut.writeUTF(nullToEmpty(split.operatorId));
            dataOut.writeUTF(nullToEmpty(split.stateName));
            dataOut.writeUTF(nullToEmpty(split.stateKind));
            dataOut.writeInt(split.resumeBucket);
            writeBytes(dataOut, split.resumePhysicalKey);
            dataOut.writeInt(split.resumeIntraEntryOffset);
            dataOut.flush();
            return out.toByteArray();
        }

        @Override
        public CobbleStateSourceSplit deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != VERSION) {
                throw new IOException("Unsupported CobbleStateSourceSplit version: " + version);
            }
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized));
            return new CobbleStateSourceSplit(
                    input.readUTF(),
                    input.readLong(),
                    input.readInt(),
                    input.readInt(),
                    input.readInt(),
                    emptyToNull(input.readUTF()),
                    emptyToNull(input.readUTF()),
                    emptyToNull(input.readUTF()),
                    input.readInt(),
                    readBytes(input),
                    input.readInt());
        }

        private static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
            if (bytes == null) {
                out.writeInt(-1);
                return;
            }
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        private static byte[] readBytes(DataInputStream input) throws IOException {
            int length = input.readInt();
            if (length < 0) return null;
            byte[] bytes = new byte[length];
            input.readFully(bytes);
            return bytes;
        }

        private static String nullToEmpty(String value) {
            return value == null ? "" : value;
        }

        private static String emptyToNull(String value) {
            return value.isEmpty() ? null : value;
        }
    }

    private static byte[] copyOrNull(byte[] bytes) {
        return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }
}
