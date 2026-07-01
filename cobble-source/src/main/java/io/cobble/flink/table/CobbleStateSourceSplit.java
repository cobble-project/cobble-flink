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

/** Checkpointed metadata for one bounded key-group range in a Cobble state checkpoint. */
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
    final int startKeyGroup;
    final byte[] startKeyExclusive;
    // Intra-entry resume state for LIST (and any multi-row kind): when a checkpoint fires
    // mid-entry, partialEntryKey is the native entry being consumed and partialEmittedCount is how
    // many of its decoded rows have already been emitted. On restore the reader re-reads that entry
    // and skips the first partialEmittedCount rows. Both are null/0 when the entry was fully
    // emitted before the checkpoint.
    final byte[] partialEntryKey;
    final int partialEmittedCount;

    CobbleStateSourceSplit(
            String splitId,
            long checkpointId,
            int totalKeyGroups,
            int keyGroupStart,
            int keyGroupEnd,
            String operatorId,
            String stateName,
            String stateKind,
            int startKeyGroup,
            byte[] startKeyExclusive) {
        this(
                splitId,
                checkpointId,
                totalKeyGroups,
                keyGroupStart,
                keyGroupEnd,
                operatorId,
                stateName,
                stateKind,
                startKeyGroup,
                startKeyExclusive,
                null,
                0);
    }

    CobbleStateSourceSplit(
            String splitId,
            long checkpointId,
            int totalKeyGroups,
            int keyGroupStart,
            int keyGroupEnd,
            String operatorId,
            String stateName,
            String stateKind,
            int startKeyGroup,
            byte[] startKeyExclusive,
            byte[] partialEntryKey,
            int partialEmittedCount) {
        this.splitId = splitId;
        this.checkpointId = checkpointId;
        this.totalKeyGroups = totalKeyGroups;
        this.keyGroupStart = keyGroupStart;
        this.keyGroupEnd = keyGroupEnd;
        this.operatorId = operatorId;
        this.stateName = stateName;
        this.stateKind = stateKind;
        this.startKeyGroup = startKeyGroup;
        this.startKeyExclusive = copyOrNull(startKeyExclusive);
        this.partialEntryKey = copyOrNull(partialEntryKey);
        this.partialEmittedCount = partialEmittedCount;
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
                null);
    }

    CobbleStateSourceSplit withStartAfter(int keyGroup, byte[] keyExclusive) {
        return new CobbleStateSourceSplit(
                splitId,
                checkpointId,
                totalKeyGroups,
                keyGroupStart,
                keyGroupEnd,
                operatorId,
                stateName,
                stateKind,
                keyGroup,
                keyExclusive,
                null,
                0);
    }

    CobbleStateSourceSplit withPartialEntry(
            int keyGroup, byte[] keyExclusive, byte[] partialEntryKey, int partialEmittedCount) {
        return new CobbleStateSourceSplit(
                splitId,
                checkpointId,
                totalKeyGroups,
                keyGroupStart,
                keyGroupEnd,
                operatorId,
                stateName,
                stateKind,
                keyGroup,
                keyExclusive,
                partialEntryKey,
                partialEmittedCount);
    }

    @Override
    public String splitId() {
        return splitId;
    }

    static String splitIdForRange(int keyGroupStart, int keyGroupEnd, int totalKeyGroups) {
        return keyGroupStart + ":" + keyGroupEnd + ":" + totalKeyGroups;
    }

    /** Serializer for checkpointing state source split metadata. */
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
            dataOut.writeInt(split.startKeyGroup);
            writeBytes(dataOut, split.startKeyExclusive);
            writeBytes(dataOut, split.partialEntryKey);
            dataOut.writeInt(split.partialEmittedCount);
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
            if (length < 0) {
                return null;
            }
            byte[] bytes = new byte[length];
            input.readFully(bytes);
            return bytes;
        }

        private static String nullToEmpty(String value) {
            return value == null ? "" : value;
        }

        private static String emptyToNull(String value) {
            return value == null || value.isEmpty() ? null : value;
        }
    }

    private static byte[] copyOrNull(byte[] bytes) {
        return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }
}
