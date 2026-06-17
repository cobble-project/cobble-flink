package io.cobble.flink.common.inspect;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Persisted metadata for a single Flink {@link TypeSerializer}, used by the monitor to restore a
 * live serializer and decode raw state bytes.
 *
 * <p>The serializer snapshot is the primary compatibility format. The optional serialized
 * serializer bytes are kept as a recovery helper for the monitor, which needs a concrete {@link
 * TypeSerializer} instance to decode values.
 *
 * <p>Binary layout written by {@link #write(DataOutputView)}:
 *
 * <ol>
 *   <li>UTF class name of the serializer
 *   <li>fixed length tag ({@code serializer.getLength()}); {@code -1} means variable-sized
 *   <li>boolean hasSnapshot
 *   <li>snapshot bytes (when present) written with {@link
 *       TypeSerializerSnapshotSerializationUtil#writeSerializerSnapshot}
 *   <li>boolean hasSerializedSerializer
 *   <li>serialized serializer bytes (when present) produced by {@link
 *       InstantiationUtil#serializeObject(Object)}
 * </ol>
 *
 * <p>The snapshot payload is length-prefixed so that readers unable to resolve the serializer
 * classes (e.g. the monitor when user state classes are missing) can skip it without corrupting the
 * surrounding schema store.
 */
public final class SerializerInspectSchema {

    private final String serializerClassName;
    private final int lengthTag;
    private final byte[] snapshotBytes;
    private final byte[] serializedSerializerBytes;

    private SerializerInspectSchema(
            String serializerClassName,
            int lengthTag,
            byte[] snapshotBytes,
            byte[] serializedSerializerBytes) {
        this.serializerClassName = serializerClassName;
        this.lengthTag = lengthTag;
        this.snapshotBytes = snapshotBytes;
        this.serializedSerializerBytes = serializedSerializerBytes;
    }

    /**
     * Captures a serializer's metadata for monitoring. Snapshot capture failures are tolerated: a
     * schema entry is still produced (class name and length tag when recoverable) so the monitor
     * can fall back to raw inspect instead of hiding the whole state. Every call into the
     * user-supplied serializer (including {@link TypeSerializer#getLength()} and {@link
     * TypeSerializer#snapshotConfiguration()}) is inside a try block, so a misbehaving custom
     * serializer cannot break schema capture or checkpoint writing.
     */
    public static <T> SerializerInspectSchema fromSerializer(TypeSerializer<T> serializer) {
        String className = serializer.getClass().getName();
        int lengthTag;
        try {
            lengthTag = serializer.getLength();
        } catch (RuntimeException e) {
            lengthTag = -1;
        }
        byte[] snapshot = captureSnapshot(serializer);
        byte[] serialized = captureSerializedSerializer(serializer);
        return new SerializerInspectSchema(className, lengthTag, snapshot, serialized);
    }

    private static <T> byte[] captureSnapshot(TypeSerializer<T> serializer) {
        try {
            TypeSerializerSnapshot<T> snapshot = serializer.snapshotConfiguration();
            DataOutputSerializer buffer = new DataOutputSerializer(128);
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(buffer, snapshot);
            return buffer.getCopyOfBuffer();
        } catch (IOException e) {
            // Serialization to an in-memory buffer should not fail in practice; degrade gracefully
            // so the monitor falls back to raw inspect rather than dropping the whole checkpoint.
            return null;
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static byte[] captureSerializedSerializer(TypeSerializer<?> serializer) {
        try {
            return InstantiationUtil.serializeObject(serializer);
        } catch (IOException e) {
            return null;
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** Serializer class name as reported by the backend at capture time. */
    public String serializerClassName() {
        return serializerClassName;
    }

    /**
     * Fixed length tag from {@link TypeSerializer#getLength()}; {@code -1} means variable-sized.
     */
    public int lengthTag() {
        return lengthTag;
    }

    /** Raw serializer snapshot bytes, or {@code null} when capture failed. */
    public byte[] snapshotBytes() {
        return snapshotBytes;
    }

    /** Serialized serializer bytes, or {@code null} when capture failed. */
    public byte[] serializedSerializerBytes() {
        return serializedSerializerBytes;
    }

    /**
     * Restores a concrete serializer using the serialized bytes when present, otherwise falls back
     * to restoring from the snapshot. Returns {@code null} when restoration fails (e.g. the class
     * loader of the monitor does not have the serializer class).
     */
    @SuppressWarnings("unchecked")
    public <T> TypeSerializer<T> restoreSerializer(ClassLoader classLoader) {
        if (serializedSerializerBytes != null) {
            try {
                return (TypeSerializer<T>)
                        InstantiationUtil.deserializeObject(serializedSerializerBytes, classLoader);
            } catch (IOException | ClassNotFoundException | RuntimeException ignored) {
                // fall through to snapshot restore
            }
        }
        if (snapshotBytes != null) {
            try {
                DataInputView input =
                        new DataInputViewStreamWrapper(new ByteArrayInputStream(snapshotBytes));
                TypeSerializerSnapshot<T> snapshot =
                        TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                                input, classLoader);
                return snapshot.restoreSerializer();
            } catch (IOException | RuntimeException ignored) {
                return null;
            }
        }
        return null;
    }

    void write(DataOutputView output) throws IOException {
        output.writeUTF(serializerClassName != null ? serializerClassName : "");
        output.writeInt(lengthTag);
        writeNullableBytes(output, snapshotBytes);
        writeNullableBytes(output, serializedSerializerBytes);
    }

    static SerializerInspectSchema read(DataInputView input) throws IOException {
        String className = input.readUTF();
        int lengthTag = input.readInt();
        byte[] snapshot = readNullableBytes(input);
        byte[] serialized = readNullableBytes(input);
        return new SerializerInspectSchema(className, lengthTag, snapshot, serialized);
    }

    private static void writeNullableBytes(DataOutputView output, byte[] bytes) throws IOException {
        if (bytes == null) {
            output.writeBoolean(false);
            return;
        }
        output.writeBoolean(true);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static byte[] readNullableBytes(DataInputView input) throws IOException {
        if (!input.readBoolean()) {
            return null;
        }
        int length = input.readInt();
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return bytes;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SerializerInspectSchema)) {
            return false;
        }
        SerializerInspectSchema that = (SerializerInspectSchema) other;
        return lengthTag == that.lengthTag
                && Objects.equals(serializerClassName, that.serializerClassName)
                && Arrays.equals(snapshotBytes, that.snapshotBytes)
                && Arrays.equals(serializedSerializerBytes, that.serializedSerializerBytes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(serializerClassName, lengthTag);
        result = 31 * result + Arrays.hashCode(snapshotBytes);
        result = 31 * result + Arrays.hashCode(serializedSerializerBytes);
        return result;
    }
}
