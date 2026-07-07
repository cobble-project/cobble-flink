package io.cobble.flink.common.inspect;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;

/**
 * Persisted metadata for a single Flink {@link TypeSerializer}, used by the monitor to restore a
 * live serializer and decode raw state bytes.
 *
 * <p>The serializer snapshot is the primary compatibility format. The optional serialized
 * serializer bytes are kept as a fallback for the monitor, which may need a concrete {@link
 * TypeSerializer} instance to decode values when the snapshot alone cannot restore one in the
 * monitor classloader.
 *
 * <p>The snapshot-derived {@link StateInspectType} is always persisted (when snapshot capture
 * succeeds), so the monitor can render semantic schemas without deserializing snapshot bytes.
 *
 * <p>Binary layout written by {@link #write(DataOutputView)}:
 *
 * <ol>
 *   <li>UTF serializer class name
 *   <li>fixed length tag ({@code serializer.getLength()}); {@code -1} means variable-sized
 *   <li>UTF snapshot class name (empty when snapshot capture failed)
 *   <li>boolean hasSnapshotBytes + snapshot bytes (when present)
 *   <li>boolean hasInspectType + {@link StateInspectType} (when present)
 *   <li>boolean hasSerializedSerializer + serialized serializer bytes (when present)
 * </ol>
 */
public final class SerializerInspectSchema {

    private final String serializerClassName;
    private final int lengthTag;
    private final String snapshotClassName;
    private final byte[] snapshotBytes;
    private final StateInspectType inspectType;
    private final byte[] serializedSerializerBytes;

    private SerializerInspectSchema(
            String serializerClassName,
            int lengthTag,
            String snapshotClassName,
            byte[] snapshotBytes,
            StateInspectType inspectType,
            byte[] serializedSerializerBytes) {
        this.serializerClassName = serializerClassName;
        this.lengthTag = lengthTag;
        this.snapshotClassName = snapshotClassName;
        this.snapshotBytes = snapshotBytes;
        this.inspectType = inspectType;
        this.serializedSerializerBytes = serializedSerializerBytes;
    }

    /**
     * Captures a serializer's metadata for monitoring.
     *
     * <p>Every step is individually guarded so that a misbehaving custom serializer cannot break
     * schema capture or checkpoint writing:
     *
     * <ul>
     *   <li>{@code snapshotConfiguration()} failure → snapshotBytes=null, snapshotClassName=null,
     *       inspectType=null; serialized fallback is attempted.
     *   <li>Snapshot type extraction failure → inspectType=null (UNKNOWN at read time); snapshot
     *       bytes are still written.
     *   <li>Snapshot serialization failure → snapshotBytes=null; serialized fallback is attempted.
     *   <li>Serialized fallback failure → serializedSerializerBytes=null; monitor uses raw
     *       fallback.
     * </ul>
     */
    public static <T> SerializerInspectSchema fromSerializer(TypeSerializer<T> serializer) {
        String className = serializer.getClass().getName();
        int lengthTag;
        try {
            lengthTag = serializer.getLength();
        } catch (RuntimeException e) {
            lengthTag = -1;
        }

        // Step 1: capture the snapshot object — may throw.
        TypeSerializerSnapshot<T> snapshot = null;
        try {
            snapshot = serializer.snapshotConfiguration();
        } catch (RuntimeException e) {
            snapshot = null;
        }
        String snapshotClassName = snapshot != null ? snapshot.getClass().getName() : null;

        // Step 2: derive inspectType — may throw, but must not affect snapshotBytes.
        StateInspectType inspectType = null;
        if (snapshot != null) {
            try {
                inspectType = SerializerSnapshotInspectTypeExtractor.extract(snapshot);
            } catch (RuntimeException e) {
                inspectType = null;
            }
        }

        // Step 3: serialize snapshot to bytes — may throw.
        byte[] snapshotBytes = null;
        if (snapshot != null) {
            snapshotBytes = captureSnapshotBytes(snapshot);
        }

        // Step 4: capture policy — conservative allowlist.
        boolean needFallback = snapshotBytes == null || !isMonitorPortable(snapshot);
        byte[] serializedBytes = needFallback ? captureSerializedSerializer(serializer) : null;

        return new SerializerInspectSchema(
                className,
                lengthTag,
                snapshotClassName,
                snapshotBytes,
                inspectType,
                serializedBytes);
    }

    private static <T> byte[] captureSnapshotBytes(TypeSerializerSnapshot<T> snapshot) {
        try {
            DataOutputSerializer buffer = new DataOutputSerializer(128);
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(buffer, snapshot);
            return buffer.getCopyOfBuffer();
        } catch (IOException e) {
            // Serialization to an in-memory buffer should not fail in practice; degrade gracefully.
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

    /**
     * Determines whether the monitor (which has Flink dependencies but NOT user job classes) can
     * restore a working serializer from the snapshot bytes alone. When this returns {@code true},
     * the serialized live serializer fallback is omitted to reduce sidecar size and classpath
     * sensitivity.
     *
     * <p>This uses a conservative simple-name allowlist rather than broad {@code instanceof}
     * checks, because some composite snapshots (e.g. {@code GenericArraySerializerSnapshot})
     * persist a component class that may be a user class.
     */
    private static boolean isMonitorPortable(TypeSerializerSnapshot<?> snapshot) {
        if (snapshot == null) {
            return false;
        }
        // Use getClass().getSimpleName() because it correctly handles nested classes like
        // RowDataSerializer$RowDataSerializerSnapshot (getSimpleName returns
        // "RowDataSerializerSnapshot", not "RowDataSerializer$RowDataSerializerSnapshot").
        String simpleName = snapshot.getClass().getSimpleName();

        // 1. SimpleTypeSerializerSnapshot — parameterless, singleton supplier, no user classes.
        //    Covers all Flink primitives/wrappers/string/date-time.
        if (snapshot instanceof SimpleTypeSerializerSnapshot) {
            return true;
        }

        // 2. Allowlisted composites — only List/Map/Tuple, recursively check nested snapshots.
        if (simpleName.equals("ListSerializerSnapshot")
                || simpleName.equals("MapSerializerSnapshot")
                || simpleName.equals("TupleSerializerSnapshot")) {
            if (!(snapshot instanceof CompositeTypeSerializerSnapshot)) {
                return false;
            }
            TypeSerializerSnapshot<?>[] nested =
                    ((CompositeTypeSerializerSnapshot<?, ?>) snapshot)
                            .getNestedSerializerSnapshots();
            if (nested == null || nested.length == 0) {
                return false;
            }
            for (TypeSerializerSnapshot<?> n : nested) {
                if (!isMonitorPortable(n)) {
                    return false;
                }
            }
            return true;
        }

        // 3. RowDataSerializerSnapshot — check nested serializers via reflection.
        //    Monitor has flink-table-runtime, so RowDataSerializer itself is restorable if all
        //    nested field serializers are monitor-portable.
        if (simpleName.equals("RowDataSerializerSnapshot")) {
            return isRowDataSnapshotPortable(snapshot);
        }

        // 4. Everything else: GenericArraySerializerSnapshot, PojoSerializerSnapshot,
        //    AvroSerializerSnapshot, unknown composite, custom → NOT portable.
        return false;
    }

    /**
     * Checks whether a RowDataSerializerSnapshot's nested serializers are all monitor-portable.
     * Uses reflection to read the {@code nestedSerializersSnapshotDelegate} field, which holds the
     * per-field serializer snapshots.
     */
    private static boolean isRowDataSnapshotPortable(TypeSerializerSnapshot<?> snapshot) {
        try {
            Field delegateField =
                    findField(snapshot.getClass(), "nestedSerializersSnapshotDelegate");
            if (delegateField == null) {
                return false;
            }
            delegateField.setAccessible(true);
            Object delegate = delegateField.get(snapshot);
            if (delegate == null) {
                return false;
            }
            Method getNested = delegate.getClass().getMethod("getNestedSerializerSnapshots");
            TypeSerializerSnapshot<?>[] nested =
                    (TypeSerializerSnapshot<?>[]) getNested.invoke(delegate);
            if (nested == null || nested.length == 0) {
                return false;
            }
            for (TypeSerializerSnapshot<?> n : nested) {
                if (!isMonitorPortable(n)) {
                    return false;
                }
            }
            return true;
        } catch (ReflectiveOperationException | RuntimeException e) {
            // If we can't determine portability, be conservative.
            return false;
        }
    }

    private static Field findField(Class<?> clazz, String name) {
        while (clazz != null) {
            try {
                return clazz.getDeclaredField(name);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        return null;
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

    /** Snapshot class name, or {@code null} when snapshot capture failed. */
    public String snapshotClassName() {
        return snapshotClassName;
    }

    /** Raw serializer snapshot bytes, or {@code null} when capture failed. */
    public byte[] snapshotBytes() {
        return snapshotBytes;
    }

    /**
     * Snapshot-derived semantic type, or {@code null} when snapshot capture or type extraction
     * failed. Callers should treat {@code null} as {@link StateInspectType#unknown()}.
     */
    public StateInspectType inspectType() {
        return inspectType;
    }

    /** Serialized serializer bytes, or {@code null} when not persisted or capture failed. */
    public byte[] serializedSerializerBytes() {
        return serializedSerializerBytes;
    }

    /**
     * Restores a concrete serializer. Priority: (1) restore from snapshot bytes, (2) fallback to
     * serialized serializer bytes, (3) return {@code null} (raw fallback). A snapshot-restore
     * failure falls through to the serialized fallback rather than returning {@code null}.
     */
    @SuppressWarnings("unchecked")
    public <T> TypeSerializer<T> restoreSerializer(ClassLoader classLoader) {
        // 1. Try snapshot restore.
        if (snapshotBytes != null) {
            try {
                DataInputView input =
                        new DataInputViewStreamWrapper(new ByteArrayInputStream(snapshotBytes));
                TypeSerializerSnapshot<T> snapshot =
                        TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                                input, classLoader);
                TypeSerializer<T> restored = snapshot.restoreSerializer();
                if (restored != null) {
                    return restored;
                }
            } catch (IOException | RuntimeException ignored) {
                // fall through to serialized bytes
            }
        }
        // 2. Fallback to serialized serializer bytes.
        if (serializedSerializerBytes != null) {
            try {
                return (TypeSerializer<T>)
                        InstantiationUtil.deserializeObject(serializedSerializerBytes, classLoader);
            } catch (IOException | ClassNotFoundException | RuntimeException ignored) {
                return null;
            }
        }
        // 3. Raw fallback.
        return null;
    }

    void write(DataOutputView output) throws IOException {
        output.writeUTF(serializerClassName != null ? serializerClassName : "");
        output.writeInt(lengthTag);
        output.writeUTF(snapshotClassName != null ? snapshotClassName : "");
        writeNullableBytes(output, snapshotBytes);
        writeNullableType(output, inspectType);
        writeNullableBytes(output, serializedSerializerBytes);
    }

    static SerializerInspectSchema read(DataInputView input) throws IOException {
        String className = input.readUTF();
        int lengthTag = input.readInt();
        String snapshotClassName = input.readUTF();
        byte[] snapshotBytes = readNullableBytes(input);
        StateInspectType inspectType = readNullableType(input);
        byte[] serializedBytes = readNullableBytes(input);
        return new SerializerInspectSchema(
                className,
                lengthTag,
                snapshotClassName,
                snapshotBytes,
                inspectType,
                serializedBytes);
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

    private static void writeNullableType(DataOutputView output, StateInspectType type)
            throws IOException {
        if (type == null) {
            output.writeBoolean(false);
            return;
        }
        output.writeBoolean(true);
        type.write(output);
    }

    private static StateInspectType readNullableType(DataInputView input) throws IOException {
        if (!input.readBoolean()) {
            return null;
        }
        return StateInspectType.read(input);
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
                && Objects.equals(snapshotClassName, that.snapshotClassName)
                && Arrays.equals(snapshotBytes, that.snapshotBytes)
                && Objects.equals(inspectType, that.inspectType)
                && Arrays.equals(serializedSerializerBytes, that.serializedSerializerBytes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(serializerClassName, lengthTag, snapshotClassName, inspectType);
        result = 31 * result + Arrays.hashCode(snapshotBytes);
        result = 31 * result + Arrays.hashCode(serializedSerializerBytes);
        return result;
    }
}
