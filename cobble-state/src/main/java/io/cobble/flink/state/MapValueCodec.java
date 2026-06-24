package io.cobble.flink.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Encodes and decodes the user-value column of a Cobble {@link CobbleMapState} row.
 *
 * <p>The wire format matches Flink's canonical RocksDB MapState row value, so {@link
 * CanonicalSavepointRestoreOperation} can persist canonical bytes byte-for-byte while {@link
 * CobbleMapState} can read its own writes:
 *
 * <pre>
 *   byte isNull;                     // 0x01 = present null, 0x00 = present non-null
 *   if (!isNull) { userValueSerializer.serialize(value); }
 * </pre>
 *
 * <p>{@code DataOutputView.writeBoolean} emits 1 for {@code true} and 0 for {@code false}; we treat
 * any non-zero leading byte as "present null" on decode so bytes written by a differently tuned
 * serializer flavor still round-trip.
 *
 * <p>Cobble Flink is unreleased; this is the only Cobble MapState row-value format. There is no
 * backward-compatibility branch for the previous raw-value encoding.
 *
 * <p>Note: all decode entry points return Java {@code null} for a present-null entry. Call sites
 * distinguish "absent row" from "present-null row" by checking row presence separately (e.g. via
 * the {@code DirectEncodedRow} they already hold).
 */
final class MapValueCodec {

    static final byte PRESENT_NON_NULL = 0x00;
    static final byte PRESENT_NULL = 0x01;

    private MapValueCodec() {}

    /**
     * Returns a reusable {@link TypeSerializer} adapter that fronts {@code userValueSerializer}
     * with the {@code isNull} prefix. Callers in the hot path should cache the returned adapter for
     * the lifetime of the MapState instance and pass it to {@link #encode} / {@link #decode};
     * constructing a new adapter on every read or write would defeat the allocation-free reuse of
     * the direct-buffer (de)serializer infrastructure.
     */
    static <V> TypeSerializer<V> adapterFor(TypeSerializer<V> userValueSerializer) {
        return new BooleanPrefixedValue<>(userValueSerializer);
    }

    /**
     * Encodes a single user value (possibly null) into the caller-supplied reusable direct
     * serializer's buffer. {@code adapter} must be the one returned by {@link #adapterFor} for the
     * MapState's user-value serializer. Returns the slice owned by the serializer; the caller must
     * copy or write the bytes before reusing the serializer for another encode.
     */
    static <V> CobbleStateKeySerializer.DirectBufferSlice encode(
            CobbleStateKeySerializer.ReusableDirectValueSerializer reusable,
            TypeSerializer<V> adapter,
            V value)
            throws IOException {
        return reusable.serialize(adapter, value);
    }

    /**
     * Decodes a Cobble MapState row value from a direct buffer slice. {@code adapter} must be the
     * one returned by {@link #adapterFor}. Returns {@code null} for a present-null entry. The
     * buffer's position is advanced past the value bytes.
     */
    static <V> V decode(
            CobbleStateKeySerializer.ReusableDirectValueDeserializer reusable,
            TypeSerializer<V> adapter,
            ByteBuffer view)
            throws IOException {
        return reusable.deserialize(adapter, view);
    }

    /**
     * Decodes a Cobble MapState row value from an InputStream via the cached {@link #adapterFor
     * adapter}. Returns {@code null} for a present-null entry. Used by call sites that go through
     * {@code DirectEncodedRow.decodeBytesColumn(input -> ...)}.
     */
    static <V> V decode(TypeSerializer<V> adapter, InputStream input) throws IOException {
        return adapter.deserialize(new DataInputViewStreamWrapper(input));
    }

    /**
     * Validates that the given bytes are a well-formed Cobble/canonical MapState row value: the
     * leading isNull byte followed by exactly the expected payload (or nothing, for present-null).
     * Throws {@link IOException} if the payload is malformed or if there are trailing bytes after
     * the user value. Used by the canonical-savepoint restore path as a per-entry check immediately
     * before that entry is written, so the same bytes can be persisted verbatim afterwards. This is
     * not a transaction-wide preflight: earlier entries in the same restore may already have been
     * written when a later validate fails; restore-level error handling is responsible for cleaning
     * up partially-restored state.
     */
    static void validate(byte[] rowValueBytes, TypeSerializer<?> userValueSerializer)
            throws IOException {
        if (rowValueBytes == null || rowValueBytes.length == 0) {
            throw new IOException(
                    "MapState row value is empty; expected at least the isNull byte.");
        }
        DataInputDeserializer input = new DataInputDeserializer(rowValueBytes);
        byte marker = input.readByte();
        if (marker == PRESENT_NON_NULL) {
            userValueSerializer.deserialize(input);
        }
        if (input.available() != 0) {
            throw new IOException(
                    "MapState row value has "
                            + input.available()
                            + " trailing byte(s) after the user value.");
        }
    }

    /**
     * Adapter that lets {@link CobbleStateKeySerializer.ReusableDirectValueSerializer} and {@link
     * CobbleStateKeySerializer.ReusableDirectValueDeserializer} drive the isNull-prefixed codec via
     * their existing {@code TypeSerializer<T>} signatures, reusing their growing direct buffers.
     * The adapter is internal and is never snapshotted.
     */
    private static final class BooleanPrefixedValue<V> extends TypeSerializer<V> {
        private static final long serialVersionUID = 1L;

        private final TypeSerializer<V> userValueSerializer;

        BooleanPrefixedValue(TypeSerializer<V> userValueSerializer) {
            this.userValueSerializer = userValueSerializer;
        }

        @Override
        public boolean isImmutableType() {
            return userValueSerializer.isImmutableType();
        }

        @Override
        public TypeSerializer<V> duplicate() {
            return new BooleanPrefixedValue<>(userValueSerializer.duplicate());
        }

        @Override
        public V createInstance() {
            return userValueSerializer.createInstance();
        }

        @Override
        public V copy(V from) {
            return from == null ? null : userValueSerializer.copy(from);
        }

        @Override
        public V copy(V from, V reuse) {
            return from == null ? null : userValueSerializer.copy(from, reuse);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(V record, DataOutputView target) throws IOException {
            if (record == null) {
                target.writeByte(PRESENT_NULL);
                return;
            }
            target.writeByte(PRESENT_NON_NULL);
            userValueSerializer.serialize(record, target);
        }

        @Override
        public V deserialize(DataInputView source) throws IOException {
            byte marker = source.readByte();
            if (marker != PRESENT_NON_NULL) {
                return null;
            }
            return userValueSerializer.deserialize(source);
        }

        @Override
        public V deserialize(V reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            byte marker = source.readByte();
            target.writeByte(marker);
            if (marker == PRESENT_NON_NULL) {
                userValueSerializer.copy(source, target);
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof BooleanPrefixedValue)) {
                return false;
            }
            BooleanPrefixedValue<?> other = (BooleanPrefixedValue<?>) obj;
            return userValueSerializer.equals(other.userValueSerializer);
        }

        @Override
        public int hashCode() {
            return userValueSerializer.hashCode();
        }

        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<V>
                snapshotConfiguration() {
            throw new UnsupportedOperationException(
                    "BooleanPrefixedValue is an internal codec adapter and is never snapshotted.");
        }
    }
}
