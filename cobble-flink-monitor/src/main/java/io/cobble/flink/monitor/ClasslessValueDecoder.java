package io.cobble.flink.monitor;

import io.cobble.flink.common.inspect.DescriptorCapability;
import io.cobble.flink.common.inspect.InspectDecoderDescriptor;
import io.cobble.flink.common.inspect.InspectDecoderDescriptorKind;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * Recursive classless value dispatcher. Decodes a single datum from a byte array or shared cursor,
 * dispatching to the appropriate classless decoder based on the {@link InspectDecoderDescriptor}
 * kind: POJO, AVRO, or PORTABLE_SNAPSHOT.
 *
 * <p>The dispatcher returns neutral {@code Object} values: {@link ClasslessPojoValue} for POJO
 * base/null rows, Avro {@code GenericRecord} for AVRO, or the restored-serializer result for
 * PORTABLE_SNAPSHOT. Tagged-subclass POJO rows return the child descriptor's neutral result as-is.
 *
 * <h2>Gatekeeper</h2>
 *
 * <ul>
 *   <li>{@link #shouldPreferClasslessSemanticDecode} - true for POJO and AVRO only (excludes
 *       PORTABLE_SNAPSHOT). Used by {@code StateInspectDecoder} for top-level semantic decode and
 *       legacy preview suppression. PORTABLE_SNAPSHOT top-level serializers continue using the
 *       existing snapshot-first {@code restore()} path.
 * </ul>
 *
 * <p>Recursive child dispatch (POJO fields, tagged subclasses, list elements) calls {@link
 * #decodeFromCursor} directly. An UNSUPPORTED child descriptor produces a descriptive {@code
 * IOException} via the {@code UNSUPPORTED} case in {@code decodeFromCursor}, which the caller wraps
 * with context (e.g. "POJO field 'x': ...").
 */
final class ClasslessValueDecoder {

    /** Immutable read cursor over a byte array, shared across recursive decode calls. */
    static final class DecodeCursor {
        private final int length;
        private final ByteArrayInputStream byteStream;
        private final DataInputViewStreamWrapper inputView;

        DecodeCursor(byte[] bytes) {
            this.length = bytes.length;
            this.byteStream = new ByteArrayInputStream(bytes);
            this.inputView = new DataInputViewStreamWrapper(byteStream);
        }

        DataInputViewStreamWrapper input() {
            return inputView;
        }

        /** Number of bytes consumed from this cursor's complete byte array. */
        int position() {
            return length - byteStream.available();
        }

        int remaining() {
            return byteStream.available();
        }

        void requireFullyConsumed(String label) throws IOException {
            if (remaining() != 0) {
                throw new DecodeFailureException(
                        DecodeIssueKind.MALFORMED_BYTES,
                        "Trailing bytes after "
                                + label
                                + ": consumed "
                                + position()
                                + " of "
                                + length
                                + " bytes");
            }
        }
    }

    private ClasslessValueDecoder() {}

    /**
     * Decodes exactly one datum from the byte array and rejects trailing bytes.
     *
     * @throws IOException if decoding fails or trailing bytes remain.
     */
    static Object decode(InspectDecoderDescriptor descriptor, byte[] bytes) throws IOException {
        DecodeCursor cursor = new DecodeCursor(bytes);
        Object result = decodeFromCursor(descriptor, cursor);
        cursor.requireFullyConsumed("datum");
        return result;
    }

    /**
     * Consumes exactly one datum from the shared cursor. The caller owns trailing-byte validation.
     *
     * @throws IOException if decoding fails.
     */
    static Object decodeFromCursor(InspectDecoderDescriptor descriptor, DecodeCursor cursor)
            throws IOException {
        InspectDecoderDescriptorKind kind = descriptor.kind();
        switch (kind) {
            case POJO:
                return PojoInspectDecoder.decodeFromCursor(descriptor, cursor);
            case AVRO:
                return AvroClasslessDecoder.decodeFromCursor(descriptor, cursor);
            case PORTABLE_SNAPSHOT:
                return decodePortableSnapshot(descriptor, cursor);
            case UNSUPPORTED:
                throw new DecodeFailureException(
                        DecodeIssueKind.CLASSLESS_UNSUPPORTED,
                        "Unsupported: " + descriptor.unsupportedReason());
            default:
                throw new DecodeFailureException(
                        DecodeIssueKind.UNKNOWN, "Unknown descriptor kind: " + kind);
        }
    }

    /**
     * Returns {@code true} if the top-level semantic decode should prefer the classless path over
     * the restored-serializer path. This is true for POJO and AVRO only; PORTABLE_SNAPSHOT
     * top-level serializers continue using the existing snapshot-first {@code restore()} path.
     */
    static boolean shouldPreferClasslessSemanticDecode(InspectDecoderDescriptor descriptor) {
        return descriptor != null
                && (descriptor.isPojo() || descriptor.isAvro())
                && descriptor.capability() != DescriptorCapability.UNSUPPORTED;
    }

    private static Object decodePortableSnapshot(
            InspectDecoderDescriptor descriptor, DecodeCursor cursor) throws IOException {
        byte[] snapshotBytes = descriptor.portableSnapshotBytes();
        TypeSerializerSnapshot<?> snapshot;
        try {
            snapshot =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            new DataInputViewStreamWrapper(new ByteArrayInputStream(snapshotBytes)),
                            descriptor.getClass().getClassLoader());
        } catch (NoClassDefFoundError e) {
            throw new DecodeFailureException(
                    DecodeIssueKind.SERIALIZER_RESTORE_FAILED,
                    "Portable serializer snapshot dependency is unavailable: "
                            + descriptor.portableSnapshotClassName(),
                    e);
        } catch (IOException | RuntimeException e) {
            throw new DecodeFailureException(
                    DecodeIssueKind.MALFORMED_BYTES,
                    "Failed to read portable serializer snapshot: "
                            + descriptor.portableSnapshotClassName(),
                    e);
        }
        TypeSerializer<?> serializer;
        try {
            serializer = snapshot.restoreSerializer();
        } catch (NoClassDefFoundError e) {
            throw new DecodeFailureException(
                    DecodeIssueKind.SERIALIZER_RESTORE_FAILED,
                    "Failed to restore portable serializer from snapshot: "
                            + descriptor.portableSnapshotClassName(),
                    e);
        }
        if (serializer == null) {
            throw new DecodeFailureException(
                    DecodeIssueKind.SERIALIZER_RESTORE_FAILED,
                    "Failed to restore portable serializer from snapshot (null): "
                            + descriptor.portableSnapshotClassName());
        }
        try {
            return serializer.deserialize(cursor.input());
        } catch (EOFException e) {
            throw new DecodeFailureException(
                    DecodeIssueKind.MALFORMED_BYTES,
                    "Portable serializer deserialize failed: " + e.getMessage(),
                    e);
        } catch (NoClassDefFoundError e) {
            throw new DecodeFailureException(
                    DecodeIssueKind.SERIALIZER_RESTORE_FAILED,
                    "Portable serializer deserialize failed: " + e.getMessage(),
                    e);
        } catch (IOException | RuntimeException e) {
            throw new DecodeFailureException(
                    DecodeIssueKind.UNKNOWN,
                    "Portable serializer deserialize failed: " + e.getMessage(),
                    e);
        }
    }
}
