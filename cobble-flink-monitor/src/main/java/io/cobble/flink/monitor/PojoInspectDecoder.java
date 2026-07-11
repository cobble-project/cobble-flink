package io.cobble.flink.monitor;

import io.cobble.flink.common.inspect.InspectDecoderDescriptor;
import io.cobble.flink.common.inspect.InspectDecoderDescriptor.PojoFieldDescriptor;
import io.cobble.flink.common.inspect.InspectDecoderDescriptor.RegisteredSubclass;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Classless decoder for Flink {@code PojoSerializer} wire format. Reads the POJO flag byte and
 * field/subclass layout without loading the user's POJO class, restoring a live {@code
 * PojoSerializer}, or requiring {@code --user-jar}.
 *
 * <h2>Wire format</h2>
 *
 * <p>Matches Flink 1.17, 1.19, and 2.0 {@code PojoSerializer}:
 *
 * <table>
 *   <tr><th>Flag</th><th>Byte</th><th>Behavior</th></tr>
 *   <tr><td>IS_NULL</td><td>0x01</td><td>Return a null POJO; consume nothing else.</td></tr>
 *   <tr><td>NO_SUBCLASS</td><td>0x02</td><td>For each field: read boolean isNull; when false, decode the child descriptor.</td></tr>
 *   <tr><td>IS_SUBCLASS</td><td>0x04</td><td>Never classlessly decode. Fail with a precise error.</td></tr>
 *   <tr><td>IS_TAGGED_SUBCLASS</td><td>0x08</td><td>Read signed byte tag, validate against registered list, decode selected child descriptor.</td></tr>
 * </table>
 *
 * <p>Zero, unknown flags, and flag combinations are rejected as malformed input.
 *
 * <h2>Return type</h2>
 *
 * <p>All decode methods return {@code Object}. For {@code NO_SUBCLASS} and {@code IS_NULL}, a
 * {@link ClasslessPojoValue} is returned. For {@code IS_TAGGED_SUBCLASS}, the child descriptor's
 * neutral result is returned as-is (it may be a {@code ClasslessPojoValue}, Avro {@code
 * GenericRecord}, or other neutral value).
 */
final class PojoInspectDecoder {

    private static final int FLAG_IS_NULL = 0x01;
    private static final int FLAG_NO_SUBCLASS = 0x02;
    private static final int FLAG_IS_SUBCLASS = 0x04;
    private static final int FLAG_IS_TAGGED_SUBCLASS = 0x08;

    private PojoInspectDecoder() {}

    /**
     * Decodes exactly one POJO datum from the byte array and rejects trailing bytes.
     *
     * @throws IOException if decoding fails or trailing bytes remain.
     */
    static Object decode(InspectDecoderDescriptor descriptor, byte[] bytes) throws IOException {
        ClasslessValueDecoder.DecodeCursor cursor = new ClasslessValueDecoder.DecodeCursor(bytes);
        Object result = decodeFromCursor(descriptor, cursor);
        cursor.requireFullyConsumed("POJO datum");
        return result;
    }

    /**
     * Consumes exactly one POJO datum from the shared cursor. The caller owns trailing-byte
     * validation.
     *
     * @throws IOException if decoding fails.
     */
    static Object decodeFromCursor(
            InspectDecoderDescriptor descriptor, ClasslessValueDecoder.DecodeCursor cursor)
            throws IOException {
        int flag;
        try {
            flag = cursor.input().readUnsignedByte();
        } catch (IOException e) {
            throw new DecodeFailureException(
                    DecodeIssueKind.MALFORMED_BYTES,
                    "Failed to read POJO flag: " + e.getMessage(),
                    e);
        }

        if (flag == FLAG_IS_NULL) {
            return ClasslessPojoValue.nullValue();
        }

        if (flag == FLAG_IS_SUBCLASS) {
            throw new DecodeFailureException(
                    DecodeIssueKind.CLASSLESS_UNSUPPORTED,
                    "Non-registered subclass: classless POJO decode not supported (flag=0x04)");
        }

        if (flag == FLAG_IS_TAGGED_SUBCLASS) {
            return decodeTaggedSubclass(descriptor, cursor);
        }

        if (flag == FLAG_NO_SUBCLASS) {
            return decodeBaseFields(descriptor, cursor);
        }

        throw new DecodeFailureException(
                DecodeIssueKind.MALFORMED_BYTES,
                "Malformed POJO flag: 0x"
                        + Integer.toHexString(flag)
                        + " (expected 0x01/0x02/0x04/0x08)");
    }

    private static Object decodeBaseFields(
            InspectDecoderDescriptor descriptor, ClasslessValueDecoder.DecodeCursor cursor)
            throws IOException {
        if (!descriptor.pojoBasePathClassless()) {
            throw new DecodeFailureException(
                    DecodeIssueKind.CLASSLESS_UNSUPPORTED,
                    "POJO base path is not classless (basePathClassless=false); "
                            + "PARTIALLY_CLASSLESS fallback required");
        }
        List<PojoFieldDescriptor> fields = descriptor.pojoFields();
        Map<String, Object> output = new LinkedHashMap<>(fields.size());
        for (PojoFieldDescriptor field : fields) {
            boolean isNull;
            try {
                isNull = cursor.input().readBoolean();
            } catch (IOException e) {
                throw new DecodeFailureException(
                        DecodeIssueKind.MALFORMED_BYTES,
                        "POJO field '"
                                + field.name()
                                + "': failed to read null flag: "
                                + e.getMessage(),
                        e);
            }
            if (isNull) {
                output.put(field.name(), null);
                continue;
            }
            try {
                Object fieldValue =
                        ClasslessValueDecoder.decodeFromCursor(field.descriptor(), cursor);
                output.put(field.name(), fieldValue);
            } catch (IOException e) {
                throw DecodeFailureException.wrap("POJO field '" + field.name() + "'", e);
            }
        }
        return ClasslessPojoValue.of(output);
    }

    private static Object decodeTaggedSubclass(
            InspectDecoderDescriptor descriptor, ClasslessValueDecoder.DecodeCursor cursor)
            throws IOException {
        int tag;
        try {
            tag = cursor.input().readByte();
        } catch (IOException e) {
            throw new DecodeFailureException(
                    DecodeIssueKind.MALFORMED_BYTES,
                    "Failed to read POJO subclass tag: " + e.getMessage(),
                    e);
        }
        List<RegisteredSubclass> subclasses = descriptor.registeredPojoSubclasses();
        if (tag < 0 || tag >= subclasses.size()) {
            throw new DecodeFailureException(
                    DecodeIssueKind.MALFORMED_BYTES,
                    "Invalid POJO subclass tag: "
                            + tag
                            + " (registered: 0.."
                            + (subclasses.size() - 1)
                            + ")");
        }
        RegisteredSubclass subclass = subclasses.get(tag);
        try {
            return ClasslessValueDecoder.decodeFromCursor(subclass.descriptor(), cursor);
        } catch (IOException e) {
            throw DecodeFailureException.wrap("POJO subclass[tag=" + tag + "]", e);
        }
    }
}
