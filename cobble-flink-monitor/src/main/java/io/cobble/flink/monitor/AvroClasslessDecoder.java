package io.cobble.flink.monitor;

import io.cobble.flink.common.inspect.DescriptorCapability;
import io.cobble.flink.common.inspect.InspectDecoderDescriptor;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Classless Avro decoder that decodes Flink {@code AvroSerializer} bytes into Avro {@link
 * GenericRecord} values without loading user classes, {@code flink-avro}, or the writer's {@code
 * AvroSerializer}.
 *
 * <p>The decoder uses a {@link FlinkDataInputDecoder} (an audited copy of Flink's {@code
 * DataInputDecoder} wire semantics) paired with Avro's {@link GenericDatumReader}. This combination
 * correctly decodes bytes written by Flink's {@code DataOutputEncoder}, which differs from standard
 * Avro binary encoding.
 *
 * <h2>Schema caching</h2>
 *
 * <p>Parsed {@link Schema} objects are immutable and cached in a small bounded LRU map. The cache
 * is keyed by SHA-256 of the schema JSON + wire format (not the raw JSON itself, to avoid retaining
 * large attacker-controlled strings in memory). The cache is bounded by both entry count (64) and
 * cumulative schema-source bytes (4 MiB). {@link GenericDatumReader} and {@link
 * FlinkDataInputDecoder} are <strong>not</strong> cached because both carry mutable decode state. A
 * fresh reader and decoder are created for each {@link #decode} call.
 *
 * <h2>Fallback behavior</h2>
 *
 * <ul>
 *   <li>{@code FULLY_CLASSLESS}: decode failure is a row-level {@code decode_error}. No {@code
 *       restoreSerializer()} fallback is attempted because no live serializer was persisted.
 *   <li>{@code PARTIALLY_CLASSLESS}: decode failure falls through to the existing snapshot/live
 *       serializer fallback. Avro runtime exceptions (e.g. {@link AvroTypeException}, {@link
 *       org.apache.avro.AvroRuntimeException}) are normalized to {@link IOException} so they enter
 *       the intended fallback path.
 * </ul>
 */
final class AvroClasslessDecoder {

    /** Maximum number of parsed schemas cached for reuse. */
    private static final int SCHEMA_CACHE_MAX_ENTRIES = 64;

    /**
     * Maximum cumulative schema-source bytes retained in the cache. Each entry's contribution is
     * the length of the original schema JSON string (in UTF-8 bytes). This bounds total memory even
     * if each individual schema is under the 16 MiB per-schema limit.
     */
    private static final int SCHEMA_CACHE_MAX_TOTAL_BYTES = 4 * 1024 * 1024; // 4 MiB

    private static final Object SCHEMA_CACHE_LOCK = new Object();
    private static final LinkedHashMap<String, CachedSchema> SCHEMA_CACHE =
            new LinkedHashMap<>(16, 0.75f, true);
    private static int cachedSchemaSourceBytes;

    /**
     * Attempts to decode the given bytes as an Avro value using the descriptor. Returns the decoded
     * {@link GenericRecord}, or throws {@link IOException} if decoding fails.
     *
     * <p>After decoding a complete datum, any trailing bytes are rejected. This method is suitable
     * for ValueState, MapState key/value, and other contexts where the byte slice represents
     * exactly one Avro datum.
     *
     * @param descriptor the AVRO descriptor (must be kind {@code AVRO})
     * @param bytes the value bytes written by Flink's {@code AvroSerializer}
     * @return the decoded {@link GenericRecord} (an {@code IndexedRecord})
     * @throws IOException if the wire format is unsupported, the schema is invalid, the bytes are
     *     malformed, or trailing bytes remain after decoding.
     */
    static GenericRecord decode(InspectDecoderDescriptor descriptor, byte[] bytes)
            throws IOException {
        return decodeStream(descriptor, bytes);
    }

    /**
     * Decodes a single Avro datum from a byte array. Unlike {@link #decode}, this method does not
     * reject trailing bytes after the datum - it returns as soon as the datum is complete. This is
     * used for ListState elements where the byte slice may contain the delimiter after the datum.
     *
     * @return the decoded {@link GenericRecord} and the number of bytes consumed.
     */
    static DecodedDatum decodeFromStream(InspectDecoderDescriptor descriptor, byte[] bytes)
            throws IOException {
        return decodeFromStream(descriptor, bytes, 0);
    }

    /**
     * Decodes a single Avro datum starting at the given offset in the byte array. Does not reject
     * trailing bytes. Used for ListState elements to avoid copying the remaining payload for each
     * element.
     *
     * @return the decoded {@link GenericRecord} and the number of bytes consumed (relative to the
     *     offset, not the array start).
     */
    static DecodedDatum decodeFromStream(
            InspectDecoderDescriptor descriptor, byte[] bytes, int offset) throws IOException {
        String wireFormat = validateDescriptor(descriptor);
        Schema schema = parseSchema(wireFormat, descriptor.avroWriterSchemaJson());
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        int length = bytes.length - offset;
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes, offset, length);
        DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(byteStream);
        FlinkDataInputDecoder decoder = new FlinkDataInputDecoder(inputView, length);

        GenericRecord record;
        try {
            record = reader.read(null, decoder);
        } catch (IOException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IOException("Avro decode failed: " + e.getMessage(), e);
        }
        return new DecodedDatum(record, decoder.bytesRead());
    }

    /**
     * Decodes a single Avro datum from a shared {@link ClasslessValueDecoder.DecodeCursor}. Does
     * not reject trailing bytes - the caller owns that validation. Used when an Avro value appears
     * as a POJO field or ListState element, where the byte stream continues after the datum.
     *
     * @return the decoded {@link GenericRecord}.
     * @throws IOException if the wire format is unsupported, the schema is invalid, or the bytes
     *     are malformed.
     */
    static Object decodeFromCursor(
            InspectDecoderDescriptor descriptor, ClasslessValueDecoder.DecodeCursor cursor)
            throws IOException {
        String wireFormat = validateDescriptor(descriptor);
        Schema schema = parseSchema(wireFormat, descriptor.avroWriterSchemaJson());
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        FlinkDataInputDecoder decoder =
                new FlinkDataInputDecoder(cursor.input(), cursor.remaining());
        try {
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IOException("Avro decode failed: " + e.getMessage(), e);
        }
    }

    /** Result of decoding a single datum from a stream. */
    static final class DecodedDatum {
        final GenericRecord record;
        final int bytesConsumed;

        DecodedDatum(GenericRecord record, int bytesConsumed) {
            this.record = record;
            this.bytesConsumed = bytesConsumed;
        }
    }

    /**
     * Internal decode that uses the full byte array and relies on {@link FlinkDataInputDecoder}'s
     * trailing-byte tracking.
     */
    private static GenericRecord decodeStream(InspectDecoderDescriptor descriptor, byte[] bytes)
            throws IOException {
        String wireFormat = validateDescriptor(descriptor);
        Schema schema = parseSchema(wireFormat, descriptor.avroWriterSchemaJson());
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(byteStream);
        FlinkDataInputDecoder decoder = new FlinkDataInputDecoder(inputView, bytes.length);

        GenericRecord record;
        try {
            record = reader.read(null, decoder);
        } catch (IOException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IOException("Avro decode failed: " + e.getMessage(), e);
        }

        // Reject trailing bytes after a complete datum.
        if (decoder.bytesRead() != bytes.length) {
            throw new IOException(
                    "Trailing bytes after Avro datum: consumed "
                            + decoder.bytesRead()
                            + " of "
                            + bytes.length
                            + " bytes");
        }
        return record;
    }

    private static String validateDescriptor(InspectDecoderDescriptor descriptor)
            throws IOException {
        if (!descriptor.isAvro()) {
            throw new IOException("Not an AVRO descriptor: " + descriptor.kind());
        }
        String wireFormat = descriptor.avroWireFormat();
        if (!InspectDecoderDescriptor.AVRO_WIRE_FORMAT_FLINK_DATA_INPUT_V1.equals(wireFormat)) {
            throw new IOException("Unsupported Avro wire format: " + wireFormat);
        }
        return wireFormat;
    }

    /**
     * Returns {@code true} if the descriptor is an AVRO descriptor that should be used for
     * classless decoding (capability is {@code FULLY_CLASSLESS} or {@code PARTIALLY_CLASSLESS}).
     */
    static boolean isClasslessAvro(InspectDecoderDescriptor descriptor) {
        return descriptor != null
                && descriptor.isAvro()
                && descriptor.capability() != DescriptorCapability.UNSUPPORTED;
    }

    private static Schema parseSchema(String wireFormat, String schemaJson) throws IOException {
        int sourceBytes = schemaJson.getBytes(StandardCharsets.UTF_8).length;
        String cacheKey = schemaCacheKey(wireFormat, schemaJson);
        CachedSchema cached;
        synchronized (SCHEMA_CACHE_LOCK) {
            cached = SCHEMA_CACHE.get(cacheKey);
        }
        if (cached != null) {
            return cached.schema;
        }
        Schema.Parser parser = new Schema.Parser();
        Schema schema;
        try {
            schema = parser.parse(schemaJson);
        } catch (RuntimeException e) {
            throw new IOException("Failed to parse Avro schema: " + e.getMessage(), e);
        }

        if (sourceBytes > SCHEMA_CACHE_MAX_TOTAL_BYTES) {
            return schema;
        }

        synchronized (SCHEMA_CACHE_LOCK) {
            cached = SCHEMA_CACHE.get(cacheKey);
            if (cached != null) {
                return cached.schema;
            }
            SCHEMA_CACHE.put(cacheKey, new CachedSchema(schema, sourceBytes));
            cachedSchemaSourceBytes += sourceBytes;
            trimSchemaCache();
        }
        return schema;
    }

    private static void trimSchemaCache() {
        Iterator<Map.Entry<String, CachedSchema>> entries = SCHEMA_CACHE.entrySet().iterator();
        while (entries.hasNext()
                && (SCHEMA_CACHE.size() > SCHEMA_CACHE_MAX_ENTRIES
                        || cachedSchemaSourceBytes > SCHEMA_CACHE_MAX_TOTAL_BYTES)) {
            cachedSchemaSourceBytes -= entries.next().getValue().sourceBytes;
            entries.remove();
        }
    }

    private static String schemaCacheKey(String wireFormat, String schemaJson) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(wireFormat.getBytes(StandardCharsets.UTF_8));
            md.update((byte) 0);
            byte[] hash = md.digest(schemaJson.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(72);
            sb.append("sha256:");
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 is unavailable", e);
        }
    }

    /** Holder for a cached parsed schema and its source byte count for memory budgeting. */
    private static final class CachedSchema {
        final Schema schema;
        final int sourceBytes;

        CachedSchema(Schema schema, int sourceBytes) {
            this.schema = schema;
            this.sourceBytes = sourceBytes;
        }
    }

    private AvroClasslessDecoder() {}
}
