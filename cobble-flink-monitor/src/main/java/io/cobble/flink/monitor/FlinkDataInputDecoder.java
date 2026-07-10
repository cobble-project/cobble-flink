/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cobble.flink.monitor;

import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An Avro {@link Decoder} that reads from a {@link DataInput} using the exact byte semantics of
 * Flink's {@code DataInputDecoder} (package {@code org.apache.flink.formats.avro.utils}).
 *
 * <p>Flink's {@code AvroSerializer} writes values through {@code DataOutputEncoder}, which
 * delegates all primitives to {@code java.io.DataOutput} (big-endian fixed-width two's complement)
 * and uses a plain base-128 unsigned varint for array/map block counts. This is
 * <strong>not</strong> standard Avro binary encoding (which uses zigzag varints). This class is an
 * audited copy of the Flink 1.17 {@code DataInputDecoder} semantics, with additional
 * malformed-input guards that reject negative lengths, overflowed varints, excessive collection
 * counts, and payloads larger than the remaining input.
 *
 * <p>Source verified against Flink branches {@code release-1.17}, {@code release-1.19} (via local
 * jar decompilation), and Flink 2.0.2. The wire protocol is byte-for-byte identical across all
 * three versions.
 *
 * <p>This class is <strong>not thread-safe</strong>. Each decode creates a fresh instance bound to
 * the current byte input.
 */
final class FlinkDataInputDecoder extends Decoder {

    /** Maximum collection block count accepted from a decoded stream. */
    static final long MAX_COLLECTION_COUNT = 10_000_000L;

    /** Maximum single byte/string payload length accepted from a decoded stream. */
    static final int MAX_PAYLOAD_LENGTH = 16 * 1024 * 1024; // 16 MB

    private final DataInput in;
    private final int totalLength;
    private int bytesRead;

    FlinkDataInputDecoder(DataInput in, int totalLength) {
        this.in = in;
        this.totalLength = totalLength;
    }

    private void checkRemaining(int needed) throws IOException {
        if (bytesRead + needed > totalLength) {
            throw new IOException(
                    "Unexpected end of Avro input: need "
                            + needed
                            + " bytes, "
                            + remaining()
                            + " remaining");
        }
    }

    private int remaining() {
        return totalLength - bytesRead;
    }

    // --------------------------------------------------------------------------------------------
    // primitives
    // --------------------------------------------------------------------------------------------

    @Override
    public void readNull() {}

    @Override
    public boolean readBoolean() throws IOException {
        checkRemaining(1);
        bytesRead += 1;
        return in.readBoolean();
    }

    @Override
    public int readInt() throws IOException {
        checkRemaining(4);
        bytesRead += 4;
        return in.readInt();
    }

    @Override
    public long readLong() throws IOException {
        checkRemaining(8);
        bytesRead += 8;
        return in.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        checkRemaining(4);
        bytesRead += 4;
        return in.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        checkRemaining(8);
        bytesRead += 8;
        return in.readDouble();
    }

    @Override
    public int readEnum() throws IOException {
        return readInt();
    }

    // --------------------------------------------------------------------------------------------
    // bytes
    // --------------------------------------------------------------------------------------------

    @Override
    public void readFixed(byte[] bytes, int start, int length) throws IOException {
        if (length < 0) {
            throw new IOException("Negative Avro fixed length: " + length);
        }
        checkRemaining(length);
        in.readFully(bytes, start, length);
        bytesRead += length;
    }

    @Override
    public ByteBuffer readBytes(ByteBuffer old) throws IOException {
        int length = readLengthPrefixed();
        ByteBuffer result;
        if (old != null && length <= old.capacity() && old.hasArray()) {
            result = old;
            result.clear();
        } else {
            result = ByteBuffer.allocate(length);
        }
        in.readFully(result.array(), result.arrayOffset() + result.position(), length);
        bytesRead += length;
        result.limit(length);
        return result;
    }

    @Override
    public void skipFixed(int length) throws IOException {
        if (length < 0) {
            throw new IOException("Negative Avro fixed skip length: " + length);
        }
        checkRemaining(length);
        skipBytes(length);
    }

    @Override
    public void skipBytes() throws IOException {
        int num = readLengthPrefixed();
        skipBytes(num);
    }

    // --------------------------------------------------------------------------------------------
    // strings
    // --------------------------------------------------------------------------------------------

    @Override
    public Utf8 readString(Utf8 old) throws IOException {
        int length = readLengthPrefixed();
        Utf8 result = (old != null ? old : new Utf8());
        result.setByteLength(length);
        if (length > 0) {
            in.readFully(result.getBytes(), 0, length);
            bytesRead += length;
        }
        return result;
    }

    @Override
    public String readString() throws IOException {
        return readString(new Utf8()).toString();
    }

    @Override
    public void skipString() throws IOException {
        int len = readLengthPrefixed();
        skipBytes(len);
    }

    // --------------------------------------------------------------------------------------------
    // collection types
    // --------------------------------------------------------------------------------------------

    @Override
    public long readArrayStart() throws IOException {
        return readVarLongCount();
    }

    @Override
    public long arrayNext() throws IOException {
        return readVarLongCount();
    }

    @Override
    public long skipArray() throws IOException {
        return readVarLongCount();
    }

    @Override
    public long readMapStart() throws IOException {
        return readVarLongCount();
    }

    @Override
    public long mapNext() throws IOException {
        return readVarLongCount();
    }

    @Override
    public long skipMap() throws IOException {
        return readVarLongCount();
    }

    // --------------------------------------------------------------------------------------------
    // union
    // --------------------------------------------------------------------------------------------

    @Override
    public int readIndex() throws IOException {
        return readInt();
    }

    // --------------------------------------------------------------------------------------------
    // utils
    // --------------------------------------------------------------------------------------------

    /**
     * Reads a 4-byte big-endian length prefix, validates it, and returns it. Used for string and
     * bytes payloads.
     */
    private int readLengthPrefixed() throws IOException {
        checkRemaining(4);
        bytesRead += 4;
        int length = in.readInt();
        if (length < 0) {
            throw new IOException("Negative Avro payload length: " + length);
        }
        if (length > MAX_PAYLOAD_LENGTH) {
            throw new IOException(
                    "Avro payload length exceeds limit: " + length + " > " + MAX_PAYLOAD_LENGTH);
        }
        if (length > remaining()) {
            throw new IOException(
                    "Avro payload length " + length + " exceeds remaining input " + remaining());
        }
        return length;
    }

    private void skipBytes(int num) throws IOException {
        if (num < 0) {
            throw new IOException("Negative Avro skip length: " + num);
        }
        checkRemaining(num);
        while (num > 0) {
            int skipped = in.skipBytes(num);
            if (skipped <= 0) {
                throw new EOFException("Unexpected EOF while skipping Avro bytes");
            }
            bytesRead += skipped;
            num -= skipped;
        }
    }

    /**
     * Reads an unsigned base-128 varint (little-endian, 7 bits per byte, MSB continuation flag).
     * This is <strong>not</strong> zigzag-encoded. Guards against overflow and excessive counts.
     */
    private long readVarLongCount() throws IOException {
        checkRemaining(1);
        bytesRead += 1;
        long value = in.readUnsignedByte();

        if ((value & 0x80) == 0) {
            validateCount(value);
            return value;
        } else {
            long curr;
            int shift = 7;
            value = value & 0x7f;
            while (((curr = readUnsignedByteGuarded()) & 0x80) != 0) {
                if (shift >= 63) {
                    throw new IOException("Avro varint count overflow (shift=" + shift + ")");
                }
                value |= (curr & 0x7f) << shift;
                shift += 7;
            }
            // Terminal byte: at shift == 63, only the lowest bit (0x01) is valid. Any higher
            // bit would overflow the 64-bit long.
            if (shift == 63 && curr > 1) {
                throw new IOException("Avro varint count overflow (terminal byte=" + curr + ")");
            }
            value |= curr << shift;
            validateCount(value);
            return value;
        }
    }

    private int readUnsignedByteGuarded() throws IOException {
        checkRemaining(1);
        bytesRead += 1;
        return in.readUnsignedByte();
    }

    private void validateCount(long count) throws IOException {
        if (count < 0) {
            throw new IOException("Negative Avro collection count: " + count);
        }
        if (count > MAX_COLLECTION_COUNT) {
            throw new IOException(
                    "Avro collection count exceeds limit: " + count + " > " + MAX_COLLECTION_COUNT);
        }
    }

    /** Returns the number of bytes read so far. Used by callers to check for trailing bytes. */
    int bytesRead() {
        return bytesRead;
    }

    int totalLength() {
        return totalLength;
    }
}
