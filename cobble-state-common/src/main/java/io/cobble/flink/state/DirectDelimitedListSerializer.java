package io.cobble.flink.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Direct-buffer serializer for RocksDB-style delimiter-separated list payloads. */
final class DirectDelimitedListSerializer {
    static final byte DELIMITER = ',';

    private final GrowingDirectBufferOutputStream outputStream;
    private final DataOutputViewStreamWrapper outputView;
    private final byte[] copyBuffer;

    DirectDelimitedListSerializer(int initialCapacityBytes) {
        this.outputStream = new GrowingDirectBufferOutputStream(initialCapacityBytes);
        this.outputView = new DataOutputViewStreamWrapper(outputStream);
        this.copyBuffer = new byte[4096];
    }

    <T> CobbleStateKeySerializer.DirectBufferSlice encodeSingle(
            TypeSerializer<T> serializer, T value) throws IOException {
        outputStream.clear();
        serializer.serialize(value, outputView);
        outputStream.write(DELIMITER);
        return outputStream.currentSlice();
    }

    <T> CobbleStateKeySerializer.DirectBufferSlice encodeAll(
            TypeSerializer<T> serializer, List<T> values) throws IOException {
        outputStream.clear();
        for (T value : values) {
            serializer.serialize(value, outputView);
            outputStream.write(DELIMITER);
        }
        return outputStream.currentSlice();
    }

    CobbleStateKeySerializer.DirectBufferSlice copyRaw(InputStream input) throws IOException {
        outputStream.clear();
        copyIntoOutput(input);
        return outputStream.currentSlice();
    }

    byte[] copyRawWithoutTrailingDelimiter(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int previous = -1;
        while (true) {
            int next = input.read();
            if (next < 0) {
                break;
            }
            if (previous >= 0) {
                output.write(previous);
            }
            previous = next;
        }
        if (previous >= 0 && previous != DELIMITER) {
            output.write(previous);
        }
        return output.toByteArray();
    }

    <T> List<T> decode(TypeSerializer<T> serializer, InputStream input) throws IOException {
        PushbackInputStream peekable = new PushbackInputStream(input, 1);
        DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(peekable);
        List<T> decoded = new ArrayList<>();
        while (true) {
            int first = peekable.read();
            if (first < 0) {
                break;
            }
            peekable.unread(first);
            decoded.add(serializer.deserialize(inputView));
            int delimiter = peekable.read();
            if (delimiter < 0) {
                break;
            }
            if ((byte) delimiter != DELIMITER) {
                throw new IOException("Invalid list delimiter in Cobble list state: " + delimiter);
            }
        }
        return decoded;
    }

    private void copyIntoOutput(InputStream input) throws IOException {
        while (true) {
            int read = input.read(copyBuffer);
            if (read < 0) {
                return;
            }
            outputStream.write(copyBuffer, 0, read);
        }
    }

    private static final class GrowingDirectBufferOutputStream extends OutputStream {
        private ByteBuffer buffer;

        private GrowingDirectBufferOutputStream(int initialSize) {
            this.buffer = ByteBuffer.allocateDirect(Math.max(1, initialSize));
        }

        @Override
        public void write(int value) {
            ensureCapacity(1);
            buffer.put((byte) value);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            if (bytes == null) {
                throw new NullPointerException("bytes");
            }
            if (offset < 0 || length < 0 || offset + length > bytes.length) {
                throw new IndexOutOfBoundsException("invalid offset/length");
            }
            ensureCapacity(length);
            buffer.put(bytes, offset, length);
        }

        private void ensureCapacity(int additionalBytes) {
            int required = buffer.position() + additionalBytes;
            if (required <= buffer.capacity()) {
                return;
            }
            int newCapacity = buffer.capacity();
            while (newCapacity < required) {
                newCapacity = Math.max(required, newCapacity << 1);
            }
            ByteBuffer replacement = ByteBuffer.allocateDirect(newCapacity);
            ByteBuffer copy = buffer.duplicate();
            ((Buffer) copy).clear();
            ((Buffer) copy).limit(buffer.position());
            replacement.put(copy);
            buffer = replacement;
        }

        private void clear() {
            ((Buffer) buffer).clear();
        }

        private CobbleStateKeySerializer.DirectBufferSlice currentSlice() {
            return new CobbleStateKeySerializer.DirectBufferSlice(buffer, buffer.position());
        }
    }
}
