package io.cobble.flink.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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

    private final GrowingDirectBufferDataOutputView outputView;
    private final byte[] copyBuffer;

    DirectDelimitedListSerializer(int initialCapacityBytes) {
        this.outputView = new GrowingDirectBufferDataOutputView(initialCapacityBytes);
        this.copyBuffer = new byte[4096];
    }

    <T> CobbleStateKeySerializer.DirectBufferSlice encodeSingle(
            TypeSerializer<T> serializer, T value) throws IOException {
        outputView.clear();
        serializer.serialize(value, outputView);
        outputView.write(DELIMITER);
        return outputView.currentSlice();
    }

    <T> CobbleStateKeySerializer.DirectBufferSlice encodeAll(
            TypeSerializer<T> serializer, List<T> values) throws IOException {
        outputView.clear();
        for (T value : values) {
            Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
            serializer.serialize(value, outputView);
            outputView.write(DELIMITER);
        }
        return outputView.currentSlice();
    }

    CobbleStateKeySerializer.DirectBufferSlice copyRaw(InputStream input) throws IOException {
        outputView.clear();
        copyIntoOutput(input);
        return outputView.currentSlice();
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
            outputView.write(copyBuffer, 0, read);
        }
    }

    private static final class GrowingDirectBufferDataOutputView extends OutputStream
            implements DataOutputView {
        private ByteBuffer buffer;
        private final DataOutputStream utfOutput;

        private GrowingDirectBufferDataOutputView(int initialSize) {
            this.buffer = ByteBuffer.allocateDirect(Math.max(1, initialSize));
            this.utfOutput = new DataOutputStream(this);
        }

        @Override
        public void write(int value) {
            ensureCapacity(1);
            buffer.put((byte) value);
        }

        @Override
        public void write(byte[] bytes) {
            write(bytes, 0, bytes.length);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            checkRange(bytes, offset, length);
            ensureCapacity(length);
            buffer.put(bytes, offset, length);
        }

        @Override
        public void writeBoolean(boolean value) {
            write(value ? 1 : 0);
        }

        @Override
        public void writeByte(int value) {
            write(value);
        }

        @Override
        public void writeShort(int value) {
            ensureCapacity(Short.BYTES);
            buffer.putShort((short) value);
        }

        @Override
        public void writeChar(int value) {
            writeShort(value);
        }

        @Override
        public void writeInt(int value) {
            ensureCapacity(Integer.BYTES);
            buffer.putInt(value);
        }

        @Override
        public void writeLong(long value) {
            ensureCapacity(Long.BYTES);
            buffer.putLong(value);
        }

        @Override
        public void writeFloat(float value) {
            writeInt(Float.floatToIntBits(value));
        }

        @Override
        public void writeDouble(double value) {
            writeLong(Double.doubleToLongBits(value));
        }

        @Override
        public void writeBytes(String value) {
            for (int i = 0; i < value.length(); i++) {
                writeByte((byte) value.charAt(i));
            }
        }

        @Override
        public void writeChars(String value) {
            for (int i = 0; i < value.length(); i++) {
                writeChar(value.charAt(i));
            }
        }

        @Override
        public void writeUTF(String value) throws IOException {
            utfOutput.writeUTF(value);
        }

        @Override
        public void skipBytesToWrite(int numBytes) throws IOException {
            if (numBytes < 0) {
                throw new IOException("numBytes must be non-negative: " + numBytes);
            }
            ensureCapacity(numBytes);
            buffer.position(buffer.position() + numBytes);
        }

        @Override
        public void write(DataInputView source, int length) throws IOException {
            if (length < 0) {
                throw new IOException("length must be non-negative: " + length);
            }
            byte[] copy = new byte[Math.min(4096, Math.max(1, length))];
            int remaining = length;
            while (remaining > 0) {
                int count = Math.min(copy.length, remaining);
                source.readFully(copy, 0, count);
                write(copy, 0, count);
                remaining -= count;
            }
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

        private static void checkRange(byte[] bytes, int offset, int length) {
            if (bytes == null) {
                throw new NullPointerException("bytes");
            }
            if (offset < 0 || length < 0 || offset > bytes.length - length) {
                throw new IndexOutOfBoundsException("invalid offset/length");
            }
        }
    }
}
