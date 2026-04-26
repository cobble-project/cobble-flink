package io.cobble.flink.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Utility methods for turning Flink keys, namespaces, and values into Cobble row bytes. */
final class CobbleStateKeySerializer {

    private CobbleStateKeySerializer() {}

    /** Serializes one Flink value using the provided serializer. */
    static <T> byte[] serialize(TypeSerializer<T> serializer, T value) throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(64);
        serializer.serialize(value, output);
        return output.getCopyOfBuffer();
    }

    /** Deserializes one Flink value from previously serialized bytes. */
    static <T> T deserialize(TypeSerializer<T> serializer, byte[] bytes) throws IOException {
        DataInputDeserializer input = new DataInputDeserializer(bytes);
        return serializer.deserialize(input);
    }

    /** Reusable direct serializer for one value into a caller-owned direct ByteBuffer. */
    static final class ReusableDirectValueSerializer {
        private final GrowingDirectBufferOutputStream outputStream;
        private final DataOutputViewStreamWrapper outputView;

        ReusableDirectValueSerializer(int initialSize) {
            this.outputStream = new GrowingDirectBufferOutputStream(initialSize);
            this.outputView = new DataOutputViewStreamWrapper(outputStream);
        }

        <T> DirectBufferSlice serialize(TypeSerializer<T> serializer, T value) throws IOException {
            outputStream.clear();
            serializer.serialize(value, outputView);
            return outputStream.currentSlice();
        }
    }

    /** Reusable direct deserializer for one value from a direct ByteBuffer slice. */
    static final class ReusableDirectValueDeserializer {
        private final ByteBufferInputStream inputStream;
        private final DataInputViewStreamWrapper inputView;

        ReusableDirectValueDeserializer() {
            this.inputStream = new ByteBufferInputStream();
            this.inputView = new DataInputViewStreamWrapper(inputStream);
        }

        <T> T deserialize(TypeSerializer<T> serializer, ByteBuffer buffer) throws IOException {
            inputStream.reset(buffer);
            return serializer.deserialize(inputView);
        }
    }

    /** Reusable serializer that caches the current key prefix and resets to it between writes. */
    static final class ReusableSerializedKeyBuilder<K> {
        private final TypeSerializer<K> keySerializer;
        private final DataOutputSerializer output;

        private K cachedKey;
        private int afterKeyMark;

        ReusableSerializedKeyBuilder(TypeSerializer<K> keySerializer, int initialSize) {
            this.keySerializer = keySerializer;
            this.output = new DataOutputSerializer(initialSize);
        }

        <N> byte[] buildKeyAndNamespace(K key, TypeSerializer<N> namespaceSerializer, N namespace)
                throws IOException {
            ensureKeySerialized(key);
            output.setPosition(afterKeyMark);
            namespaceSerializer.serialize(namespace, output);
            output.writeInt(afterKeyMark);
            return output.getCopyOfBuffer();
        }

        <K2 extends K, UK, N> byte[] buildMapKeyUserKeyAndNamespace(
                K2 key,
                TypeSerializer<UK> userKeySerializer,
                UK userKey,
                TypeSerializer<N> namespaceSerializer,
                N namespace)
                throws IOException {
            ensureKeySerialized(key);
            output.setPosition(afterKeyMark);
            output.writeByte(0);
            userKeySerializer.serialize(userKey, output);
            int afterUserKeyMark = output.length();
            namespaceSerializer.serialize(namespace, output);
            output.writeInt(afterKeyMark);
            output.writeInt(afterUserKeyMark - afterKeyMark - 1);
            return output.getCopyOfBuffer();
        }

        byte[] buildKeyPrefix(K key) throws IOException {
            ensureKeySerialized(key);
            output.setPosition(afterKeyMark);
            return output.getCopyOfBuffer();
        }

        byte[] sharedBuffer() {
            return output.getSharedBuffer();
        }

        private void ensureKeySerialized(K key) throws IOException {
            if (afterKeyMark > 0 && Objects.equals(cachedKey, key)) {
                return;
            }
            output.clear();
            keySerializer.serialize(key, output);
            afterKeyMark = output.length();
            cachedKey = key == null ? null : keySerializer.copy(key);
        }
    }

    /** Reusable key builder that writes directly into a growable direct ByteBuffer. */
    static final class ReusableSerializedDirectKeyBuilder<K> {
        private final TypeSerializer<K> keySerializer;
        private final GrowingDirectBufferOutputStream outputStream;
        private final DataOutputViewStreamWrapper outputView;
        private K cachedKey;
        private int afterKeyMark;

        ReusableSerializedDirectKeyBuilder(TypeSerializer<K> keySerializer, int initialSize) {
            this.keySerializer = keySerializer;
            this.outputStream = new GrowingDirectBufferOutputStream(initialSize);
            this.outputView = new DataOutputViewStreamWrapper(outputStream);
        }

        <N> DirectBufferSlice buildKeyAndNamespace(
                K key, TypeSerializer<N> namespaceSerializer, N namespace) throws IOException {
            ensureKeySerialized(key);
            outputStream.setPosition(afterKeyMark);
            namespaceSerializer.serialize(namespace, outputView);
            outputView.writeInt(afterKeyMark);
            return outputStream.currentSlice();
        }

        <K2 extends K, UK, N> DirectBufferSlice buildMapKeyUserKeyAndNamespace(
                K2 key,
                TypeSerializer<UK> userKeySerializer,
                UK userKey,
                TypeSerializer<N> namespaceSerializer,
                N namespace)
                throws IOException {
            ensureKeySerialized(key);
            outputStream.setPosition(afterKeyMark);
            outputView.writeByte(0);
            userKeySerializer.serialize(userKey, outputView);
            int afterUserKeyMark = outputStream.position();
            namespaceSerializer.serialize(namespace, outputView);
            outputView.writeInt(afterKeyMark);
            outputView.writeInt(afterUserKeyMark - afterKeyMark - 1);
            return outputStream.currentSlice();
        }

        private void ensureKeySerialized(K key) throws IOException {
            if (afterKeyMark > 0 && Objects.equals(cachedKey, key)) {
                return;
            }
            outputStream.clear();
            keySerializer.serialize(key, outputView);
            afterKeyMark = outputStream.position();
            cachedKey = key == null ? null : keySerializer.copy(key);
        }
    }

    static final class DirectBufferSlice {
        private final ByteBuffer buffer;
        private final int length;

        DirectBufferSlice(ByteBuffer buffer, int length) {
            this.buffer = buffer;
            this.length = length;
        }

        ByteBuffer buffer() {
            return buffer;
        }

        int length() {
            return length;
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

        private void setPosition(int nextPosition) {
            if (nextPosition < 0 || nextPosition > buffer.position()) {
                throw new IllegalArgumentException("position out of range: " + nextPosition);
            }
            ((Buffer) buffer).position(nextPosition);
        }

        private int position() {
            return buffer.position();
        }

        private DirectBufferSlice currentSlice() {
            return new DirectBufferSlice(buffer, buffer.position());
        }
    }

    private static final class ByteBufferInputStream extends InputStream {
        private ByteBuffer current;

        private void reset(ByteBuffer source) {
            ByteBuffer view = source.duplicate();
            this.current = view;
        }

        @Override
        public int read() {
            if (current == null || !current.hasRemaining()) {
                return -1;
            }
            return current.get() & 0xFF;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            if (current == null || !current.hasRemaining()) {
                return -1;
            }
            int readable = Math.min(length, current.remaining());
            current.get(bytes, offset, readable);
            return readable;
        }
    }
}
