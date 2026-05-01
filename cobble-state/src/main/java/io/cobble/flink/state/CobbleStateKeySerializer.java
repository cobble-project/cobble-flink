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
    static final class ReusableSerializedKeyBuilder<K, N> {
        private final TypeSerializer<K> keySerializer;
        private final TypeSerializer<N> namespaceSerializer;
        private final DataOutputSerializer output;
        // For key+namespace row keys, key length is only needed when both parts are variable.
        private final boolean keyLengthStoredForKeyNamespace;
        // Cached serializer length tags (-1 means variable-sized).
        private final int keyLengthTag;
        private final int namespaceLengthTag;

        private K cachedKey;
        private int afterKeyMark;
        private Object cachedNamespaceValue;
        private int namespaceLength;
        // Map user-key serializer seen in the hot path; map layout flags are recalculated only
        // when serializer instance changes.
        private TypeSerializer<?> cachedMapUserKeySerializer;
        private boolean mapKeyLengthStored;
        private boolean mapNamespaceLengthStored;

        ReusableSerializedKeyBuilder(
                TypeSerializer<K> keySerializer,
                TypeSerializer<N> namespaceSerializer,
                int initialSize) {
            this.keySerializer = keySerializer;
            this.namespaceSerializer = namespaceSerializer;
            this.output = new DataOutputSerializer(initialSize);
            this.keyLengthTag = maybeFixedLength(keySerializer);
            this.namespaceLengthTag = maybeFixedLength(namespaceSerializer);
            this.keyLengthStoredForKeyNamespace =
                    shouldStoreKeyLengthForKeyNamespace(keyLengthTag, namespaceLengthTag);
        }

        byte[] buildKeyAndNamespace(K key, N namespace) throws IOException {
            ensureKeySerialized(key);
            ensureNamespaceSerialized(namespace);
            output.setPosition(afterKeyMark + namespaceLength);
            if (keyLengthStoredForKeyNamespace) {
                output.writeInt(afterKeyMark);
            }
            return output.getCopyOfBuffer();
        }

        <K2 extends K, UK> byte[] buildMapKeyNamespaceAndUserKey(
                K2 key,
                TypeSerializer<UK> userKeySerializer,
                UK userKey,
                N namespace)
                throws IOException {
            ensureKeySerialized(key);
            ensureNamespaceSerialized(namespace);
            ensureMapKeyLayout(userKeySerializer);
            output.setPosition(afterKeyMark + namespaceLength);
            output.writeByte(0);
            userKeySerializer.serialize(userKey, output);
            if (mapKeyLengthStored) {
                output.writeInt(afterKeyMark);
            }
            if (mapNamespaceLengthStored) {
                output.writeInt(namespaceLength);
            }
            return output.getCopyOfBuffer();
        }

        byte[] buildMapKeyNamespacePrefix(K key, N namespace) throws IOException {
            ensureKeySerialized(key);
            ensureNamespaceSerialized(namespace);
            output.setPosition(afterKeyMark + namespaceLength);
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
            namespaceLength = 0;
            cachedNamespaceValue = null;
        }

        private void ensureNamespaceSerialized(N namespace) throws IOException {
            if (namespaceLength > 0 && Objects.equals(cachedNamespaceValue, namespace)) {
                return;
            }
            output.setPosition(afterKeyMark);
            namespaceSerializer.serialize(namespace, output);
            namespaceLength = output.length() - afterKeyMark;
            cachedNamespaceValue = namespace == null ? null : namespaceSerializer.copy(namespace);
        }

        private <UK> void ensureMapKeyLayout(TypeSerializer<UK> userKeySerializer) {
            if (cachedMapUserKeySerializer == userKeySerializer) {
                return;
            }
            int userKeyLengthTag = maybeFixedLength(userKeySerializer);
            mapKeyLengthStored =
                    shouldStoreMapKeyLength(keyLengthTag, namespaceLengthTag, userKeyLengthTag);
            mapNamespaceLengthStored =
                    shouldStoreMapNamespaceLength(
                            keyLengthTag, namespaceLengthTag, userKeyLengthTag);
            cachedMapUserKeySerializer = userKeySerializer;
        }
    }

    /** Reusable key builder that writes directly into a growable direct ByteBuffer. */
    static final class ReusableSerializedDirectKeyBuilder<K, N> {
        private final TypeSerializer<K> keySerializer;
        private final TypeSerializer<N> namespaceSerializer;
        private final GrowingDirectBufferOutputStream outputStream;
        private final DataOutputViewStreamWrapper outputView;
        // For key+namespace row keys, key length is only needed when both parts are variable.
        private final boolean keyLengthStoredForKeyNamespace;
        // Cached serializer length tags (-1 means variable-sized).
        private final int keyLengthTag;
        private final int namespaceLengthTag;
        private K cachedKey;
        private int afterKeyMark;
        private Object cachedNamespaceValue;
        private int namespaceLength;
        // Map user-key serializer seen in the hot path; map layout flags are recalculated only
        // when serializer instance changes.
        private TypeSerializer<?> cachedMapUserKeySerializer;
        private boolean mapKeyLengthStored;
        private boolean mapNamespaceLengthStored;

        ReusableSerializedDirectKeyBuilder(
                TypeSerializer<K> keySerializer,
                TypeSerializer<N> namespaceSerializer,
                int initialSize) {
            this.keySerializer = keySerializer;
            this.namespaceSerializer = namespaceSerializer;
            this.outputStream = new GrowingDirectBufferOutputStream(initialSize);
            this.outputView = new DataOutputViewStreamWrapper(outputStream);
            this.keyLengthTag = maybeFixedLength(keySerializer);
            this.namespaceLengthTag = maybeFixedLength(namespaceSerializer);
            this.keyLengthStoredForKeyNamespace =
                    shouldStoreKeyLengthForKeyNamespace(keyLengthTag, namespaceLengthTag);
        }

        DirectBufferSlice buildKeyAndNamespace(K key, N namespace) throws IOException {
            ensureKeySerialized(key);
            ensureNamespaceSerialized(namespace);
            outputStream.setPosition(afterKeyMark + namespaceLength);
            if (keyLengthStoredForKeyNamespace) {
                outputView.writeInt(afterKeyMark);
            }
            return outputStream.currentSlice();
        }

        <K2 extends K, UK> DirectBufferSlice buildMapKeyNamespaceAndUserKey(
                K2 key,
                TypeSerializer<UK> userKeySerializer,
                UK userKey,
                N namespace)
                throws IOException {
            ensureKeySerialized(key);
            ensureNamespaceSerialized(namespace);
            ensureMapKeyLayout(userKeySerializer);
            outputStream.setPosition(afterKeyMark + namespaceLength);
            outputView.writeByte(0);
            userKeySerializer.serialize(userKey, outputView);
            if (mapKeyLengthStored) {
                outputView.writeInt(afterKeyMark);
            }
            if (mapNamespaceLengthStored) {
                outputView.writeInt(namespaceLength);
            }
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
            namespaceLength = 0;
            cachedNamespaceValue = null;
        }

        private void ensureNamespaceSerialized(N namespace) throws IOException {
            if (namespaceLength > 0 && Objects.equals(cachedNamespaceValue, namespace)) {
                return;
            }
            outputStream.setPosition(afterKeyMark);
            namespaceSerializer.serialize(namespace, outputView);
            namespaceLength = outputStream.position() - afterKeyMark;
            cachedNamespaceValue = namespace == null ? null : namespaceSerializer.copy(namespace);
        }

        private <UK> void ensureMapKeyLayout(TypeSerializer<UK> userKeySerializer) {
            if (cachedMapUserKeySerializer == userKeySerializer) {
                return;
            }
            int userKeyLengthTag = maybeFixedLength(userKeySerializer);
            mapKeyLengthStored =
                    shouldStoreMapKeyLength(keyLengthTag, namespaceLengthTag, userKeyLengthTag);
            mapNamespaceLengthStored =
                    shouldStoreMapNamespaceLength(
                            keyLengthTag, namespaceLengthTag, userKeyLengthTag);
            cachedMapUserKeySerializer = userKeySerializer;
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

    static int maybeFixedLength(TypeSerializer<?> serializer) {
        return serializer.getLength();
    }

    // key+namespace has two parts; one length field is needed only when both are variable.
    private static boolean shouldStoreKeyLengthForKeyNamespace(int keyLength, int namespaceLength) {
        return keyLength < 0 && namespaceLength < 0;
    }

    // map key has three parts: key/namespace/userKey.
    // We only need to persist (unknownParts - 1) lengths; this method decides whether key length
    // is one of those persisted lengths.
    static boolean shouldStoreMapKeyLength(int keyLength, int namespaceLength, int userKeyLength) {
        int unknownParts =
                (keyLength < 0 ? 1 : 0)
                        + (namespaceLength < 0 ? 1 : 0)
                        + (userKeyLength < 0 ? 1 : 0);
        return keyLength < 0 && unknownParts >= 2;
    }

    // map key has three parts: key/namespace/userKey.
    // We only need to persist (unknownParts - 1) lengths; this method decides whether namespace
    // length is one of those persisted lengths.
    static boolean shouldStoreMapNamespaceLength(
            int keyLength, int namespaceLength, int userKeyLength) {
        int unknownParts =
                (keyLength < 0 ? 1 : 0)
                        + (namespaceLength < 0 ? 1 : 0)
                        + (userKeyLength < 0 ? 1 : 0);
        return namespaceLength < 0 && unknownParts >= 2;
    }
}
