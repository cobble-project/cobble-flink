package io.cobble.flink.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
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
}
