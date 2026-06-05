package io.cobble.flink.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * Shared serializer scratch space for one logical timer state.
 *
 * <p>Flink invokes timer queues from the owning task thread, so key-group child queues can share
 * these reusable buffers without synchronization or one buffer allocation per bucket.
 */
final class CobbleTimerSerializationContext<T> {

    private final DataInputDeserializer inputDeserializer;
    private final CobbleStateKeySerializer.ReusableDirectValueSerializer directKeySerializer;
    private final CobbleStateKeySerializer.ReusableDirectValueDeserializer directKeyDeserializer;

    private TypeSerializer<T> serializer;

    CobbleTimerSerializationContext(TypeSerializer<T> serializer) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
        this.inputDeserializer = new DataInputDeserializer();
        this.directKeySerializer = new CobbleStateKeySerializer.ReusableDirectValueSerializer(128);
        this.directKeyDeserializer = new CobbleStateKeySerializer.ReusableDirectValueDeserializer();
    }

    void updateSerializer(TypeSerializer<T> serializer) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
    }

    SerializedKey serializeElementKey(T element) {
        try {
            CobbleStateKeySerializer.DirectBufferSlice directSlice =
                    directKeySerializer.serialize(serializer, element);
            return new SerializedKey(
                    directSlice.buffer(),
                    directSlice.length(),
                    copyBytes(directSlice.buffer(), directSlice.length()));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Cobble timer element.", e);
        }
    }

    T deserializeElement(byte[] serializedKey) {
        try {
            inputDeserializer.setBuffer(serializedKey);
            return serializer.deserialize(inputDeserializer);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to deserialize Cobble timer element.", e);
        }
    }

    T deserializeElement(ByteBuffer serializedKey) {
        try {
            return directKeyDeserializer.deserialize(serializer, serializedKey.duplicate());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to deserialize Cobble timer element.", e);
        }
    }

    static int compareSerializedKeys(byte[] left, byte[] right) {
        int minLength = Math.min(left.length, right.length);
        for (int index = 0; index < minLength; index++) {
            int cmp = Integer.compare(left[index] & 0xFF, right[index] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    static byte[] copyBytes(ByteBuffer buffer, int length) {
        ByteBuffer view = buffer.duplicate();
        ((Buffer) view).position(0);
        ((Buffer) view).limit(length);
        byte[] copied = new byte[length];
        view.get(copied);
        return copied;
    }

    static boolean equalsSerializedKey(ByteBuffer left, byte[] right) {
        if (((Buffer) left).limit() - left.position() != right.length) {
            return false;
        }
        for (int index = 0; index < right.length; index++) {
            if ((left.get(left.position() + index) & 0xFF) != (right[index] & 0xFF)) {
                return false;
            }
        }
        return true;
    }

    /** One serialized timer key prepared for direct writes plus heap-backed lookups. */
    static final class SerializedKey {
        final ByteBuffer directBuffer;
        final int directLength;
        final byte[] heapBytes;

        private SerializedKey(ByteBuffer directBuffer, int directLength, byte[] heapBytes) {
            this.directBuffer = directBuffer;
            this.directLength = directLength;
            this.heapBytes = heapBytes;
        }
    }
}
