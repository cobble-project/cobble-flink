package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

class DirectDelimitedListSerializerTest {
    @Test
    void encodeAllMatchesFlinkSerializerBytesForLongAndStringValues() throws Exception {
        DirectDelimitedListSerializer serializer = new DirectDelimitedListSerializer(8);

        assertArrayEquals(
                expected(LongSerializer.INSTANCE, Arrays.asList(1L, 2L, 3L)),
                bytes(serializer.encodeAll(LongSerializer.INSTANCE, Arrays.asList(1L, 2L, 3L))));
        assertArrayEquals(
                expected(StringSerializer.INSTANCE, Arrays.asList("a", "你好", "")),
                bytes(
                        serializer.encodeAll(
                                StringSerializer.INSTANCE, Arrays.asList("a", "你好", ""))));
    }

    private static <T> byte[] expected(TypeSerializer<T> serializer, List<T> values)
            throws Exception {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        for (T value : values) {
            byte[] encoded = CobbleStateKeySerializer.serialize(serializer, value);
            result.write(encoded);
            result.write(DirectDelimitedListSerializer.DELIMITER);
        }
        return result.toByteArray();
    }

    private static byte[] bytes(CobbleStateKeySerializer.DirectBufferSlice slice) {
        ByteBuffer view = slice.buffer().duplicate();
        view.position(0);
        view.limit(slice.length());
        byte[] result = new byte[slice.length()];
        view.get(result);
        return result;
    }
}
