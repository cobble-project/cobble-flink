package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

class CobbleTimerWriteBufferTest {

    @Test
    void duplicatePendingWriteDoesNotGrowOrReachNativeSink() {
        List<String> writes = new ArrayList<>();
        CobbleTimerWriteBuffer<Object> buffer =
                new CobbleTimerWriteBuffer<>(4, (bucket, key) -> writes.add(write(bucket, key)));

        assertTrue(buffer.add(3, key(7)));
        assertFalse(buffer.add(3, key(7)));
        assertEquals(1, buffer.size());
        assertTrue(writes.isEmpty());
    }

    @Test
    void globalLimitFlushesEveryBucketInStableOrder() {
        List<String> writes = new ArrayList<>();
        CobbleTimerWriteBuffer<Object> buffer =
                new CobbleTimerWriteBuffer<>(3, (bucket, key) -> writes.add(write(bucket, key)));

        assertTrue(buffer.add(2, key(9)));
        assertTrue(buffer.add(1, key(8)));
        assertTrue(buffer.add(1, key(3)));

        assertEquals(Arrays.asList("1:3", "1:8", "2:9"), writes);
        assertEquals(0, buffer.size());
    }

    @Test
    void partialFlushFailureRetainsTheWholeSetForIdempotentRetry() {
        List<String> writes = new ArrayList<>();
        AtomicBoolean failSecondWrite = new AtomicBoolean(true);
        CobbleTimerWriteBuffer<Object> buffer =
                new CobbleTimerWriteBuffer<>(
                        4,
                        (bucket, key) -> {
                            writes.add(write(bucket, key));
                            if (failSecondWrite.get() && writes.size() == 2) {
                                throw new IllegalStateException("injected write failure");
                            }
                        });
        buffer.add(1, key(1));
        buffer.add(1, key(2));
        buffer.add(2, key(3));

        assertThrows(IllegalStateException.class, buffer::flushPendingWrites);
        assertEquals(3, buffer.size());

        failSecondWrite.set(false);
        buffer.flushPendingWrites();

        assertEquals(Arrays.asList("1:1", "1:2", "1:1", "1:2", "2:3"), writes);
        assertEquals(0, buffer.size());
    }

    @Test
    void pendingViewsAreOrderedAndScopedByBucket() {
        CobbleTimerWriteBuffer<Object> buffer =
                new CobbleTimerWriteBuffer<>(8, (bucket, key) -> {});
        buffer.add(4, key(9));
        buffer.add(4, key(2));
        buffer.add(5, key(1));

        assertEquals(2, buffer.first(4).serializedKey[0]);
        assertEquals(1, buffer.entries(5).size());
        assertEquals(3, buffer.size());

        assertEquals(2, buffer.remove(4, key(2)).serializedKey[0]);
        assertEquals(9, buffer.first(4).serializedKey[0]);
        assertEquals(2, buffer.size());
    }

    private static byte[] key(int value) {
        return new byte[] {(byte) value};
    }

    private static String write(int bucket, byte[] key) {
        return bucket + ":" + key[0];
    }
}
