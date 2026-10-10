package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.cobble.Config;
import io.cobble.structured.ColumnValue;
import io.cobble.structured.Db;
import io.cobble.structured.DirectPriorityQueueBatch;
import io.cobble.structured.PriorityQueue;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

import java.nio.file.Path;

class CobblePriorityQueueSetFactoryTest {

    @Test
    void adaptiveFactoryWithoutTimerRegistrationDoesNotSwitch() throws Exception {
        Db db = mock(Db.class);
        CobblePriorityQueueSetFactory factory = factory(db, Config.MemtableType.ADAPTIVE);

        assertFalse(factory.hasQueues());
        factory.close();

        verifyNoInteractions(db);
    }

    @Test
    void adaptiveTimersPinOnceBeforeNativeRegistration() throws Exception {
        Db db = mockedDb();
        CobblePriorityQueueSetFactory factory = factory(db, Config.MemtableType.ADAPTIVE);
        try {
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, VoidNamespace>> first =
                    factory.create("first", serializer(), true);
            assertSame(first, factory.create("first", serializer()));
            factory.create("second", serializer());

            InOrder order = inOrder(db);
            order.verify(db).switchMemtableType(Config.MemtableType.SKIPLIST, true);
            order.verify(db)
                    .getOrNewPriorityQueue(
                            CobblePriorityQueueSetFactory.timerQueueColumnFamilyName("first"));
            verify(db, times(1)).switchMemtableType(Config.MemtableType.SKIPLIST, true);
            verify(db, times(2)).getOrNewPriorityQueue(anyString());
        } finally {
            factory.close();
        }
    }

    @Test
    void explicitlyConfiguredMemtableTypesAreNotOverridden() throws Exception {
        for (Config.MemtableType type :
                new Config.MemtableType[] {
                    Config.MemtableType.HASH, Config.MemtableType.VEC, Config.MemtableType.SKIPLIST
                }) {
            Db db = mockedDb();
            CobblePriorityQueueSetFactory factory = factory(db, type);
            try {
                factory.create("first", serializer());
                factory.create("second", serializer());
                verify(db, never()).switchMemtableType(any(), anyBoolean());
                verify(db, times(2)).getOrNewPriorityQueue(anyString());
            } finally {
                factory.close();
            }
        }
    }

    @Test
    void failedPinIsRetriedBeforeRegisteringTimers() throws Exception {
        Db db = mockedDb();
        RuntimeException failure = new IllegalStateException("switch failed");
        doThrow(failure)
                .doNothing()
                .when(db)
                .switchMemtableType(Config.MemtableType.SKIPLIST, true);
        CobblePriorityQueueSetFactory factory = factory(db, Config.MemtableType.ADAPTIVE);
        try {
            assertSame(
                    failure,
                    assertThrows(
                            RuntimeException.class, () -> factory.create("first", serializer())));
            assertFalse(factory.hasQueues());
            verify(db, never()).getOrNewPriorityQueue(anyString());

            factory.create("first", serializer());
            factory.create("second", serializer());
            verify(db, times(2)).switchMemtableType(Config.MemtableType.SKIPLIST, true);
            verify(db, times(2)).getOrNewPriorityQueue(anyString());
        } finally {
            factory.close();
        }
    }

    @Test
    void nativeAdaptiveTimerPinPreservesDataAndTimerOrdering(@TempDir Path tempDir)
            throws Exception {
        Config config = new Config().addVolume(tempDir.toString()).numColumns(1).totalBuckets(16);
        config.memtableType = Config.MemtableType.ADAPTIVE;
        // Match the backend test fixture's process-wide direct buffer pool expansion.
        config.jniDirectBufferSize = 8 * 1024;
        config.jniDirectBufferPoolSize = 128;
        byte[] key = new byte[] {1};
        byte[] value = new byte[] {2};
        try (Db nativeDb = Db.open(config)) {
            nativeDb.put(0, key, 0, ColumnValue.ofBytes(value));
            Db db = spy(nativeDb);
            CobblePriorityQueueSetFactory factory = factory(db, config.memtableType);
            try {
                KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, VoidNamespace>>
                        queue = factory.create("first", serializer());
                factory.create("second", serializer());
                TimerHeapInternalTimer<Integer, VoidNamespace> later =
                        new TimerHeapInternalTimer<>(20L, 1, VoidNamespace.INSTANCE);
                TimerHeapInternalTimer<Integer, VoidNamespace> earlier =
                        new TimerHeapInternalTimer<>(10L, 2, VoidNamespace.INSTANCE);
                queue.add(later);
                queue.add(earlier);
                assertEquals(earlier, queue.poll());
                assertEquals(later, queue.poll());
                assertArrayEquals(value, nativeDb.get(0, key).getBytes(0));
                assertEquals(Config.MemtableType.ADAPTIVE, config.memtableType);
                verify(db, times(1)).switchMemtableType(Config.MemtableType.SKIPLIST, true);
            } finally {
                factory.close();
            }
        }
    }

    private static Db mockedDb() {
        Db db = mock(Db.class);
        when(db.getOrNewPriorityQueue(anyString()))
                .thenAnswer(
                        ignored -> {
                            PriorityQueue queue = mock(PriorityQueue.class);
                            DirectPriorityQueueBatch empty = mock(DirectPriorityQueueBatch.class);
                            when(empty.isEmpty()).thenReturn(true);
                            when(queue.peekBatchDirect(anyInt(), anyInt())).thenReturn(empty);
                            return queue;
                        });
        return db;
    }

    private static CobblePriorityQueueSetFactory factory(Db db, Config.MemtableType type) {
        return new CobblePriorityQueueSetFactory(
                db, KeyGroupRange.of(0, 15), 16, type, false, null);
    }

    private static TimerSerializer<Integer, VoidNamespace> serializer() {
        return new TimerSerializer<>(IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE);
    }
}
