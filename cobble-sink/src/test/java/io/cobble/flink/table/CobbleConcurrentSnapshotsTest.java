package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.ShardSnapshot;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

class CobbleConcurrentSnapshotsTest {
    private static final long TEST_TIMEOUT_MILLIS = 10_000L;

    @Test
    void triggersEveryBucketBeforeConsumingCompletionOrder() throws Exception {
        CountDownLatch starts = new CountDownLatch(3);
        FakeTrigger first = new FakeTrigger(0, starts);
        CountDownLatch secondPublished = new CountDownLatch(1);
        FakeTrigger second = new FakeTrigger(1, starts, new CountDownLatch(0), secondPublished);
        CountDownLatch thirdPublished = new CountDownLatch(1);
        FakeTrigger third = new FakeTrigger(2, starts, new CountDownLatch(0), thirdPublished);
        CompletableFuture<List<CobbleConcurrentSnapshots.Completed>> completed =
                new CompletableFuture<List<CobbleConcurrentSnapshots.Completed>>();
        TestWorker worker =
                start(
                        () ->
                                completed.complete(
                                        CobbleConcurrentSnapshots.snapshotAll(
                                                Arrays.asList(first, second, third),
                                                TEST_TIMEOUT_MILLIS)));

        try {
            assertTrue(starts.await(5L, TimeUnit.SECONDS));
            second.complete(1L);
            assertTrue(secondPublished.await(5L, TimeUnit.SECONDS));
            assertThrows(
                    TimeoutException.class, () -> worker.outcome.get(100L, TimeUnit.MILLISECONDS));
            third.complete(2L);
            assertTrue(thirdPublished.await(5L, TimeUnit.SECONDS));
            first.complete(0L);

            assertNull(await(worker));
            List<CobbleConcurrentSnapshots.Completed> snapshots =
                    completed.get(5L, TimeUnit.SECONDS);
            assertEquals(
                    Arrays.asList(1, 2, 0),
                    Arrays.asList(
                            snapshots.get(0).bucketId,
                            snapshots.get(1).bucketId,
                            snapshots.get(2).bucketId));
        } finally {
            finish(worker, first, second, third);
        }
    }

    @Test
    void failedUploadReturnsWithoutWaitingForOtherBucketAndClosesLater() throws Exception {
        CountDownLatch starts = new CountDownLatch(2);
        CountDownLatch closes = new CountDownLatch(2);
        FakeTrigger failed = new FakeTrigger(0, starts, closes);
        FakeTrigger pending = new FakeTrigger(1, starts, closes);
        TestWorker worker =
                start(
                        () ->
                                CobbleConcurrentSnapshots.snapshotAll(
                                        Arrays.asList(failed, pending), TEST_TIMEOUT_MILLIS));

        try {
            assertTrue(starts.await(5L, TimeUnit.SECONDS));
            failed.future.completeExceptionally(new IllegalStateException("failed"));

            assertInstanceOf(IOException.class, await(worker));
            assertEquals(0, pending.closeCalls);
            pending.complete(1L);
            assertTrue(closes.await(5L, TimeUnit.SECONDS));
            assertEquals(1, failed.closeCalls);
            assertEquals(1, pending.closeCalls);
        } finally {
            finish(worker, failed, pending);
        }
    }

    @Test
    void timeoutReturnsAndDefersTableCloseUntilUploadCompletes() throws Exception {
        CountDownLatch starts = new CountDownLatch(2);
        CountDownLatch closes = new CountDownLatch(2);
        FakeTrigger first = new FakeTrigger(0, starts, closes);
        FakeTrigger pending = new FakeTrigger(1, starts, closes);
        TestWorker worker =
                start(
                        () ->
                                CobbleConcurrentSnapshots.snapshotAll(
                                        Arrays.asList(first, pending), 250L));

        try {
            assertTrue(starts.await(5L, TimeUnit.SECONDS));
            first.complete(0L);

            assertInstanceOf(IOException.class, await(worker));
            assertEquals(0, pending.closeCalls);
            pending.complete(1L);
            assertTrue(closes.await(5L, TimeUnit.SECONDS));
            assertEquals(1, first.closeCalls);
            assertEquals(1, pending.closeCalls);
        } finally {
            finish(worker, first, pending);
        }
    }

    @Test
    void interruptionReturnsAndDefersTableCloseUntilUploadCompletes() throws Exception {
        CountDownLatch starts = new CountDownLatch(1);
        CountDownLatch closes = new CountDownLatch(1);
        FakeTrigger trigger = new FakeTrigger(0, starts, closes);
        TestWorker worker =
                start(
                        () ->
                                CobbleConcurrentSnapshots.snapshotAll(
                                        Arrays.asList(trigger), TEST_TIMEOUT_MILLIS));

        try {
            assertTrue(starts.await(5L, TimeUnit.SECONDS));
            worker.thread.interrupt();

            assertInstanceOf(InterruptedException.class, await(worker));
            assertEquals(1L, closes.getCount());
            trigger.complete(0L);
            assertTrue(closes.await(5L, TimeUnit.SECONDS));
            assertEquals(1, trigger.closeCalls);
        } finally {
            finish(worker, trigger);
        }
    }

    @Test
    void triggerFailureRetiresEarlierSnapshots() throws Exception {
        CountDownLatch starts = new CountDownLatch(2);
        CountDownLatch closes = new CountDownLatch(1);
        FakeTrigger first = new FakeTrigger(0, starts, closes);
        FakeTrigger failing = new FakeTrigger(1, starts, new CountDownLatch(0));
        failing.triggerFailure = new LinkageError("native trigger failed");
        TestWorker worker =
                start(
                        () ->
                                CobbleConcurrentSnapshots.snapshotAll(
                                        Arrays.asList(first, failing), TEST_TIMEOUT_MILLIS));

        try {
            assertTrue(starts.await(5L, TimeUnit.SECONDS));
            assertInstanceOf(LinkageError.class, await(worker));
            assertEquals(1L, closes.getCount());
            first.complete(0L);
            assertTrue(closes.await(5L, TimeUnit.SECONDS));
        } finally {
            finish(worker, first, failing);
        }
    }

    @Test
    void emptyAttemptCompletes() throws Exception {
        assertTrue(
                CobbleConcurrentSnapshots.snapshotAll(
                                Collections.<FakeTrigger>emptyList(), TEST_TIMEOUT_MILLIS)
                        .isEmpty());
    }

    @Test
    void lateOldAttemptCompletionDoesNotAffectFreshAttempt() throws Exception {
        CountDownLatch oldStarts = new CountDownLatch(1);
        CountDownLatch oldCloses = new CountDownLatch(1);
        FakeTrigger oldTrigger = new FakeTrigger(0, oldStarts, oldCloses);
        TestWorker oldWorker =
                start(
                        () ->
                                CobbleConcurrentSnapshots.snapshotAll(
                                        Collections.singletonList(oldTrigger), 250L));

        try {
            assertTrue(oldStarts.await(5L, TimeUnit.SECONDS));
            assertInstanceOf(IOException.class, await(oldWorker));

            CountDownLatch freshStarts = new CountDownLatch(1);
            FakeTrigger freshTrigger = new FakeTrigger(0, freshStarts);
            CompletableFuture<List<CobbleConcurrentSnapshots.Completed>> freshCompleted =
                    new CompletableFuture<List<CobbleConcurrentSnapshots.Completed>>();
            TestWorker freshWorker =
                    start(
                            () ->
                                    freshCompleted.complete(
                                            CobbleConcurrentSnapshots.snapshotAll(
                                                    Collections.singletonList(freshTrigger),
                                                    TEST_TIMEOUT_MILLIS)));
            try {
                assertTrue(freshStarts.await(5L, TimeUnit.SECONDS));
                oldTrigger.complete(0L);
                assertTrue(oldCloses.await(5L, TimeUnit.SECONDS));
                assertEquals(1, oldTrigger.closeCalls);
                assertEquals(0, freshTrigger.closeCalls);

                freshTrigger.complete(1L);
                assertNull(await(freshWorker));
                assertEquals(
                        1L, freshCompleted.get(5L, TimeUnit.SECONDS).get(0).snapshot.snapshotId);
            } finally {
                finish(freshWorker, freshTrigger);
            }
        } finally {
            finish(oldWorker, oldTrigger);
        }
    }

    private static TestWorker start(ThrowingRunnable action) {
        CompletableFuture<Throwable> outcome = new CompletableFuture<Throwable>();
        Thread worker =
                new Thread(
                        () -> {
                            try {
                                action.run();
                                outcome.complete(null);
                            } catch (Throwable error) {
                                outcome.complete(error);
                            }
                        },
                        "cobble-concurrent-snapshot-test");
        worker.start();
        return new TestWorker(worker, outcome);
    }

    private static Throwable await(TestWorker worker) throws Exception {
        return worker.outcome.get(5L, TimeUnit.SECONDS);
    }

    private static void finish(TestWorker worker, FakeTrigger... triggers) throws Exception {
        for (FakeTrigger trigger : triggers) {
            trigger.complete(trigger.bucket);
        }
        await(worker);
        worker.thread.join(5_000L);
        assertFalse(worker.thread.isAlive());
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static final class TestWorker {
        final Thread thread;
        final CompletableFuture<Throwable> outcome;

        private TestWorker(Thread thread, CompletableFuture<Throwable> outcome) {
            this.thread = thread;
            this.outcome = outcome;
        }
    }

    private static final class FakeTrigger implements CobbleConcurrentSnapshots.Trigger {
        final int bucket;
        final CountDownLatch starts;
        final CountDownLatch closes;
        final CompletableFuture<ShardSnapshot> future;
        volatile Error triggerFailure;
        volatile int closeCalls;

        FakeTrigger(int bucket, CountDownLatch starts) {
            this(bucket, starts, new CountDownLatch(0));
        }

        FakeTrigger(int bucket, CountDownLatch starts, CountDownLatch closes) {
            this(bucket, starts, closes, new CountDownLatch(0));
        }

        FakeTrigger(
                int bucket,
                CountDownLatch starts,
                CountDownLatch closes,
                CountDownLatch published) {
            this.bucket = bucket;
            this.starts = starts;
            this.closes = closes;
            this.future = new ObservingFuture(published);
        }

        @Override
        public int bucketId() {
            return bucket;
        }

        @Override
        public CompletableFuture<ShardSnapshot> start() {
            starts.countDown();
            if (triggerFailure != null) {
                throw triggerFailure;
            }
            return future;
        }

        @Override
        public void close() {
            closeCalls++;
            closes.countDown();
        }

        void complete(long snapshotId) {
            ShardSnapshot snapshot = new ShardSnapshot();
            snapshot.dbId = "bucket-" + bucket;
            snapshot.snapshotId = snapshotId;
            future.complete(snapshot);
        }
    }

    private static final class ObservingFuture extends CompletableFuture<ShardSnapshot> {
        private final CountDownLatch published;

        private ObservingFuture(CountDownLatch published) {
            this.published = published;
        }

        @Override
        public CompletableFuture<ShardSnapshot> whenCompleteAsync(
                BiConsumer<? super ShardSnapshot, ? super Throwable> action, Executor executor) {
            return super.whenCompleteAsync(
                    (snapshot, error) -> {
                        action.accept(snapshot, error);
                        published.countDown();
                    },
                    executor);
        }
    }
}
