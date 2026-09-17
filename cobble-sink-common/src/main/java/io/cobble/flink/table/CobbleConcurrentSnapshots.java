package io.cobble.flink.table;

import io.cobble.ShardSnapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts every bucket snapshot before consuming completed uploads in completion order.
 *
 * <p>Completion callbacks only enqueue outcomes through {@code whenCompleteAsync}; they never
 * wait for another native upload, so a slow early bucket cannot head-of-line block later ones.
 */
final class CobbleConcurrentSnapshots {
    private static final int MAX_CALLBACK_THREADS = 4;
    private static final long CALLBACK_THREAD_KEEP_ALIVE_MILLIS = 100L;
    private static final Logger LOG = LoggerFactory.getLogger(CobbleConcurrentSnapshots.class);

    private CobbleConcurrentSnapshots() {}

    interface Trigger {
        int bucketId();

        CompletableFuture<ShardSnapshot> start();

        void close();
    }

    static final class Completed {
        final int bucketId;
        final ShardSnapshot snapshot;

        private Completed(int bucketId, ShardSnapshot snapshot) {
            this.bucketId = bucketId;
            this.snapshot = snapshot;
        }
    }

    static List<Completed> snapshotAll(
            Collection<? extends Trigger> triggers, long timeoutMillis)
            throws IOException, InterruptedException {
        if (timeoutMillis <= 0L) {
            throw new IOException("Cobble bucket snapshot timeout must be greater than zero.");
        }
        return new SnapshotAttempt(triggers, timeoutMillis).await();
    }

    private static final class SnapshotAttempt {
        private final List<Started> started;
        private final BlockingQueue<Outcome> outcomes;
        private final ThreadPoolExecutor callbacks;
        private final long deadlineNanos;
        private boolean retired;

        private SnapshotAttempt(Collection<? extends Trigger> triggers, long timeoutMillis) {
            this.started = new ArrayList<Started>(triggers.size());
            this.outcomes = new ArrayBlockingQueue<Outcome>(Math.max(1, triggers.size()));
            this.callbacks = callbackExecutor(triggers.size());
            this.deadlineNanos = deadlineAfter(timeoutMillis);

            Throwable triggerFailure = null;
            for (Trigger trigger : triggers) {
                try {
                    started.add(new Started(trigger, trigger.start()));
                } catch (RuntimeException | LinkageError error) {
                    triggerFailure = error;
                    break;
                }
            }
            for (Started start : started) {
                start.future.whenCompleteAsync(
                        (snapshot, error) -> outcomes.offer(new Outcome(start, snapshot, error)),
                        callbacks);
            }
            if (triggerFailure != null) {
                retire();
                rethrowTriggerFailure(triggerFailure);
            }
        }

        private List<Completed> await() throws IOException, InterruptedException {
            List<Completed> completed = new ArrayList<Completed>(started.size());
            try {
                for (int index = 0; index < started.size(); index++) {
                    Outcome outcome = outcomes.poll(remainingNanos(), TimeUnit.NANOSECONDS);
                    if (outcome == null) {
                        throw new IOException("Timed out waiting for Cobble bucket snapshots.");
                    }
                    if (outcome.failure != null) {
                        throw new IOException(
                                "Cobble bucket "
                                        + outcome.started.trigger.bucketId()
                                        + " snapshot failed.",
                                outcome.failure);
                    }
                    completed.add(new Completed(outcome.started.trigger.bucketId(), outcome.snapshot));
                }
                callbacks.shutdown();
                return completed;
            } catch (InterruptedException error) {
                retire();
                Thread.currentThread().interrupt();
                throw error;
            } catch (IOException error) {
                retire();
                throw error;
            }
        }

        private long remainingNanos() throws IOException {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0L) {
                throw new IOException("Timed out waiting for Cobble bucket snapshots.");
            }
            return remaining;
        }

        private void retire() {
            if (retired) {
                return;
            }
            retired = true;
            if (started.isEmpty()) {
                callbacks.shutdown();
                return;
            }
            AtomicInteger remainingClosures = new AtomicInteger(started.size());
            for (Started start : started) {
                start.future.whenCompleteAsync(
                        (snapshot, error) -> {
                            try {
                                start.trigger.close();
                            } catch (RuntimeException closeError) {
                                LOG.warn(
                                        "Failed to close retired Cobble bucket {}.",
                                        Integer.valueOf(start.trigger.bucketId()),
                                        closeError);
                            } finally {
                                if (remainingClosures.decrementAndGet() == 0) {
                                    callbacks.shutdown();
                                }
                            }
                        },
                        callbacks);
            }
        }
    }

    private static ThreadPoolExecutor callbackExecutor(int triggerCount) {
        int maximumThreads = Math.max(1, Math.min(MAX_CALLBACK_THREADS, triggerCount));
        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        maximumThreads,
                        maximumThreads,
                        CALLBACK_THREAD_KEEP_ALIVE_MILLIS,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(),
                        new DaemonThreadFactory());
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    private static long deadlineAfter(long timeoutMillis) {
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        long now = System.nanoTime();
        long deadline = now + timeoutNanos;
        return deadline < now ? Long.MAX_VALUE : deadline;
    }

    private static void rethrowTriggerFailure(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        throw (LinkageError) failure;
    }

    private static final class Started {
        private final Trigger trigger;
        private final CompletableFuture<ShardSnapshot> future;

        private Started(Trigger trigger, CompletableFuture<ShardSnapshot> future) {
            this.trigger = trigger;
            this.future = future;
        }
    }

    private static final class Outcome {
        private final Started started;
        private final ShardSnapshot snapshot;
        private final Throwable failure;

        private Outcome(Started started, ShardSnapshot snapshot, Throwable failure) {
            this.started = started;
            this.snapshot = snapshot;
            this.failure = failure;
        }
    }

    private static final class DaemonThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "cobble-snapshot-completion");
            thread.setDaemon(true);
            return thread;
        }
    }
}
