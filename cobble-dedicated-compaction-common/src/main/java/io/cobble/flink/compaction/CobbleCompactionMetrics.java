package io.cobble.flink.compaction;

import io.cobble.DedicatedCompactionExecutor;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import java.util.concurrent.TimeUnit;

/** Flink operator metrics for dedicated-compaction monitoring and execution. */
final class CobbleCompactionMetrics {
    private static final String PREFIX = "cobble.";

    private CobbleCompactionMetrics() {}

    static long elapsedMillis(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }

    static final class MonitorMetrics {
        private final Counter polls;
        private final Counter plansDiscovered;
        private final Counter plansEmitted;
        private final Counter pollFailures;
        private final Counter pollDurationMillis;
        private volatile long pendingPlans;
        private volatile long lastPollDurationMillis;

        MonitorMetrics(MetricGroup group) {
            polls = group.counter(PREFIX + "monitorPollsTotal");
            plansDiscovered = group.counter(PREFIX + "plansDiscoveredTotal");
            plansEmitted = group.counter(PREFIX + "plansEmittedTotal");
            pollFailures = group.counter(PREFIX + "monitorPollFailuresTotal");
            pollDurationMillis = group.counter(PREFIX + "monitorPollDurationMillisTotal");
            group.gauge(PREFIX + "pendingPlans", (Gauge<Long>) () -> pendingPlans);
            group.gauge(
                    PREFIX + "lastMonitorPollDurationMillis",
                    (Gauge<Long>) () -> lastPollDurationMillis);
        }

        void pollSucceeded(int discovered, long durationMillis) {
            polls.inc();
            plansDiscovered.inc(discovered);
            pendingPlans += discovered;
            recordDuration(durationMillis);
        }

        void pollFailed(long durationMillis) {
            polls.inc();
            pollFailures.inc();
            recordDuration(durationMillis);
        }

        void planEmitted() {
            plansEmitted.inc();
            pendingPlans = Math.max(0L, pendingPlans - 1L);
        }

        void close() {
            pendingPlans = 0L;
        }

        private void recordDuration(long durationMillis) {
            long normalized = Math.max(0L, durationMillis);
            pollDurationMillis.inc(normalized);
            lastPollDurationMillis = normalized;
        }
    }

    static final class ExecutorMetrics {
        private final Counter executions;
        private final Counter resultsPublished;
        private final Counter stalePlans;
        private final Counter waitingForResult;
        private final Counter executionFailures;
        private final Counter executionDurationMillis;
        private volatile long executionInProgress;
        private volatile long lastExecutionDurationMillis;

        ExecutorMetrics(MetricGroup group) {
            executions = group.counter(PREFIX + "planExecutionsTotal");
            resultsPublished = group.counter(PREFIX + "resultsPublishedTotal");
            stalePlans = group.counter(PREFIX + "stalePlansTotal");
            waitingForResult = group.counter(PREFIX + "waitingForResultTotal");
            executionFailures = group.counter(PREFIX + "executionFailuresTotal");
            executionDurationMillis = group.counter(PREFIX + "executionDurationMillisTotal");
            group.gauge(
                    PREFIX + "executionInProgress", (Gauge<Long>) () -> executionInProgress);
            group.gauge(
                    PREFIX + "lastExecutionDurationMillis",
                    (Gauge<Long>) () -> lastExecutionDurationMillis);
        }

        void executionStarted() {
            executionInProgress = 1L;
        }

        void executionSucceeded(
                DedicatedCompactionExecutor.Outcome outcome, long durationMillis) {
            executions.inc();
            recordDuration(durationMillis);
            switch (outcome) {
                case RESULT_PUBLISHED:
                    resultsPublished.inc();
                    break;
                case STALE:
                    stalePlans.inc();
                    break;
                case WAITING_FOR_RESULT:
                    waitingForResult.inc();
                    break;
            }
        }

        void executionFailed(long durationMillis) {
            executions.inc();
            executionFailures.inc();
            recordDuration(durationMillis);
        }

        void executionFinished() {
            executionInProgress = 0L;
        }

        private void recordDuration(long durationMillis) {
            long normalized = Math.max(0L, durationMillis);
            executionDurationMillis.inc(normalized);
            lastExecutionDurationMillis = normalized;
        }
    }
}
