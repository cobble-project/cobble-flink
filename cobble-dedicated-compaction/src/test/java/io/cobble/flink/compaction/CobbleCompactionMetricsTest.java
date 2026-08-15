package io.cobble.flink.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cobble.DedicatedCompactionExecutor;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

class CobbleCompactionMetricsTest {
    @Test
    void monitorMetricsTrackPollsPlansFailuresAndQueueDepth() {
        RecordingMetricGroup group = new RecordingMetricGroup();
        CobbleCompactionMetrics.MonitorMetrics metrics =
                new CobbleCompactionMetrics.MonitorMetrics(group);

        metrics.pollSucceeded(3, 12L);
        metrics.planEmitted();
        metrics.pollFailed(8L);

        assertEquals(2L, group.counterValue("cobble.monitorPollsTotal"));
        assertEquals(3L, group.counterValue("cobble.plansDiscoveredTotal"));
        assertEquals(1L, group.counterValue("cobble.plansEmittedTotal"));
        assertEquals(1L, group.counterValue("cobble.monitorPollFailuresTotal"));
        assertEquals(20L, group.counterValue("cobble.monitorPollDurationMillisTotal"));
        assertEquals(2L, group.gaugeValue("cobble.pendingPlans"));
        assertEquals(8L, group.gaugeValue("cobble.lastMonitorPollDurationMillis"));

        metrics.close();
        assertEquals(0L, group.gaugeValue("cobble.pendingPlans"));
    }

    @Test
    void executorMetricsTrackEveryOutcomeAndFailure() {
        RecordingMetricGroup group = new RecordingMetricGroup();
        CobbleCompactionMetrics.ExecutorMetrics metrics =
                new CobbleCompactionMetrics.ExecutorMetrics(group);

        recordSuccess(metrics, DedicatedCompactionExecutor.Outcome.RESULT_PUBLISHED, 10L);
        recordSuccess(metrics, DedicatedCompactionExecutor.Outcome.STALE, 20L);
        recordSuccess(metrics, DedicatedCompactionExecutor.Outcome.WAITING_FOR_RESULT, 30L);
        metrics.executionStarted();
        assertEquals(1L, group.gaugeValue("cobble.executionInProgress"));
        metrics.executionFailed(40L);
        metrics.executionFinished();

        assertEquals(4L, group.counterValue("cobble.planExecutionsTotal"));
        assertEquals(1L, group.counterValue("cobble.resultsPublishedTotal"));
        assertEquals(1L, group.counterValue("cobble.stalePlansTotal"));
        assertEquals(1L, group.counterValue("cobble.waitingForResultTotal"));
        assertEquals(1L, group.counterValue("cobble.executionFailuresTotal"));
        assertEquals(100L, group.counterValue("cobble.executionDurationMillisTotal"));
        assertEquals(40L, group.gaugeValue("cobble.lastExecutionDurationMillis"));
        assertEquals(0L, group.gaugeValue("cobble.executionInProgress"));
    }

    private static void recordSuccess(
            CobbleCompactionMetrics.ExecutorMetrics metrics,
            DedicatedCompactionExecutor.Outcome outcome,
            long durationMillis) {
        metrics.executionStarted();
        metrics.executionSucceeded(outcome, durationMillis);
        metrics.executionFinished();
    }

    private static final class RecordingMetricGroup implements MetricGroup {
        private final Map<String, Counter> counters = new LinkedHashMap<>();
        private final Map<String, Gauge<?>> gauges = new LinkedHashMap<>();

        long counterValue(String name) {
            return counters.get(name).getCount();
        }

        long gaugeValue(String name) {
            return ((Number) gauges.get(name).getValue()).longValue();
        }

        @Override
        public Counter counter(String name) {
            return counters.computeIfAbsent(name, ignored -> new SimpleCounter());
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            counters.put(name, counter);
            return counter;
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            gauges.put(name, gauge);
            return gauge;
        }

        @Override
        public <H extends Histogram> H histogram(String name, H histogram) {
            return histogram;
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            return meter;
        }

        @Override
        public MetricGroup addGroup(String name) {
            return this;
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            return this;
        }

        @Override
        public String[] getScopeComponents() {
            return new String[0];
        }

        @Override
        public Map<String, String> getAllVariables() {
            return Collections.emptyMap();
        }

        @Override
        public String getMetricIdentifier(String metricName) {
            return metricName;
        }

        @Override
        public String getMetricIdentifier(String metricName, CharacterFilter filter) {
            return filter.filterCharacters(metricName);
        }
    }
}
