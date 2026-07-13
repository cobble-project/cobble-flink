package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.CounterMetricValue;
import io.cobble.GaugeMetricValue;
import io.cobble.HistogramMetricValue;
import io.cobble.MetricSample;
import io.cobble.MetricValue;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Unit coverage for the generic, cached native metric monitor. */
class CobbleNativeMetricsTest {

    @Test
    void registersEveryTypeWithCamelNamesAndSortedArbitraryLabels() {
        AtomicInteger calls = new AtomicInteger();
        CapturingMetricGroup group = new CapturingMetricGroup();
        CobbleNativeMetrics.Monitor monitor =
                CobbleNativeMetrics.register(
                        group,
                        () -> {
                            calls.incrementAndGet();
                            return Arrays.asList(
                                    sample(
                                            "compaction_read_bytes_total",
                                            labels("db_id", "hidden"),
                                            new CounterMetricValue(7L)),
                                    sample(
                                            "__future___queue_depth__",
                                            labels(
                                                    "z_label",
                                                    "last",
                                                    "db_id",
                                                    "hidden",
                                                    "a__label",
                                                    "first"),
                                            new GaugeMetricValue(3.5d)),
                                    sample(
                                            "sst_block_compression_ratio",
                                            labels("compression", "lz4", "db_id", "hidden"),
                                            new HistogramMetricValue(4L, 10.0d, 1.0d, 4.0d)),
                                    null);
                        },
                        60_000L);

        Counter counter = group.findCounter("cobble.compactionReadBytesTotal");
        Gauge<?> gauge = group.findGauge("aLabel=first.zLabel=last.cobble.futureQueueDepth");
        Histogram histogram =
                group.findHistogram("compression=lz4.cobble.sstBlockCompressionRatio");
        assertInstanceOf(View.class, counter);
        assertInstanceOf(View.class, gauge);
        assertInstanceOf(View.class, histogram);
        assertEquals(7L, counter.getCount());
        assertEquals(3.5d, gauge.getValue());
        assertEquals(4L, histogram.getCount());
        assertEquals(4, histogram.getStatistics().size());
        assertEquals(2.5d, histogram.getStatistics().getMean());
        assertEquals(1L, histogram.getStatistics().getMin());
        assertEquals(4L, histogram.getStatistics().getMax());
        assertTrue(Double.isNaN(histogram.getStatistics().getQuantile(0.5d)));
        assertTrue(Double.isNaN(histogram.getStatistics().getStdDev()));
        assertEquals(1, calls.get());
        assertFalse(group.metricNames().stream().anyMatch(name -> name.contains("dbId")));

        counter.inc();
        counter.inc(5L);
        counter.dec();
        counter.dec(5L);
        assertEquals(7L, counter.getCount(), "native counters are read-only snapshots");
        monitor.close();
    }

    @Test
    void convertsSnakeCaseDefensively() {
        assertEquals("queueDepth", CobbleNativeMetrics.toLowerCamelCase("queueDepth"));
        assertEquals(
                "compactionReadBytesTotal",
                CobbleNativeMetrics.toLowerCamelCase("__COMPACTION___READ_BYTES_TOTAL__"));
        assertEquals(
                "leadingTrailing", CobbleNativeMetrics.toLowerCamelCase("_leading__trailing_"));
        assertEquals("dbId", CobbleNativeMetrics.toLowerCamelCase("db_id"));
        assertEquals("", CobbleNativeMetrics.toLowerCamelCase("___"));
        assertEquals("", CobbleNativeMetrics.toLowerCamelCase(null));
    }

    @Test
    void discoversFutureFamiliesOncePerFreshSnapshotAndRejectsCollisions() {
        AtomicInteger calls = new AtomicInteger();
        CapturingMetricGroup group = new CapturingMetricGroup();
        CobbleNativeMetrics.Monitor monitor =
                CobbleNativeMetrics.register(
                        group, () -> discoverySamples(calls.incrementAndGet()), 0L);

        Counter initial = group.findCounter("cobble.futureMetricTotal");
        assertEquals(1L, initial.getCount());
        assertEquals(1, group.metricNames().size());

        ((View) initial).update();
        assertEquals(2L, initial.getCount());
        assertEquals(8.0d, group.findGauge("kind=new.cobble.lateQueueDepth").getValue());
        assertEquals(3, group.metricNames().size());
        assertNull(group.findGauge("cobble.futureMetricTotal"));
        assertEquals(1.0d, group.findGauge("aB=same.cobble.badLabelMetric").getValue());

        ((View) initial).update();
        assertEquals(3L, initial.getCount());
        assertEquals(3, group.metricNames().size(), "fresh discovery must remain idempotent");
        assertEquals(3, calls.get());
        monitor.close();
    }

    @Test
    void retainsValuesOnFailureRecoversAndStopsReadsAfterClose() {
        AtomicInteger calls = new AtomicInteger();
        CapturingMetricGroup group = new CapturingMetricGroup();
        CobbleNativeMetrics.Monitor monitor =
                CobbleNativeMetrics.register(
                        group,
                        () -> {
                            int call = calls.incrementAndGet();
                            if (call == 2) {
                                throw new IllegalStateException("native metrics unavailable");
                            }
                            return Arrays.asList(
                                    sample(
                                            "memtable_flushes_total",
                                            Collections.<String, String>emptyMap(),
                                            new CounterMetricValue(call == 1 ? 11L : 17L)),
                                    sample(
                                            "recovered_metric",
                                            Collections.<String, String>emptyMap(),
                                            new GaugeMetricValue(9.0d)));
                        },
                        0L);
        Counter counter = group.findCounter("cobble.memtableFlushesTotal");
        assertEquals(11L, counter.getCount());

        ((View) counter).update();
        assertEquals(11L, counter.getCount());
        ((View) counter).update();
        assertEquals(17L, counter.getCount());
        assertEquals(9.0d, group.findGauge("cobble.recoveredMetric").getValue());

        monitor.close();
        ((View) counter).update();
        assertEquals(3, calls.get());
        assertEquals(17L, counter.getCount());
    }

    @Test
    void registrationFailureIsBestEffortAfterInitialSnapshot() {
        AtomicInteger calls = new AtomicInteger();
        CobbleNativeMetrics.Monitor monitor =
                CobbleNativeMetrics.register(
                        new PartiallyThrowingMetricGroup(),
                        () -> {
                            calls.incrementAndGet();
                            return Collections.singletonList(
                                    sample(
                                            "arbitrary_future_total",
                                            Collections.<String, String>emptyMap(),
                                            new CounterMetricValue(1L)));
                        },
                        0L);

        assertEquals(1, calls.get());
        monitor.close();
    }

    @Test
    void concurrentViewUpdatesReuseOneGenerationWithoutDuplicateRegistration() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CapturingMetricGroup group = new CapturingMetricGroup();
        CobbleNativeMetrics.Monitor monitor =
                CobbleNativeMetrics.register(
                        group,
                        () -> {
                            calls.incrementAndGet();
                            return Arrays.asList(
                                    sample(
                                            "concurrent_counter_total",
                                            Collections.<String, String>emptyMap(),
                                            new CounterMetricValue(5L)),
                                    sample(
                                            "concurrent_gauge",
                                            labels("worker_kind", "test"),
                                            new GaugeMetricValue(2.0d)));
                        },
                        60_000L);
        View counter = (View) group.findCounter("cobble.concurrentCounterTotal");
        View gauge = (View) group.findGauge("workerKind=test.cobble.concurrentGauge");
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> updates = new ArrayList<Future<?>>();
            for (int index = 0; index < 100; index++) {
                final View view = index % 2 == 0 ? counter : gauge;
                updates.add(executor.submit(view::update));
            }
            for (Future<?> update : updates) {
                update.get(5L, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
            monitor.close();
        }
        assertEquals(1, calls.get());
        assertEquals(2, group.metricNames().size());
    }

    private static List<MetricSample> discoverySamples(int generation) {
        List<MetricSample> samples = new ArrayList<MetricSample>();
        samples.add(
                sample(
                        "future_metric_total",
                        Collections.<String, String>emptyMap(),
                        new CounterMetricValue(generation)));
        if (generation > 1) {
            samples.add(
                    sample("late_queue_depth", labels("kind", "new"), new GaugeMetricValue(8.0d)));
            samples.add(
                    sample(
                            "future_metric_total",
                            Collections.<String, String>emptyMap(),
                            new GaugeMetricValue(99.0d)));
            samples.add(
                    sample(
                            "future__metric_total",
                            Collections.<String, String>emptyMap(),
                            new CounterMetricValue(99L)));
            samples.add(
                    sample(
                            "bad_label_metric",
                            labels("a_b", "first", "a__b", "second"),
                            new GaugeMetricValue(1.0d)));
            samples.add(
                    sample("bad_label_metric", labels("a_b", "same"), new GaugeMetricValue(1.0d)));
            samples.add(
                    sample("bad_label_metric", labels("a__b", "same"), new GaugeMetricValue(2.0d)));
        }
        return samples;
    }

    private static MetricSample sample(String name, Map<String, String> labels, MetricValue value) {
        return new MetricSample(name, labels, value);
    }

    private static Map<String, String> labels(String... values) {
        Map<String, String> labels = new LinkedHashMap<String, String>();
        for (int i = 0; i < values.length; i += 2) {
            labels.put(values[i], values[i + 1]);
        }
        return labels;
    }

    private static final class CapturingMetricGroup implements MetricGroup {
        private final Map<String, Metric> metrics;
        private final String prefix;

        private CapturingMetricGroup() {
            this(new LinkedHashMap<String, Metric>(), "");
        }

        private CapturingMetricGroup(Map<String, Metric> metrics, String prefix) {
            this.metrics = metrics;
            this.prefix = prefix;
        }

        private String metricName(String name) {
            return prefix.isEmpty() ? name : prefix + "." + name;
        }

        private Counter findCounter(String name) {
            Metric metric = metrics.get(name);
            return metric instanceof Counter ? (Counter) metric : null;
        }

        private Gauge<?> findGauge(String name) {
            Metric metric = metrics.get(name);
            return metric instanceof Gauge ? (Gauge<?>) metric : null;
        }

        private Histogram findHistogram(String name) {
            Metric metric = metrics.get(name);
            return metric instanceof Histogram ? (Histogram) metric : null;
        }

        private List<String> metricNames() {
            return new ArrayList<String>(metrics.keySet());
        }

        @Override
        public Counter counter(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            metrics.put(metricName(name), counter);
            return counter;
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            metrics.put(metricName(name), gauge);
            return gauge;
        }

        @Override
        public <H extends Histogram> H histogram(String name, H histogram) {
            metrics.put(metricName(name), histogram);
            return histogram;
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            metrics.put(metricName(name), meter);
            return meter;
        }

        @Override
        public MetricGroup addGroup(String name) {
            return new CapturingMetricGroup(metrics, metricName(name));
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            return new CapturingMetricGroup(metrics, metricName(key + "=" + value));
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
            return metricName(metricName);
        }

        @Override
        public String getMetricIdentifier(
                String metricName, org.apache.flink.metrics.CharacterFilter filter) {
            return metricName(metricName);
        }
    }

    private static final class PartiallyThrowingMetricGroup implements MetricGroup {
        @Override
        public Counter counter(String name) {
            throw new IllegalStateException("reporter unavailable");
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            throw new IllegalStateException("reporter unavailable");
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            throw new IllegalStateException("reporter unavailable");
        }

        @Override
        public <H extends Histogram> H histogram(String name, H histogram) {
            throw new IllegalStateException("reporter unavailable");
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            throw new IllegalStateException("reporter unavailable");
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
        public String getMetricIdentifier(
                String metricName, org.apache.flink.metrics.CharacterFilter filter) {
            return metricName;
        }
    }
}
