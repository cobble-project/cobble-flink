package io.cobble.flink.common;

import io.cobble.CounterMetricValue;
import io.cobble.GaugeMetricValue;
import io.cobble.HistogramMetricValue;
import io.cobble.MetricSample;
import io.cobble.MetricType;
import io.cobble.MetricValue;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;

import java.io.Closeable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Bridges all Cobble native metric samples to cached Flink metric views. */
public final class CobbleNativeMetrics {
    private static final Logger LOG = Logger.getLogger(CobbleNativeMetrics.class.getName());
    private static final AtomicBoolean REGISTRATION_WARNING_LOGGED = new AtomicBoolean();
    private static final AtomicBoolean SAMPLE_WARNING_LOGGED = new AtomicBoolean();
    private static final long DEFAULT_TTL_MILLIS = 5_000L;
    private static final String PREFIX = "cobble.";

    private CobbleNativeMetrics() {}

    /** Fetches the current native metric samples. Implementations should not block reporters. */
    public interface MetricSnapshotProvider {
        List<MetricSample> metrics();
    }

    /** Registers the current native samples with the default five-second snapshot TTL. */
    public static Monitor register(MetricGroup parent, MetricSnapshotProvider provider) {
        return register(parent, provider, DEFAULT_TTL_MILLIS);
    }

    static Monitor register(MetricGroup parent, MetricSnapshotProvider provider, long ttlMillis) {
        if (parent == null || provider == null) {
            throw new IllegalArgumentException("parent and provider must not be null");
        }
        if (ttlMillis < 0L) {
            throw new IllegalArgumentException("ttlMillis must not be negative");
        }

        Monitor monitor = new Monitor(parent, provider, ttlMillis);
        try {
            monitor.initialize();
        } catch (RuntimeException | LinkageError error) {
            warnRegistration(error);
        }
        return monitor;
    }

    static String toLowerCamelCase(String value) {
        if (value == null) {
            return "";
        }
        if (value.indexOf('_') < 0) {
            return value;
        }
        StringBuilder result = new StringBuilder(value.length());
        boolean capitalize = false;
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (current == '_') {
                capitalize = result.length() > 0;
                continue;
            }
            char lower = Character.toLowerCase(current);
            if (result.length() == 0) {
                result.append(lower);
            } else if (capitalize) {
                result.append(Character.toUpperCase(lower));
            } else {
                result.append(lower);
            }
            capitalize = false;
        }
        return result.toString();
    }

    private static Map<MetricKey, MetricValue> normalizeSamples(List<MetricSample> source) {
        Map<MetricKey, MetricValue> values = new LinkedHashMap<MetricKey, MetricValue>();
        Map<MetricKey, Map<String, String>> sourceLabels =
                new LinkedHashMap<MetricKey, Map<String, String>>();
        if (source == null) {
            warnSample("native metric snapshot was null", null);
            return values;
        }
        for (MetricSample sample : source) {
            if (sample == null) {
                warnSample("native metric snapshot contained a null sample", null);
                continue;
            }
            try {
                String name = sample.name();
                String flinkName = toLowerCamelCase(name);
                MetricType type = sample.type();
                MetricValue value = sample.value();
                NormalizedLabels labels = normalizeLabels(sample.labels());
                if (name == null
                        || name.isEmpty()
                        || flinkName.isEmpty()
                        || labels == null
                        || type == null
                        || !matchesType(type, value)) {
                    warnSample("ignored malformed native metric sample", null);
                    continue;
                }
                MetricKey key = new MetricKey(name, flinkName, labels.normalized, type);
                Map<String, String> existingSourceLabels = sourceLabels.get(key);
                if (existingSourceLabels != null && !existingSourceLabels.equals(labels.source)) {
                    warnSample(
                            "ignored native metric whose labels collide after lowerCamelCase"
                                    + " normalization",
                            null);
                    continue;
                }
                values.put(key, value);
                sourceLabels.put(key, labels.source);
            } catch (RuntimeException | LinkageError error) {
                warnSample("ignored malformed native metric sample", error);
            }
        }
        return values;
    }

    private static NormalizedLabels normalizeLabels(Map<String, String> source) {
        if (source == null) {
            return null;
        }
        Map<String, String> labels = new TreeMap<String, String>();
        Map<String, String> sourceLabels = new TreeMap<String, String>();
        for (Map.Entry<String, String> entry : source.entrySet()) {
            String rawKey = entry.getKey();
            String value = entry.getValue();
            if ("db_id".equals(rawKey)) {
                continue;
            }
            String key = toLowerCamelCase(rawKey);
            if (rawKey == null || key.isEmpty() || value == null || labels.containsKey(key)) {
                return null;
            }
            labels.put(key, value);
            sourceLabels.put(rawKey, value);
        }
        return new NormalizedLabels(
                Collections.unmodifiableMap(new LinkedHashMap<String, String>(labels)),
                Collections.unmodifiableMap(new LinkedHashMap<String, String>(sourceLabels)));
    }

    private static final class NormalizedLabels {
        private final Map<String, String> normalized;
        private final Map<String, String> source;

        private NormalizedLabels(Map<String, String> normalized, Map<String, String> source) {
            this.normalized = normalized;
            this.source = source;
        }
    }

    private static boolean matchesType(MetricType type, MetricValue value) {
        if (type == MetricType.COUNTER) {
            return value instanceof CounterMetricValue;
        }
        if (type == MetricType.GAUGE) {
            return value instanceof GaugeMetricValue;
        }
        return type == MetricType.HISTOGRAM && value instanceof HistogramMetricValue;
    }

    private static void warnRegistration(Throwable error) {
        if (REGISTRATION_WARNING_LOGGED.compareAndSet(false, true)) {
            LOG.log(
                    Level.WARNING,
                    "Unable to register a Cobble native metric; continuing without it: "
                            + message(error));
        }
    }

    private static void warnSample(String detail, Throwable error) {
        if (SAMPLE_WARNING_LOGGED.compareAndSet(false, true)) {
            LOG.log(Level.WARNING, detail + (error == null ? "" : ": " + message(error)));
        }
    }

    private static String message(Throwable error) {
        return error == null || error.getMessage() == null ? "unknown error" : error.getMessage();
    }

    /** Closeable native metric monitor shared by all views registered for one Cobble DB. */
    public static final class Monitor implements Closeable {
        private final Object lock = new Object();
        private final MetricGroup parent;
        private final SnapshotCache cache;
        private final Map<MetricKey, NativeView> views = new LinkedHashMap<MetricKey, NativeView>();
        private final Map<FlinkMetricKey, MetricKey> registrations =
                new LinkedHashMap<FlinkMetricKey, MetricKey>();
        private long discoveredGeneration = -1L;
        private boolean closed;

        private Monitor(MetricGroup parent, MetricSnapshotProvider provider, long ttlMillis) {
            this.parent = parent;
            this.cache = new SnapshotCache(provider, ttlMillis);
        }

        private void initialize() {
            synchronized (lock) {
                discover(cache.snapshot());
            }
        }

        private void update(NativeView view) {
            synchronized (lock) {
                if (!closed) {
                    Snapshot snapshot = cache.snapshot();
                    discover(snapshot);
                    view.setValue(snapshot.samples.get(view.key));
                }
            }
        }

        private void discover(Snapshot snapshot) {
            if (snapshot.generation == discoveredGeneration) {
                return;
            }
            for (Map.Entry<MetricKey, MetricValue> entry : snapshot.samples.entrySet()) {
                MetricKey key = entry.getKey();
                if (views.containsKey(key)) {
                    continue;
                }
                FlinkMetricKey flinkKey = new FlinkMetricKey(key.flinkName, key.labels);
                MetricKey existing = registrations.get(flinkKey);
                if (existing != null && !existing.equals(key)) {
                    warnSample(
                            "ignored native metric whose mapped Flink name collides with an"
                                    + " existing metric",
                            null);
                    continue;
                }
                register(key, flinkKey, entry.getValue());
            }
            discoveredGeneration = snapshot.generation;
        }

        private void register(MetricKey key, FlinkMetricKey flinkKey, MetricValue initialValue) {
            NativeView view = createView(key);
            MetricGroup group = parent;
            try {
                for (Map.Entry<String, String> label : key.labels.entrySet()) {
                    group = group.addGroup(label.getKey(), label.getValue());
                }
                String name = PREFIX + key.flinkName;
                if (view instanceof NativeHistogramView) {
                    group.histogram(name, (NativeHistogramView) view);
                } else if (view instanceof NativeCounterView) {
                    group.counter(name, (NativeCounterView) view);
                } else {
                    group.gauge(name, (NativeGaugeView) view);
                }
            } catch (RuntimeException | LinkageError error) {
                view.close();
                warnRegistration(error);
                return;
            }
            view.setValue(initialValue);
            registrations.put(flinkKey, key);
            views.put(key, view);
        }

        private NativeView createView(MetricKey key) {
            if (key.type == MetricType.COUNTER) {
                return new NativeCounterView(this, key);
            }
            if (key.type == MetricType.GAUGE) {
                return new NativeGaugeView(this, key);
            }
            return new NativeHistogramView(this, key);
        }

        @Override
        public void close() {
            synchronized (lock) {
                if (closed) {
                    return;
                }
                closed = true;
                for (NativeView view : views.values()) {
                    view.close();
                }
            }
        }
    }

    private static final class SnapshotCache {
        private final MetricSnapshotProvider provider;
        private final long ttlNanos;
        private Map<MetricKey, MetricValue> samples = Collections.emptyMap();
        private long lastFetchNanos;
        private long generation;
        private boolean fetched;
        private boolean warned;

        private SnapshotCache(MetricSnapshotProvider provider, long ttlMillis) {
            this.provider = provider;
            this.ttlNanos = TimeUnit.MILLISECONDS.toNanos(ttlMillis);
        }

        private Snapshot snapshot() {
            long now = System.nanoTime();
            if (!fetched || now - lastFetchNanos >= ttlNanos) {
                try {
                    samples = normalizeSamples(provider.metrics());
                    generation++;
                } catch (RuntimeException | LinkageError error) {
                    if (!warned) {
                        warned = true;
                        LOG.log(
                                Level.WARNING,
                                "Unable to fetch Cobble native metrics; retaining last snapshot: "
                                        + message(error));
                    }
                } finally {
                    lastFetchNanos = now;
                    fetched = true;
                }
            }
            return new Snapshot(samples, generation);
        }
    }

    private static final class Snapshot {
        private final Map<MetricKey, MetricValue> samples;
        private final long generation;

        private Snapshot(Map<MetricKey, MetricValue> samples, long generation) {
            this.samples = samples;
            this.generation = generation;
        }
    }

    private static final class MetricKey {
        private final String name;
        private final String flinkName;
        private final Map<String, String> labels;
        private final MetricType type;

        private MetricKey(
                String name, String flinkName, Map<String, String> labels, MetricType type) {
            this.name = name;
            this.flinkName = flinkName;
            this.labels = labels;
            this.type = type;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof MetricKey)) {
                return false;
            }
            MetricKey that = (MetricKey) other;
            return name.equals(that.name) && labels.equals(that.labels) && type == that.type;
        }

        @Override
        public int hashCode() {
            return name.hashCode() * 31 * 31 + labels.hashCode() * 31 + type.hashCode();
        }
    }

    private static final class FlinkMetricKey {
        private final String name;
        private final Map<String, String> labels;

        private FlinkMetricKey(String name, Map<String, String> labels) {
            this.name = name;
            this.labels = labels;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof FlinkMetricKey)) {
                return false;
            }
            FlinkMetricKey that = (FlinkMetricKey) other;
            return name.equals(that.name) && labels.equals(that.labels);
        }

        @Override
        public int hashCode() {
            return name.hashCode() * 31 + labels.hashCode();
        }
    }

    private abstract static class NativeView implements View {
        private final Monitor monitor;
        private final MetricKey key;
        private volatile boolean closed;

        private NativeView(Monitor monitor, MetricKey key) {
            this.monitor = monitor;
            this.key = key;
        }

        @Override
        public final void update() {
            if (!closed) {
                monitor.update(this);
            }
        }

        private void close() {
            closed = true;
        }

        abstract void setValue(MetricValue value);
    }

    private static final class NativeCounterView extends NativeView implements Counter {
        private volatile long value;

        private NativeCounterView(Monitor monitor, MetricKey key) {
            super(monitor, key);
        }

        @Override
        public void inc() {}

        @Override
        public void inc(long n) {}

        @Override
        public void dec() {}

        @Override
        public void dec(long n) {}

        @Override
        public long getCount() {
            return value;
        }

        @Override
        void setValue(MetricValue value) {
            if (value instanceof CounterMetricValue) {
                this.value = ((CounterMetricValue) value).value();
            }
        }
    }

    private static final class NativeGaugeView extends NativeView implements Gauge<Double> {
        private volatile double value;

        private NativeGaugeView(Monitor monitor, MetricKey key) {
            super(monitor, key);
        }

        @Override
        public Double getValue() {
            return value;
        }

        @Override
        void setValue(MetricValue value) {
            if (value instanceof GaugeMetricValue) {
                this.value = ((GaugeMetricValue) value).value();
            }
        }
    }

    private static final class NativeHistogramView extends NativeView implements Histogram {
        private volatile HistogramMetricValue value =
                new HistogramMetricValue(0L, 0.0d, 0.0d, 0.0d);

        private NativeHistogramView(Monitor monitor, MetricKey key) {
            super(monitor, key);
        }

        @Override
        public void update(long value) {}

        @Override
        public long getCount() {
            return value.count();
        }

        @Override
        public HistogramStatistics getStatistics() {
            return new NativeHistogramStatistics(value);
        }

        @Override
        void setValue(MetricValue value) {
            if (value instanceof HistogramMetricValue) {
                this.value = (HistogramMetricValue) value;
            }
        }
    }

    private static final class NativeHistogramStatistics extends HistogramStatistics {
        private final HistogramMetricValue value;

        private NativeHistogramStatistics(HistogramMetricValue value) {
            this.value = value;
        }

        @Override
        public double getQuantile(double quantile) {
            return Double.NaN;
        }

        @Override
        public long[] getValues() {
            return new long[0];
        }

        @Override
        public int size() {
            return (int) Math.min(value.count(), Integer.MAX_VALUE);
        }

        @Override
        public double getMean() {
            return value.count() == 0L ? 0.0d : value.sum() / value.count();
        }

        @Override
        public double getStdDev() {
            return Double.NaN;
        }

        @Override
        public long getMax() {
            return Math.round(value.max());
        }

        @Override
        public long getMin() {
            return Math.round(value.min());
        }
    }
}
