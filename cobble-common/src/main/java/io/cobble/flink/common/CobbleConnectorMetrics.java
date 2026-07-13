package io.cobble.flink.common;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

/** Small counter sets shared by Cobble source, lookup, and sink runtimes. */
public final class CobbleConnectorMetrics {
    private CobbleConnectorMetrics() {}

    public static SourceMetrics source(SourceReaderMetricGroup group) {
        if (group == null) {
            return noOpSource();
        }
        try {
            return new SourceMetrics(
                    group.getIOMetricGroup().getNumRecordsInCounter(),
                    group.getIOMetricGroup().getNumBytesInCounter(),
                    group.getNumRecordsInErrorsCounter(),
                    group.counter("cobble.nativeEntriesReadTotal"),
                    group.counter("cobble.nativeBytesReadTotal"));
        } catch (RuntimeException | LinkageError ignored) {
            return noOpSource();
        }
    }

    public static LookupMetrics lookup(MetricGroup group) {
        if (group == null) {
            return noOpLookup();
        }
        try {
            return new LookupMetrics(
                    group.counter("cobble.lookupRequestsTotal"),
                    group.counter("cobble.lookupHitsTotal"),
                    group.counter("cobble.lookupMissesTotal"),
                    group.counter("cobble.lookupErrorsTotal"),
                    group.counter("cobble.lookupBytesReadTotal"));
        } catch (RuntimeException | LinkageError ignored) {
            return noOpLookup();
        }
    }

    public static SinkMetrics sink(SinkWriterMetricGroup group) {
        if (group == null) {
            return noOpSink();
        }
        try {
            return new SinkMetrics(
                    group.getNumRecordsSendCounter(),
                    group.getNumBytesSendCounter(),
                    group.getNumRecordsSendErrorsCounter());
        } catch (RuntimeException | LinkageError ignored) {
            return noOpSink();
        }
    }

    public static long nativeEntryBytes(byte[] key, byte[][] columns) {
        long bytes = key == null ? 0L : key.length;
        if (columns != null) {
            for (byte[] column : columns) {
                if (column != null) {
                    bytes = saturatingAdd(bytes, column.length);
                }
            }
        }
        return bytes;
    }

    public static long saturatingAdd(long left, long right) {
        if (right <= 0L) {
            return left;
        }
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static SourceMetrics noOpSource() {
        return new SourceMetrics(
                NoOpCounter.INSTANCE,
                NoOpCounter.INSTANCE,
                NoOpCounter.INSTANCE,
                NoOpCounter.INSTANCE,
                NoOpCounter.INSTANCE);
    }

    private static LookupMetrics noOpLookup() {
        return new LookupMetrics(
                NoOpCounter.INSTANCE,
                NoOpCounter.INSTANCE,
                NoOpCounter.INSTANCE,
                NoOpCounter.INSTANCE,
                NoOpCounter.INSTANCE);
    }

    private static SinkMetrics noOpSink() {
        return new SinkMetrics(NoOpCounter.INSTANCE, NoOpCounter.INSTANCE, NoOpCounter.INSTANCE);
    }

    public static final class SourceMetrics {
        private final Counter recordsIn;
        private final Counter bytesIn;
        private final Counter errors;
        private final Counter nativeEntries;
        private final Counter nativeBytes;

        private SourceMetrics(
                Counter recordsIn,
                Counter bytesIn,
                Counter errors,
                Counter nativeEntries,
                Counter nativeBytes) {
            this.recordsIn = recordsIn;
            this.bytesIn = bytesIn;
            this.errors = errors;
            this.nativeEntries = nativeEntries;
            this.nativeBytes = nativeBytes;
        }

        public void nativeEntry(byte[] key, byte[][] columns) {
            long bytes = nativeEntryBytes(key, columns);
            nativeEntries.inc();
            nativeBytes.inc(bytes);
            bytesIn.inc(bytes);
        }

        public void emittedRow() {
            recordsIn.inc();
        }

        public void error() {
            errors.inc();
        }
    }

    public static final class LookupMetrics {
        private final Counter requests;
        private final Counter hits;
        private final Counter misses;
        private final Counter errors;
        private final Counter bytes;

        private LookupMetrics(
                Counter requests, Counter hits, Counter misses, Counter errors, Counter bytes) {
            this.requests = requests;
            this.hits = hits;
            this.misses = misses;
            this.errors = errors;
            this.bytes = bytes;
        }

        public void request() {
            requests.inc();
        }

        public void hit(byte[] key, byte[][] columns) {
            hits.inc();
            bytes.inc(nativeEntryBytes(key, columns));
        }

        public void miss() {
            misses.inc();
        }

        public void error() {
            errors.inc();
        }
    }

    public static final class SinkMetrics {
        private final Counter records;
        private final Counter bytes;
        private final Counter errors;

        private SinkMetrics(Counter records, Counter bytes, Counter errors) {
            this.records = records;
            this.bytes = bytes;
            this.errors = errors;
        }

        public void sent(long sentBytes) {
            records.inc();
            bytes.inc(sentBytes);
        }

        public void error() {
            errors.inc();
        }
    }

    private enum NoOpCounter implements Counter {
        INSTANCE;

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
            return 0L;
        }
    }
}
