package io.cobble.flink.compaction;

import io.cobble.DedicatedCompactionMonitor;
import io.cobble.DedicatedCompactionPlan;
import io.cobble.flink.common.CobbleLoader;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** Single-reader source that monitors Cobble DBs and emits independently executable plans. */
public final class CobbleCompactionSource
        implements Source<CobbleCompactionPlan, CobbleCompactionSource.MonitorSplit, Boolean> {
    private static final long serialVersionUID = 1L;

    private final String configPath;
    private final String path;
    private final long pollIntervalMillis;

    public static CobbleCompactionSource scan(
            String configPath, String path, long pollIntervalMillis) {
        return new CobbleCompactionSource(configPath, path, pollIntervalMillis);
    }

    private CobbleCompactionSource(
            String configPath,
            String path,
            long pollIntervalMillis) {
        if (configPath == null || configPath.trim().isEmpty()) {
            throw new IllegalArgumentException("configPath must not be blank");
        }
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("path must not be blank");
        }
        if (pollIntervalMillis <= 0) {
            throw new IllegalArgumentException("pollIntervalMillis must be greater than zero");
        }
        this.configPath = configPath;
        this.path = path;
        this.pollIntervalMillis = pollIntervalMillis;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<CobbleCompactionPlan, MonitorSplit> createReader(
            SourceReaderContext readerContext) {
        return new MonitorReader(
                readerContext, configPath, path, pollIntervalMillis);
    }

    @Override
    public SplitEnumerator<MonitorSplit, Boolean> createEnumerator(
            SplitEnumeratorContext<MonitorSplit> context) {
        return new MonitorEnumerator(context, false);
    }

    @Override
    public SplitEnumerator<MonitorSplit, Boolean> restoreEnumerator(
            SplitEnumeratorContext<MonitorSplit> context, Boolean assigned) {
        return new MonitorEnumerator(context, Boolean.TRUE.equals(assigned));
    }

    @Override
    public SimpleVersionedSerializer<MonitorSplit> getSplitSerializer() {
        return MonitorSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<Boolean> getEnumeratorCheckpointSerializer() {
        return BooleanSerializer.INSTANCE;
    }

    /** The monitor has exactly one logical split, regardless of executor parallelism. */
    public static final class MonitorSplit implements SourceSplit, Serializable {
        private static final long serialVersionUID = 1L;
        private static final String ID = "cobble-dedicated-compaction-monitor";

        @Override
        public String splitId() {
            return ID;
        }
    }

    private static final class MonitorEnumerator
            implements SplitEnumerator<MonitorSplit, Boolean> {
        private final SplitEnumeratorContext<MonitorSplit> context;
        private boolean assigned;

        private MonitorEnumerator(
                SplitEnumeratorContext<MonitorSplit> context, boolean assigned) {
            this.context = context;
            this.assigned = assigned;
        }

        @Override
        public void start() {
            assignIfPossible();
        }

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            if (subtaskId == 0) {
                assignIfPossible();
            } else {
                context.signalNoMoreSplits(subtaskId);
            }
        }

        @Override
        public void addSplitsBack(List<MonitorSplit> splits, int subtaskId) {
            if (!splits.isEmpty()) {
                assigned = false;
                assignIfPossible();
            }
        }

        @Override
        public void addReader(int subtaskId) {
            if (subtaskId == 0) {
                assignIfPossible();
            } else {
                context.signalNoMoreSplits(subtaskId);
            }
        }

        @Override
        public Boolean snapshotState(long checkpointId) {
            return assigned;
        }

        @Override
        public void close() {}

        private void assignIfPossible() {
            if (!assigned && context.registeredReaders().containsKey(0)) {
                context.assignSplit(new MonitorSplit(), 0);
                assigned = true;
            }
        }
    }

    private static final class MonitorReader
            implements SourceReader<CobbleCompactionPlan, MonitorSplit> {
        private static final CompletableFuture<Void> AVAILABLE =
                CompletableFuture.completedFuture(null);

        private final SourceReaderContext context;
        private final String configPath;
        private final String path;
        private final long pollIntervalMillis;
        private final Deque<CobbleCompactionPlan> pending =
                new ArrayDeque<CobbleCompactionPlan>();
        private final ScheduledExecutorService availabilityExecutor;

        private DedicatedCompactionMonitor monitor;
        private CompletableFuture<Void> availability;
        private long nextPollMillis;
        private boolean assigned;
        private boolean closed;

        private MonitorReader(
                SourceReaderContext context,
                String configPath,
                String path,
                long pollIntervalMillis) {
            this.context = context;
            this.configPath = configPath;
            this.path = path;
            this.pollIntervalMillis = pollIntervalMillis;
            this.availabilityExecutor =
                    Executors.newSingleThreadScheduledExecutor(
                            new DaemonThreadFactory("cobble-compaction-source-availability"));
            this.availability = new CompletableFuture<Void>();
        }

        @Override
        public void start() {
            if (!assigned) {
                context.sendSplitRequest();
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<CobbleCompactionPlan> output) {
            if (!assigned) {
                return InputStatus.NOTHING_AVAILABLE;
            }
            long now = System.currentTimeMillis();
            if (pending.isEmpty() && now >= nextPollMillis) {
                pollMonitor();
                nextPollMillis = now + pollIntervalMillis;
            }
            CobbleCompactionPlan plan = pending.pollFirst();
            if (plan == null) {
                return InputStatus.NOTHING_AVAILABLE;
            }
            output.collect(plan);
            return pending.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
        }

        @Override
        public List<MonitorSplit> snapshotState(long checkpointId) {
            return assigned
                    ? Collections.singletonList(new MonitorSplit())
                    : Collections.<MonitorSplit>emptyList();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            if (!assigned) {
                return availability;
            }
            if (!pending.isEmpty() || System.currentTimeMillis() >= nextPollMillis) {
                return AVAILABLE;
            }
            if (availability.isDone()) {
                availability = new CompletableFuture<Void>();
                long delay = Math.max(1L, nextPollMillis - System.currentTimeMillis());
                final CompletableFuture<Void> scheduled = availability;
                availabilityExecutor.schedule(
                        new Runnable() {
                            @Override
                            public void run() {
                                scheduled.complete(null);
                            }
                        },
                        delay,
                        TimeUnit.MILLISECONDS);
            }
            return availability;
        }

        @Override
        public void addSplits(List<MonitorSplit> splits) {
            if (!splits.isEmpty()) {
                assigned = true;
                availability.complete(null);
            }
        }

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() {
            closed = true;
            availability.complete(null);
            availabilityExecutor.shutdownNow();
            if (monitor != null) {
                monitor.close();
                monitor = null;
            }
        }

        private void pollMonitor() {
            if (closed) {
                return;
            }
            if (monitor == null) {
                CobbleLoader.ensureCobbleLoaded();
                monitor = DedicatedCompactionMonitor.scan(Paths.get(configPath), path);
            }
            for (DedicatedCompactionPlan plan : monitor.poll()) {
                pending.addLast(new CobbleCompactionPlan(plan.encode()));
            }
        }
    }

    private static final class MonitorSplitSerializer
            implements SimpleVersionedSerializer<MonitorSplit> {
        private static final MonitorSplitSerializer INSTANCE = new MonitorSplitSerializer();

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(MonitorSplit split) {
            return new byte[0];
        }

        @Override
        public MonitorSplit deserialize(int version, byte[] serialized) throws IOException {
            requireVersion(version);
            return new MonitorSplit();
        }
    }

    private static final class BooleanSerializer implements SimpleVersionedSerializer<Boolean> {
        private static final BooleanSerializer INSTANCE = new BooleanSerializer();

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Boolean value) {
            return new byte[] {(byte) (Boolean.TRUE.equals(value) ? 1 : 0)};
        }

        @Override
        public Boolean deserialize(int version, byte[] serialized) throws IOException {
            requireVersion(version);
            if (serialized.length != 1) {
                throw new IOException("Invalid monitor enumerator state length: " + serialized.length);
            }
            return serialized[0] != 0;
        }
    }

    private static void requireVersion(int version) throws IOException {
        if (version != 1) {
            throw new IOException("Unsupported compaction source serializer version: " + version);
        }
    }

    private static final class DaemonThreadFactory implements ThreadFactory {
        private final String name;

        private DaemonThreadFactory(String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        }
    }
}
