package io.cobble.flink.table;

import io.cobble.flink.catalog.CobbleCatalogTableReference;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.NativeTableScanReadProvider;
import io.cobble.table.TableReadCursor;
import io.cobble.table.TableReadEntry;
import io.cobble.table.TableReadProvider;
import io.cobble.table.TableReadRange;
import io.cobble.table.TableReadSession;
import io.cobble.table.TableReader;
import io.cobble.table.TableScanSplit;
import io.cobble.table.Value;

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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Bounded catalog scan whose native assignments pin both schema and snapshot. */
final class CobbleCatalogScanSource
        implements Source<
                RowData, CobbleCatalogScanSource.Split, List<CobbleCatalogScanSource.Split>> {
    private static final long serialVersionUID = 1L;
    private final CobbleCatalogTableReference reference;
    private final RowType rowType;
    private final String snapshot;

    CobbleCatalogScanSource(
            CobbleCatalogTableReference reference, RowType rowType, String snapshot) {
        this.reference = reference;
        this.rowType = rowType;
        this.snapshot = snapshot;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, Split> createReader(SourceReaderContext context) {
        return new Reader(reference, rowType, context);
    }

    @Override
    public SplitEnumerator<Split, List<Split>> createEnumerator(
            SplitEnumeratorContext<Split> context) {
        List<Split> splits = new ArrayList<>();
        try (TableReader reader = CobbleCatalogDynamicTableSource.openReader(reference, snapshot)) {
            if (reader != null) {
                for (TableScanSplit nativeSplit : reader.scanPlan().splits()) {
                    splits.add(new Split(Integer.toString(splits.size()), nativeSplit, 0));
                }
            }
        }
        return new Enumerator(context, splits);
    }

    @Override
    public SplitEnumerator<Split, List<Split>> restoreEnumerator(
            SplitEnumeratorContext<Split> context, List<Split> state) {
        return new Enumerator(context, state);
    }

    @Override
    public SimpleVersionedSerializer<Split> getSplitSerializer() {
        return new Serializer<>();
    }

    @Override
    public SimpleVersionedSerializer<List<Split>> getEnumeratorCheckpointSerializer() {
        return new Serializer<>();
    }

    static final class Split implements SourceSplit, Serializable {
        private static final long serialVersionUID = 1L;
        final String id;
        final TableScanSplit plan;
        final long emitted;

        Split(String id, TableScanSplit plan, long emitted) {
            this.id = id;
            this.plan = plan;
            this.emitted = emitted;
        }

        @Override
        public String splitId() {
            return id;
        }
    }

    private static final class Enumerator implements SplitEnumerator<Split, List<Split>> {
        private final SplitEnumeratorContext<Split> context;
        private final ArrayDeque<Split> pending;

        Enumerator(SplitEnumeratorContext<Split> context, List<Split> splits) {
            this.context = context;
            pending = new ArrayDeque<>(splits);
        }

        @Override
        public void start() {}

        @Override
        public void addReader(int subtask) {}

        @Override
        public void handleSplitRequest(int subtask, String hostname) {
            if (!context.registeredReaders().containsKey(subtask)) return;
            Split split = pending.pollFirst();
            if (split != null) context.assignSplit(split, subtask);
            else context.signalNoMoreSplits(subtask);
        }

        @Override
        public void addSplitsBack(List<Split> splits, int subtask) {
            pending.addAll(splits);
        }

        @Override
        public List<Split> snapshotState(long checkpointId) {
            return new ArrayList<>(pending);
        }

        @Override
        public void close() {}
    }

    private static final class Reader implements SourceReader<RowData, Split> {
        private final CobbleCatalogTableReference reference;
        private final CobbleTableRowConverter converter;
        private final SourceReaderContext context;
        private final ArrayDeque<Split> pending = new ArrayDeque<>();
        private CompletableFuture<Void> available = new CompletableFuture<>();
        private Split current;
        private TableReadProvider<List<Value>, Void> provider;
        private TableReadSession<List<Value>, Void> session;
        private TableReadCursor<List<Value>> cursor;
        private long emitted;
        private boolean finished;

        Reader(
                CobbleCatalogTableReference reference,
                RowType rowType,
                SourceReaderContext context) {
            this.reference = reference;
            converter = new CobbleTableRowConverter(rowType);
            this.context = context;
        }

        @Override
        public void start() {
            context.sendSplitRequest();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
            if (current == null) {
                current = pending.pollFirst();
                if (current == null) {
                    if (finished) return InputStatus.END_OF_INPUT;
                    if (available.isDone()) available = new CompletableFuture<>();
                    return InputStatus.NOTHING_AVAILABLE;
                }
                // Revalidate identity before opening the durable fixed assignment after failover.
                try (CobbleCatalogTableReference.Opened ignored = reference.openValidated()) {
                    provider =
                            new NativeTableScanReadProvider(
                                    reference.runtimeConfig(), current.plan);
                    session = provider.open();
                    cursor = session.scan(new TableReadRange(0, Integer.MAX_VALUE), null);
                }
                emitted = 0;
                // Native plans expose no seek token yet. Replaying a fixed immutable split
                // preserves
                // exact progress without deriving private physical key or manifest layouts.
                while (emitted < current.emitted) {
                    if (cursor.next() == null)
                        throw new IOException(
                                "Catalog scan split ended before its checkpoint position.");
                    emitted++;
                }
            }
            TableReadEntry<List<Value>> entry = cursor.next();
            if (entry != null) {
                output.collect(converter.toRowData(entry.value()));
                emitted++;
                context.metricGroup().getIOMetricGroup().getNumRecordsInCounter().inc();
                return InputStatus.MORE_AVAILABLE;
            }
            closeCursor();
            current = null;
            context.sendSplitRequest();
            if (!pending.isEmpty()) return InputStatus.MORE_AVAILABLE;
            if (finished) return InputStatus.END_OF_INPUT;
            available = new CompletableFuture<>();
            return InputStatus.NOTHING_AVAILABLE;
        }

        @Override
        public List<Split> snapshotState(long checkpointId) {
            List<Split> state = new ArrayList<>(pending);
            if (current != null) state.add(new Split(current.id, current.plan, emitted));
            return state;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return available;
        }

        @Override
        public void addSplits(List<Split> splits) {
            pending.addAll(splits);
            available.complete(null);
        }

        @Override
        public void notifyNoMoreSplits() {
            finished = true;
            available.complete(null);
        }

        @Override
        public void close() {
            closeCursor();
        }

        private void closeCursor() {
            if (cursor != null) cursor.close();
            cursor = null;
            if (session != null) session.close();
            session = null;
            if (provider != null) provider.close();
            provider = null;
        }
    }

    private static final class Serializer<T> implements SimpleVersionedSerializer<T> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(T value) throws IOException {
            return InstantiationUtil.serializeObject(value);
        }

        @Override
        public T deserialize(int version, byte[] data) throws IOException {
            if (version != 1)
                throw new IOException("Unsupported catalog scan checkpoint version: " + version);
            try {
                return InstantiationUtil.deserializeObject(
                        data, CobbleCatalogScanSource.class.getClassLoader());
            } catch (ClassNotFoundException error) {
                throw new IOException("Cannot restore catalog scan checkpoint.", error);
            }
        }
    }
}
