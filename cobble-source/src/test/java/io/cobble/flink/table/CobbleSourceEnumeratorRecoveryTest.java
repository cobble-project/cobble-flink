package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cobble.GlobalSnapshot;
import io.cobble.table.Value;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/** Recovery coverage for replacement events that race an enumerator checkpoint. */
class CobbleSourceEnumeratorRecoveryTest {

    @TempDir private Path tempDir;

    @Test
    void restoreReissuesReplacementWhenReaderCheckpointStillOwnsOlderSnapshot() throws Exception {
        Path tablePath = tempDir.resolve("replacement-recovery");
        GlobalSnapshot snapshot1 =
                CobbleTableSourceTestData.write(
                        tablePath,
                        1,
                        CobbleTableSourceTestData.idNameSchema(),
                        CobbleTableSourceTestData.idNameRows());
        GlobalSnapshot snapshot2 =
                CobbleTableSourceTestData.write(
                        tablePath,
                        1,
                        CobbleTableSourceTestData.idNameSchema(),
                        Collections.singletonList(
                                Arrays.asList(Value.int64(2L), Value.string("two"))));

        CobbleDynamicTableSource.SerializableConfig config = sourceConfig(tablePath);
        CobbleSourceEnumeratorState.Serializer serializer =
                new CobbleSourceEnumeratorState.Serializer();
        CobbleSourceEnumeratorState checkpoint =
                serializer.deserialize(
                        serializer.getVersion(),
                        serializer.serialize(
                                new CobbleSourceEnumeratorState(
                                        snapshot2.id, snapshot2.id + 1L, Collections.emptyList())));
        TestingContext context = new TestingContext();
        CobbleSourceEnumerator enumerator = new CobbleSourceEnumerator(config, context, checkpoint);
        String splitId = CobbleSourceSplit.splitIdForRange(0, 0, 1);

        enumerator.handleSourceEvent(
                0,
                new CobbleSourceEvents.OwnedSplitsEvent(
                        Collections.singletonMap(splitId, Long.valueOf(snapshot1.id))));

        assertEquals(1, context.events.size());
        CobbleSourceEvents.ReplaceSplitEvent replacement =
                (CobbleSourceEvents.ReplaceSplitEvent) context.events.get(0);
        assertEquals(snapshot2.id, replacement.split.snapshotId);

        context.events.clear();
        enumerator.handleSourceEvent(
                0,
                new CobbleSourceEvents.OwnedSplitsEvent(
                        Collections.singletonMap(splitId, Long.valueOf(snapshot2.id))));
        assertEquals(0, context.events.size());
    }

    @Test
    void boundedEnumeratorSignalsNoMoreSplitsToLateRegisteredReader() throws Exception {
        Path tablePath = tempDir.resolve("bounded-late-reader");
        GlobalSnapshot snapshot =
                CobbleTableSourceTestData.write(
                        tablePath,
                        1,
                        CobbleTableSourceTestData.idNameSchema(),
                        CobbleTableSourceTestData.idNameRows());
        TestingContext context = new TestingContext();
        CobbleSourceEnumerator enumerator =
                new CobbleSourceEnumerator(
                        boundedSourceConfig(tablePath, snapshot.id), context, null);

        enumerator.start();
        assertEquals(Collections.singletonList(Integer.valueOf(0)), context.noMoreSplits);

        context.registerReader(1);
        enumerator.addReader(1);
        assertEquals(Arrays.asList(Integer.valueOf(0), Integer.valueOf(1)), context.noMoreSplits);
    }

    private static CobbleDynamicTableSource.SerializableConfig sourceConfig(Path tablePath) {
        return new CobbleDynamicTableSource.SerializableConfig(
                tablePath.toUri().toString(),
                1,
                "latest",
                "streaming",
                50L,
                0L,
                Collections.singletonList(
                        new CobbleDynamicTableSource.SerializableField("id", "BIGINT", 0, -1)),
                Collections.singletonList(
                        new CobbleDynamicTableSource.SerializableField(
                                "name", "VARCHAR(2147483647)", 1, 0)));
    }

    private static CobbleDynamicTableSource.SerializableConfig boundedSourceConfig(
            Path tablePath, long snapshotId) {
        return new CobbleDynamicTableSource.SerializableConfig(
                tablePath.toUri().toString(),
                1,
                Long.toString(snapshotId),
                "batch",
                50L,
                0L,
                Collections.singletonList(
                        new CobbleDynamicTableSource.SerializableField("id", "BIGINT", 0, -1)),
                Collections.singletonList(
                        new CobbleDynamicTableSource.SerializableField(
                                "name", "VARCHAR(2147483647)", 1, 0)));
    }

    private static final class TestingContext implements SplitEnumeratorContext<CobbleSourceSplit> {
        private final Map<Integer, ReaderInfo> readers = new LinkedHashMap<>();
        private final List<SourceEvent> events = new ArrayList<>();
        private final List<Integer> noMoreSplits = new ArrayList<>();

        private TestingContext() {
            registerReader(0);
        }

        private void registerReader(int subtaskId) {
            readers.put(Integer.valueOf(subtaskId), new ReaderInfo(subtaskId, "localhost"));
        }

        @Override
        public SplitEnumeratorMetricGroup metricGroup() {
            return null;
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
            events.add(event);
        }

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public Map<Integer, ReaderInfo> registeredReaders() {
            return readers;
        }

        @Override
        public void assignSplits(SplitsAssignment<CobbleSourceSplit> assignment) {}

        @Override
        public void signalNoMoreSplits(int subtask) {
            noMoreSplits.add(Integer.valueOf(subtask));
        }

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {}

        @Override
        public <T> void callAsync(
                Callable<T> callable,
                BiConsumer<T, Throwable> handler,
                long initialDelay,
                long period) {}

        @Override
        public void runInCoordinatorThread(Runnable runnable) {
            runnable.run();
        }
    }
}
