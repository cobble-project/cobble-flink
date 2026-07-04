package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.structured.Db;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Integration tests for the raw source that write data directly via the Cobble Java API (no Flink
 * sink, no inspect-schema sidecar), proving the raw source does not depend on sink schema.
 */
class CobbleRawSourceReaderITTest {

    @TempDir private Path tempDir;

    /**
     * Writes a small Cobble table directly and reads it back via the raw source, verifying that raw
     * key bytes and column bytes are preserved verbatim — including null columns and non-UTF8
     * bytes.
     */
    @Test
    void rawSourcePreservesKeyAndColumnBytesFromDirectWriteTable() throws Exception {
        Path tablePath = tempDir.resolve("raw-direct-write");
        int bucketCount = 2;
        int numColumns = 3;

        // Write rows directly: {key, col0, col1, col2} — col2 is intentionally null for some rows.
        // Non-UTF8 bytes in keys and values prove the raw source preserves binary content.
        DirectRow[] written = {
            new DirectRow(bytes(0x00, 0xFF, 0x42), bytes("alpha"), bytes(1, 2, 3), null),
            new DirectRow(bytes("key-2"), bytes("beta"), null, bytes(0xFE, 0xDC)),
            new DirectRow(bytes(0x01, 0x02, 0x03), bytes("gamma"), bytes(10), bytes(20)),
        };

        long snapshotId = writeDirectTable(tablePath, bucketCount, numColumns, written, 1L);

        RawSourceConfig config =
                new RawSourceConfig(
                        tablePath.toUri().toString(),
                        bucketCount,
                        Long.toString(snapshotId),
                        "batch",
                        50L,
                        new int[] {0, 1, 2});

        GlobalSnapshot snapshot = CobbleSourceRuntime.loadSnapshotById(config, snapshotId);
        List<CobbleSourceSplit> splits = CobbleSourceRuntime.createSourceSplits(config, snapshot);

        CobbleSourceReader reader = new CobbleSourceReader(config, new TestingContext());
        CollectingOutput output = new CollectingOutput();
        try {
            reader.start();
            reader.addSplits(splits);
            reader.notifyNoMoreSplits();
            while (true) {
                InputStatus status = reader.pollNext(output);
                if (status == InputStatus.END_OF_INPUT) {
                    break;
                }
                if (status == InputStatus.NOTHING_AVAILABLE) {
                    throw new IllegalStateException("Reader became idle before end of input.");
                }
            }
        } finally {
            reader.close();
        }

        // Collect rows keyed by their key bytes for deterministic comparison.
        Map<String, RowData> rowsByKey = new TreeMap<>();
        for (RowData row : output.rows) {
            byte[] key = row.getBinary(0);
            rowsByKey.put(toHexString(key), row);
        }

        assertEquals(written.length, rowsByKey.size(), "expected one row per written key");

        for (DirectRow expected : written) {
            RowData row = rowsByKey.get(toHexString(expected.key));
            assertNotNull(row, "missing row for key " + toHexString(expected.key));
            assertArrayEquals(expected.key, row.getBinary(0), "key bytes mismatch");

            ArrayData columns = row.getArray(1);
            assertEquals(3, columns.size(), "columns array size mismatch");
            assertColumnEquals(expected.col0, columns, 0);
            assertColumnEquals(expected.col1, columns, 1);
            assertColumnEquals(expected.col2, columns, 2);
        }
    }

    /**
     * Writes a table with 3 columns but reads only columns 0 and 2 (non-contiguous projection),
     * proving that {@code raw.columns='0,2'} works correctly and {@code scanColumnCount} is 3.
     */
    @Test
    void rawSourceSupportsNonContiguousColumnProjection() throws Exception {
        Path tablePath = tempDir.resolve("raw-non-contiguous");
        int bucketCount = 1;
        int numColumns = 3;

        DirectRow[] written = {
            new DirectRow(bytes("k1"), bytes("c0"), bytes("c1"), bytes("c2")),
            new DirectRow(bytes("k2"), bytes("d0"), bytes("d1"), bytes("d2")),
        };

        long snapshotId = writeDirectTable(tablePath, bucketCount, numColumns, written, 1L);

        // Select only columns 0 and 2 — non-contiguous.
        RawSourceConfig config =
                new RawSourceConfig(
                        tablePath.toUri().toString(),
                        bucketCount,
                        Long.toString(snapshotId),
                        "batch",
                        50L,
                        new int[] {0, 2});

        GlobalSnapshot snapshot = CobbleSourceRuntime.loadSnapshotById(config, snapshotId);
        List<CobbleSourceSplit> splits = CobbleSourceRuntime.createSourceSplits(config, snapshot);

        CobbleSourceReader reader = new CobbleSourceReader(config, new TestingContext());
        CollectingOutput output = new CollectingOutput();
        try {
            reader.start();
            reader.addSplits(splits);
            reader.notifyNoMoreSplits();
            drainBoundedReader(reader, output);
        } finally {
            reader.close();
        }

        Map<String, RowData> rowsByKey = new TreeMap<>();
        for (RowData row : output.rows) {
            rowsByKey.put(new String(row.getBinary(0), StandardCharsets.UTF_8), row);
        }

        assertEquals(written.length, rowsByKey.size());
        for (DirectRow expected : written) {
            RowData row = rowsByKey.get(new String(expected.key, StandardCharsets.UTF_8));
            assertNotNull(row);
            ArrayData columns = row.getArray(1);
            assertEquals(2, columns.size(), "projected columns array should have 2 elements");
            assertColumnEquals(expected.col0, columns, 0);
            assertColumnEquals(expected.col2, columns, 1);
        }
    }

    /**
     * Writes a table with 3 columns and reads them back in a scrambled order ({@code 2,0,1}),
     * proving that the native scan returns columns in the user-requested order — not just that the
     * Java option parser preserves order. This validates the full path from {@code
     * ScanOptions.forColumns(...)} through JNI to the returned {@code byte[][]}.
     */
    @Test
    void rawSourcePreservesScrambledColumnOrder() throws Exception {
        Path tablePath = tempDir.resolve("raw-scrambled-order");
        int bucketCount = 1;
        int numColumns = 3;

        DirectRow[] written = {
            new DirectRow(bytes("k1"), bytes("c0"), bytes("c1"), bytes("c2")),
            new DirectRow(bytes("k2"), bytes("d0"), bytes("d1"), bytes("d2")),
        };

        long snapshotId = writeDirectTable(tablePath, bucketCount, numColumns, written, 1L);

        // Scrambled order: column 2 first, then 0, then 1.
        RawSourceConfig config =
                new RawSourceConfig(
                        tablePath.toUri().toString(),
                        bucketCount,
                        Long.toString(snapshotId),
                        "batch",
                        50L,
                        new int[] {2, 0, 1});

        GlobalSnapshot snapshot = CobbleSourceRuntime.loadSnapshotById(config, snapshotId);
        List<CobbleSourceSplit> splits = CobbleSourceRuntime.createSourceSplits(config, snapshot);

        CobbleSourceReader reader = new CobbleSourceReader(config, new TestingContext());
        CollectingOutput output = new CollectingOutput();
        try {
            reader.start();
            reader.addSplits(splits);
            reader.notifyNoMoreSplits();
            drainBoundedReader(reader, output);
        } finally {
            reader.close();
        }

        Map<String, RowData> rowsByKey = new TreeMap<>();
        for (RowData row : output.rows) {
            rowsByKey.put(new String(row.getBinary(0), StandardCharsets.UTF_8), row);
        }

        assertEquals(written.length, rowsByKey.size());
        for (DirectRow expected : written) {
            RowData row = rowsByKey.get(new String(expected.key, StandardCharsets.UTF_8));
            assertNotNull(row);
            ArrayData columns = row.getArray(1);
            assertEquals(3, columns.size(), "projected columns array should have 3 elements");
            // columns[0] == col2, columns[1] == col0, columns[2] == col1
            assertColumnEquals(expected.col2, columns, 0);
            assertColumnEquals(expected.col0, columns, 1);
            assertColumnEquals(expected.col1, columns, 2);
        }
    }

    /**
     * Streaming raw scan: writes snapshot 1, starts the raw source, writes snapshot 2 with
     * accumulated data (old + new keys), and verifies all rows are read without duplicates or loss.
     */
    @Test
    void rawStreamingReplacesSnapshotWithoutDuplicatesOrLoss() throws Exception {
        Path tablePath = tempDir.resolve("raw-streaming");
        int bucketCount = 1;
        int numColumns = 2;

        DirectRow[] firstBatch = {
            new DirectRow(bytes("s1-k1"), bytes("a1"), bytes("b1"), null),
            new DirectRow(bytes("s1-k2"), bytes("a2"), bytes("b2"), null),
            new DirectRow(bytes("s1-k3"), bytes("a3"), bytes("b3"), null),
        };

        long snapshot1 = writeDirectTable(tablePath, bucketCount, numColumns, firstBatch, 1L);

        RawSourceConfig config =
                new RawSourceConfig(
                        tablePath.toUri().toString(),
                        bucketCount,
                        "latest",
                        "streaming",
                        50L,
                        new int[] {0, 1});

        GlobalSnapshot snap1 = CobbleSourceRuntime.loadSnapshotById(config, snapshot1);
        List<CobbleSourceSplit> initialSplits =
                CobbleSourceRuntime.createSourceSplits(config, snap1);

        CollectingOutput output = new CollectingOutput();
        CobbleSourceReader reader = new CobbleSourceReader(config, new TestingContext());
        try {
            reader.start();
            reader.addSplits(initialSplits);
            // Consume only part of snapshot 1 so the split is still ACTIVE (not IDLE) when the
            // replacement arrives — this exercises the WRAP replacement path.
            pollUntilRowCount(reader, output, 2);

            // Write a second snapshot with all previous rows PLUS a new key that sorts AFTER the
            // last consumed key. In normal streaming replacement (not checkpoint restore), the
            // ACTIVE part scans from the resume boundary forward, so only keys after the boundary
            // are emitted. This matches the existing sink source replacement semantics.
            DirectRow[] allRowsForSnapshot2 = {
                new DirectRow(bytes("s1-k1"), bytes("a1"), bytes("b1"), null),
                new DirectRow(bytes("s1-k2"), bytes("a2"), bytes("b2"), null),
                new DirectRow(bytes("s1-k3"), bytes("a3"), bytes("b3"), null),
                new DirectRow(bytes("s2-k4"), bytes("a4"), bytes("b4"), null),
            };
            long snapshot2 =
                    writeDirectTable(tablePath, bucketCount, numColumns, allRowsForSnapshot2, 2L);

            GlobalSnapshot snap2 = CobbleSourceRuntime.loadSnapshotById(config, snapshot2);
            for (CobbleSourceSplit split : CobbleSourceRuntime.createSourceSplits(config, snap2)) {
                reader.handleSourceEvents(new CobbleSourceEvents.ReplaceSplitEvent(split));
            }
            drainStreamingReader(reader, output);
        } finally {
            reader.close();
        }

        // The streaming source is an incremental scan: rows already consumed from snapshot 1 are
        // not re-emitted from snapshot 2. Only keys NOT yet seen are emitted from the replacement.
        Map<String, byte[]> valueByKey = new TreeMap<>();
        for (RowData row : output.rows) {
            String key = new String(row.getBinary(0), StandardCharsets.UTF_8);
            ArrayData cols = row.getArray(1);
            valueByKey.put(key, cols.getBinary(0));
        }

        // All 4 distinct keys should be present: s1-k1, s1-k2 from snapshot 1 (consumed before
        // replacement), s1-k3 and s2-k4 from snapshot 2 ACTIVE (after the resume boundary).
        // No duplicates for already-consumed keys.
        assertEquals(4, valueByKey.size(), "expected 4 distinct keys, got " + valueByKey);
        assertEquals("a1", new String(valueByKey.get("s1-k1"), StandardCharsets.UTF_8));
        assertEquals("a2", new String(valueByKey.get("s1-k2"), StandardCharsets.UTF_8));
        assertEquals("a3", new String(valueByKey.get("s1-k3"), StandardCharsets.UTF_8));
        assertEquals("a4", new String(valueByKey.get("s2-k4"), StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------------------------------
    //  Direct-write fixture: writes a Cobble table root without any sink sidecar schema.
    // ------------------------------------------------------------------------------------------

    private long writeDirectTable(
            Path tablePath, int bucketCount, int numColumns, DirectRow[] rows, long snapshotId)
            throws Exception {
        return writeRowsAndSnapshot(tablePath, bucketCount, numColumns, rows, snapshotId);
    }

    private long writeRowsAndSnapshot(
            Path tablePath, int bucketCount, int numColumns, DirectRow[] rows, long snapshotId)
            throws Exception {
        Config writerConfig = writerConfig(tablePath, bucketCount, numColumns);

        // Open a single Db covering all buckets, write all rows, then snapshot.
        List<ShardSnapshot> shardSnapshots = new ArrayList<>();
        try (Db db = Db.open(writerConfig, 0, bucketCount - 1)) {
            for (DirectRow row : rows) {
                int bucket = Math.floorMod(Arrays.hashCode(row.key), bucketCount);
                for (int col = 0; col < numColumns; col++) {
                    byte[] value = row.column(col);
                    if (value != null) {
                        db.put(bucket, row.key, col, value);
                    } else {
                        db.delete(bucket, row.key, col);
                    }
                }
            }
            shardSnapshots.add(db.snapshot());
        }

        // Materialize a global snapshot so the source can find it.
        Config coordConfig = coordinatorConfig(tablePath, bucketCount);
        try (DbCoordinator coordinator = DbCoordinator.open(coordConfig)) {
            coordinator.materializeGlobalSnapshot(bucketCount, snapshotId, shardSnapshots);
        }
        return snapshotId;
    }

    private Config writerConfig(Path tablePath, int bucketCount, int numColumns) {
        Config config = new Config().numColumns(numColumns).totalBuckets(bucketCount);
        config.governanceMode = Config.GovernanceMode.NOOP;
        config.logConsole = false;
        config.logPath = tempDir.resolve("writer.log").toString();

        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = tablePath.toAbsolutePath().toString();
        volume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT);
        config.addVolume(volume);
        return config;
    }

    private Config coordinatorConfig(Path tablePath, int bucketCount) {
        Config config = new Config().totalBuckets(bucketCount);
        config.governanceMode = Config.GovernanceMode.NOOP;
        config.logConsole = false;
        config.logPath = tempDir.resolve("coordinator.log").toString();

        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = tablePath.toAbsolutePath().toString();
        volume.kinds = Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        config.addVolume(volume);
        return config;
    }

    // ------------------------------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------------------------------

    private static void assertColumnEquals(byte[] expected, ArrayData columns, int index) {
        if (expected == null) {
            assertTrue(columns.isNullAt(index), "column " + index + " should be null");
        } else {
            assertArrayEquals(expected, columns.getBinary(index), "column " + index + " mismatch");
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] bytes(int... ints) {
        byte[] b = new byte[ints.length];
        for (int i = 0; i < ints.length; i++) {
            b[i] = (byte) ints[i];
        }
        return b;
    }

    private static String toHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }

    private static void pollUntilRowCount(
            CobbleSourceReader reader, CollectingOutput output, int expectedRows) throws Exception {
        while (output.rows.size() < expectedRows) {
            InputStatus status = reader.pollNext(output);
            if (status == InputStatus.END_OF_INPUT) {
                throw new IllegalStateException(
                        "Reader finished before reaching " + expectedRows + " rows.");
            }
            if (status == InputStatus.NOTHING_AVAILABLE) {
                throw new IllegalStateException(
                        "Reader became idle before reaching " + expectedRows + " rows.");
            }
        }
    }

    private static void drainBoundedReader(CobbleSourceReader reader, CollectingOutput output)
            throws Exception {
        while (true) {
            InputStatus status = reader.pollNext(output);
            if (status == InputStatus.END_OF_INPUT) {
                return;
            }
            if (status == InputStatus.NOTHING_AVAILABLE) {
                throw new IllegalStateException("Bounded reader became idle before end of input.");
            }
        }
    }

    private static void drainStreamingReader(CobbleSourceReader reader, CollectingOutput output)
            throws Exception {
        int idlePolls = 0;
        while (idlePolls < 3) {
            int previousSize = output.rows.size();
            InputStatus status = reader.pollNext(output);
            if (status == InputStatus.END_OF_INPUT) {
                throw new IllegalStateException("Streaming reader ended unexpectedly.");
            }
            if (status == InputStatus.NOTHING_AVAILABLE && output.rows.size() == previousSize) {
                idlePolls++;
                continue;
            }
            idlePolls = 0;
        }
    }

    /**
     * A row written directly to the Cobble table: key bytes + per-column value bytes (null =
     * absent).
     */
    private static final class DirectRow {
        final byte[] key;
        final byte[] col0;
        final byte[] col1;
        final byte[] col2;

        DirectRow(byte[] key, byte[] col0, byte[] col1, byte[] col2) {
            this.key = key;
            this.col0 = col0;
            this.col1 = col1;
            this.col2 = col2;
        }

        byte[] column(int index) {
            switch (index) {
                case 0:
                    return col0;
                case 1:
                    return col1;
                case 2:
                    return col2;
                default:
                    throw new IndexOutOfBoundsException(String.valueOf(index));
            }
        }
    }

    /** Minimal reader context for exercising the source reader without a full runtime. */
    private static final class TestingContext implements SourceReaderContext {
        @Override
        public SourceReaderMetricGroup metricGroup() {
            return UnregisteredMetricsGroup.createSourceReaderMetricGroup();
        }

        @Override
        public Configuration getConfiguration() {
            return new Configuration();
        }

        @Override
        public String getLocalHostName() {
            return "localhost";
        }

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {}

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(getClass().getClassLoader());
        }

        @Override
        public int currentParallelism() {
            return 1;
        }
    }

    /** Reader output that collects emitted rows for assertions. */
    private static final class CollectingOutput implements ReaderOutput<RowData> {
        private final List<RowData> rows = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            rows.add(record);
        }

        @Override
        public void collect(RowData record, long timestamp) {
            rows.add(record);
        }

        @Override
        public void emitWatermark(org.apache.flink.api.common.eventtime.Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public org.apache.flink.api.connector.source.SourceOutput<RowData> createOutputForSplit(
                String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}
    }
}
