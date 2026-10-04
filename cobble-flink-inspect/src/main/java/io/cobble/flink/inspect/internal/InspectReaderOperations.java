package io.cobble.flink.inspect.internal;

import io.cobble.ReadOptions;
import io.cobble.Reader;
import io.cobble.ScanCursor;
import io.cobble.ScanOptions;

import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableSet;

/** Shared native-reader operations used by both the SDK session and the HTTP adapter. */
public final class InspectReaderOperations {
    private InspectReaderOperations() {}

    /**
     * Scans one bucket and returns the number of accepted rows. Unknown column families are treated
     * as empty because a shard may never have registered a state that exists in another shard.
     */
    public static int scanBucket(
            Reader reader,
            int bucket,
            byte[] start,
            byte[] end,
            byte[] startAfter,
            ScanOptions options,
            int maxAcceptedRows,
            EntryVisitor visitor) {
        return scanBucket(
                reader,
                bucket,
                start,
                end,
                startAfter,
                options,
                maxAcceptedRows,
                visitor,
                Collections.emptyNavigableSet());
    }

    static int scanBucket(
            Reader reader,
            int bucket,
            byte[] start,
            byte[] end,
            byte[] startAfter,
            ScanOptions options,
            int maxAcceptedRows,
            EntryVisitor visitor,
            NavigableSet<byte[]> legacyKeys) {
        if (maxAcceptedRows <= 0) {
            return 0;
        }
        ScanCursor cursor = null;
        try {
            cursor = reader.scanWithOptions(bucket, start, end, options);
        } catch (RuntimeException error) {
            if (isUnknownColumnFamily(error)) {
                // A legacy-only timer queue may have no native column family in this shard.
            } else {
                throw error;
            }
        }

        int accepted = 0;
        Iterator<byte[]> legacy =
                legacyKeys.isEmpty()
                        ? Collections.emptyIterator()
                        : legacyKeys.tailSet(start, true).iterator();
        byte[] legacyKey = legacy.hasNext() ? legacy.next() : null;
        try (ScanCursor ignored = cursor) {
            ScanCursor.Entry entry = cursor == null ? null : cursor.nextEntry();
            while ((entry != null || legacyKey != null) && accepted < maxAcceptedRows) {
                if (legacyKey != null && compareKeys(legacyKey, end) >= 0) {
                    legacyKey = null;
                }
                if (entry == null && legacyKey == null) {
                    break;
                }
                int comparison =
                        legacyKey == null
                                ? 1
                                : entry == null ? -1 : compareKeys(legacyKey, entry.key);
                byte[] key = comparison < 0 ? legacyKey : entry.key;
                byte[][] columns = comparison < 0 ? new byte[0][] : entry.columns;
                if ((startAfter == null || compareKeys(key, startAfter) > 0)
                        && visitor.visit(bucket, key, columns)) {
                    accepted++;
                }
                if (comparison <= 0) {
                    legacyKey = legacy.hasNext() ? legacy.next() : null;
                }
                if (comparison >= 0) {
                    entry = cursor.nextEntry();
                }
            }
        }
        return accepted;
    }

    static int compareKeys(byte[] left, byte[] right) {
        for (int index = 0; index < Math.min(left.length, right.length); index++) {
            int comparison = Integer.compare(left[index] & 0xff, right[index] & 0xff);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    public static boolean isUnknownColumnFamily(RuntimeException error) {
        String message = error.getMessage();
        if (message == null) {
            return false;
        }
        if (message.startsWith("IO error: ")) {
            message = message.substring("IO error: ".length());
        }
        return message.equals("Unknown column family")
                || message.startsWith("Unknown column family:")
                || message.startsWith("Unknown column family ")
                || message.startsWith("Unknown column family '");
    }

    /** Returns {@code null} for both a missing row and an unregistered column family. */
    public static byte[][] lookup(Reader reader, int bucket, byte[] key, ReadOptions options) {
        try {
            return reader.getWithOptions(bucket, key, options);
        } catch (RuntimeException error) {
            if (isUnknownColumnFamily(error)) {
                return null;
            }
            throw error;
        }
    }

    @FunctionalInterface
    public interface EntryVisitor {
        /** Returns whether the row counts toward the requested result limit. */
        boolean visit(int bucket, byte[] key, byte[][] columns);
    }
}
