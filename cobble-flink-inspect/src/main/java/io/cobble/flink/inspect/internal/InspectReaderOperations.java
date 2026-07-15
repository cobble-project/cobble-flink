package io.cobble.flink.inspect.internal;

import io.cobble.ReadOptions;
import io.cobble.Reader;
import io.cobble.ScanCursor;
import io.cobble.ScanOptions;

import java.util.Arrays;

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
        if (maxAcceptedRows <= 0) {
            return 0;
        }
        final ScanCursor cursor;
        try {
            cursor = reader.scanWithOptions(bucket, start, end, options);
        } catch (RuntimeException error) {
            if (isUnknownColumnFamily(error)) {
                return 0;
            }
            throw error;
        }

        int accepted = 0;
        boolean skipStartAfter = startAfter != null;
        try (ScanCursor ignored = cursor) {
            ScanCursor.Entry entry = cursor.nextEntry();
            while (entry != null && accepted < maxAcceptedRows) {
                if (skipStartAfter) {
                    skipStartAfter = false;
                    if (entry.bucket == bucket && Arrays.equals(entry.key, startAfter)) {
                        entry = cursor.nextEntry();
                        continue;
                    }
                }
                if (visitor.visit(entry.bucket, entry.key, entry.columns)) {
                    accepted++;
                }
                entry = cursor.nextEntry();
            }
        }
        return accepted;
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
