package io.cobble.flink.inspect.internal;

import io.cobble.GlobalSnapshot;
import io.cobble.ReadOptions;
import io.cobble.Reader;
import io.cobble.ScanOptions;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.inspect.DecodeIssue;
import io.cobble.flink.inspect.DecodedValue;
import io.cobble.flink.inspect.InspectCatalog;
import io.cobble.flink.inspect.InspectErrorCode;
import io.cobble.flink.inspect.InspectException;
import io.cobble.flink.inspect.InspectPage;
import io.cobble.flink.inspect.InspectRow;
import io.cobble.flink.inspect.InspectSelection;
import io.cobble.flink.inspect.InspectSession;
import io.cobble.flink.inspect.InspectSessionInfo;
import io.cobble.flink.inspect.LookupKey;
import io.cobble.flink.inspect.LookupRequest;
import io.cobble.flink.inspect.LookupResult;
import io.cobble.flink.inspect.PageToken;
import io.cobble.flink.inspect.RawBytes;
import io.cobble.flink.inspect.ScanRequest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Default pinned SDK session. All native and temporary resources are owned by this object. */
final class InspectSessionImpl implements InspectSession {
    private static final byte[] EMPTY_KEY = new byte[0];
    private static final byte[] MAX_KEY = maxKey();

    private final InspectCatalog catalog;
    private final InspectSelection selection;
    private final MonitorReaderSession readerSession;
    private final ClassLoader userClassLoader;
    private final List<InspectTarget> internalTargets;
    private final List<io.cobble.flink.inspect.InspectTarget> targets;
    private final int totalBuckets;
    private boolean closed;

    InspectSessionImpl(
            InspectCatalog catalog,
            InspectSelection selection,
            MonitorReaderSession readerSession,
            String sourceKind,
            String sourceRoot,
            CheckpointEntry checkpoint,
            OperatorEntry operator,
            CobbleConnectorStorageOptions storageOptions,
            ClassLoader userClassLoader,
            int configuredTotalBuckets) {
        this.catalog = catalog;
        this.selection = selection;
        this.readerSession = readerSession;
        this.userClassLoader = userClassLoader;

        Reader reader = readerSession.reader();
        GlobalSnapshot snapshot = reader.currentGlobalSnapshot();
        this.totalBuckets =
                snapshot != null && snapshot.totalBuckets > 0
                        ? snapshot.totalBuckets
                        : configuredTotalBuckets;
        SchemaResolveResult stateSchema = resolveStateSchema(sourceKind, checkpoint, operator);
        SinkSchemaResolveResult sinkSchema =
                resolveSinkSchema(sourceKind, sourceRoot, checkpoint, storageOptions);
        this.internalTargets =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                StateInspectTargetBuilder.build(
                                        snapshot, stateSchema, sinkSchema)));
        List<io.cobble.flink.inspect.InspectTarget> publicTargets = new ArrayList<>();
        for (InspectTarget target : internalTargets) {
            publicTargets.add(PublicInspectModels.target(target));
        }
        this.targets = Collections.unmodifiableList(publicTargets);
    }

    @Override
    public InspectCatalog catalog() {
        ensureOpen();
        return catalog;
    }

    @Override
    public InspectSessionInfo info() {
        ensureOpen();
        return new InspectSessionInfo(selection, true);
    }

    @Override
    public List<io.cobble.flink.inspect.InspectTarget> targets() {
        ensureOpen();
        return targets;
    }

    @Override
    public InspectPage scan(ScanRequest request) {
        ensureOpen();
        validateScan(request);
        InspectTarget target = target(request.targetId());
        int limit = request.limit();
        int[] columns = target.allowsColumns ? request.columns() : null;
        validateColumns(request.columns(), target);
        byte[] prefix = rawPrefix(request.prefix());
        byte[] end = prefix.length == 0 ? MAX_KEY : prefixUpperBound(prefix);
        if (end == null) {
            throw invalid("Scan prefix has no finite upper bound");
        }
        CursorPosition position = CursorPosition.parse(request.pageToken(), request.targetId());
        int firstBucket =
                position == null
                        ? request.bucket() == null ? 0 : request.bucket()
                        : position.bucket;
        if (firstBucket < 0 || firstBucket >= totalBuckets) {
            throw invalid("Bucket must be between 0 and " + (totalBuckets - 1));
        }
        int lastBucket = request.bucket() == null ? totalBuckets - 1 : request.bucket();
        if (position != null && request.bucket() != null && position.bucket != request.bucket()) {
            throw invalid("Page token does not belong to the requested bucket");
        }

        List<InspectRow> rows = new ArrayList<>();
        boolean hasMore = false;
        for (int bucket = firstBucket; bucket <= lastBucket && !hasMore; bucket++) {
            byte[] start =
                    position != null && bucket == position.bucket
                            ? position.key
                            : prefix.length == 0 ? EMPTY_KEY : prefix;
            boolean skipPosition = position != null && bucket == position.bucket;
            try {
                InspectReaderOperations.scanBucket(
                        readerSession.reader(),
                        bucket,
                        start,
                        end,
                        skipPosition ? position.key : null,
                        scanOptions(target.columnFamily, columns, limit + 1),
                        limit + 1 - rows.size(),
                        (entryBucket, key, entryColumns) -> {
                            rows.add(row(target, entryBucket, key, entryColumns, columns, true));
                            return true;
                        });
            } catch (RuntimeException error) {
                throw unreadable("Failed to scan bucket " + bucket, error);
            }
            if (rows.size() > limit) {
                rows.remove(rows.size() - 1);
                hasMore = true;
            }
        }
        PageToken next =
                hasMore && !rows.isEmpty()
                        ? CursorPosition.encode(
                                request.targetId(),
                                rows.get(rows.size() - 1).bucket(),
                                rows.get(rows.size() - 1).key().value())
                        : null;
        return new InspectPage(rows, next);
    }

    @Override
    public LookupResult lookup(LookupRequest request) {
        ensureOpen();
        if (request == null || request.targetId() == null || request.targetId().trim().isEmpty()) {
            throw invalid("Lookup target is required");
        }
        if (request.keys().isEmpty()) {
            throw invalid("Lookup keys must not be empty");
        }
        InspectTarget target = target(request.targetId());
        int[] columns = target.allowsColumns ? request.columns() : null;
        validateColumns(request.columns(), target);
        List<InspectRow> rows = new ArrayList<>(request.keys().size());
        try (ReadOptions options = readOptions(target.columnFamily, columns)) {
            for (LookupKey key : request.keys()) {
                if (key == null || key.key() == null || key.key().value() == null) {
                    throw invalid("Lookup key bytes must not be null");
                }
                if (key.bucket() < 0 || key.bucket() >= totalBuckets) {
                    throw invalid("Bucket must be between 0 and " + (totalBuckets - 1));
                }
                byte[] rawKey = key.key().value();
                byte[][] values;
                try {
                    values =
                            InspectReaderOperations.lookup(
                                    readerSession.reader(), key.bucket(), rawKey, options);
                } catch (RuntimeException error) {
                    throw unreadable("Failed to look up key", error);
                }
                rows.add(row(target, key.bucket(), rawKey, values, columns, values != null));
            }
        }
        return new LookupResult(rows);
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            readerSession.close();
        }
    }

    private InspectRow row(
            InspectTarget target,
            int bucket,
            byte[] key,
            byte[][] columns,
            int[] projection,
            boolean found) {
        List<RawBytes> rawColumns = rawColumns(columns);
        DecodedValue decodedKey = null;
        DecodedValue decodedValue = null;
        List<DecodedValue> decodedColumns = Collections.emptyList();
        Map<String, DecodedValue> decodedParts = Collections.emptyMap();
        List<DecodeIssue> issues = Collections.emptyList();
        if (found && target.sinkSchema != null) {
            SinkInspectDecoder.DecodedRow decoded =
                    UserClassLoaderScope.call(
                            userClassLoader,
                            () -> SinkInspectDecoder.decode(target, key, columns, projection));
            decodedKey =
                    decoded.decodedKey == null
                            ? null
                            : PublicInspectModels.sinkRow(decoded.decodedKey);
            decodedColumns = PublicInspectModels.sinkFields(decoded.decodedColumns);
            issues = PublicInspectModels.issues(decoded.decodeError);
        } else if (found && target.schema != null) {
            StateInspectDecoder.DecodedRow decoded =
                    UserClassLoaderScope.call(
                            userClassLoader,
                            () -> StateInspectDecoder.decode(target, key, columns));
            decodedKey =
                    decoded.decodedKey == null
                            ? null
                            : PublicInspectModels.decoded(decoded.decodedKey);
            decodedValue =
                    decoded.decodedValue == null
                            ? null
                            : PublicInspectModels.decoded(decoded.decodedValue);
            decodedParts = PublicInspectModels.parts(decoded.decodedParts);
            issues = PublicInspectModels.issues(decoded);
        }
        return new InspectRow(
                bucket,
                new RawBytes(key),
                rawColumns,
                found,
                rawColumns.isEmpty() ? null : rawColumns.get(0),
                decodedKey,
                decodedValue,
                decodedColumns,
                decodedParts,
                issues);
    }

    private InspectTarget target(String targetId) {
        for (InspectTarget target : internalTargets) {
            if (target.id.equals(targetId)
                    || target.name.equals(targetId)
                    || (target.columnFamily != null && target.columnFamily.equals(targetId))) {
                return target;
            }
        }
        throw new InspectException(
                InspectErrorCode.NOT_FOUND, "Unknown inspect target: " + targetId);
    }

    private void validateScan(ScanRequest request) {
        if (request == null || request.targetId() == null || request.targetId().trim().isEmpty()) {
            throw invalid("Scan target is required");
        }
        if (request.limit() <= 0) {
            throw invalid("Scan limit must be greater than zero");
        }
    }

    private static byte[] rawPrefix(RawBytes prefix) {
        if (prefix == null) {
            return EMPTY_KEY;
        }
        byte[] bytes = prefix.value();
        if (bytes == null) {
            throw invalid("Scan prefix bytes must not be null");
        }
        return bytes;
    }

    private static void validateColumns(int[] columns, InspectTarget target) {
        if (columns == null) {
            return;
        }
        if (!target.allowsColumns) {
            throw invalid("Column projection is not supported for target " + target.id);
        }
        Set<Integer> seen = new HashSet<>();
        for (int column : columns) {
            if (column < 0) {
                throw invalid("Column indexes must not be negative");
            }
            if (!seen.add(column)) {
                throw invalid("Column indexes must not contain duplicates");
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new InspectException(InspectErrorCode.CLOSED, "InspectSession is closed");
        }
    }

    private static SchemaResolveResult resolveStateSchema(
            String sourceKind, CheckpointEntry checkpoint, OperatorEntry operator) {
        if ("data_source".equals(sourceKind)) {
            return SchemaResolveResult.unsupported(
                    "State schemas are not used for Cobble data sources");
        }
        try {
            return MonitorInspectSchemaResolver.resolve(checkpoint, operator);
        } catch (Exception error) {
            return SchemaResolveResult.unavailable(
                    "Failed to resolve state schema: " + message(error));
        }
    }

    private static SinkSchemaResolveResult resolveSinkSchema(
            String sourceKind,
            String sourceRoot,
            CheckpointEntry checkpoint,
            CobbleConnectorStorageOptions storageOptions) {
        if (!"data_source".equals(sourceKind)) {
            return SinkSchemaResolveResult.unsupported(
                    "Sink schemas are only used for Cobble data sources");
        }
        try {
            return SinkInspectSchemaResolver.resolve(sourceRoot, checkpoint.id, storageOptions);
        } catch (Exception error) {
            return SinkSchemaResolveResult.unavailable(
                    "Failed to resolve sink schema: " + message(error));
        }
    }

    private static ScanOptions scanOptions(String columnFamily, int[] columns, int maxRows) {
        ScanOptions options = new ScanOptions().maxRows(maxRows);
        if (columnFamily != null) {
            options.columnFamily(columnFamily);
        }
        if (columns != null) {
            options.columns(columns);
        }
        return options;
    }

    private static ReadOptions readOptions(String columnFamily, int[] columns) {
        ReadOptions options = new ReadOptions();
        if (columnFamily != null) {
            options.columnFamily(columnFamily);
        }
        if (columns == null) {
            options.clearColumns();
        } else {
            options.columns(columns);
        }
        return options;
    }

    private static List<RawBytes> rawColumns(byte[][] columns) {
        if (columns == null) {
            return Collections.emptyList();
        }
        List<RawBytes> output = new ArrayList<>(columns.length);
        for (byte[] column : columns) {
            output.add(new RawBytes(column));
        }
        return output;
    }

    private static byte[] prefixUpperBound(byte[] prefix) {
        byte[] end = Arrays.copyOf(prefix, prefix.length);
        for (int index = end.length - 1; index >= 0; index--) {
            int value = end[index] & 0xff;
            if (value != 0xff) {
                end[index] = (byte) (value + 1);
                return Arrays.copyOf(end, index + 1);
            }
        }
        return null;
    }

    private static byte[] maxKey() {
        byte[] key = new byte[64];
        Arrays.fill(key, (byte) 0xff);
        return key;
    }

    private static InspectException invalid(String message) {
        return new InspectException(InspectErrorCode.INVALID_INPUT, message);
    }

    private static InspectException unreadable(String message, RuntimeException error) {
        return new InspectException(
                InspectErrorCode.UNREADABLE, message + ": " + message(error), error);
    }

    private static String message(Throwable error) {
        return error.getMessage() == null ? error.getClass().getName() : error.getMessage();
    }

    private static final class CursorPosition {
        private static final String VERSION = "v1";

        private final int bucket;
        private final byte[] key;

        private CursorPosition(int bucket, byte[] key) {
            this.bucket = bucket;
            this.key = key;
        }

        private static PageToken encode(String targetId, int bucket, byte[] key) {
            String target =
                    Base64.getUrlEncoder()
                            .withoutPadding()
                            .encodeToString(targetId.getBytes(StandardCharsets.UTF_8));
            String encodedKey = Base64.getUrlEncoder().withoutPadding().encodeToString(key);
            return new PageToken(VERSION + "." + target + "." + bucket + "." + encodedKey);
        }

        private static CursorPosition parse(PageToken token, String targetId) {
            if (token == null) {
                return null;
            }
            String[] parts = token.value().split("\\.", -1);
            if (parts.length != 4 || !VERSION.equals(parts[0])) {
                throw invalid("Invalid scan page token");
            }
            try {
                String target =
                        new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
                if (!targetId.equals(target)) {
                    throw invalid("Page token belongs to a different target");
                }
                return new CursorPosition(
                        Integer.parseInt(parts[2]), Base64.getUrlDecoder().decode(parts[3]));
            } catch (IllegalArgumentException error) {
                throw invalid("Invalid scan page token");
            }
        }
    }
}
