package io.cobble.flink.inspect.internal;

import io.cobble.GlobalSnapshot;
import io.cobble.ReadOptions;
import io.cobble.Reader;
import io.cobble.ScanOptions;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.inspect.StateInspectExactLookupSupport;
import io.cobble.flink.common.inspect.StateKind;
import io.cobble.flink.inspect.DecodeIssue;
import io.cobble.flink.inspect.DecodedValue;
import io.cobble.flink.inspect.InspectCatalog;
import io.cobble.flink.inspect.InspectErrorCode;
import io.cobble.flink.inspect.InspectException;
import io.cobble.flink.inspect.InspectOverview;
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
import io.cobble.flink.inspect.ScanFilter;
import io.cobble.flink.inspect.ScanRequest;
import io.cobble.flink.inspect.StateKey;
import io.cobble.flink.inspect.TypedLookupKey;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
    private final String sourcePath;
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
        this.sourcePath = sourceRoot;
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
    public InspectOverview overview() {
        ensureOpen();
        return InspectOverviewGenerator.generate(
                sourcePath, selection.checkpointId(), selection.operatorId(), internalTargets);
    }

    @Override
    public InspectPage scan(ScanRequest request) {
        ensureOpen();
        validateScan(request);
        InspectTarget target = target(request.targetId());
        int limit = request.limit();
        int[] columns = target.allowsColumns ? request.columns() : null;
        validateColumns(request.columns(), target);
        ScanPlan plan = scanPlan(request, target);
        byte[] prefix = plan.prefix;
        byte[] end = prefix.length == 0 ? MAX_KEY : prefixUpperBound(prefix);
        if (end == null) {
            throw invalid("Scan prefix has no finite upper bound");
        }
        String requestIdentity = scanIdentity(request, plan);
        CursorPosition position = CursorPosition.parse(request.pageToken(), requestIdentity);
        int firstBucket =
                position == null ? plan.bucket == null ? 0 : plan.bucket : position.bucket;
        if (firstBucket < 0 || firstBucket >= totalBuckets) {
            throw invalid("Bucket must be between 0 and " + (totalBuckets - 1));
        }
        int lastBucket = plan.bucket == null ? totalBuckets - 1 : plan.bucket;
        if (position != null && plan.bucket != null && position.bucket != plan.bucket) {
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
                        scanOptions(
                                target.columnFamily,
                                columns,
                                plan.filter == null ? limit + 1 : Integer.MAX_VALUE),
                        limit + 1 - rows.size(),
                        (entryBucket, key, entryColumns) -> {
                            if (!plan.matches(key, entryColumns)) {
                                return false;
                            }
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
                                requestIdentity,
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
                ResolvedLookup resolved = resolveLookup(target, key);
                if (resolved.bucket < 0 || resolved.bucket >= totalBuckets) {
                    throw invalid("Bucket must be between 0 and " + (totalBuckets - 1));
                }
                byte[][] values;
                try {
                    values =
                            InspectReaderOperations.lookup(
                                    readerSession.reader(), resolved.bucket, resolved.key, options);
                } catch (RuntimeException error) {
                    throw unreadable("Failed to look up key", error);
                }
                rows.add(
                        row(
                                target,
                                resolved.bucket,
                                resolved.key,
                                values,
                                columns,
                                values != null));
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

    private ScanPlan scanPlan(ScanRequest request, InspectTarget target) {
        ScanFilter filter = request.filter();
        if (filter == null) {
            return new ScanPlan(rawPrefix(request.prefix()), request.bucket(), null);
        }
        if (request.prefix() != null) {
            throw invalid("Raw prefix cannot be combined with a typed scan filter");
        }
        validateFilterShape(filter);
        if (!filter.sinkKeyFields().isEmpty()) {
            if (target.sinkSchema == null || filter.stateKey() != null) {
                throw invalid("Sink key filters require a sink target");
            }
            List<String> values =
                    TypedInputs.sink(filter.sinkKeyFields(), target.sinkSchema.keyFields(), false);
            try {
                List<String> encodedPrefixValues =
                        values.subList(0, Math.max(0, values.size() - 1));
                String lastField =
                        filter.sinkKeyFields().get(filter.sinkKeyFields().size() - 1).name();
                String lastPrefix = values.get(values.size() - 1);
                return new ScanPlan(
                        SinkInspectDecoder.encodeKeyPrefix(target, encodedPrefixValues),
                        request.bucket(),
                        (key, columns) ->
                                sinkKeyStartsWith(
                                        SinkInspectDecoder.decode(target, key, columns, null),
                                        lastField,
                                        lastPrefix));
            } catch (java.io.IOException error) {
                throw invalid("Failed to encode sink scan filter: " + message(error));
            }
        }
        if (filter.stateKey() == null || target.schema == null || target.semanticSchema == null) {
            throw invalid("State filters require a schema-aware state target");
        }
        StateFilterValues stateFilter =
                validateStateFilter(target, filter.stateKey(), filter.autoKeyGroup());
        List<String> stateValues = stateFilter.stateValues;
        List<String> namespaceValues = stateFilter.namespaceValues;
        List<String> mapValues = stateFilter.mapValues;
        boolean stateExact = stateFilter.stateExact;
        boolean namespaceExact = stateFilter.namespaceExact;
        Integer bucket = request.bucket();
        if (filter.autoKeyGroup()) {
            List<String> completeState =
                    TypedInputs.semantic(
                            filter.stateKey().stateKeyFields(),
                            target.semanticSchema.stateKey(),
                            "key",
                            true,
                            "state key");
            try {
                int calculated =
                        UserClassLoaderScope.call(
                                userClassLoader,
                                () -> {
                                    try {
                                        return StateInspectDecoder.keyGroupForSemanticStateKey(
                                                target, completeState, totalBuckets);
                                    } catch (java.io.IOException error) {
                                        throw new TypedEncodingException(error);
                                    }
                                });
                if (bucket != null && bucket.intValue() != calculated) {
                    throw invalid(
                            "Requested bucket "
                                    + bucket
                                    + " does not match calculated Key Group "
                                    + calculated);
                }
                bucket = calculated;
            } catch (TypedEncodingException error) {
                throw invalid("Failed to calculate Key Group: " + message(error.getCause()));
            }
        }
        RowFilter rowFilter =
                (key, entryColumns) -> {
                    StateInspectDecoder.DecodedRow decoded =
                            UserClassLoaderScope.call(
                                    userClassLoader,
                                    () -> StateInspectDecoder.decode(target, key, entryColumns));
                    return matchesPart(
                                    decoded.decodedParts,
                                    "state_key",
                                    target.semanticSchema.stateKey(),
                                    stateValues,
                                    stateExact)
                            && matchesPart(
                                    decoded.decodedParts,
                                    "namespace",
                                    target.semanticSchema.namespace(),
                                    namespaceValues,
                                    namespaceExact)
                            && matchesPart(
                                    decoded.decodedParts,
                                    "map_key",
                                    target.semanticSchema.mapUserKey(),
                                    mapValues,
                                    false);
                };
        return new ScanPlan(EMPTY_KEY, bucket, rowFilter);
    }

    static StateFilterValues validateStateFilter(
            InspectTarget target, StateKey state, boolean autoKeyGroup) {
        if (state == null) {
            throw invalid("State scan filter key is required");
        }
        if (state.stateKeyFields().isEmpty()
                && state.namespaceFields().isEmpty()
                && state.mapKeyFields().isEmpty()) {
            throw invalid("Typed state scan filter must include at least one field");
        }
        if (state.stateKeyFields().isEmpty()
                && (!state.namespaceFields().isEmpty() || !state.mapKeyFields().isEmpty())) {
            throw invalid("State key fields are required before namespace or map key fields");
        }
        boolean hasNamespace = !state.namespaceFields().isEmpty();
        boolean hasMapKey = !state.mapKeyFields().isEmpty();
        boolean stateExact = hasNamespace || hasMapKey || autoKeyGroup;
        boolean namespaceExact = hasMapKey;
        List<String> stateValues =
                TypedInputs.semantic(
                        state.stateKeyFields(),
                        target.semanticSchema.stateKey(),
                        "key",
                        stateExact,
                        "state key");
        List<String> namespaceValues =
                voidNamespace(target)
                        ? requireEmpty(state.namespaceFields(), "VoidNamespace")
                        : TypedInputs.semantic(
                                state.namespaceFields(),
                                target.semanticSchema.namespace(),
                                "namespace",
                                namespaceExact,
                                "namespace");
        List<String> mapValues =
                target.schema.stateKind() == StateKind.MAP
                        ? TypedInputs.semantic(
                                state.mapKeyFields(),
                                target.semanticSchema.mapUserKey(),
                                "map_key",
                                false,
                                "map key")
                        : requireEmpty(state.mapKeyFields(), "Non-map state");
        return new StateFilterValues(
                stateValues, namespaceValues, mapValues, stateExact, namespaceExact);
    }

    static void validateFilterShape(ScanFilter filter) {
        if (filter == null) {
            return;
        }
        if (filter.sinkKeyFields().isEmpty() && filter.stateKey() == null) {
            throw invalid("Typed scan filter must include sink or state key fields");
        }
        if (!filter.sinkKeyFields().isEmpty() && filter.stateKey() != null) {
            throw invalid("Sink and state scan filters are mutually exclusive");
        }
    }

    private ResolvedLookup resolveLookup(InspectTarget target, LookupKey lookup) {
        if (lookup == null) {
            throw invalid("Lookup key must not be null");
        }
        if (!lookup.typed()) {
            if (lookup.key() == null || lookup.key().value() == null) {
                throw invalid("Lookup key bytes must not be null");
            }
            return new ResolvedLookup(lookup.bucket(), lookup.key().value());
        }
        TypedLookupKey typed = lookup.typedKey();
        if (target.sinkSchema != null) {
            if (typed.kind() != TypedLookupKey.Kind.SINK) {
                throw invalid("A sink target requires a typed sink key");
            }
            List<String> values =
                    TypedInputs.sink(typed.sinkKeyFields(), target.sinkSchema.keyFields(), true);
            try {
                byte[] key = SinkInspectDecoder.encodeKeyPrefix(target, values);
                return new ResolvedLookup(Math.floorMod(Arrays.hashCode(key), totalBuckets), key);
            } catch (java.io.IOException error) {
                throw invalid("Failed to encode sink lookup key: " + message(error));
            }
        }
        if (target.schema == null || target.semanticSchema == null) {
            throw invalid("Typed lookup requires schema metadata");
        }
        if (typed.kind() != TypedLookupKey.Kind.STATE || typed.stateKey() == null) {
            throw invalid("A state target requires a typed state key");
        }
        StateKind kind = target.schema.stateKind();
        if (kind == StateKind.TIMER || kind == StateKind.LIST) {
            throw invalid(kind + " state does not support typed exact lookup");
        }
        StateInspectExactLookupSupport.Result support =
                StateInspectExactLookupSupport.evaluate(target.schema, target.semanticSchema);
        if (!support.supported()) {
            throw invalid("Typed exact lookup is unavailable: " + support.reason());
        }
        StateKey state = typed.stateKey();
        List<String> stateValues =
                TypedInputs.semantic(
                        state.stateKeyFields(),
                        target.semanticSchema.stateKey(),
                        "key",
                        true,
                        "state key");
        List<String> namespaceValues =
                voidNamespace(target)
                        ? requireEmpty(state.namespaceFields(), "VoidNamespace")
                        : TypedInputs.semantic(
                                state.namespaceFields(),
                                target.semanticSchema.namespace(),
                                "namespace",
                                true,
                                "namespace");
        List<String> mapValues =
                kind == StateKind.MAP
                        ? TypedInputs.semantic(
                                state.mapKeyFields(),
                                target.semanticSchema.mapUserKey(),
                                "map_key",
                                true,
                                "map key")
                        : requireEmpty(state.mapKeyFields(), "Non-map state");
        try {
            StateInspectDecoder.EncodedStateKey encoded =
                    UserClassLoaderScope.call(
                            userClassLoader,
                            () -> {
                                try {
                                    return StateInspectDecoder.encodeExactStateKey(
                                            target,
                                            stateValues,
                                            namespaceValues,
                                            mapValues,
                                            totalBuckets);
                                } catch (java.io.IOException error) {
                                    throw new TypedEncodingException(error);
                                }
                            });
            return new ResolvedLookup(encoded.keyGroup, encoded.rowKey);
        } catch (TypedEncodingException error) {
            throw invalid("Failed to encode state lookup key: " + message(error.getCause()));
        }
    }

    private static boolean matchesPart(
            Map<String, Object> decoded,
            String name,
            io.cobble.flink.common.inspect.StateInspectType type,
            List<String> values,
            boolean exact) {
        return values == null
                || values.isEmpty()
                || StateInspectDecoder.matchesSemanticPartFilter(
                        decoded, name, type, values, exact);
    }

    private static boolean sinkKeyStartsWith(
            SinkInspectDecoder.DecodedRow decoded, String fieldName, String prefix) {
        if (decoded.decodedKey == null) {
            return false;
        }
        for (Map<String, Object> field : decoded.decodedKey) {
            if (!fieldName.equals(field.get("name"))) {
                continue;
            }
            Object value = field.get("value");
            if (value == null) {
                return false;
            }
            if (value instanceof Map) {
                Object utf8 = ((Map<?, ?>) value).get("utf8");
                if (utf8 != null) {
                    return String.valueOf(utf8).startsWith(prefix);
                }
                Object b64 = ((Map<?, ?>) value).get("b64");
                return b64 != null && String.valueOf(b64).startsWith(prefix);
            }
            return String.valueOf(value).startsWith(prefix);
        }
        return false;
    }

    private static List<String> requireEmpty(
            List<io.cobble.flink.inspect.FieldValue> values, String label) {
        if (values != null && !values.isEmpty()) {
            throw invalid(label + " does not accept these fields");
        }
        return Collections.emptyList();
    }

    private static boolean voidNamespace(InspectTarget target) {
        return target.schema.namespaceSerializer() != null
                && "org.apache.flink.runtime.state.VoidNamespaceSerializer"
                        .equals(target.schema.namespaceSerializer().serializerClassName());
    }

    private static String scanIdentity(ScanRequest request, ScanPlan plan) {
        StringBuilder identity =
                new StringBuilder(request.targetId())
                        .append('|')
                        .append(plan.bucket)
                        .append('|')
                        .append(Base64.getEncoder().encodeToString(plan.prefix))
                        .append('|')
                        .append(Arrays.toString(request.columns()));
        ScanFilter filter = request.filter();
        if (filter != null) {
            identity.append('|').append(TypedInputs.identity(filter.sinkKeyFields()));
            if (filter.stateKey() != null) {
                identity.append('|')
                        .append(TypedInputs.identity(filter.stateKey().stateKeyFields()))
                        .append('|')
                        .append(TypedInputs.identity(filter.stateKey().namespaceFields()))
                        .append('|')
                        .append(TypedInputs.identity(filter.stateKey().mapKeyFields()))
                        .append('|')
                        .append(filter.autoKeyGroup());
            }
        }
        return digest(identity.toString());
    }

    private static String digest(String value) {
        try {
            byte[] hash =
                    MessageDigest.getInstance("SHA-256")
                            .digest(value.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(Arrays.copyOf(hash, 16));
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
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

    static InspectException invalid(String message) {
        return new InspectException(InspectErrorCode.INVALID_INPUT, message);
    }

    private static InspectException unreadable(String message, RuntimeException error) {
        InspectErrorCode code =
                CheckpointUnavailableClassifier.isCheckpointUnavailable(error)
                        ? InspectErrorCode.CHECKPOINT_UNAVAILABLE
                        : InspectErrorCode.UNREADABLE;
        return new InspectException(code, message + ": " + message(error), error);
    }

    static String message(Throwable error) {
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

        private static PageToken encode(String identity, int bucket, byte[] key) {
            String encodedKey = Base64.getUrlEncoder().withoutPadding().encodeToString(key);
            return new PageToken(VERSION + "." + identity + "." + bucket + "." + encodedKey);
        }

        private static CursorPosition parse(PageToken token, String identity) {
            if (token == null) {
                return null;
            }
            String[] parts = token.value().split("\\.", -1);
            if (parts.length != 4 || !VERSION.equals(parts[0])) {
                throw invalid("Invalid scan page token");
            }
            try {
                if (!identity.equals(parts[1])) {
                    throw invalid("Page token belongs to different scan filters or projection");
                }
                return new CursorPosition(
                        Integer.parseInt(parts[2]), Base64.getUrlDecoder().decode(parts[3]));
            } catch (IllegalArgumentException error) {
                throw invalid("Invalid scan page token");
            }
        }
    }

    private interface RowFilter {
        boolean matches(byte[] key, byte[][] columns);
    }

    private static final class ScanPlan {
        private final byte[] prefix;
        private final Integer bucket;
        private final RowFilter filter;

        private ScanPlan(byte[] prefix, Integer bucket, RowFilter filter) {
            this.prefix = prefix;
            this.bucket = bucket;
            this.filter = filter;
        }

        private boolean matches(byte[] key, byte[][] columns) {
            return filter == null || filter.matches(key, columns);
        }
    }

    private static final class ResolvedLookup {
        private final int bucket;
        private final byte[] key;

        private ResolvedLookup(int bucket, byte[] key) {
            this.bucket = bucket;
            this.key = key;
        }
    }

    static final class StateFilterValues {
        final List<String> stateValues;
        final List<String> namespaceValues;
        final List<String> mapValues;
        final boolean stateExact;
        final boolean namespaceExact;

        private StateFilterValues(
                List<String> stateValues,
                List<String> namespaceValues,
                List<String> mapValues,
                boolean stateExact,
                boolean namespaceExact) {
            this.stateValues = stateValues;
            this.namespaceValues = namespaceValues;
            this.mapValues = mapValues;
            this.stateExact = stateExact;
            this.namespaceExact = namespaceExact;
        }
    }

    private static final class TypedEncodingException extends RuntimeException {
        private TypedEncodingException(Throwable cause) {
            super(cause);
        }
    }
}
