package io.cobble.flink.monitor;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.ReadOptions;
import io.cobble.Reader;
import io.cobble.ScanCursor;
import io.cobble.ScanOptions;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class CobbleFlinkMonitorServer {

    private static final String CHECKPOINT_PREFIX = "chk-";
    private static final String COBBLE_MANIFEST_PREFIX = "COBBLE-SNAPSHOT-";
    private static final String COBBLE_MANIFEST_SUFFIX = "-MANIFEST";
    private static final byte[] EMPTY_SCAN_KEY = new byte[0];
    private static final byte[] MAX_SCAN_KEY = maxScanKey();
    private static final Gson GSON =
            new GsonBuilder()
                    .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                    .create();

    private CobbleFlinkMonitorServer() {}

    public static void main(String[] args) throws Exception {
        ServerConfig config = ServerConfig.parse(args);
        preloadRuntimeClasses();
        config.flinkConfiguration = FlinkMonitorFileSystems.initialize(config.flinkConfPath);
        MonitorState state = MonitorState.open(config);
        ExecutorService executor = Executors.newCachedThreadPool();
        HttpServer server =
                HttpServer.create(new InetSocketAddress(config.bindAddress, config.port), 0);
        server.setExecutor(executor);

        Router router = new Router(state);
        server.createContext("/", router);
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    state.close();
                                    server.stop(0);
                                    executor.shutdownNow();
                                },
                                "cobble-flink-monitor-shutdown"));

        server.start();
        InetSocketAddress address = server.getAddress();
        System.out.println(
                "cobble-flink-monitor listening on http://"
                        + address.getHostString()
                        + ":"
                        + address.getPort()
                        + "/");
        new CountDownLatch(1).await();
    }

    private static void preloadRuntimeClasses() throws ClassNotFoundException {
        ClassLoader loader = CobbleFlinkMonitorServer.class.getClassLoader();
        for (String className :
                Arrays.asList(
                        "io.cobble.Config",
                        "io.cobble.Config$VolumeDescriptor",
                        "io.cobble.GlobalSnapshot",
                        "io.cobble.ReadOptions",
                        "io.cobble.Reader",
                        "io.cobble.ScanCursor",
                        "io.cobble.ScanCursor$Entry",
                        "io.cobble.ScanOptions",
                        "io.cobble.ShardSnapshot")) {
            Class.forName(className, false, loader);
        }
    }

    private static final class Router implements HttpHandler {

        private final MonitorState state;

        private Router(MonitorState state) {
            this.state = state;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String method = exchange.getRequestMethod();
                String path = exchange.getRequestURI().getPath();
                if ("GET".equals(method) && "/healthz".equals(path)) {
                    sendJson(exchange, 200, singleton("status", "ok"));
                    return;
                }
                if (path.startsWith("/api/v1/")) {
                    handleApi(exchange, method, path);
                    return;
                }
                handleStatic(exchange, path);
            } catch (InputException e) {
                sendError(exchange, 400, e.getMessage());
            } catch (Throwable t) {
                sendError(exchange, 500, t.getMessage() == null ? t.toString() : t.getMessage());
            } finally {
                exchange.close();
            }
        }

        private void handleApi(HttpExchange exchange, String method, String path)
                throws IOException {
            if ("GET".equals(method) && "/api/v1/meta".equals(path)) {
                sendJson(exchange, 200, state.meta());
                return;
            }
            if ("GET".equals(method) && "/api/v1/snapshots".equals(path)) {
                sendJson(exchange, 200, state.snapshots());
                return;
            }
            if ("POST".equals(method) && "/api/v1/mode".equals(path)) {
                JsonObject body = readJsonObject(exchange);
                sendJson(exchange, 200, state.switchMode(body));
                return;
            }
            if ("GET".equals(method) && "/api/v1/inspect".equals(path)) {
                sendJson(exchange, 200, state.inspect(parseQuery(exchange.getRequestURI())));
                return;
            }
            sendError(exchange, 404, "not found");
        }

        private void handleStatic(HttpExchange exchange, String path) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendError(exchange, 405, "method not allowed");
                return;
            }
            String resourcePath = path;
            if (resourcePath == null || "/".equals(resourcePath) || resourcePath.isEmpty()) {
                resourcePath = "/index.html";
            }
            if (!hasFileExtension(resourcePath)) {
                resourcePath = "/index.html";
            }
            byte[] payload = StaticWebResources.read(resourcePath);
            if (payload == null) {
                sendError(exchange, 404, "not found");
                return;
            }
            Headers headers = exchange.getResponseHeaders();
            headers.set("Content-Type", contentType(resourcePath));
            headers.set("Cache-Control", cacheControl(resourcePath));
            exchange.sendResponseHeaders(200, payload.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(payload);
            }
        }
    }

    private static final class MonitorState implements AutoCloseable {

        private final ServerConfig config;
        private CheckpointCatalog catalog;
        private final long startedAtMillis;
        private boolean selectedLatest;
        private CheckpointEntry selectedCheckpoint;
        private OperatorEntry selectedOperator;
        private SchemaResolveResult selectedSchema;
        private SinkSchemaResolveResult selectedSinkSchema;
        private Reader reader;
        private List<File> readerTemporaryDirectories;

        private MonitorState(
                ServerConfig config,
                CheckpointCatalog catalog,
                boolean selectedLatest,
                CheckpointEntry selectedCheckpoint,
                OperatorEntry selectedOperator,
                ReaderHandle readerHandle) {
            this.config = config;
            this.catalog = catalog;
            this.selectedLatest = selectedLatest;
            this.selectedCheckpoint = selectedCheckpoint;
            this.selectedOperator = selectedOperator;
            this.selectedSchema =
                    selectedCheckpoint != null && selectedOperator != null
                            ? resolveSchema(selectedCheckpoint, selectedOperator)
                            : null;
            this.selectedSinkSchema =
                    catalog != null && selectedCheckpoint != null
                            ? resolveSinkSchema(catalog, selectedCheckpoint)
                            : null;
            this.reader = readerHandle.reader;
            this.readerTemporaryDirectories = readerHandle.temporaryDirectories;
            this.startedAtMillis = System.currentTimeMillis();
        }

        static MonitorState open(ServerConfig config) {
            if (config.checkpointRoot == null) {
                return new MonitorState(
                        config,
                        null,
                        true,
                        null,
                        null,
                        new ReaderHandle(null, Collections.emptyList()));
            }
            CheckpointCatalog catalog = CheckpointCatalog.discover(config.checkpointRoot);
            ReaderSelection selection = openLatestReadable(config, catalog, null);
            return new MonitorState(
                    config,
                    catalog,
                    true,
                    selection.checkpoint,
                    selection.operator,
                    selection.readerHandle);
        }

        private static ReaderSelection openLatestReadable(
                ServerConfig config, CheckpointCatalog catalog, String preferredOperatorId) {
            RuntimeException firstFailure = null;
            for (CheckpointEntry checkpoint : catalog.checkpoints) {
                OperatorEntry operator =
                        preferredOperatorId == null
                                ? checkpoint.defaultOperator()
                                : checkpoint.findOperatorOrDefault(preferredOperatorId);
                try {
                    return new ReaderSelection(
                            checkpoint, operator, openReader(config, checkpoint, operator));
                } catch (RuntimeException e) {
                    if (firstFailure == null) {
                        firstFailure = e;
                    }
                }
            }
            if (firstFailure != null) {
                throw firstFailure;
            }
            throw new InputException(
                    "No readable checkpoints found under " + catalog.rootDirectory);
        }

        private static ReaderHandle openReader(
                ServerConfig config, CheckpointEntry checkpoint, OperatorEntry operator) {
            if (operator.globalSnapshotLayout) {
                return openGlobalSnapshotReader(config, checkpoint, operator);
            }
            Config cobbleConfig = CobbleReaderConfigs.base(config.totalBuckets);
            cobbleConfig.snapshotRetention = null;
            for (String volumeDirectory : operator.readerVolumeDirectories) {
                CobbleReaderConfigs.addVolume(
                        cobbleConfig, volumeDirectory, config.flinkConfiguration);
            }
            return new ReaderHandle(
                    Reader.open(cobbleConfig, checkpoint.id), Collections.emptyList());
        }

        private static ReaderHandle openGlobalSnapshotReader(
                ServerConfig config, CheckpointEntry checkpoint, OperatorEntry operator) {
            List<File> temporaryDirectories = new ArrayList<>();
            Reader bootstrapReader = null;
            try {
                File unifiedVolume =
                        java.nio.file.Files.createTempDirectory(
                                        "cobble-flink-monitor-"
                                                + checkpoint.id
                                                + "-"
                                                + safeFileName(operator.operatorId)
                                                + "-")
                                .toFile();
                temporaryDirectories.add(unifiedVolume);
                copyGlobalManifest(operator, checkpoint.id, unifiedVolume);

                Config bootstrapConfig = CobbleReaderConfigs.base(config.totalBuckets);
                CobbleReaderConfigs.addVolume(
                        bootstrapConfig,
                        pathToCobbleConfigString(unifiedVolume),
                        config.flinkConfiguration);
                bootstrapReader = Reader.open(bootstrapConfig, checkpoint.id);
                GlobalSnapshot snapshot = bootstrapReader.currentGlobalSnapshot();
                int totalBuckets =
                        snapshot == null || snapshot.totalBuckets <= 0
                                ? config.totalBuckets
                                : snapshot.totalBuckets;

                Map<String, String> shardVolumeDirectories = new LinkedHashMap<>();
                if (snapshot != null && snapshot.shardSnapshots != null) {
                    for (ShardSnapshot shardSnapshot : snapshot.shardSnapshots) {
                        String shardVolumeDirectory =
                                copyShardMetadata(shardSnapshot, unifiedVolume);
                        if (shardVolumeDirectory != null) {
                            shardVolumeDirectories.putIfAbsent(
                                    shardVolumeDirectory, shardVolumeDirectory);
                        }
                    }
                }
                bootstrapReader.close();
                bootstrapReader = null;

                Config cobbleConfig = CobbleReaderConfigs.base(totalBuckets);
                CobbleReaderConfigs.addVolume(
                        cobbleConfig,
                        pathToCobbleConfigString(unifiedVolume),
                        config.flinkConfiguration);
                for (String shardVolumeDirectory : shardVolumeDirectories.values()) {
                    CobbleReaderConfigs.addVolume(
                            cobbleConfig, shardVolumeDirectory, config.flinkConfiguration);
                }
                for (String volumeDirectory : operator.readerVolumeDirectories) {
                    if (!volumeDirectory.equals(operator.operatorSnapshotDirectory)) {
                        CobbleReaderConfigs.addVolume(
                                cobbleConfig, volumeDirectory, config.flinkConfiguration);
                    }
                }
                return new ReaderHandle(
                        Reader.open(cobbleConfig, checkpoint.id), temporaryDirectories);
            } catch (IOException e) {
                if (bootstrapReader != null) {
                    bootstrapReader.close();
                }
                deleteTemporaryDirectories(temporaryDirectories);
                throw new InputException(
                        "Failed to prepare Cobble checkpoint metadata for operator "
                                + operator.operatorId
                                + ": "
                                + e.getMessage());
            } catch (RuntimeException e) {
                if (bootstrapReader != null) {
                    bootstrapReader.close();
                }
                deleteTemporaryDirectories(temporaryDirectories);
                throw e;
            }
        }

        synchronized Map<String, Object> meta() {
            refreshIfNeeded();
            GlobalSnapshot current = reader == null ? null : reader.currentGlobalSnapshot();
            Map<String, Object> output = new LinkedHashMap<>();
            output.put("bind_addr", config.bindAddress + ":" + config.port);
            output.put("source_open", reader != null);
            output.put("source_kind", catalog == null ? null : catalog.sourceKind);
            output.put("source_path", catalog == null ? null : catalog.rootDirectory);
            output.put("checkpoint_root", catalog == null ? null : catalog.rootDirectory);
            output.put(
                    "selected_checkpoint",
                    selectedCheckpoint == null
                            ? null
                            : selectedLatest ? "latest" : selectedCheckpoint.id);
            output.put(
                    "selected_checkpoint_id",
                    selectedCheckpoint == null ? null : selectedCheckpoint.id);
            output.put(
                    "selected_operator_id",
                    selectedOperator == null ? null : selectedOperator.operatorId);
            output.put(
                    "selected_checkpoint_directory",
                    selectedCheckpoint == null ? null : selectedCheckpoint.directory);
            output.put(
                    "selected_operator_snapshot_directory",
                    selectedOperator == null ? null : selectedOperator.operatorSnapshotDirectory);
            output.put("read_mode", reader == null ? null : reader.readMode());
            output.put(
                    "configured_snapshot_id",
                    reader == null ? null : reader.configuredSnapshotId());
            output.put("current_global_snapshot_id", current == null ? null : current.id);
            output.put("total_buckets", current == null ? null : current.totalBuckets);
            output.put(
                    "shard_snapshot_count",
                    current == null || current.shardSnapshots == null
                            ? 0
                            : current.shardSnapshots.size());
            output.put("inspect_kind", inspectKind(current, selectedSchema, selectedSinkSchema));
            output.put(
                    "inspect_targets", inspectTargets(current, selectedSchema, selectedSinkSchema));
            output.put("schema", selectedSchema == null ? null : selectedSchema.toJson());
            output.put(
                    "sink_schema", selectedSinkSchema == null ? null : selectedSinkSchema.toJson());
            output.put("catalog", catalog == null ? null : catalog.toJson());
            output.put("inspect_default_limit", config.inspectDefaultLimit);
            output.put("inspect_max_limit", config.inspectMaxLimit);
            output.put("uptime_millis", System.currentTimeMillis() - startedAtMillis);
            return output;
        }

        synchronized Map<String, Object> snapshots() {
            refreshCatalogForListing();
            Map<String, Object> output = new LinkedHashMap<>();
            output.put("source_open", reader != null);
            output.put("source_kind", catalog == null ? null : catalog.sourceKind);
            output.put("source_path", catalog == null ? null : catalog.rootDirectory);
            output.put(
                    "selected_checkpoint",
                    selectedCheckpoint == null
                            ? null
                            : selectedLatest ? "latest" : selectedCheckpoint.id);
            output.put(
                    "selected_checkpoint_id",
                    selectedCheckpoint == null ? null : selectedCheckpoint.id);
            output.put(
                    "selected_operator_id",
                    selectedOperator == null ? null : selectedOperator.operatorId);
            output.put(
                    "snapshots",
                    catalog == null ? Collections.emptyList() : catalog.checkpointsToJson());
            return output;
        }

        synchronized Map<String, Object> switchMode(JsonObject body) {
            String source =
                    blankToNull(
                            stringField(
                                    body, "source", stringField(body, "checkpoint_root", null)));
            CheckpointCatalog nextCatalog = catalog;
            if (source != null) {
                config.checkpointRoot = source;
                nextCatalog = CheckpointCatalog.discover(source);
            }
            if (nextCatalog == null) {
                throw new InputException("open a checkpoint root or Cobble data source first");
            }
            String checkpointSelection =
                    blankToNull(
                            stringField(
                                    body, "checkpoint", stringField(body, "checkpoint_id", null)));
            Long checkpointId =
                    checkpointSelection == null || "latest".equalsIgnoreCase(checkpointSelection)
                            ? null
                            : parseLong(checkpointSelection, "checkpoint_id");
            String operatorId = blankToNull(stringField(body, "operator_id", null));
            catalog = nextCatalog.refresh();
            CheckpointEntry checkpoint;
            OperatorEntry operator;
            ReaderHandle readerHandle;
            if (checkpointId == null) {
                ReaderSelection selection = openLatestReadable(config, catalog, operatorId);
                checkpoint = selection.checkpoint;
                operator = selection.operator;
                readerHandle = selection.readerHandle;
            } else {
                checkpoint = catalog.findCheckpoint(checkpointId);
                operator =
                        operatorId == null
                                ? checkpoint.defaultOperator()
                                : checkpoint.findOperator(operatorId);
                readerHandle = openReader(config, checkpoint, operator);
            }
            replaceReader(readerHandle);
            selectedCheckpoint = checkpoint;
            selectedOperator = operator;
            selectedLatest = checkpointId == null;
            selectedSchema = resolveSchema(checkpoint, operator);
            selectedSinkSchema = resolveSinkSchema(catalog, checkpoint);

            Map<String, Object> output = new LinkedHashMap<>();
            output.put("read_mode", reader.readMode());
            output.put("configured_snapshot_id", reader.configuredSnapshotId());
            output.put("source_kind", catalog.sourceKind);
            output.put("source_path", catalog.rootDirectory);
            output.put("selected_checkpoint", selectedLatest ? "latest" : selectedCheckpoint.id);
            output.put("selected_checkpoint_id", selectedCheckpoint.id);
            output.put("selected_operator_id", selectedOperator.operatorId);
            output.put("schema", selectedSchema.toJson());
            output.put("sink_schema", selectedSinkSchema.toJson());
            return output;
        }

        synchronized Map<String, Object> inspect(Map<String, List<String>> params) {
            ensureSourceOpen();
            refreshIfNeeded();
            String mode = first(params, "mode", "scan");
            if ("lookup".equals(mode)) {
                return inspectLookup(params);
            }
            if ("scan".equals(mode)) {
                return inspectScan(params);
            }
            throw new InputException("invalid mode: " + mode + " (expect lookup|scan)");
        }

        private Map<String, Object> inspectLookup(Map<String, List<String>> params) {
            InspectTarget target = selectedInspectTarget(params);
            int[] columns =
                    target.allowsColumns ? parseColumns(first(params, "columns", null)) : null;
            List<LookupItem> lookupItems = parseLookupItems(params);
            if (lookupItems.isEmpty()) {
                throw new InputException(
                        "lookup mode requires non-empty `keys`, `keys_b64`, or `lookup_items`");
            }

            List<Map<String, Object>> items = new ArrayList<>(lookupItems.size());
            try (ReadOptions options = readOptions(target.columnFamily, columns)) {
                for (LookupItem item : lookupItems) {
                    byte[][] columnsValue = reader.getWithOptions(item.bucket, item.key, options);
                    Map<String, Object> output = new LinkedHashMap<>();
                    output.put("bucket", item.bucket);
                    output.put("key_b64", b64(item.key));
                    output.put("key_utf8", utf8(item.key));
                    output.put(
                            "value",
                            columnsValue == null
                                    ? null
                                    : valueToJson(columnsValue, target.allowsColumns));
                    decorateDecodedRow(output, target, item.key, columnsValue, columns);
                    items.add(output);
                }
            }

            Map<String, Object> output = new LinkedHashMap<>();
            output.put("mode", "lookup");
            output.put("read_mode", reader.readMode());
            output.put("configured_snapshot_id", reader.configuredSnapshotId());
            output.put("selected_checkpoint", selectedLatest ? "latest" : selectedCheckpoint.id);
            output.put("selected_checkpoint_id", selectedCheckpoint.id);
            output.put("selected_operator_id", selectedOperator.operatorId);
            output.put("inspect_target", target.toJson());
            output.put("schema", selectedSchema == null ? null : selectedSchema.toJson());
            output.put(
                    "sink_schema", selectedSinkSchema == null ? null : selectedSinkSchema.toJson());
            output.put("lookup", items);
            return output;
        }

        private Map<String, Object> inspectScan(Map<String, List<String>> params) {
            Integer bucket = optionalBucketParam(params);
            InspectTarget target = selectedInspectTarget(params);
            int[] columns =
                    target.allowsColumns ? parseColumns(first(params, "columns", null)) : null;
            byte[] prefix =
                    decodeOptionalBytes(
                            first(params, "prefix", null), first(params, "prefix_b64", null));
            SinkKeyRowFilter sinkKeyFilter = sinkKeyFilter(params, target);
            if (sinkKeyFilter != null) {
                if (prefix != null && prefix.length > 0) {
                    throw new InputException("`prefix` cannot be combined with `sink_key`");
                }
                prefix = sinkKeyFilter.prefix;
            }
            TimerRowFilter timerFilter = timerRowFilter(params, target);
            if (timerFilter != null) {
                prefix = null;
            }
            StateRowFilter stateFilter =
                    stateRowFilter(
                            params,
                            target,
                            "true".equalsIgnoreCase(first(params, "auto_key_group", null)),
                            "true".equalsIgnoreCase(first(params, "key_group_last_complete", null)),
                            currentTotalBuckets());
            if (bucket == null && stateFilter != null && stateFilter.keyGroup != null) {
                bucket = stateFilter.keyGroup;
            }
            if (prefix != null && prefix.length > 0 && stateFilter != null) {
                throw new InputException("`prefix` cannot be combined with state key filters");
            }
            if (stateFilter != null) {
                prefix = stateFilter.prefix;
            }
            if (prefix == null) {
                prefix = new byte[0];
            }
            byte[] startAfter =
                    decodeOptionalBytes(
                            first(params, "start_after", null),
                            first(params, "start_after_b64", null));
            if (startAfter != null && prefix.length > 0 && !startsWith(startAfter, prefix)) {
                throw new InputException("`start_after` must share the same prefix");
            }
            int limit =
                    Math.min(
                            intParam(params, "limit", config.inspectDefaultLimit),
                            config.inspectMaxLimit);
            if (limit <= 0) {
                throw new InputException("`limit` must be greater than 0");
            }

            byte[] start = startAfter == null && prefix.length == 0 ? EMPTY_SCAN_KEY : startAfter;
            if (start == null && prefix.length > 0) {
                start = prefix;
            }
            byte[] end = prefix.length == 0 ? MAX_SCAN_KEY : prefixUpperBound(prefix);
            List<Map<String, Object>> items = new ArrayList<>();
            int maxItems = limit + 1;
            if (bucket == null) {
                int totalBuckets = currentTotalBuckets();
                int startBucket = intParam(params, "start_bucket", 0);
                if (startBucket >= totalBuckets) {
                    throw new InputException(
                            "`start_bucket` must be less than total buckets " + totalBuckets);
                }
                for (int currentBucket = 0;
                        currentBucket < totalBuckets && items.size() < maxItems;
                        currentBucket++) {
                    if (currentBucket < startBucket) {
                        continue;
                    }
                    byte[] bucketStart =
                            currentBucket == startBucket
                                    ? start
                                    : prefix.length == 0 ? EMPTY_SCAN_KEY : prefix;
                    byte[] bucketStartAfter = currentBucket == startBucket ? startAfter : null;
                    collectScanItems(
                            currentBucket,
                            bucketStart,
                            end,
                            bucketStartAfter,
                            target,
                            columns,
                            stateFilter,
                            sinkKeyFilter,
                            timerFilter,
                            maxItems,
                            items);
                }
            } else {
                collectScanItems(
                        bucket,
                        start,
                        end,
                        startAfter,
                        target,
                        columns,
                        stateFilter,
                        sinkKeyFilter,
                        timerFilter,
                        maxItems,
                        items);
            }
            boolean hasMore = items.size() > limit;
            if (hasMore) {
                items.remove(items.size() - 1);
            }

            Map<String, Object> scan = new LinkedHashMap<>();
            scan.put("bucket", bucket == null ? "all" : bucket);
            scan.put("prefix_b64", b64(prefix));
            scan.put("state_filter", stateFilterToJson(params, target));
            scan.put("start_after_b64", startAfter == null ? null : b64(startAfter));
            scan.put("limit", limit);
            scan.put("items", items);
            if (hasMore && !items.isEmpty()) {
                Map<String, Object> lastItem = items.get(items.size() - 1);
                scan.put("next_start_bucket", lastItem.get("bucket"));
                scan.put("next_start_after_b64", lastItem.get("key_b64"));
            } else {
                scan.put("next_start_bucket", null);
                scan.put("next_start_after_b64", null);
            }

            Map<String, Object> output = new LinkedHashMap<>();
            output.put("mode", "scan");
            output.put("read_mode", reader.readMode());
            output.put("configured_snapshot_id", reader.configuredSnapshotId());
            output.put("selected_checkpoint", selectedLatest ? "latest" : selectedCheckpoint.id);
            output.put("selected_checkpoint_id", selectedCheckpoint.id);
            output.put("selected_operator_id", selectedOperator.operatorId);
            output.put("inspect_target", target.toJson());
            output.put("schema", selectedSchema == null ? null : selectedSchema.toJson());
            output.put(
                    "sink_schema", selectedSinkSchema == null ? null : selectedSinkSchema.toJson());
            output.put("scan", scan);
            return output;
        }

        private void collectScanItems(
                int bucket,
                byte[] start,
                byte[] end,
                byte[] startAfter,
                InspectTarget target,
                int[] columns,
                StateRowFilter stateFilter,
                SinkKeyRowFilter sinkKeyFilter,
                TimerRowFilter timerFilter,
                int limit,
                List<Map<String, Object>> items) {
            boolean skippedStartAfter = startAfter == null;
            int rawLimit =
                    timerFilter == null && !hasPostScanFilter(stateFilter, sinkKeyFilter)
                            ? limit + 1
                            : Math.max(limit + 1, config.inspectMaxLimit);
            try (ScanOptions options = scanOptions(target.columnFamily, columns, rawLimit);
                    ScanCursor cursor = reader.scanWithOptions(bucket, start, end, options)) {
                ScanCursor.Entry entry = cursor.nextEntry();
                while (entry != null && items.size() < limit) {
                    if (!skippedStartAfter) {
                        if (entry.bucket == bucket && bytesEqual(entry.key, startAfter)) {
                            entry = cursor.nextEntry();
                            skippedStartAfter = true;
                            continue;
                        }
                        skippedStartAfter = true;
                    }
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("bucket", entry.bucket);
                    item.put("key_b64", b64(entry.key));
                    item.put("key_utf8", utf8(entry.key));
                    if (target.allowsColumns) {
                        item.put("columns", columnsToJson(entry.columns));
                    } else {
                        item.put("value", firstColumnToJson(entry.columns));
                    }
                    decorateDecodedRow(item, target, entry.key, entry.columns, columns);
                    if (stateFilter != null && !stateFilter.matches(item, target, entry.key)) {
                        entry = cursor.nextEntry();
                        continue;
                    }
                    if (sinkKeyFilter != null && !sinkKeyFilter.matches(item)) {
                        entry = cursor.nextEntry();
                        continue;
                    }
                    if (timerFilter != null && !timerFilter.matches(item)) {
                        entry = cursor.nextEntry();
                        continue;
                    }
                    items.add(item);
                    entry = cursor.nextEntry();
                }
            }
        }

        private static boolean hasPostScanFilter(
                StateRowFilter stateFilter, SinkKeyRowFilter sinkKeyFilter) {
            return sinkKeyFilter != null
                    || (stateFilter != null
                            && (stateFilter.mapKeyPrefix != null
                                    || stateFilter.mapKeyBytesPrefix != null
                                    || stateFilter.hasSemanticPartFilters()));
        }

        private int currentTotalBuckets() {
            GlobalSnapshot snapshot = reader.currentGlobalSnapshot();
            if (snapshot != null && snapshot.totalBuckets > 0) {
                return snapshot.totalBuckets;
            }
            return config.totalBuckets;
        }

        private InspectTarget selectedInspectTarget(Map<String, List<String>> params) {
            GlobalSnapshot snapshot = reader.currentGlobalSnapshot();
            List<InspectTarget> targets =
                    StateInspectTargetBuilder.build(snapshot, selectedSchema, selectedSinkSchema);
            String requested = first(params, "target", first(params, "state", null));
            if (requested == null || requested.trim().isEmpty()) {
                return targets.isEmpty() ? InspectTarget.sink("sink") : targets.get(0);
            }
            for (InspectTarget target : targets) {
                if (target.id.equals(requested)
                        || target.name.equals(requested)
                        || (target.columnFamily != null && target.columnFamily.equals(requested))) {
                    return target;
                }
            }
            throw new InputException("unknown inspect target: " + requested);
        }

        private void refreshIfNeeded() {
            if (!selectedLatest || catalog == null) {
                return;
            }
            CheckpointCatalog refreshed = catalog.refresh();
            CheckpointEntry latest = refreshed.defaultCheckpoint();
            OperatorEntry operator = latest.findOperatorOrDefault(selectedOperator.operatorId);
            catalog = refreshed;
            if (latest.id == selectedCheckpoint.id
                    && operator.operatorId.equals(selectedOperator.operatorId)) {
                return;
            }

            ReaderSelection selection =
                    openLatestReadableOrCurrent(refreshed, selectedOperator.operatorId);
            if (selection.readerHandle != null) {
                replaceReader(selection.readerHandle);
            }
            selectedCheckpoint = selection.checkpoint;
            selectedOperator = selection.operator;
            selectedSchema = resolveSchema(selection.checkpoint, selection.operator);
            selectedSinkSchema = resolveSinkSchema(catalog, selection.checkpoint);
        }

        private ReaderSelection openLatestReadableOrCurrent(
                CheckpointCatalog refreshed, String preferredOperatorId) {
            RuntimeException firstFailure = null;
            for (CheckpointEntry checkpoint : refreshed.checkpoints) {
                OperatorEntry operator = checkpoint.findOperatorOrDefault(preferredOperatorId);
                if (selectedCheckpoint != null
                        && selectedOperator != null
                        && checkpoint.id == selectedCheckpoint.id
                        && operator.operatorId.equals(selectedOperator.operatorId)
                        && reader != null) {
                    return new ReaderSelection(checkpoint, operator, null);
                }
                try {
                    return new ReaderSelection(
                            checkpoint, operator, openReader(config, checkpoint, operator));
                } catch (RuntimeException e) {
                    if (firstFailure == null) {
                        firstFailure = e;
                    }
                }
            }
            if (reader != null && selectedCheckpoint != null && selectedOperator != null) {
                return new ReaderSelection(selectedCheckpoint, selectedOperator, null);
            }
            if (firstFailure != null) {
                throw firstFailure;
            }
            throw new InputException(
                    "No readable checkpoints found under " + refreshed.rootDirectory);
        }

        private void refreshCatalogForListing() {
            if (catalog == null) {
                return;
            }
            if (selectedLatest) {
                refreshIfNeeded();
            } else {
                catalog = catalog.refresh();
            }
        }

        private void replaceReader(ReaderHandle next) {
            Reader previous = this.reader;
            List<File> previousTemporaryDirectories = this.readerTemporaryDirectories;
            this.reader = next.reader;
            this.readerTemporaryDirectories = next.temporaryDirectories;
            if (previous != null) {
                previous.close();
            }
            deleteTemporaryDirectories(previousTemporaryDirectories);
        }

        private void ensureSourceOpen() {
            if (reader == null
                    || catalog == null
                    || selectedCheckpoint == null
                    || selectedOperator == null) {
                throw new InputException("open a checkpoint root or Cobble data source first");
            }
        }

        private static void decorateDecodedRow(
                Map<String, Object> item,
                InspectTarget target,
                byte[] key,
                byte[][] columns,
                int[] projection) {
            if (target != null && target.sinkSchema != null) {
                SinkInspectDecoder.DecodedRow decoded =
                        SinkInspectDecoder.decode(target, key, columns, projection);
                if (!decoded.hasOutput()) {
                    return;
                }
                if (decoded.decodedKey != null) {
                    item.put("decoded_key", decoded.decodedKey);
                }
                if (decoded.decodedColumns != null) {
                    item.put("decoded_columns", decoded.decodedColumns);
                }
                if (decoded.decodeError != null) {
                    item.put("decode_error", decoded.decodeError);
                }
                return;
            }
            StateInspectDecoder.DecodedRow decoded =
                    StateInspectDecoder.decode(target, key, columns);
            if (!decoded.hasOutput()) {
                return;
            }
            if (decoded.decodedKey != null) {
                item.put("decoded_key", decoded.decodedKey);
            }
            if (decoded.decodedValue != null) {
                item.put("decoded_value", decoded.decodedValue);
            }
            if (decoded.decodedParts != null) {
                item.put("decoded_parts", decoded.decodedParts);
            }
            if (decoded.decodeError != null) {
                item.put("decode_error", decoded.decodeError);
            }
        }

        private static StateRowFilter stateRowFilter(
                Map<String, List<String>> params,
                InspectTarget target,
                boolean autoKeyGroup,
                boolean lastKeyComplete,
                int totalKeyGroups) {
            String key = blankToNull(first(params, "state_key", null));
            String namespace = blankToNull(first(params, "namespace", null));
            String mapKey = blankToNull(first(params, "map_key", null));
            byte[] keyBytes =
                    decodeOptionalB64(first(params, "state_key_b64", null), "state_key_b64");
            byte[] namespaceBytes =
                    decodeOptionalB64(first(params, "namespace_b64", null), "namespace_b64");
            byte[] mapKeyBytes =
                    decodeOptionalB64(first(params, "map_key_b64", null), "map_key_b64");
            SemanticPartFilter stateKeyFields =
                    semanticPartFilter(params, "state_key_field", "state key", target, "state_key");
            SemanticPartFilter namespaceFields =
                    semanticPartFilter(params, "namespace_field", "namespace", target, "namespace");
            SemanticPartFilter mapKeyFields =
                    semanticPartFilter(params, "map_key_field", "map key", target, "map_key");
            boolean hasKey = key != null || keyBytes != null;
            boolean hasNamespace = namespace != null || namespaceBytes != null;
            boolean hasMapKey = mapKey != null || mapKeyBytes != null;
            boolean hasRawFilter = hasKey || hasNamespace || hasMapKey;
            boolean hasSemanticFilter =
                    stateKeyFields != null || namespaceFields != null || mapKeyFields != null;
            if (hasRawFilter && hasSemanticFilter) {
                throw new InputException(
                        "Raw state key filters cannot be combined with field table filters");
            }
            if (key == null
                    && namespace == null
                    && mapKey == null
                    && keyBytes == null
                    && namespaceBytes == null
                    && mapKeyBytes == null
                    && !hasSemanticFilter) {
                if (autoKeyGroup && lastKeyComplete) {
                    throw new InputException("State key is required to calculate a Key Group");
                }
                return null;
            }
            if (hasSemanticFilter) {
                if ((namespaceFields != null || mapKeyFields != null) && stateKeyFields == null) {
                    throw new InputException(
                            "State key fields are required before namespace or map key fields");
                }
                if (!"MAP".equals(target.stateKind) && mapKeyFields != null) {
                    throw new InputException("Map key fields are only valid for MapState");
                }
                boolean stateKeyComplete =
                        stateKeyFields != null
                                && (namespaceFields != null
                                        || mapKeyFields != null
                                        || lastKeyComplete);
                if (stateKeyComplete && !completeSemanticPart(stateKeyFields, "state key fields")) {
                    throw new InputException(
                            "All state key fields are required before namespace or map key fields");
                }
                boolean namespaceComplete =
                        namespaceFields != null && (mapKeyFields != null || lastKeyComplete);
                if (namespaceComplete
                        && !completeSemanticPart(namespaceFields, "namespace fields")) {
                    throw new InputException(
                            "All namespace fields are required before map key fields");
                }
                if (autoKeyGroup && lastKeyComplete && stateKeyFields == null) {
                    throw new InputException(
                            "State key fields are required to calculate a Key Group");
                }
                Integer keyGroup =
                        autoKeyGroup && stateKeyComplete
                                ? keyGroupForSemanticStateKey(
                                        target, stateKeyFields.values, totalKeyGroups)
                                : null;
                return new StateRowFilter(
                        null,
                        null,
                        null,
                        stateKeyFields == null
                                ? null
                                : stateKeyFields.withLastFieldExact(stateKeyComplete),
                        namespaceFields == null
                                ? null
                                : namespaceFields.withLastFieldExact(namespaceComplete),
                        mapKeyFields == null
                                ? null
                                : mapKeyFields.withLastFieldExact(lastKeyComplete),
                        keyGroup);
            }
            if (!"MAP".equals(target.stateKind) && (mapKey != null || mapKeyBytes != null)) {
                throw new InputException("Map key filter is only valid for MapState");
            }
            try {
                byte[] prefix =
                        StateInspectDecoder.encodeStateKeyPrefix(
                                target, key, keyBytes, namespace, namespaceBytes, null, null);
                boolean stateKeyComplete = hasKey && (hasNamespace || hasMapKey || lastKeyComplete);
                if (autoKeyGroup && lastKeyComplete && !hasKey) {
                    throw new InputException("State key is required to calculate a Key Group");
                }
                Integer keyGroup =
                        autoKeyGroup && stateKeyComplete
                                ? StateInspectDecoder.keyGroupForRawStateKey(
                                        target, key, keyBytes, totalKeyGroups)
                                : null;
                return new StateRowFilter(prefix, mapKey, mapKeyBytes, null, null, null, keyGroup);
            } catch (IOException e) {
                throw new InputException(e.getMessage());
            }
        }

        private static boolean completeSemanticPart(SemanticPartFilter filter, String label) {
            try {
                return StateInspectDecoder.hasCompleteSemanticPartFilter(
                        filter.type, filter.values, label);
            } catch (IOException e) {
                throw new InputException(e.getMessage());
            }
        }

        private static int keyGroupForSemanticStateKey(
                InspectTarget target, List<String> values, int totalKeyGroups) {
            try {
                return StateInspectDecoder.keyGroupForSemanticStateKey(
                        target, values, totalKeyGroups);
            } catch (IOException e) {
                throw new InputException(e.getMessage());
            }
        }

        private static SemanticPartFilter semanticPartFilter(
                Map<String, List<String>> params,
                String parameter,
                String label,
                InspectTarget target,
                String partName) {
            List<String> values = params.get(parameter);
            if (values == null || values.isEmpty()) {
                return null;
            }
            StateInspectType type = semanticPartType(target, partName);
            try {
                StateInspectDecoder.validateSemanticPartFilter(type, values, label);
                return new SemanticPartFilter(partName, type, values, false);
            } catch (IOException e) {
                throw new InputException(e.getMessage());
            }
        }

        private static StateInspectType semanticPartType(InspectTarget target, String partName) {
            StateInspectSemanticSchema schema = target == null ? null : target.semanticSchema;
            if (schema == null) {
                return null;
            }
            switch (partName) {
                case "state_key":
                    return schema.stateKey();
                case "namespace":
                    return schema.namespace();
                case "map_key":
                    return schema.mapUserKey();
                default:
                    return null;
            }
        }

        private static SinkKeyRowFilter sinkKeyFilter(
                Map<String, List<String>> params, InspectTarget target) {
            if (target == null || target.sinkSchema == null) {
                return null;
            }
            List<String> rawValues = params.get("sink_key");
            if (rawValues == null || rawValues.isEmpty()) {
                return null;
            }
            List<String> values = new ArrayList<>();
            boolean foundGap = false;
            for (String value : rawValues) {
                if (value == null || value.isEmpty()) {
                    foundGap = true;
                    continue;
                }
                if (foundGap) {
                    throw new InputException(
                            "Sink key fields must be supplied in order without gaps");
                }
                values.add(value);
            }
            if (values.isEmpty()) {
                return null;
            }
            if (values.size() > target.sinkSchema.keyFields().size()) {
                throw new InputException(
                        "Too many sink key values (expected at most "
                                + target.sinkSchema.keyFields().size()
                                + ")");
            }
            try {
                int lastIndex = values.size() - 1;
                byte[] prefix =
                        SinkInspectDecoder.encodeKeyPrefix(target, values.subList(0, lastIndex));
                return new SinkKeyRowFilter(
                        prefix,
                        target.sinkSchema.keyFields().get(lastIndex).name(),
                        values.get(lastIndex));
            } catch (IOException e) {
                throw new InputException(e.getMessage());
            }
        }

        private static TimerRowFilter timerRowFilter(
                Map<String, List<String>> params, InspectTarget target) {
            if (target == null || !"timer".equals(target.kind)) {
                return null;
            }
            String prefix = blankToNull(first(params, "prefix", null));
            String prefixB64 = blankToNull(first(params, "prefix_b64", null));
            if (prefixB64 != null) {
                throw new InputException("`prefix_b64` is not supported for decoded timer filters");
            }
            return prefix == null ? null : new TimerRowFilter(prefix);
        }

        private static Map<String, Object> stateFilterToJson(
                Map<String, List<String>> params, InspectTarget target) {
            Map<String, Object> output = new LinkedHashMap<>();
            String key = blankToNull(first(params, "state_key", null));
            String namespace = blankToNull(first(params, "namespace", null));
            String mapKey = blankToNull(first(params, "map_key", null));
            String keyB64 = blankToNull(first(params, "state_key_b64", null));
            String namespaceB64 = blankToNull(first(params, "namespace_b64", null));
            String mapKeyB64 = blankToNull(first(params, "map_key_b64", null));
            List<String> stateKeyFields = params.get("state_key_field");
            List<String> namespaceFields = params.get("namespace_field");
            List<String> mapKeyFields = params.get("map_key_field");
            if (key == null
                    && namespace == null
                    && mapKey == null
                    && keyB64 == null
                    && namespaceB64 == null
                    && mapKeyB64 == null
                    && (stateKeyFields == null || stateKeyFields.isEmpty())
                    && (namespaceFields == null || namespaceFields.isEmpty())
                    && (mapKeyFields == null || mapKeyFields.isEmpty())) {
                return output;
            }
            output.put("state_kind", target.stateKind);
            putIfPresent(output, "state_key", key);
            putIfPresent(output, "namespace", namespace);
            putIfPresent(output, "map_key", mapKey);
            putIfPresent(output, "state_key_b64", keyB64);
            putIfPresent(output, "namespace_b64", namespaceB64);
            putIfPresent(output, "map_key_b64", mapKeyB64);
            if (stateKeyFields != null && !stateKeyFields.isEmpty()) {
                output.put("state_key_field", stateKeyFields);
            }
            if (namespaceFields != null && !namespaceFields.isEmpty()) {
                output.put("namespace_field", namespaceFields);
            }
            if (mapKeyFields != null && !mapKeyFields.isEmpty()) {
                output.put("map_key_field", mapKeyFields);
            }
            return output;
        }

        @Override
        public synchronized void close() {
            if (reader != null) {
                reader.close();
                reader = null;
            }
            deleteTemporaryDirectories(readerTemporaryDirectories);
            readerTemporaryDirectories = Collections.emptyList();
        }

        private static SchemaResolveResult resolveSchema(
                CheckpointEntry checkpoint, OperatorEntry operator) {
            try {
                return MonitorInspectSchemaResolver.resolve(checkpoint, operator);
            } catch (Exception e) {
                return SchemaResolveResult.unavailable(
                        "Failed to resolve schema registry: " + e.getMessage());
            }
        }

        private static SinkSchemaResolveResult resolveSinkSchema(
                CheckpointCatalog catalog, CheckpointEntry checkpoint) {
            if (catalog == null || checkpoint == null) {
                return SinkSchemaResolveResult.unsupported("No data source is selected.");
            }
            if (!"data_source".equals(catalog.sourceKind)) {
                return SinkSchemaResolveResult.unsupported(
                        "Sink schema registry resolution is only available for data sources.");
            }
            try {
                return SinkInspectSchemaResolver.resolve(catalog.rootDirectory, checkpoint.id);
            } catch (Exception e) {
                return SinkSchemaResolveResult.unavailable(
                        "Failed to resolve sink schema registry: " + e.getMessage());
            }
        }

        private static final class ReaderSelection {
            private final CheckpointEntry checkpoint;
            private final OperatorEntry operator;
            private final ReaderHandle readerHandle;

            private ReaderSelection(
                    CheckpointEntry checkpoint, OperatorEntry operator, ReaderHandle readerHandle) {
                this.checkpoint = checkpoint;
                this.operator = operator;
                this.readerHandle = readerHandle;
            }
        }

        private static final class StateRowFilter {
            private final byte[] prefix;
            private final String mapKeyPrefix;
            private final byte[] mapKeyBytesPrefix;
            private final SemanticPartFilter stateKeyFields;
            private final SemanticPartFilter namespaceFields;
            private final SemanticPartFilter mapKeyFields;
            private final Integer keyGroup;

            private StateRowFilter(
                    byte[] prefix,
                    String mapKeyPrefix,
                    byte[] mapKeyBytesPrefix,
                    SemanticPartFilter stateKeyFields,
                    SemanticPartFilter namespaceFields,
                    SemanticPartFilter mapKeyFields,
                    Integer keyGroup) {
                this.prefix = prefix == null ? new byte[0] : prefix;
                this.mapKeyPrefix = mapKeyPrefix;
                this.mapKeyBytesPrefix = mapKeyBytesPrefix;
                this.stateKeyFields = stateKeyFields;
                this.namespaceFields = namespaceFields;
                this.mapKeyFields = mapKeyFields;
                this.keyGroup = keyGroup;
            }

            private boolean matches(Map<String, Object> item, InspectTarget target, byte[] rowKey) {
                if (mapKeyPrefix != null && !decodedMapKeyStartsWith(item, mapKeyPrefix)) {
                    return false;
                }
                if (mapKeyBytesPrefix != null) {
                    try {
                        if (!StateInspectDecoder.mapKeyBytesStartsWith(
                                target, rowKey, mapKeyBytesPrefix)) {
                            return false;
                        }
                    } catch (IOException e) {
                        return false;
                    }
                }
                if (!hasSemanticPartFilters()) {
                    return true;
                }
                Object decoded = item.get("decoded_parts");
                if (!(decoded instanceof Map)) {
                    return false;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> decodedParts = (Map<String, Object>) decoded;
                return matches(decodedParts, stateKeyFields)
                        && matches(decodedParts, namespaceFields)
                        && matches(decodedParts, mapKeyFields);
            }

            private boolean hasSemanticPartFilters() {
                return stateKeyFields != null || namespaceFields != null || mapKeyFields != null;
            }

            private static boolean matches(
                    Map<String, Object> decodedParts, SemanticPartFilter filter) {
                return filter == null
                        || StateInspectDecoder.matchesSemanticPartFilter(
                                decodedParts,
                                filter.partName,
                                filter.type,
                                filter.values,
                                filter.lastFieldExact);
            }

            @SuppressWarnings("unchecked")
            private static boolean decodedMapKeyStartsWith(
                    Map<String, Object> item, String mapKeyPrefix) {
                Object decoded = item.get("decoded_key");
                if (!(decoded instanceof Map)) {
                    return false;
                }
                Object value = ((Map<String, Object>) decoded).get("map_key");
                if (value == null) {
                    return false;
                }
                if (value instanceof Map) {
                    Object utf8 = ((Map<String, Object>) value).get("utf8");
                    if (utf8 != null && String.valueOf(utf8).startsWith(mapKeyPrefix)) {
                        return true;
                    }
                    Object b64 = ((Map<String, Object>) value).get("b64");
                    return b64 != null && String.valueOf(b64).startsWith(mapKeyPrefix);
                }
                return String.valueOf(value).startsWith(mapKeyPrefix);
            }
        }

        private static final class SemanticPartFilter {
            private final String partName;
            private final StateInspectType type;
            private final List<String> values;
            private final boolean lastFieldExact;

            private SemanticPartFilter(
                    String partName,
                    StateInspectType type,
                    List<String> values,
                    boolean lastFieldExact) {
                this.partName = partName;
                this.type = type;
                this.values = Collections.unmodifiableList(new ArrayList<>(values));
                this.lastFieldExact = lastFieldExact;
            }

            private SemanticPartFilter withLastFieldExact(boolean exact) {
                return exact == lastFieldExact
                        ? this
                        : new SemanticPartFilter(partName, type, values, exact);
            }
        }

        private static final class SinkKeyRowFilter {
            private final byte[] prefix;
            private final String fieldName;
            private final String valuePrefix;

            private SinkKeyRowFilter(byte[] prefix, String fieldName, String valuePrefix) {
                this.prefix = prefix == null ? new byte[0] : prefix;
                this.fieldName = fieldName;
                this.valuePrefix = valuePrefix;
            }

            @SuppressWarnings("unchecked")
            private boolean matches(Map<String, Object> item) {
                Object decoded = item.get("decoded_key");
                if (!(decoded instanceof List)) {
                    return false;
                }
                for (Object field : (List<?>) decoded) {
                    if (!(field instanceof Map)) {
                        continue;
                    }
                    Map<String, Object> decodedField = (Map<String, Object>) field;
                    if (!fieldName.equals(decodedField.get("name"))) {
                        continue;
                    }
                    return displayValueStartsWith(decodedField.get("value"), valuePrefix);
                }
                return false;
            }

            @SuppressWarnings("unchecked")
            private static boolean displayValueStartsWith(Object value, String prefix) {
                if (value == null) {
                    return false;
                }
                if (value instanceof Map) {
                    Map<String, Object> bytes = (Map<String, Object>) value;
                    Object utf8 = bytes.get("utf8");
                    if (utf8 != null) {
                        return String.valueOf(utf8).startsWith(prefix);
                    }
                    Object b64 = bytes.get("b64");
                    return b64 != null && String.valueOf(b64).startsWith(prefix);
                }
                return String.valueOf(value).startsWith(prefix);
            }
        }

        private static final class TimerRowFilter {
            private final String decodedPrefix;

            private TimerRowFilter(String decodedPrefix) {
                this.decodedPrefix = decodedPrefix;
            }

            @SuppressWarnings("unchecked")
            private boolean matches(Map<String, Object> item) {
                Object decoded = item.get("decoded_key");
                if (!(decoded instanceof Map)) {
                    return false;
                }
                Map<String, Object> decodedKey = (Map<String, Object>) decoded;
                return decodedValueStartsWith(decodedKey.get("key"), decodedPrefix)
                        || decodedValueStartsWith(decodedKey.get("timestamp"), decodedPrefix);
            }

            @SuppressWarnings("unchecked")
            private static boolean decodedValueStartsWith(Object value, String prefix) {
                if (value == null) {
                    return false;
                }
                if (value instanceof Map) {
                    Object utf8 = ((Map<String, Object>) value).get("utf8");
                    if (utf8 != null && String.valueOf(utf8).startsWith(prefix)) {
                        return true;
                    }
                    Object rawValue = ((Map<String, Object>) value).get("value");
                    if (rawValue != null && String.valueOf(rawValue).startsWith(prefix)) {
                        return true;
                    }
                    Object b64 = ((Map<String, Object>) value).get("b64");
                    return b64 != null && String.valueOf(b64).startsWith(prefix);
                }
                return String.valueOf(value).startsWith(prefix);
            }
        }
    }

    private static void copyGlobalManifest(
            OperatorEntry operator, long checkpointId, File unifiedVolume) throws IOException {
        java.nio.file.Path target =
                unifiedVolume.toPath().resolve("snapshot").resolve("SNAPSHOT-" + checkpointId);
        Path fallback = operatorSnapshotManifest(operator.operatorSnapshotDirectory, checkpointId);
        Path source =
                operator.manifestCopyPath == null ? fallback : new Path(operator.manifestCopyPath);
        try {
            copyFile(source, target);
        } catch (IOException primaryError) {
            if (pathToStorageString(source).equals(pathToStorageString(fallback))) {
                throw primaryError;
            }
            try {
                copyFile(fallback, target);
            } catch (IOException fallbackError) {
                throw new IOException(
                        primaryError.getMessage()
                                + "; fallback "
                                + fallback
                                + " also failed: "
                                + fallbackError.getMessage(),
                        primaryError);
            }
        }
    }

    private static Path operatorSnapshotManifest(
            String operatorSnapshotDirectory, long checkpointId) {
        return new Path(
                new Path(operatorSnapshotDirectory, "snapshot"), "SNAPSHOT-" + checkpointId);
    }

    private static String copyShardMetadata(ShardSnapshot shardSnapshot, File unifiedVolume)
            throws IOException {
        if (shardSnapshot.manifestPath == null || shardSnapshot.manifestPath.trim().isEmpty()) {
            return null;
        }
        Path manifest = new Path(shardSnapshot.manifestPath);
        Path snapshotDirectory = manifest.getParent();
        if (snapshotDirectory == null) {
            return null;
        }
        Path shardRoot = snapshotDirectory.getParent();
        if (shardRoot == null) {
            return null;
        }

        java.nio.file.Path localShardRoot = unifiedVolume.toPath().resolve(shardSnapshot.dbId);
        copyDirectoryIfExists(snapshotDirectory, localShardRoot.resolve("snapshot"));
        copyDirectoryIfExists(new Path(shardRoot, "schema"), localShardRoot.resolve("schema"));
        return pathToStorageString(shardRoot);
    }

    private static void copyDirectoryIfExists(
            Path sourceDirectory, java.nio.file.Path targetDirectory) throws IOException {
        FileSystem fileSystem = sourceDirectory.getFileSystem();
        if (!fileSystem.exists(sourceDirectory)) {
            return;
        }
        FileStatus status = fileSystem.getFileStatus(sourceDirectory);
        if (!status.isDir()) {
            return;
        }
        java.nio.file.Files.createDirectories(targetDirectory);
        FileStatus[] children = fileSystem.listStatus(sourceDirectory);
        if (children == null) {
            return;
        }
        for (FileStatus child : children) {
            java.nio.file.Path childTarget = targetDirectory.resolve(child.getPath().getName());
            if (child.isDir()) {
                copyDirectoryIfExists(child.getPath(), childTarget);
            } else {
                copyFile(child.getPath(), childTarget);
            }
        }
    }

    private static void copyFile(Path source, java.nio.file.Path target) throws IOException {
        java.nio.file.Files.createDirectories(target.getParent());
        try (InputStream input = source.getFileSystem().open(source)) {
            java.nio.file.Files.copy(
                    input, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static String pathToCobbleConfigString(File directory) {
        return directory.getAbsoluteFile().toPath().normalize().toString();
    }

    private static String safeFileName(String value) {
        StringBuilder builder = new StringBuilder(value.length());
        for (int index = 0; index < value.length(); index++) {
            char c = value.charAt(index);
            builder.append(Character.isLetterOrDigit(c) || c == '-' || c == '_' ? c : '-');
        }
        return builder.length() == 0 ? "operator" : builder.toString();
    }

    private static void deleteTemporaryDirectories(List<File> directories) {
        if (directories == null) {
            return;
        }
        for (File directory : directories) {
            deleteTemporaryDirectory(directory);
        }
    }

    private static void deleteTemporaryDirectory(File directory) {
        if (directory == null || !directory.exists()) {
            return;
        }
        try {
            java.nio.file.Files.walk(directory.toPath())
                    .sorted(Comparator.reverseOrder())
                    .forEach(CobbleFlinkMonitorServer::deletePathQuietly);
        } catch (IOException ignored) {
            // Best effort cleanup for per-reader metadata copies.
        }
    }

    private static void deletePathQuietly(java.nio.file.Path path) {
        try {
            java.nio.file.Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // Best effort cleanup for per-reader metadata copies.
        }
    }

    private static final class CheckpointCatalog {
        private final String rootDirectory;
        private final String sourceKind;
        private final List<CheckpointEntry> checkpoints;

        private CheckpointCatalog(
                String rootDirectory, String sourceKind, List<CheckpointEntry> checkpoints) {
            this.rootDirectory = rootDirectory;
            this.sourceKind = sourceKind;
            this.checkpoints = checkpoints;
        }

        private static CheckpointCatalog discover(String checkpointRoot) {
            try {
                return discoverCheckpointRoot(checkpointRoot);
            } catch (InputException checkpointError) {
                try {
                    return discoverDataSourceRoot(checkpointRoot);
                } catch (InputException dataSourceError) {
                    throw new InputException(
                            checkpointError.getMessage()
                                    + " Also failed to open it as a Cobble data source: "
                                    + dataSourceError.getMessage());
                }
            }
        }

        private CheckpointCatalog refresh() {
            return discover(rootDirectory);
        }

        private static CheckpointCatalog discoverCheckpointRoot(String checkpointRoot) {
            String normalizedRoot = normalizeStorageDirectory(checkpointRoot);
            Path requested = new Path(normalizedRoot);
            FileSystem fileSystem;
            FileStatus requestedStatus;
            try {
                fileSystem = requested.getFileSystem();
                requestedStatus = fileSystem.getFileStatus(requested);
            } catch (IOException e) {
                throw new InputException(
                        "Failed to open checkpoint path " + checkpointRoot + ": " + e.getMessage());
            }
            Path root =
                    checkpointDirectoryId(requested) == null ? requested : requested.getParent();
            if (root == null || !requestedStatus.isDir()) {
                throw new InputException(
                        "--checkpoint must point to a checkpoint directory: " + checkpointRoot);
            }

            List<Path> checkpointDirectories = discoverCheckpointDirectories(fileSystem, requested);
            Map<Long, Path> checkpointDirectoryById = new HashMap<>();
            Map<Long, CheckpointEntry> checkpointById = new LinkedHashMap<>();
            for (Path checkpointDirectory : checkpointDirectories) {
                Long checkpointId = checkpointDirectoryId(checkpointDirectory);
                if (checkpointId == null) {
                    continue;
                }
                checkpointDirectoryById.put(checkpointId, checkpointDirectory);
                List<OperatorEntry> operators =
                        discoverOperators(fileSystem, root, checkpointDirectory, checkpointId);
                if (!operators.isEmpty()) {
                    checkpointById.put(
                            checkpointId,
                            new CheckpointEntry(
                                    checkpointId,
                                    pathToStorageString(checkpointDirectory),
                                    operators));
                }
            }
            Map<Long, List<OperatorEntry>> globalSnapshotOperators =
                    discoverCobbleGlobalSnapshotOperators(fileSystem, root);
            for (Map.Entry<Long, List<OperatorEntry>> entry : globalSnapshotOperators.entrySet()) {
                Path checkpointDirectory = checkpointDirectoryById.get(entry.getKey());
                if (checkpointDirectory == null) {
                    continue;
                }
                if (checkpointById.containsKey(entry.getKey())) {
                    continue;
                }
                checkpointById.put(
                        entry.getKey(),
                        new CheckpointEntry(
                                entry.getKey(),
                                pathToStorageString(checkpointDirectory),
                                entry.getValue()));
            }
            Map<Long, List<OperatorEntry>> sharedOperators =
                    discoverSharedSnapshotOperators(fileSystem, root);
            for (Map.Entry<Long, List<OperatorEntry>> entry : sharedOperators.entrySet()) {
                Path checkpointDirectory = checkpointDirectoryById.get(entry.getKey());
                if (checkpointDirectory == null) {
                    continue;
                }
                if (checkpointById.containsKey(entry.getKey())) {
                    continue;
                }
                checkpointById.put(
                        entry.getKey(),
                        new CheckpointEntry(
                                entry.getKey(),
                                pathToStorageString(checkpointDirectory),
                                entry.getValue()));
            }
            List<CheckpointEntry> checkpoints = new ArrayList<>(checkpointById.values());
            checkpoints.sort(
                    Comparator.comparingLong((CheckpointEntry item) -> item.id).reversed());
            if (checkpoints.isEmpty()) {
                throw new InputException(
                        "No Cobble Flink checkpoints found under "
                                + root
                                + ". Expected chk-* with Cobble manifest copies or shared/op_*/"
                                + "<volume>/snapshot/SNAPSHOT-* files.");
            }
            return new CheckpointCatalog(root.toString(), "checkpoint", checkpoints);
        }

        private static CheckpointCatalog discoverDataSourceRoot(String sourceRoot) {
            String normalizedRoot = normalizeStorageDirectory(sourceRoot);
            Path root = new Path(normalizedRoot);
            FileSystem fileSystem;
            FileStatus rootStatus;
            try {
                fileSystem = root.getFileSystem();
                rootStatus = fileSystem.getFileStatus(root);
            } catch (IOException e) {
                throw new InputException(
                        "Failed to open data source path " + sourceRoot + ": " + e.getMessage());
            }
            if (!rootStatus.isDir()) {
                throw new InputException(
                        "--checkpoint/source must point to a directory: " + sourceRoot);
            }

            Path snapshotDirectory = new Path(root, "snapshot");
            FileStatus[] snapshotFiles;
            try {
                if (!fileSystem.exists(snapshotDirectory)
                        || !fileSystem.getFileStatus(snapshotDirectory).isDir()) {
                    throw new InputException(
                            "No Cobble snapshot directory found at " + snapshotDirectory);
                }
                snapshotFiles = fileSystem.listStatus(snapshotDirectory);
            } catch (IOException e) {
                throw new InputException(
                        "Failed to list Cobble data source snapshots "
                                + snapshotDirectory
                                + ": "
                                + e.getMessage());
            }

            String rootString = pathToStorageString(root);
            List<CheckpointEntry> snapshots = new ArrayList<>();
            if (snapshotFiles != null) {
                for (FileStatus snapshotStatus : snapshotFiles) {
                    if (snapshotStatus.isDir()) {
                        continue;
                    }
                    Long snapshotId = snapshotManifestId(snapshotStatus.getPath().getName());
                    if (snapshotId == null
                            || !snapshotManifestLooksGlobal(fileSystem, snapshotStatus.getPath())) {
                        continue;
                    }
                    OperatorEntry operator =
                            new OperatorEntry(
                                    "sink",
                                    pathToStorageString(snapshotStatus.getPath()),
                                    rootString,
                                    Collections.singletonList(rootString),
                                    true);
                    snapshots.add(
                            new CheckpointEntry(
                                    snapshotId, rootString, Collections.singletonList(operator)));
                }
            }
            snapshots.sort(Comparator.comparingLong((CheckpointEntry item) -> item.id).reversed());
            if (snapshots.isEmpty()) {
                throw new InputException(
                        "No Cobble data source snapshots found under " + snapshotDirectory);
            }
            return new CheckpointCatalog(rootString, "data_source", snapshots);
        }

        private CheckpointEntry defaultCheckpoint() {
            return checkpoints.get(0);
        }

        private CheckpointEntry findCheckpoint(Long checkpointId) {
            if (checkpointId == null) {
                return defaultCheckpoint();
            }
            for (CheckpointEntry checkpoint : checkpoints) {
                if (checkpoint.id == checkpointId) {
                    return checkpoint;
                }
            }
            throw new InputException("unknown checkpoint_id: " + checkpointId);
        }

        private Map<String, Object> toJson() {
            Map<String, Object> output = new LinkedHashMap<>();
            output.put("root_directory", rootDirectory);
            output.put("source_kind", sourceKind);
            output.put("checkpoints", checkpointsToJson());
            return output;
        }

        private List<Map<String, Object>> checkpointsToJson() {
            List<Map<String, Object>> output = new ArrayList<>(checkpoints.size());
            for (CheckpointEntry checkpoint : checkpoints) {
                output.add(checkpoint.toJson());
            }
            return output;
        }
    }

    private static List<Path> discoverCheckpointDirectories(FileSystem fileSystem, Path root) {
        List<Path> checkpointDirectories = new ArrayList<>();
        collectCheckpointDirectories(fileSystem, root, 0, checkpointDirectories);
        return checkpointDirectories;
    }

    private static void collectCheckpointDirectories(
            FileSystem fileSystem, Path directory, int depth, List<Path> checkpointDirectories) {
        Long checkpointId = checkpointDirectoryId(directory);
        if (checkpointId != null) {
            checkpointDirectories.add(directory);
            return;
        }
        if (depth >= 4) {
            return;
        }
        FileStatus[] children;
        try {
            children = fileSystem.listStatus(directory);
        } catch (IOException e) {
            throw new InputException(
                    "Failed to list checkpoint directory " + directory + ": " + e.getMessage());
        }
        if (children == null) {
            return;
        }
        for (FileStatus childStatus : children) {
            if (!childStatus.isDir()) {
                continue;
            }
            String name = childStatus.getPath().getName();
            if ("shared".equals(name) || "taskowned".equals(name)) {
                continue;
            }
            collectCheckpointDirectories(
                    fileSystem, childStatus.getPath(), depth + 1, checkpointDirectories);
        }
    }

    private static List<OperatorEntry> discoverOperators(
            FileSystem fileSystem,
            Path checkpointRoot,
            Path checkpointDirectory,
            long checkpointId) {
        Map<String, OperatorEntry> operators = new LinkedHashMap<>();
        FileStatus[] checkpointFiles;
        try {
            checkpointFiles = fileSystem.listStatus(checkpointDirectory);
        } catch (IOException e) {
            throw new InputException(
                    "Failed to list checkpoint directory "
                            + checkpointDirectory
                            + ": "
                            + e.getMessage());
        }
        if (checkpointFiles != null) {
            for (FileStatus fileStatus : checkpointFiles) {
                String fileName = fileStatus.getPath().getName();
                if (fileStatus.isDir() || !isCobbleManifestCopy(fileName)) {
                    continue;
                }
                String operatorId = operatorIdFromManifestCopy(fileName);
                String operatorDirectory =
                        operatorSnapshotDirectory(checkpointRoot, checkpointDirectory, operatorId);
                String manifestCopyPath =
                        preferStableManifestPath(
                                fileSystem,
                                checkpointId,
                                pathToStorageString(fileStatus.getPath()),
                                operatorDirectory);
                operators.put(
                        operatorId,
                        new OperatorEntry(
                                operatorId,
                                manifestCopyPath,
                                operatorDirectory,
                                readerVolumeDirectories(
                                        fileSystem,
                                        checkpointRoot,
                                        checkpointDirectory,
                                        operatorId,
                                        operatorDirectory),
                                true));
            }
        }

        for (Path cobbleRoot : cobbleRootCandidates(checkpointRoot, checkpointDirectory)) {
            FileStatus[] operatorDirectories;
            try {
                operatorDirectories =
                        fileSystem.exists(cobbleRoot) ? fileSystem.listStatus(cobbleRoot) : null;
            } catch (IOException e) {
                throw new InputException(
                        "Failed to list Cobble operator directory "
                                + cobbleRoot
                                + ": "
                                + e.getMessage());
            }
            if (operatorDirectories == null) {
                continue;
            }
            for (FileStatus operatorStatus : operatorDirectories) {
                if (!operatorStatus.isDir()) {
                    continue;
                }
                Path operatorDirectory = operatorStatus.getPath();
                Path manifest =
                        new Path(
                                new Path(operatorDirectory, "snapshot"),
                                "SNAPSHOT-" + checkpointId);
                try {
                    if (!fileSystem.exists(manifest)
                            || fileSystem.getFileStatus(manifest).isDir()) {
                        continue;
                    }
                    String operatorId = operatorDirectory.getName();
                    String operatorDirectoryString = pathToStorageString(operatorDirectory);
                    operators.putIfAbsent(
                            operatorId,
                            new OperatorEntry(
                                    operatorId,
                                    null,
                                    operatorDirectoryString,
                                    readerVolumeDirectories(
                                            fileSystem,
                                            checkpointRoot,
                                            checkpointDirectory,
                                            operatorId,
                                            operatorDirectoryString),
                                    true));
                } catch (IOException e) {
                    throw new InputException(
                            "Failed to inspect Cobble manifest "
                                    + manifest
                                    + ": "
                                    + e.getMessage());
                }
            }
        }

        discoverSharedOperators(
                fileSystem, checkpointRoot, checkpointDirectory, checkpointId, operators);

        List<OperatorEntry> output = new ArrayList<>(operators.values());
        output.sort(Comparator.comparing(operator -> operator.operatorId));
        return output;
    }

    private static String preferStableManifestPath(
            FileSystem fileSystem,
            long checkpointId,
            String manifestCopyPath,
            String operatorDirectory) {
        Path stableManifest = operatorSnapshotManifest(operatorDirectory, checkpointId);
        try {
            if (fileSystem.exists(stableManifest)
                    && !fileSystem.getFileStatus(stableManifest).isDir()) {
                return pathToStorageString(stableManifest);
            }
        } catch (IOException ignored) {
            // Keep the checkpoint-local manifest copy as a fallback candidate.
        }
        return manifestCopyPath;
    }

    private static void discoverSharedOperators(
            FileSystem fileSystem,
            Path checkpointRoot,
            Path checkpointDirectory,
            long checkpointId,
            Map<String, OperatorEntry> operators) {
        for (Path rootCandidate : sharedRootCandidates(checkpointRoot, checkpointDirectory)) {
            Path sharedRoot = new Path(rootCandidate, "shared");
            FileStatus sharedStatus;
            try {
                if (!fileSystem.exists(sharedRoot)) {
                    continue;
                }
                sharedStatus = fileSystem.getFileStatus(sharedRoot);
            } catch (IOException e) {
                throw new InputException(
                        "Failed to inspect Flink shared directory "
                                + sharedRoot
                                + ": "
                                + e.getMessage());
            }
            if (!sharedStatus.isDir()) {
                continue;
            }
            discoverSharedOperatorsInRoot(fileSystem, sharedRoot, checkpointId, operators);
        }
    }

    private static Map<Long, List<OperatorEntry>> discoverCobbleGlobalSnapshotOperators(
            FileSystem fileSystem, Path checkpointRoot) {
        List<Path> cobbleRoots = new ArrayList<>();
        collectCobbleRoots(fileSystem, checkpointRoot, 0, cobbleRoots);
        Map<Long, Map<String, OperatorEntry>> operatorsByCheckpoint = new LinkedHashMap<>();
        for (Path cobbleRoot : cobbleRoots) {
            discoverCobbleGlobalSnapshotOperatorsInRoot(
                    fileSystem, cobbleRoot, operatorsByCheckpoint);
        }
        Map<Long, List<OperatorEntry>> output = new LinkedHashMap<>();
        for (Map.Entry<Long, Map<String, OperatorEntry>> entry : operatorsByCheckpoint.entrySet()) {
            List<OperatorEntry> operators = new ArrayList<>(entry.getValue().values());
            operators.sort(Comparator.comparing(operator -> operator.operatorId));
            output.put(entry.getKey(), operators);
        }
        return output;
    }

    private static void collectCobbleRoots(
            FileSystem fileSystem, Path directory, int depth, List<Path> cobbleRoots) {
        if ("cobble".equals(directory.getName())) {
            cobbleRoots.add(directory);
            return;
        }
        if (depth >= 3 || checkpointDirectoryId(directory) != null) {
            return;
        }
        FileStatus[] children;
        try {
            children = fileSystem.listStatus(directory);
        } catch (IOException e) {
            throw new InputException(
                    "Failed to list checkpoint directory " + directory + ": " + e.getMessage());
        }
        if (children == null) {
            return;
        }
        for (FileStatus childStatus : children) {
            if (!childStatus.isDir()) {
                continue;
            }
            String name = childStatus.getPath().getName();
            if ("shared".equals(name)
                    || "taskowned".equals(name)
                    || name.startsWith(CHECKPOINT_PREFIX)) {
                continue;
            }
            collectCobbleRoots(fileSystem, childStatus.getPath(), depth + 1, cobbleRoots);
        }
    }

    private static void discoverCobbleGlobalSnapshotOperatorsInRoot(
            FileSystem fileSystem,
            Path cobbleRoot,
            Map<Long, Map<String, OperatorEntry>> operatorsByCheckpoint) {
        FileStatus[] operatorDirectories;
        try {
            operatorDirectories = fileSystem.listStatus(cobbleRoot);
        } catch (IOException e) {
            throw new InputException(
                    "Failed to list Cobble operator directory "
                            + cobbleRoot
                            + ": "
                            + e.getMessage());
        }
        if (operatorDirectories == null) {
            return;
        }
        for (FileStatus operatorStatus : operatorDirectories) {
            if (!operatorStatus.isDir()) {
                continue;
            }
            Path operatorDirectory = operatorStatus.getPath();
            Path snapshotDirectory = new Path(operatorDirectory, "snapshot");
            FileStatus[] snapshotFiles;
            try {
                if (!fileSystem.exists(snapshotDirectory)
                        || !fileSystem.getFileStatus(snapshotDirectory).isDir()) {
                    continue;
                }
                snapshotFiles = fileSystem.listStatus(snapshotDirectory);
            } catch (IOException e) {
                throw new InputException(
                        "Failed to list Cobble global snapshot directory "
                                + snapshotDirectory
                                + ": "
                                + e.getMessage());
            }
            if (snapshotFiles == null) {
                continue;
            }
            for (FileStatus snapshotStatus : snapshotFiles) {
                if (snapshotStatus.isDir()) {
                    continue;
                }
                Long checkpointId = snapshotManifestId(snapshotStatus.getPath().getName());
                if (checkpointId == null
                        || !snapshotManifestLooksGlobal(fileSystem, snapshotStatus.getPath())) {
                    continue;
                }
                Map<String, OperatorEntry> operators =
                        operatorsByCheckpoint.computeIfAbsent(
                                checkpointId, ignored -> new LinkedHashMap<>());
                String operatorId = operatorDirectory.getName();
                String operatorDirectoryString = pathToStorageString(operatorDirectory);
                operators.putIfAbsent(
                        operatorId,
                        new OperatorEntry(
                                operatorId,
                                pathToStorageString(snapshotStatus.getPath()),
                                operatorDirectoryString,
                                readerVolumeDirectories(
                                        fileSystem,
                                        cobbleRoot.getParent(),
                                        null,
                                        operatorId,
                                        operatorDirectoryString),
                                true));
            }
        }
    }

    private static Map<Long, List<OperatorEntry>> discoverSharedSnapshotOperators(
            FileSystem fileSystem, Path checkpointRoot) {
        Map<Long, Map<String, OperatorEntry>> operatorsByCheckpoint = new LinkedHashMap<>();
        for (Path rootCandidate : sharedRootCandidates(checkpointRoot, checkpointRoot)) {
            Path sharedRoot = new Path(rootCandidate, "shared");
            try {
                if (!fileSystem.exists(sharedRoot)
                        || !fileSystem.getFileStatus(sharedRoot).isDir()) {
                    continue;
                }
            } catch (IOException e) {
                throw new InputException(
                        "Failed to inspect Flink shared directory "
                                + sharedRoot
                                + ": "
                                + e.getMessage());
            }
            discoverSharedSnapshotOperatorsInRoot(fileSystem, sharedRoot, operatorsByCheckpoint);
        }
        Map<Long, List<OperatorEntry>> output = new LinkedHashMap<>();
        for (Map.Entry<Long, Map<String, OperatorEntry>> entry : operatorsByCheckpoint.entrySet()) {
            List<OperatorEntry> operators = new ArrayList<>(entry.getValue().values());
            operators.sort(Comparator.comparing(operator -> operator.operatorId));
            output.put(entry.getKey(), operators);
        }
        return output;
    }

    private static void discoverSharedSnapshotOperatorsInRoot(
            FileSystem fileSystem,
            Path sharedRoot,
            Map<Long, Map<String, OperatorEntry>> operatorsByCheckpoint) {
        FileStatus[] operatorDirectories;
        try {
            operatorDirectories = fileSystem.listStatus(sharedRoot);
        } catch (IOException e) {
            throw new InputException(
                    "Failed to list Flink shared directory " + sharedRoot + ": " + e.getMessage());
        }
        if (operatorDirectories == null) {
            return;
        }
        for (FileStatus operatorStatus : operatorDirectories) {
            if (!operatorStatus.isDir()) {
                continue;
            }
            Path operatorDirectory = operatorStatus.getPath();
            FileStatus[] volumeDirectories;
            try {
                volumeDirectories = fileSystem.listStatus(operatorDirectory);
            } catch (IOException e) {
                throw new InputException(
                        "Failed to list Flink shared operator directory "
                                + operatorDirectory
                                + ": "
                                + e.getMessage());
            }
            if (volumeDirectories == null) {
                continue;
            }
            for (FileStatus volumeStatus : volumeDirectories) {
                if (!volumeStatus.isDir()) {
                    continue;
                }
                discoverSharedVolumeSnapshots(
                        fileSystem,
                        operatorDirectory,
                        volumeStatus.getPath(),
                        operatorsByCheckpoint);
            }
        }
    }

    private static void discoverSharedVolumeSnapshots(
            FileSystem fileSystem,
            Path operatorDirectory,
            Path volumeDirectory,
            Map<Long, Map<String, OperatorEntry>> operatorsByCheckpoint) {
        Path snapshotDirectory = new Path(volumeDirectory, "snapshot");
        FileStatus[] snapshotFiles;
        try {
            if (!fileSystem.exists(snapshotDirectory)
                    || !fileSystem.getFileStatus(snapshotDirectory).isDir()) {
                return;
            }
            snapshotFiles = fileSystem.listStatus(snapshotDirectory);
        } catch (IOException e) {
            throw new InputException(
                    "Failed to list Cobble snapshot directory "
                            + snapshotDirectory
                            + ": "
                            + e.getMessage());
        }
        if (snapshotFiles == null) {
            return;
        }
        for (FileStatus snapshotStatus : snapshotFiles) {
            if (snapshotStatus.isDir()) {
                continue;
            }
            Long checkpointId = snapshotManifestId(snapshotStatus.getPath().getName());
            if (checkpointId == null) {
                continue;
            }
            if (!snapshotManifestLooksGlobal(fileSystem, snapshotStatus.getPath())) {
                continue;
            }
            Map<String, OperatorEntry> operators =
                    operatorsByCheckpoint.computeIfAbsent(
                            checkpointId, ignored -> new LinkedHashMap<>());
            String operatorId =
                    uniqueOperatorId(
                            operators,
                            operatorDirectory.getName(),
                            volumeDirectory.getName(),
                            volumeDirectory);
            operators.putIfAbsent(
                    operatorId,
                    new OperatorEntry(
                            operatorId,
                            pathToStorageString(snapshotStatus.getPath()),
                            pathToStorageString(volumeDirectory),
                            Collections.singletonList(pathToStorageString(volumeDirectory)),
                            true));
        }
    }

    private static boolean snapshotManifestLooksGlobal(FileSystem fileSystem, Path manifest) {
        byte[] buffer = new byte[8192];
        int bytesRead;
        try (InputStream input = fileSystem.open(manifest)) {
            bytesRead = input.read(buffer);
        } catch (IOException e) {
            throw new InputException(
                    "Failed to inspect Cobble snapshot manifest "
                            + manifest
                            + ": "
                            + e.getMessage());
        }
        if (bytesRead <= 0) {
            return false;
        }
        String prefix = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
        return prefix.contains("\"total_buckets\"") || prefix.contains("\"totalBuckets\"");
    }

    private static List<Path> sharedRootCandidates(Path checkpointRoot, Path checkpointDirectory) {
        Map<String, Path> candidates = new LinkedHashMap<>();
        addPathCandidate(candidates, checkpointRoot);
        addPathCandidate(candidates, checkpointDirectory.getParent());
        Path checkpointParent = checkpointDirectory.getParent();
        if (checkpointParent != null) {
            addPathCandidate(candidates, checkpointParent.getParent());
        }
        return new ArrayList<>(candidates.values());
    }

    private static void addPathCandidate(Map<String, Path> candidates, Path path) {
        if (path != null) {
            candidates.putIfAbsent(path.toString(), path);
        }
    }

    private static void discoverSharedOperatorsInRoot(
            FileSystem fileSystem,
            Path sharedRoot,
            long checkpointId,
            Map<String, OperatorEntry> operators) {
        FileStatus[] operatorDirectories;
        try {
            operatorDirectories = fileSystem.listStatus(sharedRoot);
        } catch (IOException e) {
            throw new InputException(
                    "Failed to list Flink shared directory " + sharedRoot + ": " + e.getMessage());
        }
        if (operatorDirectories == null) {
            return;
        }
        for (FileStatus operatorStatus : operatorDirectories) {
            if (!operatorStatus.isDir()) {
                continue;
            }
            Path operatorDirectory = operatorStatus.getPath();
            FileStatus[] volumeDirectories;
            try {
                volumeDirectories = fileSystem.listStatus(operatorDirectory);
            } catch (IOException e) {
                throw new InputException(
                        "Failed to list Flink shared operator directory "
                                + operatorDirectory
                                + ": "
                                + e.getMessage());
            }
            if (volumeDirectories == null) {
                continue;
            }
            for (FileStatus volumeStatus : volumeDirectories) {
                if (!volumeStatus.isDir()) {
                    continue;
                }
                Path volumeDirectory = volumeStatus.getPath();
                Path manifest =
                        new Path(new Path(volumeDirectory, "snapshot"), "SNAPSHOT-" + checkpointId);
                try {
                    if (!fileSystem.exists(manifest)
                            || fileSystem.getFileStatus(manifest).isDir()) {
                        continue;
                    }
                } catch (IOException e) {
                    throw new InputException(
                            "Failed to inspect Cobble shared manifest "
                                    + manifest
                                    + ": "
                                    + e.getMessage());
                }
                String operatorId =
                        uniqueOperatorId(
                                operators,
                                operatorDirectory.getName(),
                                volumeDirectory.getName(),
                                volumeDirectory);
                operators.putIfAbsent(
                        operatorId,
                        new OperatorEntry(
                                operatorId,
                                pathToStorageString(manifest),
                                pathToStorageString(volumeDirectory)));
            }
        }
    }

    private static String uniqueOperatorId(
            Map<String, OperatorEntry> operators,
            String operatorId,
            String volumeId,
            Path volumeDirectory) {
        OperatorEntry existing = operators.get(operatorId);
        if (existing == null
                || existing.operatorSnapshotDirectory.equals(
                        pathToStorageString(volumeDirectory))) {
            return operatorId;
        }
        return operatorId + "/" + volumeId;
    }

    private static Long checkpointDirectoryId(Path directory) {
        String name = directory.getName();
        if (!name.startsWith(CHECKPOINT_PREFIX)) {
            return null;
        }
        String rawId = name.substring(CHECKPOINT_PREFIX.length());
        if (rawId.isEmpty()) {
            return null;
        }
        for (int index = 0; index < rawId.length(); index++) {
            if (!Character.isDigit(rawId.charAt(index))) {
                return null;
            }
        }
        return Long.parseLong(rawId);
    }

    private static Long snapshotManifestId(String fileName) {
        if (!fileName.startsWith("SNAPSHOT-")) {
            return null;
        }
        String rawId = fileName.substring("SNAPSHOT-".length());
        if (rawId.isEmpty()) {
            return null;
        }
        for (int index = 0; index < rawId.length(); index++) {
            if (!Character.isDigit(rawId.charAt(index))) {
                return null;
            }
        }
        return Long.parseLong(rawId);
    }

    private static List<String> readerVolumeDirectories(
            FileSystem fileSystem,
            Path checkpointRoot,
            Path checkpointDirectory,
            String operatorId,
            String operatorDirectory) {
        Map<String, String> volumes = new LinkedHashMap<>();
        volumes.put(operatorDirectory, operatorDirectory);
        Path sharedCandidateBase =
                checkpointDirectory == null ? checkpointRoot : checkpointDirectory;
        for (Path rootCandidate : sharedRootCandidates(checkpointRoot, sharedCandidateBase)) {
            Path sharedOperatorRoot =
                    new Path(new Path(rootCandidate, "shared"), "op_" + operatorId);
            try {
                if (!fileSystem.exists(sharedOperatorRoot)
                        || !fileSystem.getFileStatus(sharedOperatorRoot).isDir()) {
                    continue;
                }
            } catch (IOException e) {
                throw new InputException(
                        "Failed to inspect Cobble shared operator directory "
                                + sharedOperatorRoot
                                + ": "
                                + e.getMessage());
            }
            String volumeDirectory = pathToStorageString(sharedOperatorRoot);
            volumes.putIfAbsent(volumeDirectory, volumeDirectory);
        }
        return new ArrayList<>(volumes.values());
    }

    private static boolean isCobbleManifestCopy(String fileName) {
        return fileName.startsWith(COBBLE_MANIFEST_PREFIX)
                && fileName.endsWith(COBBLE_MANIFEST_SUFFIX)
                && fileName.length()
                        > COBBLE_MANIFEST_PREFIX.length() + COBBLE_MANIFEST_SUFFIX.length();
    }

    private static String operatorIdFromManifestCopy(String fileName) {
        return fileName.substring(
                COBBLE_MANIFEST_PREFIX.length(),
                fileName.length() - COBBLE_MANIFEST_SUFFIX.length());
    }

    private static List<Path> cobbleRootCandidates(Path checkpointRoot, Path checkpointDirectory) {
        Map<String, Path> candidates = new LinkedHashMap<>();
        Path checkpointParent = checkpointDirectory.getParent();
        if (checkpointParent != null) {
            addPathCandidate(candidates, new Path(checkpointParent, "cobble"));
        }
        addPathCandidate(candidates, new Path(checkpointRoot, "cobble"));
        return new ArrayList<>(candidates.values());
    }

    private static String operatorSnapshotDirectory(
            Path checkpointRoot, Path checkpointDirectory, String operatorId) {
        Path checkpointParent = checkpointDirectory.getParent();
        if (checkpointParent != null) {
            return pathToStorageString(new Path(new Path(checkpointParent, "cobble"), operatorId));
        }
        return pathToStorageString(new Path(new Path(checkpointRoot, "cobble"), operatorId));
    }

    private static String pathToStorageString(Path path) {
        URI uri = path.toUri();
        String scheme = uri.getScheme();
        if (scheme == null || scheme.trim().isEmpty()) {
            return new File(path.toString()).getAbsoluteFile().toPath().normalize().toString();
        }
        if ("file".equalsIgnoreCase(scheme)) {
            return new File(uri).getAbsoluteFile().toPath().normalize().toString();
        }
        String normalizedScheme = normalizeCheckpointScheme(scheme);
        if (normalizedScheme.equals(scheme.toLowerCase(Locale.ROOT))) {
            return path.toString();
        }
        try {
            return new URI(
                            normalizedScheme,
                            uri.getAuthority(),
                            uri.getPath(),
                            uri.getQuery(),
                            uri.getFragment())
                    .toString();
        } catch (URISyntaxException e) {
            throw new InputException("failed to normalize storage path: " + path);
        }
    }

    private static ReadOptions readOptions(String columnFamily, int[] columns) {
        ReadOptions options = new ReadOptions();
        if (columnFamily != null) {
            options.columnFamily(columnFamily);
        }
        if (columns != null) {
            options.columns(columns);
        } else {
            options.clearColumns();
        }
        return options;
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

    private static List<LookupItem> parseLookupItems(Map<String, List<String>> params) {
        int defaultBucket = intParam(params, "bucket", 0);
        String rawLookupItems = first(params, "lookup_items", null);
        if (rawLookupItems != null && !rawLookupItems.trim().isEmpty()) {
            JsonArray array = JsonParser.parseString(rawLookupItems).getAsJsonArray();
            List<LookupItem> items = new ArrayList<>(array.size());
            for (JsonElement element : array) {
                JsonObject object = element.getAsJsonObject();
                int bucket =
                        object.has("bucket") && !object.get("bucket").isJsonNull()
                                ? object.get("bucket").getAsInt()
                                : defaultBucket;
                String keyB64 = stringField(object, "key_b64", null);
                if (keyB64 == null) {
                    throw new InputException("lookup_items[].key_b64 is required");
                }
                items.add(new LookupItem(bucket, decodeB64(keyB64, "lookup_items[].key_b64")));
            }
            return items;
        }

        String keysB64 = first(params, "keys_b64", null);
        if (keysB64 != null) {
            List<LookupItem> items = new ArrayList<>();
            for (String part : keysB64.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    items.add(new LookupItem(defaultBucket, decodeB64(trimmed, "keys_b64")));
                }
            }
            return items;
        }

        String keys = first(params, "keys", null);
        if (keys != null) {
            List<LookupItem> items = new ArrayList<>();
            for (String part : keys.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    items.add(
                            new LookupItem(
                                    defaultBucket, trimmed.getBytes(StandardCharsets.UTF_8)));
                }
            }
            return items;
        }
        return Collections.emptyList();
    }

    private static List<Map<String, Object>> columnsToJson(byte[][] columns) {
        if (columns == null) {
            return null;
        }
        List<Map<String, Object>> output = new ArrayList<>(columns.length);
        for (byte[] column : columns) {
            if (column == null) {
                output.add(null);
            } else {
                Map<String, Object> value = new LinkedHashMap<>();
                value.put("b64", b64(column));
                value.put("utf8", utf8(column));
                output.add(value);
            }
        }
        return output;
    }

    private static Object valueToJson(byte[][] columns, boolean keepColumns) {
        return keepColumns ? columnsToJson(columns) : firstColumnToJson(columns);
    }

    private static Map<String, Object> firstColumnToJson(byte[][] columns) {
        if (columns == null || columns.length == 0 || columns[0] == null) {
            return null;
        }
        return bytesJson(columns[0]);
    }

    static Map<String, Object> bytesJson(byte[] bytes) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("b64", b64(bytes));
        value.put("utf8", utf8(bytes));
        return value;
    }

    private static String inspectKind(
            GlobalSnapshot snapshot,
            SchemaResolveResult schema,
            SinkSchemaResolveResult sinkSchema) {
        List<InspectTarget> targets = StateInspectTargetBuilder.build(snapshot, schema, sinkSchema);
        if (targets.isEmpty()) {
            return "empty";
        }
        if (targets.size() == 1 && "sink".equals(targets.get(0).kind)) {
            return "sink";
        }
        return "state";
    }

    private static List<Map<String, Object>> inspectTargets(
            GlobalSnapshot snapshot,
            SchemaResolveResult schema,
            SinkSchemaResolveResult sinkSchema) {
        List<InspectTarget> targets = StateInspectTargetBuilder.build(snapshot, schema, sinkSchema);
        List<Map<String, Object>> output = new ArrayList<>(targets.size());
        for (InspectTarget target : targets) {
            output.add(target.toJson());
        }
        return output;
    }

    private static List<Map<String, Object>> columnFamilies(GlobalSnapshot snapshot) {
        if (snapshot == null || snapshot.columnFamilyIds == null) {
            return Collections.emptyList();
        }
        List<Map.Entry<String, Integer>> entries =
                new ArrayList<>(snapshot.columnFamilyIds.entrySet());
        Collections.sort(entries, Comparator.comparing(Map.Entry::getValue));
        List<Map<String, Object>> output = new ArrayList<>(entries.size());
        for (Map.Entry<String, Integer> entry : entries) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("name", entry.getKey());
            item.put("id", entry.getValue());
            item.put("shard_count", countShardsWithColumnFamily(snapshot, entry.getKey()));
            output.add(item);
        }
        return output;
    }

    private static int countShardsWithColumnFamily(GlobalSnapshot snapshot, String columnFamily) {
        if (snapshot.shardSnapshots == null) {
            return 0;
        }
        int count = 0;
        for (ShardSnapshot shard : snapshot.shardSnapshots) {
            if (shard.columnFamilyIds != null && shard.columnFamilyIds.containsKey(columnFamily)) {
                count++;
            }
        }
        return count;
    }

    private static int[] parseColumns(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return null;
        }
        String[] parts = raw.split(",");
        int[] columns = new int[parts.length];
        int count = 0;
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                int value = parsePositiveOrZeroInt(trimmed, "columns");
                columns[count++] = value;
            }
        }
        if (count == columns.length) {
            return columns;
        }
        int[] compact = new int[count];
        System.arraycopy(columns, 0, compact, 0, count);
        return compact;
    }

    private static byte[] decodeOptionalBytes(String plainValue, String b64Value) {
        if (b64Value != null) {
            return decodeB64(b64Value, "base64 value");
        }
        if (plainValue == null) {
            return null;
        }
        return plainValue.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] decodeOptionalB64(String value, String fieldName) {
        return value == null || value.trim().isEmpty() ? null : decodeB64(value, fieldName);
    }

    private static byte[] decodeB64(String value, String fieldName) {
        try {
            return Base64.getDecoder().decode(value);
        } catch (IllegalArgumentException e) {
            throw new InputException("invalid `" + fieldName + "` base64: " + e.getMessage());
        }
    }

    private static String b64(byte[] value) {
        return Base64.getEncoder().encodeToString(value);
    }

    private static void putIfPresent(Map<String, Object> output, String key, String value) {
        if (value != null) {
            output.put(key, value);
        }
    }

    private static String utf8(byte[] value) {
        CharsetDecoder decoder =
                StandardCharsets.UTF_8
                        .newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return decoder.decode(java.nio.ByteBuffer.wrap(value)).toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    private static Map<String, List<String>> parseQuery(URI uri) throws IOException {
        Map<String, List<String>> output = new HashMap<>();
        String query = uri.getRawQuery();
        if (query == null || query.isEmpty()) {
            return output;
        }
        for (String pair : query.split("&")) {
            int separator = pair.indexOf('=');
            String key = separator < 0 ? pair : pair.substring(0, separator);
            String value = separator < 0 ? "" : pair.substring(separator + 1);
            key = urlDecode(key);
            value = urlDecode(value);
            output.computeIfAbsent(key, ignored -> new ArrayList<>()).add(value);
        }
        return output;
    }

    private static String urlDecode(String value) throws IOException {
        return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
    }

    private static String first(Map<String, List<String>> params, String key, String fallback) {
        List<String> values = params.get(key);
        return values == null || values.isEmpty() ? fallback : values.get(0);
    }

    private static int intParam(Map<String, List<String>> params, String key, Integer fallback) {
        String raw = first(params, key, null);
        if (raw == null || raw.trim().isEmpty()) {
            if (fallback == null) {
                throw new InputException("query param `" + key + "` is required");
            }
            return fallback;
        }
        return parsePositiveOrZeroInt(raw, key);
    }

    private static Integer optionalBucketParam(Map<String, List<String>> params) {
        String raw = first(params, "bucket", null);
        if (raw == null || raw.trim().isEmpty() || "all".equalsIgnoreCase(raw.trim())) {
            return null;
        }
        return parsePositiveOrZeroInt(raw, "bucket");
    }

    private static Map<String, Object> singleton(String key, Object value) {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put(key, value);
        return output;
    }

    private static JsonObject readJsonObject(HttpExchange exchange) throws IOException {
        String body = new String(readAll(exchange.getRequestBody()), StandardCharsets.UTF_8);
        if (body.trim().isEmpty()) {
            return new JsonObject();
        }
        return JsonParser.parseString(body).getAsJsonObject();
    }

    private static String stringField(JsonObject object, String field, String fallback) {
        if (object == null || !object.has(field) || object.get(field).isJsonNull()) {
            return fallback;
        }
        return object.get(field).getAsString();
    }

    private static Long longField(JsonObject object, String field, Long fallback) {
        if (object == null || !object.has(field) || object.get(field).isJsonNull()) {
            return fallback;
        }
        return object.get(field).getAsLong();
    }

    private static void sendJson(HttpExchange exchange, int status, Object body)
            throws IOException {
        byte[] payload = GSON.toJson(body).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, payload.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(payload);
        }
    }

    private static void sendError(HttpExchange exchange, int status, String message)
            throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", message == null || message.trim().isEmpty() ? "unknown error" : message);
        sendJson(exchange, status, body);
    }

    private static byte[] readAll(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int read;
        while ((read = input.read(buffer)) >= 0) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private static boolean hasFileExtension(String path) {
        int slash = path.lastIndexOf('/');
        int dot = path.lastIndexOf('.');
        return dot > slash;
    }

    private static String contentType(String path) {
        String lower = path.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".html")) {
            return "text/html; charset=utf-8";
        }
        if (lower.endsWith(".css")) {
            return "text/css; charset=utf-8";
        }
        if (lower.endsWith(".js")) {
            return "application/javascript; charset=utf-8";
        }
        if (lower.endsWith(".json")) {
            return "application/json; charset=utf-8";
        }
        return "application/octet-stream";
    }

    private static String cacheControl(String path) {
        return "no-store";
    }

    private static int parsePositiveOrZeroInt(String raw, String field) {
        try {
            int value = Integer.parseInt(raw);
            if (value < 0) {
                throw new InputException("`" + field + "` must be >= 0");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new InputException("invalid integer for `" + field + "`: " + raw);
        }
    }

    private static long parseLong(String raw, String field) {
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new InputException("invalid long for `" + field + "`: " + raw);
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value;
    }

    private static boolean startsWith(byte[] value, byte[] prefix) {
        if (value.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (value[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean bytesEqual(byte[] left, byte[] right) {
        return Objects.deepEquals(new byte[][] {left}, new byte[][] {right});
    }

    private static byte[] prefixUpperBound(byte[] prefix) {
        if (prefix.length == 0) {
            return null;
        }
        byte[] end = new byte[prefix.length];
        System.arraycopy(prefix, 0, end, 0, prefix.length);
        for (int index = end.length - 1; index >= 0; index--) {
            int unsigned = end[index] & 0xFF;
            if (unsigned != 0xFF) {
                end[index] = (byte) (unsigned + 1);
                byte[] compact = new byte[index + 1];
                System.arraycopy(end, 0, compact, 0, index + 1);
                return compact;
            }
        }
        return null;
    }

    private static byte[] maxScanKey() {
        byte[] key = new byte[64];
        Arrays.fill(key, (byte) 0xFF);
        return key;
    }

    private static String cobbleOperatorSnapshotDirectory(String checkpoint, String operatorId) {
        return appendPath(appendPath(checkpointParentDirectory(checkpoint), "cobble"), operatorId);
    }

    private static String checkpointParentDirectory(String checkpointExternalPointer) {
        String normalized = normalizeStorageDirectory(checkpointExternalPointer);
        URI uri = URI.create(normalized);
        if ("file".equals(uri.getScheme())) {
            File parent = new File(uri).getParentFile();
            if (parent == null) {
                throw new InputException(
                        "checkpoint external pointer has no parent: " + checkpointExternalPointer);
            }
            return normalizeLocalPath(parent);
        }
        String value = stripTrailingSlash(normalized);
        int slash = value.lastIndexOf('/');
        if (slash < 0) {
            throw new InputException(
                    "checkpoint external pointer has no parent: " + checkpointExternalPointer);
        }
        return value.substring(0, slash);
    }

    private static String appendPath(String base, String segment) {
        String normalized = normalizeStorageDirectory(base);
        URI uri = URI.create(normalized);
        if ("file".equals(uri.getScheme())) {
            return normalizeLocalPath(new File(new File(uri), segment));
        }
        return stripTrailingSlash(normalized) + "/" + segment;
    }

    private static String normalizeStorageDirectory(String directory) {
        if (directory == null || directory.trim().isEmpty()) {
            throw new InputException("storage directory must not be blank");
        }
        URI uri = URI.create(directory);
        String scheme = uri.getScheme();
        if (scheme == null || scheme.trim().isEmpty()) {
            return normalizeLocalPath(new File(directory));
        }
        String normalizedScheme = normalizeCheckpointScheme(scheme);
        if ("file".equals(normalizedScheme)) {
            return normalizeLocalPath(new File(uri));
        }
        if (normalizedScheme.equals(scheme.toLowerCase(Locale.ROOT))) {
            return directory;
        }
        try {
            return new URI(
                            normalizedScheme,
                            uri.getAuthority(),
                            uri.getPath(),
                            uri.getQuery(),
                            uri.getFragment())
                    .toString();
        } catch (URISyntaxException e) {
            throw new InputException("failed to normalize storage directory: " + directory);
        }
    }

    private static String normalizeLocalPath(File file) {
        try {
            String path = file.getAbsoluteFile().toPath().normalize().toString();
            path = path.replace(File.separatorChar, '/');
            if (!path.startsWith("/")) {
                path = "/" + path;
            }
            return new URI("file", "", path, null, null).toString();
        } catch (URISyntaxException e) {
            throw new InputException("failed to normalize local path: " + file);
        }
    }

    private static String normalizeCheckpointScheme(String scheme) {
        String normalized = scheme.toLowerCase(Locale.ROOT);
        if ("s3a".equals(normalized) || "s3p".equals(normalized)) {
            return "s3";
        }
        return normalized;
    }

    private static String stripTrailingSlash(String value) {
        String output = value;
        while (output.endsWith("/") && output.length() > 1) {
            output = output.substring(0, output.length() - 1);
        }
        return output;
    }
}
