package io.cobble.flink.monitor;

import io.cobble.flink.inspect.CobbleInspectClient;
import io.cobble.flink.inspect.InspectCatalog;
import io.cobble.flink.inspect.InspectErrorCode;
import io.cobble.flink.inspect.InspectException;
import io.cobble.flink.inspect.InspectPage;
import io.cobble.flink.inspect.InspectSelection;
import io.cobble.flink.inspect.InspectSession;
import io.cobble.flink.inspect.LookupRequest;
import io.cobble.flink.inspect.LookupResult;
import io.cobble.flink.inspect.ScanRequest;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Loopback-first HTTP/UI adapter for the public Cobble inspect SDK. */
public final class CobbleFlinkMonitorServer {
    private static final Gson GSON = new Gson();

    private CobbleFlinkMonitorServer() {}

    public static void main(String[] args) throws Exception {
        ServerConfig config = ServerConfig.parse(args);
        preloadRuntimeClasses();
        RunningServer running = start(config);
        Runtime.getRuntime()
                .addShutdownHook(new Thread(running::close, "cobble-flink-monitor-shutdown"));
        InetSocketAddress address = running.address();
        if (!isLoopback(config.bindAddress)) {
            System.err.println(
                    "WARNING: non-loopback binding exposes an unauthenticated read-only inspect API");
        }
        System.out.println(
                "cobble-flink-monitor listening on http://"
                        + address.getHostString()
                        + ":"
                        + address.getPort()
                        + "/");
        new CountDownLatch(1).await();
    }

    static RunningServer start(ServerConfig config) throws IOException {
        CobbleInspectClient client =
                CobbleInspectClient.builder()
                        .storageOptions(config.storageOptions)
                        .flinkConfigPath(config.flinkConfPath)
                        .userJars(config.userJars)
                        .totalBuckets(config.totalBuckets)
                        .build();
        return start(config, client, true);
    }

    static RunningServer start(ServerConfig config, CobbleInspectClient client, boolean closeClient)
            throws IOException {
        InspectSessionRegistry registry =
                new InspectSessionRegistry(
                        config.maxSessions, config.sessionIdleTimeoutSeconds * 1000L);
        long idleTimeoutMillis = config.sessionIdleTimeoutSeconds * 1000L;
        ExecutorService executor =
                Executors.newCachedThreadPool(
                        new ThreadFactory() {
                            private final AtomicInteger counter = new AtomicInteger();

                            @Override
                            public Thread newThread(Runnable runnable) {
                                Thread thread =
                                        new Thread(
                                                runnable,
                                                "cobble-flink-monitor-http-"
                                                        + counter.incrementAndGet());
                                thread.setDaemon(true);
                                return thread;
                            }
                        });
        ScheduledExecutorService reaper =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(runnable, "cobble-flink-monitor-session-reaper");
                            thread.setDaemon(true);
                            return thread;
                        });
        long reaperIntervalMillis = Math.max(100L, Math.min(30_000L, idleTimeoutMillis / 2L));
        HttpServer server = null;
        try {
            server = HttpServer.create(new InetSocketAddress(config.bindAddress, config.port), 0);
            server.setExecutor(executor);
            server.createContext("/", new Router(config, client, registry));
            reaper.scheduleWithFixedDelay(
                    () -> {
                        try {
                            registry.expireIdle();
                        } catch (RuntimeException error) {
                            System.err.println("WARNING: failed to expire an inspect session");
                        }
                    },
                    reaperIntervalMillis,
                    reaperIntervalMillis,
                    TimeUnit.MILLISECONDS);
            server.start();
            return new RunningServer(server, executor, reaper, registry, client, closeClient);
        } catch (IOException | RuntimeException error) {
            if (server != null) server.stop(0);
            reaper.shutdownNow();
            executor.shutdownNow();
            try {
                registry.close();
            } catch (RuntimeException closeError) {
                error.addSuppressed(closeError);
            }
            if (closeClient) {
                try {
                    client.close();
                } catch (RuntimeException closeError) {
                    error.addSuppressed(closeError);
                }
            }
            throw error;
        }
    }

    private static void preloadRuntimeClasses() throws ClassNotFoundException {
        ClassLoader loader = CobbleFlinkMonitorServer.class.getClassLoader();
        for (String className :
                Arrays.asList(
                        "io.cobble.Config",
                        "io.cobble.GlobalSnapshot",
                        "io.cobble.Reader",
                        "io.cobble.ScanCursor")) {
            Class.forName(className, false, loader);
        }
    }

    private static boolean isLoopback(String bindAddress) {
        try {
            return InetAddress.getByName(bindAddress).isLoopbackAddress();
        } catch (IOException error) {
            return false;
        }
    }

    static final class RunningServer implements AutoCloseable {
        private final HttpServer server;
        private final ExecutorService executor;
        private final ScheduledExecutorService reaper;
        private final InspectSessionRegistry registry;
        private final CobbleInspectClient client;
        private final boolean closeClient;
        private boolean closed;

        private RunningServer(
                HttpServer server,
                ExecutorService executor,
                ScheduledExecutorService reaper,
                InspectSessionRegistry registry,
                CobbleInspectClient client,
                boolean closeClient) {
            this.server = server;
            this.executor = executor;
            this.reaper = reaper;
            this.registry = registry;
            this.client = client;
            this.closeClient = closeClient;
        }

        InetSocketAddress address() {
            return server.getAddress();
        }

        int sessionCount() {
            return registry.size();
        }

        @Override
        public synchronized void close() {
            if (closed) return;
            closed = true;
            reaper.shutdownNow();
            RuntimeException failure = null;
            try {
                registry.close();
            } catch (RuntimeException error) {
                failure = error;
            } finally {
                server.stop(0);
                executor.shutdownNow();
            }
            if (closeClient) {
                try {
                    client.close();
                } catch (RuntimeException error) {
                    if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                }
            }
            if (failure != null) throw failure;
        }
    }

    private static final class Router implements HttpHandler {
        private final ServerConfig config;
        private final CobbleInspectClient client;
        private final InspectSessionRegistry registry;
        private final SecureRandom random = new SecureRandom();

        private Router(
                ServerConfig config, CobbleInspectClient client, InspectSessionRegistry registry) {
            this.config = config;
            this.client = client;
            this.registry = registry;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String requestId = requestId();
            exchange.getResponseHeaders().set("X-Request-Id", requestId);
            try {
                route(exchange);
            } catch (PayloadTooLargeException error) {
                sendError(exchange, 413, "REQUEST_TOO_LARGE", error.getMessage(), requestId, null);
            } catch (MalformedJsonException error) {
                sendError(exchange, 400, "MALFORMED_JSON", error.getMessage(), requestId, null);
            } catch (InputException error) {
                sendError(
                        exchange,
                        400,
                        "INVALID_INPUT",
                        safeTransportMessage(error.getMessage(), "The inspect request is invalid"),
                        requestId,
                        null);
            } catch (RouteException error) {
                sendError(
                        exchange,
                        error.status,
                        error.status == 405 ? "METHOD_NOT_ALLOWED" : "ROUTE_NOT_FOUND",
                        error.getMessage(),
                        requestId,
                        null);
            } catch (InspectSessionRegistry.SessionMissingException error) {
                sendError(exchange, 410, "SESSION_EXPIRED", error.getMessage(), requestId, null);
            } catch (InspectSessionRegistry.SessionLimitException error) {
                sendError(exchange, 429, "SESSION_LIMIT", error.getMessage(), requestId, null);
            } catch (InspectException error) {
                sendInspectError(exchange, error, requestId);
            } catch (Throwable error) {
                sendError(
                        exchange,
                        500,
                        "INTERNAL_ERROR",
                        "The inspect request failed",
                        requestId,
                        null);
            } finally {
                exchange.close();
            }
        }

        private void route(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getPath();
            if ("GET".equals(method) && "/healthz".equals(path)) {
                JsonObject output = new JsonObject();
                output.addProperty("status", "ok");
                sendJson(exchange, 200, output);
                return;
            }
            if ("POST".equals(method) && "/api/v1/discovery".equals(path)) {
                JsonObject body = readJsonObject(exchange, config.requestBodyMaxBytes);
                InspectCatalog catalog = client.discover(InspectHttpJson.source(body));
                sendJson(exchange, 200, InspectHttpJson.discovery(catalog));
                return;
            }
            if ("POST".equals(method) && "/api/v1/sessions".equals(path)) {
                JsonObject body = readJsonObject(exchange, config.requestBodyMaxBytes);
                InspectSelection selection = InspectHttpJson.selection(body);
                InspectSession session = client.open(selection);
                String id = registry.add(session);
                try {
                    try (InspectSessionRegistry.Lease lease = registry.acquire(id)) {
                        sendJson(exchange, 201, sessionJson(id, lease.session()));
                    }
                } catch (IOException | RuntimeException error) {
                    try {
                        registry.delete(id);
                    } catch (RuntimeException closeError) {
                        error.addSuppressed(closeError);
                    }
                    throw error;
                }
                return;
            }
            if (path.startsWith("/api/v1/sessions/")) {
                routeSession(exchange, method, path);
                return;
            }
            if (path.startsWith("/api/v1/")) {
                sendError(
                        exchange,
                        404,
                        "ROUTE_NOT_FOUND",
                        "API route not found",
                        exchange.getResponseHeaders().getFirst("X-Request-Id"),
                        null);
                return;
            }
            serveStatic(exchange, method, path);
        }

        private void routeSession(HttpExchange exchange, String method, String path)
                throws IOException {
            String remainder = path.substring("/api/v1/sessions/".length());
            String[] parts = remainder.split("/", -1);
            if (parts.length == 0 || parts[0].isEmpty() || parts.length > 2) {
                throw new InspectSessionRegistry.SessionMissingException(
                        "Unknown or expired inspect session");
            }
            String id = parts[0];
            String operation = parts.length == 1 ? "" : parts[1];
            if ("DELETE".equals(method) && operation.isEmpty()) {
                if (!registry.delete(id)) {
                    throw new InspectSessionRegistry.SessionMissingException(
                            "Unknown or expired inspect session");
                }
                exchange.sendResponseHeaders(204, -1);
                return;
            }
            try (InspectSessionRegistry.Lease lease = registry.acquire(id)) {
                InspectSession session = lease.session();
                if ("GET".equals(method) && operation.isEmpty()) {
                    sendJson(exchange, 200, sessionJson(id, session));
                    return;
                }
                if ("GET".equals(method) && "overview".equals(operation)) {
                    sendJson(exchange, 200, InspectHttpJson.overview(session.overview()));
                    return;
                }
                if ("POST".equals(method) && "scan".equals(operation)) {
                    JsonObject body = readJsonObject(exchange, config.requestBodyMaxBytes);
                    ScanRequest request =
                            InspectHttpJson.scanRequest(
                                    body, id, config.inspectDefaultLimit, config.inspectMaxLimit);
                    InspectPage page = session.scan(request);
                    sendJson(exchange, 200, InspectHttpJson.page(id, page));
                    return;
                }
                if ("POST".equals(method) && "lookup".equals(operation)) {
                    JsonObject body = readJsonObject(exchange, config.requestBodyMaxBytes);
                    LookupRequest request =
                            InspectHttpJson.lookupRequest(body, config.lookupMaxKeys);
                    LookupResult result = session.lookup(request);
                    sendJson(exchange, 200, InspectHttpJson.lookup(result));
                    return;
                }
            }
            throw new RouteException(404, "Session operation not found");
        }

        private JsonObject sessionJson(String id, InspectSession session) {
            JsonObject output = InspectHttpJson.session(id, session);
            output.addProperty("total_buckets", config.totalBuckets);
            return output;
        }

        private void serveStatic(HttpExchange exchange, String method, String path)
                throws IOException {
            if (!"GET".equals(method)) {
                throw new RouteException(405, "Method not allowed");
            }
            String resource = path;
            if (resource == null || resource.isEmpty() || "/".equals(resource)) {
                resource = "/index.html";
            } else if (!hasFileExtension(resource)) {
                resource = "/index.html";
            }
            byte[] payload = StaticWebResources.read(resource);
            if (payload == null) throw new RouteException(404, "Static resource not found");
            if ("/index.html".equals(resource) && config.checkpointRoot != null) {
                String html = new String(payload, StandardCharsets.UTF_8);
                String initialSource =
                        "<script>window.COBBLE_MONITOR_INITIAL_SOURCE = "
                                + GSON.toJson(config.checkpointRoot)
                                + ";</script>\n    ";
                payload =
                        html.replace(
                                        "<script src=\"/app.js",
                                        initialSource + "<script src=\"/app.js")
                                .getBytes(StandardCharsets.UTF_8);
            }
            Headers headers = exchange.getResponseHeaders();
            headers.set("Content-Type", contentType(resource));
            headers.set("Cache-Control", cacheControl(resource));
            exchange.sendResponseHeaders(200, payload.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(payload);
            }
        }

        private String requestId() {
            byte[] bytes = new byte[12];
            random.nextBytes(bytes);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        }
    }

    private static JsonObject readJsonObject(HttpExchange exchange, int maxBytes)
            throws IOException {
        String length = exchange.getRequestHeaders().getFirst("Content-Length");
        if (length != null) {
            try {
                if (Long.parseLong(length) > maxBytes) {
                    throw new PayloadTooLargeException(
                            "JSON request body exceeds " + maxBytes + " bytes");
                }
            } catch (NumberFormatException error) {
                throw new MalformedJsonException("Invalid Content-Length");
            }
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream(Math.min(maxBytes, 8192));
        byte[] buffer = new byte[8192];
        int total = 0;
        try (InputStream input = exchange.getRequestBody()) {
            int read;
            while ((read = input.read(buffer)) >= 0) {
                total += read;
                if (total > maxBytes) {
                    throw new PayloadTooLargeException(
                            "JSON request body exceeds " + maxBytes + " bytes");
                }
                output.write(buffer, 0, read);
            }
        }
        String body = decodeUtf8(output.toByteArray());
        if (body.trim().isEmpty()) return new JsonObject();
        try {
            JsonElement parsed = JsonParser.parseString(body);
            if (!parsed.isJsonObject())
                throw new MalformedJsonException("JSON body must be an object");
            return parsed.getAsJsonObject();
        } catch (JsonParseException error) {
            throw new MalformedJsonException("Malformed JSON request body");
        }
    }

    private static String decodeUtf8(byte[] bytes) {
        CharsetDecoder decoder =
                StandardCharsets.UTF_8
                        .newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return decoder.decode(java.nio.ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException error) {
            throw new MalformedJsonException("JSON request body must be UTF-8");
        }
    }

    private static void sendInspectError(
            HttpExchange exchange, InspectException error, String requestId) throws IOException {
        InspectErrorCode code = error.errorCode();
        if (code == InspectErrorCode.INVALID_INPUT) {
            sendError(exchange, 400, "INVALID_INPUT", safeMessage(error), requestId, null);
        } else if (code == InspectErrorCode.NOT_FOUND) {
            String wireCode =
                    safeMessage(error).startsWith("Unknown inspect target")
                            ? "UNKNOWN_TARGET"
                            : "NOT_FOUND";
            sendError(exchange, 404, wireCode, safeMessage(error), requestId, null);
        } else if (code == InspectErrorCode.UNSUPPORTED) {
            sendError(exchange, 422, "UNSUPPORTED", safeMessage(error), requestId, null);
        } else if (code == InspectErrorCode.CLOSED) {
            sendError(
                    exchange,
                    410,
                    "SESSION_EXPIRED",
                    "The inspect session is closed",
                    requestId,
                    null);
        } else if (code == InspectErrorCode.CHECKPOINT_UNAVAILABLE) {
            sendError(
                    exchange,
                    410,
                    "CHECKPOINT_UNAVAILABLE",
                    "The checkpoint is incomplete or expired",
                    requestId,
                    null);
        } else if (code == InspectErrorCode.UNREADABLE) {
            sendError(
                    exchange,
                    422,
                    "SOURCE_UNREADABLE",
                    "The source could not be discovered or opened",
                    requestId,
                    null);
        } else {
            sendError(
                    exchange, 500, "INTERNAL_ERROR", "The inspect request failed", requestId, null);
        }
    }

    private static String safeMessage(InspectException error) {
        String value = error.getMessage();
        if (value == null || value.trim().isEmpty()) return "Inspect request failed";
        return safeTransportMessage(
                value,
                error.errorCode() == InspectErrorCode.NOT_FOUND
                        ? "The requested inspect resource was not found"
                        : "The inspect request is invalid");
    }

    private static String safeTransportMessage(String value, String fallback) {
        if (value == null || value.trim().isEmpty()) return fallback;
        return value.matches("(?s).*\\b[a-zA-Z][a-zA-Z0-9+.-]*://.*") ? fallback : value;
    }

    private static void sendError(
            HttpExchange exchange,
            int status,
            String code,
            String message,
            String requestId,
            JsonObject details)
            throws IOException {
        JsonObject output = new JsonObject();
        output.addProperty("code", code);
        output.addProperty("message", message);
        output.addProperty("request_id", requestId);
        if (details != null) output.add("details", details);
        sendJson(exchange, status, output);
    }

    private static void sendJson(HttpExchange exchange, int status, JsonElement body)
            throws IOException {
        byte[] payload = GSON.toJson(body).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, payload.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(payload);
        }
    }

    private static boolean hasFileExtension(String path) {
        int slash = path.lastIndexOf('/');
        return path.lastIndexOf('.') > slash;
    }

    private static String contentType(String path) {
        if (path.endsWith(".html")) return "text/html; charset=utf-8";
        if (path.endsWith(".js")) return "application/javascript; charset=utf-8";
        if (path.endsWith(".css")) return "text/css; charset=utf-8";
        if (path.endsWith(".json")) return "application/json; charset=utf-8";
        if (path.endsWith(".yaml") || path.endsWith(".yml")) {
            return "application/yaml; charset=utf-8";
        }
        return "application/octet-stream";
    }

    private static String cacheControl(String path) {
        return path.endsWith("index.html") ? "no-store" : "public, max-age=300";
    }

    private static final class PayloadTooLargeException extends RuntimeException {
        private PayloadTooLargeException(String message) {
            super(message);
        }
    }

    private static final class MalformedJsonException extends RuntimeException {
        private MalformedJsonException(String message) {
            super(message);
        }
    }

    private static final class RouteException extends RuntimeException {
        private final int status;

        private RouteException(int status, String message) {
            super(message);
            this.status = status;
        }
    }
}
