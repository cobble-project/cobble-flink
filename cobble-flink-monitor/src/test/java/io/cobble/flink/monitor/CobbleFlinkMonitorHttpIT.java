package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleSnapshotMetadataCodec;
import io.cobble.flink.common.CobbleSnapshotMetadataPayload;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.inspect.internal.InspectTarget;
import io.cobble.flink.inspect.internal.SinkInspectDecoder;
import io.cobble.structured.Db;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

class CobbleFlinkMonitorHttpIT {
    @TempDir private Path tempDir;

    @Test
    void realServerSupportsIndependentSessionsScanTypedAndRawLookupAndErrors() throws Exception {
        Path sink = tempDir.resolve("sink");
        Path raw = tempDir.resolve("raw");
        SinkInspectSchema schema =
                new SinkInspectSchema(
                        Collections.singletonList(SinkInspectField.key("id", "VARCHAR", 0, -1)),
                        Collections.singletonList(
                                SinkInspectField.value("payload", "VARCHAR", 1, 0)));
        byte[] encoded =
                SinkInspectDecoder.encodeKeyPrefix(
                        InspectTarget.sink("sink", schema), Collections.singletonList("target"));
        writeTable(sink, 7L, encoded, bytes("malformed"));
        writeRegistry(sink, 7L, schema);
        writeTable(raw, 11L, bytes("alpha"), bytes("omega"));

        ServerConfig config = new ServerConfig();
        config.port = 0;
        config.totalBuckets = 1;
        config.inspectMaxLimit = 2;
        config.inspectDefaultLimit = 1;
        config.lookupMaxKeys = 2;
        config.requestBodyMaxBytes = 1024;
        config.sessionIdleTimeoutSeconds = 2;
        config.maxSessions = 4;
        config.checkpointRoot = sink.toUri().toString();

        try (CobbleFlinkMonitorServer.RunningServer server =
                CobbleFlinkMonitorServer.start(config)) {
            URI base = URI.create("http://127.0.0.1:" + server.address().getPort());
            HttpClient http = HttpClient.newHttpClient();
            HttpClient secondHttp = HttpClient.newHttpClient();

            assertEquals(200, get(http, base.resolve("/healthz")).statusCode());
            String index = get(http, base.resolve("/")).body();
            assertTrue(index.contains("COBBLE_MONITOR_INITIAL_SOURCE"));
            assertTrue(index.contains(sink.toUri().toString()));
            assertEquals(
                    "MALFORMED_JSON",
                    json(post(http, base.resolve("/api/v1/discovery"), "{").body())
                            .get("code")
                            .getAsString());
            HttpResponse<String> coercedSource =
                    post(http, base.resolve("/api/v1/discovery"), "{\"source\":12}");
            assertEquals(400, coercedSource.statusCode());
            assertEquals("INVALID_INPUT", json(coercedSource.body()).get("code").getAsString());
            HttpResponse<String> oversized =
                    post(
                            http,
                            base.resolve("/api/v1/discovery"),
                            "{\"source\":\"" + "x".repeat(1100) + "\"}");
            assertEquals(413, oversized.statusCode());
            assertEquals("REQUEST_TOO_LARGE", json(oversized.body()).get("code").getAsString());

            JsonObject discovered =
                    json(
                            post(
                                            http,
                                            base.resolve("/api/v1/discovery"),
                                            "{\"source\":\"" + sink.toUri() + "\"}")
                                    .body());
            assertTrue(discovered.get("available").getAsBoolean());
            assertEquals(
                    7L,
                    discovered
                            .getAsJsonArray("checkpoints")
                            .get(0)
                            .getAsJsonObject()
                            .get("checkpoint_id")
                            .getAsLong());
            assertEquals(0, server.sessionCount(), "discovery must not retain a reader");

            JsonObject sinkSession = createSession(http, base, sink);
            JsonObject rawSession = createSession(secondHttp, base, raw);
            String sinkId = sinkSession.get("session_id").getAsString();
            String rawId = rawSession.get("session_id").getAsString();
            String sinkTarget =
                    sinkSession
                            .getAsJsonArray("targets")
                            .get(0)
                            .getAsJsonObject()
                            .get("id")
                            .getAsString();
            String rawTarget =
                    rawSession
                            .getAsJsonArray("targets")
                            .get(0)
                            .getAsJsonObject()
                            .get("id")
                            .getAsString();
            assertNotEquals(sinkId, rawId);
            assertTrue(sinkId.length() >= 22);
            assertEquals(7L, sinkSession.get("checkpoint_id").getAsLong());
            assertEquals(11L, rawSession.get("checkpoint_id").getAsLong());

            writeTable(raw, 12L, bytes("beta"), bytes("zeta"));
            JsonObject refreshedCatalog =
                    json(
                            post(
                                            secondHttp,
                                            base.resolve("/api/v1/discovery"),
                                            "{\"source\":\"" + raw.toUri() + "\"}")
                                    .body());
            assertEquals(
                    12L,
                    refreshedCatalog
                            .getAsJsonArray("checkpoints")
                            .get(0)
                            .getAsJsonObject()
                            .get("checkpoint_id")
                            .getAsLong());
            JsonObject replacement = createSession(secondHttp, base, raw);
            assertEquals(12L, replacement.get("checkpoint_id").getAsLong());
            assertEquals(
                    11L,
                    json(get(http, base.resolve("/api/v1/sessions/" + rawId)).body())
                            .get("checkpoint_id")
                            .getAsLong());
            assertEquals(
                    204,
                    delete(
                                    secondHttp,
                                    base.resolve(
                                            "/api/v1/sessions/"
                                                    + replacement.get("session_id").getAsString()))
                            .statusCode());

            JsonObject extraA = createSession(http, base, raw);
            JsonObject extraB = createSession(secondHttp, base, raw);
            HttpResponse<String> sessionLimit =
                    post(
                            http,
                            base.resolve("/api/v1/sessions"),
                            "{\"source\":\"" + raw.toUri() + "\",\"checkpoint\":\"latest\"}");
            assertEquals(429, sessionLimit.statusCode());
            assertEquals("SESSION_LIMIT", json(sessionLimit.body()).get("code").getAsString());
            assertEquals(
                    204,
                    delete(
                                    http,
                                    base.resolve(
                                            "/api/v1/sessions/"
                                                    + extraA.get("session_id").getAsString()))
                            .statusCode());
            assertEquals(
                    204,
                    delete(
                                    secondHttp,
                                    base.resolve(
                                            "/api/v1/sessions/"
                                                    + extraB.get("session_id").getAsString()))
                            .statusCode());

            JsonObject overview =
                    json(
                            get(http, base.resolve("/api/v1/sessions/" + sinkId + "/overview"))
                                    .body());
            String ddl =
                    overview.getAsJsonArray("items")
                            .get(0)
                            .getAsJsonObject()
                            .getAsJsonObject("source_sql")
                            .get("ddl")
                            .getAsString();
            assertTrue(ddl.contains("'scan.checkpoint-id' = '7'"));

            CompletableFuture<HttpResponse<String>> sinkScan =
                    CompletableFuture.supplyAsync(
                            () ->
                                    postUnchecked(
                                            http,
                                            base.resolve("/api/v1/sessions/" + sinkId + "/scan"),
                                            "{\"target_id\":\"" + sinkTarget + "\",\"limit\":1}"));
            CompletableFuture<HttpResponse<String>> rawScan =
                    CompletableFuture.supplyAsync(
                            () ->
                                    postUnchecked(
                                            secondHttp,
                                            base.resolve("/api/v1/sessions/" + rawId + "/scan"),
                                            "{\"target_id\":\"" + rawTarget + "\",\"limit\":1}"));
            assertEquals(200, sinkScan.get().statusCode());
            assertEquals(200, rawScan.get().statusCode());

            HttpResponse<String> capped =
                    post(
                            http,
                            base.resolve("/api/v1/sessions/" + rawId + "/scan"),
                            "{\"target_id\":\"" + rawTarget + "\",\"limit\":3}");
            assertEquals(400, capped.statusCode());

            JsonObject firstPage =
                    json(
                            post(
                                            http,
                                            base.resolve("/api/v1/sessions/" + rawId + "/scan"),
                                            "{\"target_id\":\"" + rawTarget + "\",\"limit\":1}")
                                    .body());
            assertEquals(1, firstPage.getAsJsonArray("rows").size());
            String token = firstPage.get("next_page_token").getAsString();
            HttpResponse<String> wrongSessionToken =
                    post(
                            http,
                            base.resolve("/api/v1/sessions/" + sinkId + "/scan"),
                            "{\"target_id\":\""
                                    + sinkTarget
                                    + "\",\"limit\":1,\"page_token\":\""
                                    + token
                                    + "\"}");
            assertEquals(400, wrongSessionToken.statusCode());
            assertTrue(
                    json(wrongSessionToken.body())
                            .get("message")
                            .getAsString()
                            .contains("different session"));
            HttpResponse<String> credentialTarget =
                    post(
                            http,
                            base.resolve("/api/v1/sessions/" + rawId + "/scan"),
                            "{\"target_id\":\"s3://AK:SECRET@example/path\",\"limit\":1}");
            assertEquals(404, credentialTarget.statusCode());
            assertFalse(credentialTarget.body().contains("SECRET"));

            JsonObject rawLookup =
                    json(
                            post(
                                            http,
                                            base.resolve("/api/v1/sessions/" + rawId + "/lookup"),
                                            "{\"target_id\":\""
                                                    + rawTarget
                                                    + "\",\"keys\":["
                                                    + "{\"kind\":\"raw\",\"bucket\":0,\"key_b64\":\"YWxwaGE=\"},"
                                                    + "{\"kind\":\"raw\",\"bucket\":0,\"key_b64\":\"bWlzc2luZw==\"}]}")
                                    .body());
            assertTrue(
                    rawLookup
                            .getAsJsonArray("rows")
                            .get(0)
                            .getAsJsonObject()
                            .get("found")
                            .getAsBoolean());
            assertFalse(
                    rawLookup
                            .getAsJsonArray("rows")
                            .get(1)
                            .getAsJsonObject()
                            .get("found")
                            .getAsBoolean());
            HttpResponse<String> oversizedBatch =
                    post(
                            secondHttp,
                            base.resolve("/api/v1/sessions/" + rawId + "/lookup"),
                            "{\"target_id\":\""
                                    + rawTarget
                                    + "\",\"keys\":["
                                    + "{\"kind\":\"raw\",\"bucket\":0,\"key_b64\":\"YQ==\"},"
                                    + "{\"kind\":\"raw\",\"bucket\":0,\"key_b64\":\"Yg==\"},"
                                    + "{\"kind\":\"raw\",\"bucket\":0,\"key_b64\":\"Yw==\"}]}");
            assertEquals(400, oversizedBatch.statusCode());

            JsonObject typedLookup =
                    json(
                            post(
                                            http,
                                            base.resolve("/api/v1/sessions/" + sinkId + "/lookup"),
                                            "{\"target_id\":\""
                                                    + sinkTarget
                                                    + "\",\"keys\":[{\"kind\":\"sink\","
                                                    + "\"fields\":[{\"name\":\"id\",\"value\":{\"kind\":\"STRING\",\"value\":\"target\"}}]}]}")
                                    .body());
            assertTrue(
                    typedLookup
                            .getAsJsonArray("rows")
                            .get(0)
                            .getAsJsonObject()
                            .get("found")
                            .getAsBoolean());

            JsonObject malformedRowPage =
                    json(
                            post(
                                            http,
                                            base.resolve("/api/v1/sessions/" + sinkId + "/scan"),
                                            "{\"target_id\":\"" + sinkTarget + "\",\"limit\":2}")
                                    .body());
            assertEquals(
                    200,
                    post(
                                    http,
                                    base.resolve("/api/v1/sessions/" + sinkId + "/scan"),
                                    "{\"target_id\":\"" + sinkTarget + "\",\"limit\":2}")
                            .statusCode());
            boolean hasDecodeIssue = false;
            for (JsonElement element : malformedRowPage.getAsJsonArray("rows")) {
                hasDecodeIssue |=
                        element.getAsJsonObject().getAsJsonArray("decode_issues").size() > 0;
            }
            assertTrue(hasDecodeIssue);

            assertEquals(204, delete(http, base.resolve("/api/v1/sessions/" + rawId)).statusCode());
            HttpResponse<String> expired = get(http, base.resolve("/api/v1/sessions/" + rawId));
            assertEquals(410, expired.statusCode());
            assertEquals("SESSION_EXPIRED", json(expired.body()).get("code").getAsString());

            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(6);
            while (server.sessionCount() != 0 && System.nanoTime() < deadline) {
                Thread.sleep(50L);
            }
            assertEquals(0, server.sessionCount(), "periodic reaper must retire idle sessions");
            HttpResponse<String> idleExpired =
                    get(http, base.resolve("/api/v1/sessions/" + sinkId));
            assertEquals(410, idleExpired.statusCode());
            assertEquals("SESSION_EXPIRED", json(idleExpired.body()).get("code").getAsString());
        }
    }

    @Test
    void embeddedCheckpointWithoutSidecarsSupportsHttpLifecycleAndSchemaAwareScan()
            throws Exception {
        OperatorID operatorId = new OperatorID(17L, 19L);
        try (EmbeddedCheckpoint fixture = writeEmbeddedCheckpoint(operatorId)) {
            ServerConfig config = new ServerConfig();
            config.port = 0;
            config.totalBuckets = 1;
            try (CobbleFlinkMonitorServer.RunningServer server =
                    CobbleFlinkMonitorServer.start(config)) {
                URI base = URI.create("http://127.0.0.1:" + server.address().getPort());
                HttpClient http = HttpClient.newHttpClient();
                String metadata = fixture.directory.resolve("_metadata").toUri().toString();

                JsonObject discovery =
                        json(
                                post(
                                                http,
                                                base.resolve("/api/v1/discovery"),
                                                "{\"source\":\"" + metadata + "\"}")
                                        .body());
                assertEquals("checkpoint", discovery.get("source_kind").getAsString());
                JsonObject discoveredOperator =
                        discovery
                                .getAsJsonArray("checkpoints")
                                .get(0)
                                .getAsJsonObject()
                                .getAsJsonArray("operators")
                                .get(0)
                                .getAsJsonObject();
                assertFalse(discoveredOperator.get("global_snapshot_available").getAsBoolean());
                assertTrue(discoveredOperator.get("embedded_metadata_available").getAsBoolean());

                HttpResponse<String> created =
                        post(
                                http,
                                base.resolve("/api/v1/sessions"),
                                "{\"source\":\"" + metadata + "\",\"checkpoint\":\"latest\"}");
                assertEquals(201, created.statusCode(), created.body());
                JsonObject session = json(created.body());
                assertEquals(3L, session.get("checkpoint_id").getAsLong());
                assertEquals(operatorId.toHexString(), session.get("operator_id").getAsString());
                String sessionId = session.get("session_id").getAsString();
                String targetId =
                        session.getAsJsonArray("targets")
                                .get(0)
                                .getAsJsonObject()
                                .get("id")
                                .getAsString();
                String temporaryPrefix =
                        "cobble-flink-embedded-checkpoint-3-" + operatorId.toHexString();
                List<Path> temporaryDirectories;
                try (java.util.stream.Stream<Path> paths =
                        Files.list(java.nio.file.Paths.get(System.getProperty("java.io.tmpdir")))) {
                    temporaryDirectories =
                            paths.filter(
                                            path ->
                                                    path.getFileName()
                                                            .toString()
                                                            .startsWith(temporaryPrefix))
                                    .collect(Collectors.toList());
                }
                assertFalse(temporaryDirectories.isEmpty());

                JsonObject overview =
                        json(
                                get(
                                                http,
                                                base.resolve(
                                                        "/api/v1/sessions/"
                                                                + sessionId
                                                                + "/overview"))
                                        .body());
                JsonObject item = overview.getAsJsonArray("items").get(0).getAsJsonObject();
                assertEquals("VALUE", item.get("kind").getAsString());
                assertTrue(
                        item.getAsJsonObject("source_sql")
                                .get("ddl")
                                .getAsString()
                                .contains("'scan.checkpoint-id' = '3'"));

                JsonObject page =
                        json(
                                post(
                                                http,
                                                base.resolve(
                                                        "/api/v1/sessions/" + sessionId + "/scan"),
                                                "{\"target_id\":\"" + targetId + "\",\"limit\":10}")
                                        .body());
                assertEquals(1, page.getAsJsonArray("rows").size());
                assertTrue(
                        page.getAsJsonArray("rows")
                                .get(0)
                                .getAsJsonObject()
                                .get("found")
                                .getAsBoolean());

                assertEquals(
                        204,
                        delete(http, base.resolve("/api/v1/sessions/" + sessionId)).statusCode());
                for (Path directory : temporaryDirectories) {
                    assertFalse(Files.exists(directory));
                }
                assertEquals(
                        410, get(http, base.resolve("/api/v1/sessions/" + sessionId)).statusCode());
            }
        }
    }

    @Test
    void openApiContainsEveryPublicRouteAndExamples() throws Exception {
        String contract =
                new String(
                        CobbleFlinkMonitorHttpIT.class
                                .getResourceAsStream("/openapi/cobble-inspect-v1.yaml")
                                .readAllBytes(),
                        StandardCharsets.UTF_8);
        for (String route :
                Arrays.asList(
                        "/healthz:",
                        "/api/v1/discovery:",
                        "/api/v1/sessions:",
                        "/api/v1/sessions/{session_id}:",
                        "/api/v1/sessions/{session_id}/overview:",
                        "/api/v1/sessions/{session_id}/scan:",
                        "/api/v1/sessions/{session_id}/lookup:")) {
            assertTrue(contract.contains(route), route);
        }
        assertTrue(contract.contains("example:"));
        assertTrue(contract.contains("request_id"));
        assertTrue(contract.contains("DiscoveryResponse:"));
        assertTrue(contract.contains("Session:"));
        assertTrue(contract.contains("TypedValue:"));
        assertTrue(contract.contains("DecodedValue:"));
        assertTrue(contract.contains("key_b64"));
        assertTrue(contract.contains("next_page_token"));
        assertTrue(contract.contains("CHECKPOINT_UNAVAILABLE"));
    }

    private JsonObject createSession(HttpClient http, URI base, Path source) throws Exception {
        HttpResponse<String> response =
                post(
                        http,
                        base.resolve("/api/v1/sessions"),
                        "{\"source\":\"" + source.toUri() + "\",\"checkpoint\":\"latest\"}");
        assertEquals(201, response.statusCode(), response.body());
        return json(response.body());
    }

    private void writeTable(Path table, long snapshot, byte[] first, byte[] second)
            throws Exception {
        ShardSnapshot shard;
        try (Db db = Db.open(config(table, true), 0, 0)) {
            db.put(0, first, 0, bytes("one"));
            db.put(0, second, 0, bytes("two"));
            shard = db.snapshot();
        }
        try (DbCoordinator coordinator = DbCoordinator.open(config(table, false))) {
            coordinator.materializeGlobalSnapshot(1, snapshot, Collections.singletonList(shard));
        }
    }

    private void writeRegistry(Path table, long snapshot, SinkInspectSchema schema)
            throws Exception {
        byte[] blob = SinkInspectSchemaStore.of(schema).toBytes();
        String hash = InspectSchemaRegistryLayout.sha256(blob);
        Path events = Files.createDirectories(table.resolve("inspect-schema/events"));
        Path blobs = Files.createDirectories(table.resolve("inspect-schema/blobs"));
        Files.write(blobs.resolve(InspectSchemaRegistryLayout.blobFileName(hash)), blob);
        Files.write(
                events.resolve(InspectSchemaRegistryLayout.eventFileName(snapshot, hash)),
                new byte[0]);
    }

    private EmbeddedCheckpoint writeEmbeddedCheckpoint(OperatorID operatorId) throws Exception {
        Path directory = Files.createDirectory(tempDir.resolve("chk-3"));
        Path volume = Files.createDirectory(directory.resolve("volume"));
        Config nativeConfig = config(volume, true);
        Db db = Db.open(nativeConfig, 0, 0);
        DataOutputSerializer key = new DataOutputSerializer(8);
        IntSerializer.INSTANCE.serialize(7, key);
        VoidNamespaceSerializer.INSTANCE.serialize(VoidNamespace.INSTANCE, key);
        DataOutputSerializer value = new DataOutputSerializer(4);
        IntSerializer.INSTANCE.serialize(42, value);
        db.put(0, key.getCopyOfBuffer(), 0, value.getCopyOfBuffer());
        ShardSnapshot shard = db.asyncSnapshot().get();
        db.retainSnapshot(shard.snapshotId);

        StateInspectSchema stateSchema =
                StateInspectSchema.forValue(
                        "state",
                        "default",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        LinkedHashMap<String, StateInspectSemanticSchema> semantic = new LinkedHashMap<>();
        semantic.put(
                "state",
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("INT")));
        Path taskState = directory.resolve("task-state");
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(taskState))) {
            CobbleSnapshotMetadataCodec.write(
                    new CobbleSnapshotMetadataPayload(
                            shard,
                            false,
                            Collections.emptyList(),
                            new StateInspectSchemaStore(
                                    Collections.singletonList(stateSchema), semantic)),
                    new DataOutputViewStreamWrapper(output));
        }
        IncrementalRemoteKeyedStateHandle handle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        new KeyGroupRange(0, 0),
                        3L,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new FileStateHandle(
                                new org.apache.flink.core.fs.Path(taskState.toUri()),
                                Files.size(taskState)));
        OperatorState operator = new OperatorState(operatorId, 1, 1);
        operator.putState(
                0,
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(
                                StateObjectCollection.<KeyedStateHandle>singleton(handle))
                        .build());
        try (DataOutputStream output =
                new DataOutputStream(Files.newOutputStream(directory.resolve("_metadata")))) {
            Checkpoints.storeCheckpointMetadata(
                    new CheckpointMetadata(
                            3L, Collections.singletonList(operator), Collections.emptyList()),
                    output);
        }
        return new EmbeddedCheckpoint(directory, db);
    }

    private Config config(Path table, boolean data) {
        Config config = new Config().numColumns(1).totalBuckets(1);
        config.governanceMode = Config.GovernanceMode.NOOP;
        config.logConsole = false;
        config.logPath = tempDir.resolve(data ? "writer.log" : "coordinator.log").toString();
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = table.toAbsolutePath().toString();
        volume.kinds =
                data
                        ? Arrays.asList(
                                Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                                Config.VolumeUsageKind.META,
                                Config.VolumeUsageKind.SNAPSHOT)
                        : Arrays.asList(
                                Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        config.addVolume(volume);
        return config;
    }

    private static HttpResponse<String> get(HttpClient client, URI uri) throws Exception {
        return client.send(
                HttpRequest.newBuilder(uri).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> delete(HttpClient client, URI uri) throws Exception {
        return client.send(
                HttpRequest.newBuilder(uri).DELETE().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> post(HttpClient client, URI uri, String body)
            throws Exception {
        return client.send(
                HttpRequest.newBuilder(uri)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> postUnchecked(HttpClient client, URI uri, String body) {
        try {
            return post(client, uri, body);
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
    }

    private static JsonObject json(String value) {
        return JsonParser.parseString(value).getAsJsonObject();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class EmbeddedCheckpoint implements AutoCloseable {
        private final Path directory;
        private final Db db;

        private EmbeddedCheckpoint(Path directory, Db db) {
            this.directory = directory;
            this.db = db;
        }

        @Override
        public void close() {
            db.close();
        }
    }
}
