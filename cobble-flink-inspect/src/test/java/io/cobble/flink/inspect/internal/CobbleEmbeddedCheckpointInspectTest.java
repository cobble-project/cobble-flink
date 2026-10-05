package io.cobble.flink.inspect.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleEmbeddedCheckpoint;
import io.cobble.flink.common.CobbleEmbeddedCheckpointReader;
import io.cobble.flink.common.CobbleFlinkFileSystemResolver;
import io.cobble.flink.common.CobbleFlinkStorageConfig;
import io.cobble.flink.common.CobbleSnapshotMetadataCodec;
import io.cobble.flink.common.CobbleSnapshotMetadataPayload;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.inspect.CobbleInspectClient;
import io.cobble.flink.inspect.InspectPage;
import io.cobble.flink.inspect.InspectSelection;
import io.cobble.flink.inspect.InspectSession;
import io.cobble.flink.inspect.LookupKey;
import io.cobble.flink.inspect.LookupRequest;
import io.cobble.flink.inspect.PageToken;
import io.cobble.flink.inspect.RawBytes;
import io.cobble.flink.inspect.ScanRequest;
import io.cobble.structured.Db;
import io.cobble.structured.PriorityQueue;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.PostVersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.streaming.api.operators.InternalTimersSnapshot;
import org.apache.flink.streaming.api.operators.InternalTimersSnapshotReaderWriters;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

class CobbleEmbeddedCheckpointInspectTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void explicitOptionsOverrideGlobalFilesystemAndEmptyOptionsUseIt() throws Exception {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        PluginManager plugins =
                new PluginManager() {
                    @Override
                    public <P> Iterator<P> load(Class<P> service) {
                        return service == FileSystemFactory.class
                                ? Collections.singletonList(
                                                service.cast(
                                                        new ConfiguredCheckpointFileSystem
                                                                .Factory()))
                                        .iterator()
                                : Collections.emptyIterator();
                    }
                };
        Configuration global = new Configuration();
        global.setString("s3.access-key", "global-access");
        global.setString("s3.secret-key", "global-secret");
        global.setString("s3.endpoint", "http://global-endpoint");
        try {
            Thread.currentThread()
                    .setContextClassLoader(
                            ConfiguredCheckpointFileSystem.classLoader(tempDir, original));
            try (ReadableCheckpoint fixture =
                    writeCheckpoint(new OperatorID(79L, 81L), false, true)) {
                Path metadata =
                        ConfiguredCheckpointFileSystem.remote(
                                fixture.directory.resolve("_metadata"));
                FileSystem.initialize(global, plugins);
                assertThrows(IOException.class, () -> CobbleEmbeddedCheckpoint.read(metadata));
                assertEquals(
                        3L,
                        CobbleEmbeddedCheckpoint.read(
                                        metadata, ConfiguredCheckpointFileSystem.options())
                                .checkpointId());
                java.nio.file.Path conf = Files.createDirectory(tempDir.resolve("flink-conf"));
                Files.write(
                        conf.resolve("flink-conf.yaml"),
                        Arrays.asList(
                                "s3.access-key: request-access", "s3.secret-key: request-secret",
                                "s3.endpoint: http://request-endpoint",
                                        "plugin.custom.setting: retained"),
                        StandardCharsets.UTF_8);
                try (CobbleInspectClient client =
                                CobbleInspectClient.builder()
                                        .flinkConfigPath(conf.toString())
                                        .storageOptions(
                                                CobbleConnectorStorageOptions.fromStorageOptions(
                                                        Collections.singletonMap(
                                                                "s3.region", "request-region")))
                                        .build();
                        InspectSession session =
                                client.open(
                                        InspectSelection.checkpoint(
                                                metadata.toString(), 3L, null))) {
                    assertEquals(1, session.scan(new ScanRequest("state", 10, null)).rows().size());
                }
                global.setString("s3.access-key", "request-access");
                global.setString("s3.secret-key", "request-secret");
                global.setString("s3.endpoint", "http://request-endpoint");
                FileSystem.initialize(global, plugins);
                assertEquals(3L, CobbleEmbeddedCheckpoint.read(metadata).checkpointId());
                assertEquals(
                        3L,
                        CobbleEmbeddedCheckpoint.read(
                                        metadata,
                                        CobbleConnectorStorageOptions.fromStorageOptions(
                                                Collections.singletonMap(
                                                        "s3.endpoint", "http://request-endpoint")))
                                .checkpointId());
                try (CobbleInspectClient client = CobbleInspectClient.builder().build()) {
                    assertEquals(
                            3L,
                            client.discover(metadata.toString())
                                    .checkpoints()
                                    .get(0)
                                    .checkpointId());
                }
            }
        } finally {
            CobbleFlinkFileSystemResolver.initialize(new Configuration());
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void scopedCredentialsReadRemoteCheckpointReferencesAndLegacyTimers() throws Exception {
        OperatorID operatorId = new OperatorID(71L, 73L);
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread()
                    .setContextClassLoader(
                            ConfiguredCheckpointFileSystem.classLoader(tempDir, original));
            try (ReadableCheckpoint fixture = writeCheckpoint(operatorId, true, true);
                    CobbleInspectClient client =
                            CobbleInspectClient.builder()
                                    .storageOptions(ConfiguredCheckpointFileSystem.options())
                                    .totalBuckets(4)
                                    .build()) {
                String remoteRoot = ConfiguredCheckpointFileSystem.remote(tempDir).toString();
                assertEquals(3L, client.discover(remoteRoot).checkpoints().get(0).checkpointId());
                Path metadata =
                        ConfiguredCheckpointFileSystem.remote(
                                fixture.directory.resolve("_metadata"));
                assertEquals(
                        3L,
                        CobbleEmbeddedCheckpoint.readLocation(
                                        metadata, ConfiguredCheckpointFileSystem.options())
                                .checkpoint()
                                .checkpointId());
                try (InspectSession session =
                        client.open(
                                InspectSelection.checkpoint(
                                        remoteRoot, 3L, operatorId.toHexString()))) {
                    assertEquals(1, session.scan(new ScanRequest("state", 10, null)).rows().size());
                    assertEquals(
                            5,
                            session.scan(
                                            new ScanRequest(
                                                    "timer:_timer_state/event_service", 10, null))
                                    .rows()
                                    .size());
                    assertEquals(
                            1,
                            session.scan(
                                            new ScanRequest(
                                                    "timer:_timer_state/processing_service",
                                                    10,
                                                    null))
                                    .rows()
                                    .size());
                }
            }
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void scopedCredentialsStageRemoteGlobalManifestAndSchemaRegistry() throws Exception {
        OperatorID operatorId = new OperatorID(75L, 77L);
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread()
                    .setContextClassLoader(
                            ConfiguredCheckpointFileSystem.classLoader(tempDir, original));
            try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
                CheckpointEntry local = mergedGlobalEntry(fixture, operatorId, true);
                OperatorEntry source = local.defaultOperator();
                Path remoteManifest =
                        ConfiguredCheckpointFileSystem.remote(
                                fixture.directory.resolve(
                                        "COBBLE-SNAPSHOT-"
                                                + operatorId.toHexString()
                                                + "-MANIFEST"));
                OperatorEntry remote =
                        new OperatorEntry(
                                source.operatorId,
                                remoteManifest.toString(),
                                source.operatorSnapshotDirectory,
                                source.readerVolumeDirectories,
                                true);
                CheckpointEntry checkpoint =
                        new CheckpointEntry(
                                3L,
                                ConfiguredCheckpointFileSystem.remote(fixture.directory).toString(),
                                Collections.singletonList(remote));
                writeRegistry(
                        operatorId.toHexString(), 3L, source.embeddedCheckpoint.schemaStore());
                SchemaResolveResult schema =
                        MonitorInspectSchemaResolver.resolve(
                                checkpoint, remote, ConfiguredCheckpointFileSystem.options());
                assertEquals(SchemaResolveResult.STATUS_AVAILABLE, schema.status);
                assertTrue(schema.eventPath.startsWith("s3://fixture"));
                List<ShardSnapshot> globalShards;
                try (MonitorReaderSession session =
                        MonitorReaderSession.open(
                                4,
                                ConfiguredCheckpointFileSystem.options(),
                                "checkpoint",
                                checkpoint,
                                remote)) {
                    assertEquals("value", utf8(session.reader().get(0, utf8("key"), 0)));
                    globalShards = session.reader().currentGlobalSnapshot().shardSnapshots;
                }
                Config readerConfig = nativeConfig(fixture.directory.resolve("volume"), 4);
                Config.VolumeDescriptor route =
                        Config.VolumeDescriptor.singleVolume(remoteManifest.getParent().toString());
                ConfiguredCheckpointFileSystem.options().applyTo(route);
                readerConfig.addVolume(route);
                try (CobbleEmbeddedCheckpointReader.Prepared prepared =
                        CobbleEmbeddedCheckpointReader.open(
                                readerConfig, 3L, 4, globalShards, remoteManifest.toString())) {
                    assertEquals("value", utf8(prepared.reader().get(0, utf8("key"), 0)));
                }
                // Native data remains local; exercise remote shard/schema staging separately so
                // this filesystem fixture never pretends to provide native S3 data access.
                ShardSnapshot shard = globalShards.get(0).copy();
                java.nio.file.Path manifest = Paths.get(URI.create(shard.manifestPath));
                shard.manifestPath = ConfiguredCheckpointFileSystem.remote(manifest).toString();
                for (Class<?> owner :
                        Arrays.asList(
                                MonitorReaderSession.class, CobbleEmbeddedCheckpointReader.class)) {
                    java.nio.file.Path staged = Files.createTempDirectory(tempDir, "staged-");
                    Method copy =
                            owner == MonitorReaderSession.class
                                    ? owner.getDeclaredMethod(
                                            "copyShardMetadata",
                                            ShardSnapshot.class,
                                            File.class,
                                            CobbleConnectorStorageOptions.class)
                                    : owner.getDeclaredMethod(
                                            "copyShardMetadata",
                                            ShardSnapshot.class,
                                            File.class,
                                            CobbleFlinkStorageConfig.class,
                                            CobbleConnectorStorageOptions.class);
                    copy.setAccessible(true);
                    if (owner == MonitorReaderSession.class) {
                        copy.invoke(
                                null,
                                shard,
                                staged.toFile(),
                                ConfiguredCheckpointFileSystem.options());
                    } else {
                        copy.invoke(
                                null,
                                shard,
                                staged.toFile(),
                                CobbleFlinkStorageConfig.empty(),
                                ConfiguredCheckpointFileSystem.options());
                    }
                    assertTrue(
                            Files.exists(
                                    staged.resolve(shard.dbId)
                                            .resolve("snapshot/SNAPSHOT-" + shard.snapshotId)));
                    java.nio.file.Path schemaDirectory =
                            manifest.getParent().getParent().resolve("schema");
                    try (Stream<java.nio.file.Path> files = Files.list(schemaDirectory)) {
                        for (java.nio.file.Path file :
                                (Iterable<java.nio.file.Path>) files::iterator) {
                            assertArrayEquals(
                                    Files.readAllBytes(file),
                                    Files.readAllBytes(
                                            staged.resolve(shard.dbId)
                                                    .resolve("schema")
                                                    .resolve(file.getFileName())));
                        }
                    }
                }
            }
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void timerScanMergesLegacyPrefixAndNativeTailWithStablePages() throws Exception {
        OperatorID operatorId = new OperatorID(51L, 53L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId, true);
                CobbleInspectClient client = CobbleInspectClient.builder().totalBuckets(4).build();
                InspectSession session =
                        client.open(
                                InspectSelection.checkpoint(
                                        fixture.directory.toUri().toString(),
                                        3L,
                                        operatorId.toHexString()))) {
            String target = "timer:_timer_state/event_service";
            List<Long> times = new ArrayList<>();
            PageToken token = null;
            do {
                InspectPage page = session.scan(new ScanRequest(target, 1, token));
                assertEquals(1, page.rows().size());
                times.add(
                        ByteBuffer.wrap(page.rows().get(0).key().value()).getLong()
                                ^ Long.MIN_VALUE);
                token = page.nextPageToken();
            } while (token != null);
            assertEquals(Arrays.asList(10L, 20L, 30L, 40L, 50L), times);
            assertEquals(
                    1,
                    session.scan(
                                    new ScanRequest(
                                            target, 10, null, 0, new RawBytes(timerKey(20)), null))
                            .rows()
                            .size());
            assertTrue(
                    session.lookup(
                                    new LookupRequest(
                                            target,
                                            Collections.singletonList(
                                                    new LookupKey(0, new RawBytes(timerKey(10))))))
                            .rows()
                            .get(0)
                            .found());
            String legacyOnly = "timer:_timer_state/processing_service";
            InspectPage page = session.scan(new ScanRequest(legacyOnly, 1, null));
            assertEquals(1, page.rows().size());
            assertEquals(1, page.rows().get(0).bucket());
            assertNull(page.nextPageToken());
            assertFalse(
                    session.targets().stream()
                            .filter(item -> item.id().equals(target))
                            .findFirst()
                            .get()
                            .exactLookupSupported());
        }
    }

    @Test
    void discoversAndReadsEmbeddedCheckpointWithoutSidecars() throws Exception {
        OperatorID operatorId = new OperatorID(11L, 13L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            InspectCatalogDiscovery.Result catalog =
                    InspectCatalogDiscovery.discover(
                            fixture.directory.toUri().toString(),
                            CobbleConnectorStorageOptions.empty());
            CheckpointEntry checkpoint = catalog.checkpoints.get(0);
            OperatorEntry operator = checkpoint.findOperator(operatorId.toHexString());
            assertTrue(
                    operator.readerVolumeDirectories.stream()
                            .anyMatch(root -> root.endsWith("/old-volume")));

            SchemaResolveResult schema = MonitorInspectSchemaResolver.resolve(checkpoint, operator);
            assertEquals("checkpoint", catalog.sourceKind);
            assertEquals(3L, checkpoint.id);
            assertEquals(SchemaResolveResult.STATUS_AVAILABLE, schema.status);
            assertEquals(1, schema.store.schemas().size());

            List<File> temporaryDirectories;
            try (MonitorReaderSession reader =
                    MonitorReaderSession.open(
                            4,
                            CobbleConnectorStorageOptions.empty(),
                            catalog.sourceKind,
                            checkpoint,
                            operator)) {
                temporaryDirectories = reader.temporaryDirectories();
                assertTrue(
                        temporaryDirectories.get(0).getName().startsWith("cobble-embedded-read-"));
                assertEquals("value", utf8(reader.reader().get(0, utf8("key"), 0)));
            }
            for (File directory : temporaryDirectories) {
                assertFalse(directory.exists());
            }
        }
    }

    @Test
    void globalEntryFallsBackToEmbeddedSchemaWhenRegistryIsAbsent() throws Exception {
        OperatorID operatorId = new OperatorID(21L, 23L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, false);

            SchemaResolveResult result =
                    MonitorInspectSchemaResolver.resolve(
                            merged, merged.findOperator(operatorId.toHexString()));

            assertEquals(SchemaResolveResult.STATUS_AVAILABLE, result.status);
            assertEquals("embedded Flink checkpoint metadata", result.eventPath);
            assertEquals(1, result.store.schemas().size());
            assertTrue(result.warning.contains("Schema registry missing"));
        }
    }

    @Test
    void matchingRegistryRemainsPreferredOverEmbeddedSchema() throws Exception {
        OperatorID operatorId = new OperatorID(25L, 27L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, false);
            StateInspectSchemaStore embedded =
                    CobbleEmbeddedCheckpoint.select(new Path(fixture.directory.toUri()), "3")
                            .checkpoint()
                            .operator(operatorId.toHexString())
                            .schemaStore();
            writeRegistry(operatorId.toHexString(), 3L, embedded);

            SchemaResolveResult result =
                    MonitorInspectSchemaResolver.resolve(
                            merged, merged.findOperator(operatorId.toHexString()));

            assertEquals(SchemaResolveResult.STATUS_AVAILABLE, result.status);
            assertTrue(result.eventPath.contains("inspect-schema/events"));
            assertEquals(3L, result.schemaCheckpointId);
            assertNull(result.warning);
        }
    }

    @Test
    void staleRegistryIsReplacedByExactEmbeddedSchema() throws Exception {
        OperatorID operatorId = new OperatorID(26L, 28L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, false);
            StateInspectSchemaStore stale =
                    new StateInspectSchemaStore(
                            Collections.singletonList(
                                    StateInspectSchema.forValue(
                                            "stale",
                                            "default",
                                            false,
                                            IntSerializer.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            IntSerializer.INSTANCE)));
            writeRegistry(operatorId.toHexString(), 2L, stale);

            SchemaResolveResult result =
                    MonitorInspectSchemaResolver.resolve(
                            merged, merged.findOperator(operatorId.toHexString()));

            assertEquals(SchemaResolveResult.STATUS_AVAILABLE, result.status);
            assertEquals("embedded Flink checkpoint metadata", result.eventPath);
            assertEquals("state", result.store.schemas().get(0).stateName());
            assertEquals(3L, result.schemaCheckpointId);
            assertTrue(result.warning.contains("stale or mismatched"));
        }
    }

    @Test
    void globalReaderRemainsPreferredWhenEmbeddedMetadataIsAttached() throws Exception {
        OperatorID operatorId = new OperatorID(29L, 31L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, true);
            List<File> temporaryDirectories;
            try (MonitorReaderSession reader = openReader(merged, operatorId)) {
                temporaryDirectories = reader.temporaryDirectories();
                assertTrue(
                        temporaryDirectories.get(0).getName().startsWith("cobble-flink-monitor-"));
                assertEquals("value", utf8(reader.reader().get(0, utf8("key"), 0)));
            }
            for (File directory : temporaryDirectories) {
                assertFalse(directory.exists());
            }
        }
    }

    @Test
    void embeddedReaderIsFallbackWhenGlobalReaderCannotOpen() throws Exception {
        OperatorID operatorId = new OperatorID(33L, 35L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            CheckpointEntry merged = mergedGlobalEntry(fixture, operatorId, false);
            List<File> temporaryDirectories;
            try (MonitorReaderSession reader = openReader(merged, operatorId)) {
                temporaryDirectories = reader.temporaryDirectories();
                assertTrue(
                        temporaryDirectories.get(0).getName().startsWith("cobble-embedded-read-"));
                assertEquals("value", utf8(reader.reader().get(0, utf8("key"), 0)));
            }
            for (File directory : temporaryDirectories) {
                assertFalse(directory.exists());
            }
        }
    }

    @Test
    void directMetadataDiscoversGlobalSidecarAndEmbeddedFallback() throws Exception {
        OperatorID operatorId = new OperatorID(37L, 39L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            mergedGlobalEntry(fixture, operatorId, true);

            InspectCatalogDiscovery.Result catalog =
                    InspectCatalogDiscovery.discover(
                            fixture.directory.resolve("_metadata").toUri().toString(),
                            CobbleConnectorStorageOptions.empty());
            OperatorEntry operator =
                    catalog.checkpoints.get(0).findOperator(operatorId.toHexString());

            assertTrue(operator.globalSnapshotLayout);
            assertTrue(operator.embeddedCheckpoint != null);
            assertTrue(operator.manifestCopyPath != null);
        }
    }

    @Test
    void directMetadataWithoutSidecarRemainsEmbeddedOnly() throws Exception {
        OperatorID operatorId = new OperatorID(41L, 43L);
        try (ReadableCheckpoint fixture = writeCheckpoint(operatorId)) {
            InspectCatalogDiscovery.Result catalog =
                    InspectCatalogDiscovery.discover(
                            fixture.directory.resolve("_metadata").toUri().toString(),
                            CobbleConnectorStorageOptions.empty());
            OperatorEntry operator =
                    catalog.checkpoints.get(0).findOperator(operatorId.toHexString());

            assertFalse(operator.globalSnapshotLayout);
            assertTrue(operator.embeddedCheckpoint != null);
        }
    }

    private CheckpointEntry mergedGlobalEntry(
            ReadableCheckpoint fixture, OperatorID operatorId, boolean materializeGlobalSnapshot)
            throws Exception {
        CobbleEmbeddedCheckpoint.Location location =
                CobbleEmbeddedCheckpoint.select(new Path(fixture.directory.toUri()), "3");
        java.nio.file.Path operatorPath =
                tempDir.resolve("cobble").resolve(operatorId.toHexString());
        String operatorDirectory = operatorPath.toUri().toString();
        if (materializeGlobalSnapshot) {
            Config coordinatorConfig = nativeConfig(operatorPath, 4);
            try (DbCoordinator coordinator = DbCoordinator.open(coordinatorConfig)) {
                coordinator.materializeGlobalSnapshot(
                        4, 3L, Collections.singletonList(fixture.db.snapshot()));
            }
            Files.copy(
                    operatorPath.resolve("snapshot/SNAPSHOT-3"),
                    fixture.directory.resolve(
                            "COBBLE-SNAPSHOT-" + operatorId.toHexString() + "-MANIFEST"));
        }
        CheckpointEntry sidecar =
                new CheckpointEntry(
                        3L,
                        fixture.directory.toUri().toString(),
                        Collections.singletonList(
                                new OperatorEntry(
                                        operatorId.toHexString(),
                                        null,
                                        operatorDirectory,
                                        Collections.singletonList(operatorDirectory),
                                        true)));
        return InspectCatalogDiscovery.mergeEmbeddedCheckpoint(sidecar, location);
    }

    private MonitorReaderSession openReader(CheckpointEntry checkpoint, OperatorID operatorId) {
        return MonitorReaderSession.open(
                4,
                CobbleConnectorStorageOptions.empty(),
                "checkpoint",
                checkpoint,
                checkpoint.findOperator(operatorId.toHexString()));
    }

    private void writeRegistry(String operatorId, long checkpointId, StateInspectSchemaStore store)
            throws Exception {
        byte[] bytes = store.toBytes();
        String hash = InspectSchemaRegistryLayout.sha256(bytes);
        java.nio.file.Path schema =
                tempDir.resolve("cobble").resolve(operatorId).resolve("inspect-schema");
        Files.createDirectories(schema.resolve("events"));
        Files.createDirectories(schema.resolve("blobs"));
        Files.write(
                schema.resolve("blobs").resolve(InspectSchemaRegistryLayout.blobFileName(hash)),
                bytes);
        Files.write(
                schema.resolve("events")
                        .resolve(InspectSchemaRegistryLayout.eventFileName(checkpointId, hash)),
                Arrays.asList("checkpoint_id=" + checkpointId),
                StandardCharsets.UTF_8);
    }

    private ReadableCheckpoint writeCheckpoint(OperatorID operatorId) throws Exception {
        return writeCheckpoint(operatorId, false);
    }

    private ReadableCheckpoint writeCheckpoint(OperatorID operatorId, boolean timers)
            throws Exception {
        return writeCheckpoint(operatorId, timers, false);
    }

    private ReadableCheckpoint writeCheckpoint(
            OperatorID operatorId, boolean timers, boolean remoteHandles) throws Exception {
        java.nio.file.Path directory = Files.createDirectory(tempDir.resolve("chk-3"));
        java.nio.file.Path volume = Files.createDirectory(directory.resolve("volume"));
        Config config = nativeConfig(volume, 4);
        Db db = Db.open(config, 0, 3);
        db.put(0, utf8("key"), 0, utf8("value"));
        if (timers) {
            try (PriorityQueue queue =
                    db.newPriorityQueue("__cobble_timer___timer_state/event_service")) {
                queue.offer(0, timerKey(20), new byte[0]);
                queue.offer(0, timerKey(30), new byte[0]);
                queue.offer(0, timerKey(40), new byte[0]);
                queue.offer(0, timerKey(50), new byte[0]);
            }
        }
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
        List<StateInspectSchema> schemas = new ArrayList<>(Collections.singletonList(stateSchema));
        if (timers) {
            for (String domain : Arrays.asList("event", "processing")) {
                String name = "_timer_state/" + domain + "_service";
                schemas.add(
                        StateInspectSchema.forTimer(
                                name,
                                "__cobble_timer__" + name,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE));
                semantic.put(
                        name,
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                StateInspectType.unknown()));
            }
        }
        java.nio.file.Path state = directory.resolve("task-state");
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(state))) {
            CobbleSnapshotMetadataCodec.write(
                    new CobbleSnapshotMetadataPayload(
                            Collections.singletonList(
                                    Files.createDirectories(tempDir.resolve("old-volume"))
                                            .toUri()
                                            .toString()),
                            shard,
                            false,
                            Collections.emptyList(),
                            new StateInspectSchemaStore(schemas, semantic)),
                    new DataOutputViewStreamWrapper(output));
        }
        IncrementalRemoteKeyedStateHandle handle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        new KeyGroupRange(0, 3),
                        3L,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        remoteHandles
                                ? timers
                                        ? new RelativeFileStateHandle(
                                                ConfiguredCheckpointFileSystem.remote(state),
                                                "task-state",
                                                Files.size(state))
                                        : new FileStateHandle(
                                                ConfiguredCheckpointFileSystem.remote(state),
                                                Files.size(state))
                                : new FileStateHandle(new Path(state.toUri()), Files.size(state)));
        OperatorState operator = new OperatorState(operatorId, 1, 4);
        OperatorSubtaskState.Builder subtask =
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(
                                StateObjectCollection.<KeyedStateHandle>singleton(handle));
        if (timers) {
            KeyGroupsStateHandle timerHandle = legacyTimers();
            if (remoteHandles) {
                java.nio.file.Path timerFile = directory.resolve("legacy-timers");
                Files.write(timerFile, timerHandle.asBytesIfInMemory().get());
                timerHandle =
                        new KeyGroupsStateHandle(
                                timerHandle.getGroupRangeOffsets(),
                                new RelativeFileStateHandle(
                                        ConfiguredCheckpointFileSystem.remote(timerFile),
                                        "legacy-timers",
                                        Files.size(timerFile)));
            }
            subtask.setRawKeyedState(
                    StateObjectCollection.<KeyedStateHandle>singleton(timerHandle));
        }
        operator.putState(0, subtask.build());
        try (DataOutputStream output =
                new DataOutputStream(Files.newOutputStream(directory.resolve("_metadata")))) {
            Checkpoints.storeCheckpointMetadata(
                    new CheckpointMetadata(
                            3L, Collections.singletonList(operator), Collections.emptyList()),
                    output);
        }
        return new ReadableCheckpoint(directory, db);
    }

    private static byte[] timerKey(long timestamp) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(32);
        new TimerSerializer<>(IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE)
                .serialize(
                        new TimerHeapInternalTimer<>(timestamp, 7, VoidNamespace.INSTANCE), output);
        return output.getCopyOfBuffer();
    }

    private static KeyGroupsStateHandle legacyTimers() throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(256);
        long[] offsets = new long[2];
        for (int group = 0; group < 2; group++) {
            offsets[group] = output.length();
            new PostVersionedIOReadableWritable() {
                @Override
                public int getVersion() {
                    return 2;
                }

                @Override
                protected void read(DataInputView input, boolean wasVersioned) {
                    throw new UnsupportedOperationException();
                }
            }.write(output);
            output.writeInt(1);
            output.writeUTF("service");
            Set<TimerHeapInternalTimer<Integer, VoidNamespace>> event = new HashSet<>();
            Set<TimerHeapInternalTimer<Integer, VoidNamespace>> processing = new HashSet<>();
            if (group == 0) {
                event.add(new TimerHeapInternalTimer<>(10L, 7, VoidNamespace.INSTANCE));
                event.add(new TimerHeapInternalTimer<>(20L, 7, VoidNamespace.INSTANCE));
            } else {
                processing.add(new TimerHeapInternalTimer<>(60L, 7, VoidNamespace.INSTANCE));
            }
            InternalTimersSnapshot<Integer, VoidNamespace> snapshot =
                    new InternalTimersSnapshot<>(
                            IntSerializer.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            event,
                            processing);
            InternalTimersSnapshotReaderWriters.getWriterForVersion(
                            2, snapshot, IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE)
                    .writeTimersSnapshot(output);
        }
        return new KeyGroupsStateHandle(
                new KeyGroupRangeOffsets(0, 1, offsets),
                new ByteStreamStateHandle("legacy-timers", output.getCopyOfBuffer()));
    }

    private Config nativeConfig(java.nio.file.Path volume, int totalBuckets) {
        Config config = new Config().numColumns(1).totalBuckets(totalBuckets);
        config.governanceMode = Config.GovernanceMode.NOOP;
        config.logConsole = false;
        config.addVolume(volume.toString());
        return config;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String utf8(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private static final class ReadableCheckpoint implements AutoCloseable {
        private final java.nio.file.Path directory;
        private final Db db;

        private ReadableCheckpoint(java.nio.file.Path directory, Db db) {
            this.directory = directory;
            this.db = db;
        }

        @Override
        public void close() {
            db.close();
        }
    }
}
