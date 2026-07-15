package io.cobble.flink.inspect.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;
import io.cobble.flink.inspect.CobbleInspectClient;
import io.cobble.flink.inspect.FieldValue;
import io.cobble.flink.inspect.InspectException;
import io.cobble.flink.inspect.InspectPage;
import io.cobble.flink.inspect.InspectSession;
import io.cobble.flink.inspect.LookupKey;
import io.cobble.flink.inspect.LookupRequest;
import io.cobble.flink.inspect.LookupResult;
import io.cobble.flink.inspect.ScanFilter;
import io.cobble.flink.inspect.ScanRequest;
import io.cobble.flink.inspect.TypedLookupKey;
import io.cobble.flink.inspect.TypedValue;
import io.cobble.structured.Db;

import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.StringDataSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class InspectTypedSinkIT {
    @TempDir private Path tempDir;

    @Test
    void typedScanFindsLateMatchAndTypedLookupRequiresCompletePk() throws Exception {
        Path table = tempDir.resolve("sink");
        int buckets = 1;
        long snapshot = 8L;
        SinkInspectSchema schema =
                new SinkInspectSchema(
                        Arrays.asList(
                                SinkInspectField.key("region", "VARCHAR", 0, -1),
                                SinkInspectField.key("id", "VARCHAR", 1, -1)),
                        Collections.singletonList(
                                SinkInspectField.value("payload", "VARCHAR", 2, 0)));
        InspectTarget target = InspectTarget.sink("sink", schema);
        List<byte[]> keys = new ArrayList<>();
        for (int index = 0; index < 8; index++) {
            keys.add(SinkInspectDecoder.encodeKeyPrefix(target, Arrays.asList("us", "a" + index)));
        }
        byte[] matching =
                SinkInspectDecoder.encodeKeyPrefix(target, Arrays.asList("us", "target-1"));
        keys.add(matching);
        keys.add(SinkInspectDecoder.encodeKeyPrefix(target, Arrays.asList("us", "target-2")));
        writeTable(table, buckets, snapshot, keys);
        writeRegistry(table, snapshot, schema);

        try (CobbleInspectClient client =
                        CobbleInspectClient.builder().totalBuckets(buckets).build();
                InspectSession session = client.openDataSource(table.toString())) {
            List<FieldValue> filterFields =
                    Arrays.asList(
                            new FieldValue("region", TypedValue.string("us")),
                            new FieldValue("id", TypedValue.string("target")));
            InspectPage page =
                    session.scan(
                            new ScanRequest(
                                    "sink",
                                    1,
                                    null,
                                    null,
                                    null,
                                    null,
                                    ScanFilter.sink(filterFields)));
            assertEquals(1, page.rows().size());
            assertTrue(page.rows().get(0).found());
            assertTrue(page.nextPageToken() != null);

            InspectPage next =
                    session.scan(
                            new ScanRequest(
                                    "sink",
                                    1,
                                    page.nextPageToken(),
                                    null,
                                    null,
                                    null,
                                    ScanFilter.sink(filterFields)));
            assertEquals(1, next.rows().size());
            assertTrue(next.nextPageToken() == null);

            InspectException changedFilter =
                    assertThrows(
                            InspectException.class,
                            () ->
                                    session.scan(
                                            new ScanRequest(
                                                    "sink",
                                                    1,
                                                    page.nextPageToken(),
                                                    null,
                                                    null,
                                                    null,
                                                    ScanFilter.sink(
                                                            Arrays.asList(
                                                                    new FieldValue(
                                                                            "region",
                                                                            TypedValue.string(
                                                                                    "us")),
                                                                    new FieldValue(
                                                                            "id",
                                                                            TypedValue.string(
                                                                                    "other")))))));
            assertTrue(changedFilter.getMessage().contains("different scan filters"));

            LookupResult lookup =
                    session.lookup(
                            new LookupRequest(
                                    "sink",
                                    Collections.singletonList(
                                            LookupKey.typed(
                                                    TypedLookupKey.sink(
                                                            Arrays.asList(
                                                                    new FieldValue(
                                                                            "region",
                                                                            TypedValue.string(
                                                                                    "us")),
                                                                    new FieldValue(
                                                                            "id",
                                                                            TypedValue.string(
                                                                                    "target-1"))))))));
            assertTrue(lookup.rows().get(0).found());

            InspectException partial =
                    assertThrows(
                            InspectException.class,
                            () ->
                                    session.lookup(
                                            new LookupRequest(
                                                    "sink",
                                                    Collections.singletonList(
                                                            LookupKey.typed(
                                                                    TypedLookupKey.sink(
                                                                            Collections
                                                                                    .singletonList(
                                                                                            new FieldValue(
                                                                                                    "region",
                                                                                                    TypedValue
                                                                                                            .string(
                                                                                                                    "us")))))))));
            assertTrue(partial.getMessage().contains("requires all fields"));

            InspectException wrongType =
                    assertThrows(
                            InspectException.class,
                            () ->
                                    session.lookup(
                                            new LookupRequest(
                                                    "sink",
                                                    Collections.singletonList(
                                                            LookupKey.typed(
                                                                    TypedLookupKey.sink(
                                                                            Arrays.asList(
                                                                                    new FieldValue(
                                                                                            "region",
                                                                                            TypedValue
                                                                                                    .integer(
                                                                                                            1)),
                                                                                    new FieldValue(
                                                                                            "id",
                                                                                            TypedValue
                                                                                                    .string(
                                                                                                            "target-1")))))))));
            assertFalse(wrongType.getMessage().isEmpty());

            String ddl = session.overview().items().get(0).sourceSql().ddl();
            StreamExecutionEnvironment environment =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            environment.setParallelism(1);
            StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
            tableEnvironment.executeSql(ddl);
            List<String> sqlRows = new ArrayList<>();
            try (CloseableIterator<Row> rows =
                    tableEnvironment.executeSql("SELECT region, id FROM sink").collect()) {
                while (rows.hasNext()) {
                    Row row = rows.next();
                    sqlRows.add(row.getFieldAs(0) + ":" + row.getFieldAs(1));
                }
            }
            assertEquals(keys.size(), sqlRows.size());
            assertTrue(sqlRows.contains("us:target-1"));
        }
    }

    private void writeTable(Path table, int buckets, long snapshot, List<byte[]> keys)
            throws Exception {
        ShardSnapshot shard;
        try (Db db = Db.open(config(table, buckets, true), 0, buckets - 1)) {
            for (byte[] key : keys) {
                db.put(0, key, 0, encodeString("value"));
            }
            shard = db.snapshot();
        }
        try (DbCoordinator coordinator = DbCoordinator.open(config(table, buckets, false))) {
            coordinator.materializeGlobalSnapshot(
                    buckets, snapshot, Collections.singletonList(shard));
        }
    }

    private void writeRegistry(Path table, long snapshot, SinkInspectSchema schema)
            throws Exception {
        byte[] blob = SinkInspectSchemaStore.of(schema).toBytes();
        String hash = InspectSchemaRegistryLayout.sha256(blob);
        Path events = table.resolve("inspect-schema/events");
        Path blobs = table.resolve("inspect-schema/blobs");
        Files.createDirectories(events);
        Files.createDirectories(blobs);
        Files.write(blobs.resolve(InspectSchemaRegistryLayout.blobFileName(hash)), blob);
        Files.write(
                events.resolve(InspectSchemaRegistryLayout.eventFileName(snapshot, hash)),
                new byte[0]);
    }

    private Config config(Path table, int buckets, boolean data) {
        Config config = new Config().numColumns(1).totalBuckets(buckets);
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

    private static byte[] encodeString(String value) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(32);
        StringDataSerializer.INSTANCE.serialize(StringData.fromString(value), output);
        return output.getCopyOfBuffer();
    }
}
