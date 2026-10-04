package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleFlinkStorageConfig;
import io.cobble.flink.common.CobbleMetadataFileIO;
import io.cobble.structured.Db;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * SQL-level E2E test for {@code source.kind='raw'}: writes a Cobble table via the direct Java API
 * (no sink sidecar), then reads it back through the Flink SQL planner using the raw source DDL.
 */
class CobbleRawSourceSqlITTest {

    @TempDir private Path tempDir;

    @Test
    void coldS3SqlSinkTypedAndRawSourcesHonorDefaultsAndExplicitOptions() throws Exception {
        Assumptions.assumeTrue(
                System.getenv("COBBLE_TEST_S3_BUCKET") != null,
                "S3 fixture environment is not configured");
        Configuration global = s3Configuration();
        FileSystem.initialize(global, PluginUtils.createPluginManagerFromRootFolder(global));
        String root =
                "s3://"
                        + System.getenv("COBBLE_TEST_S3_BUCKET")
                        + "/cobble-flink-validation/sql-table/"
                        + java.util.UUID.randomUUID();
        CobbleConnectorStorageOptions runtime =
                CobbleFlinkStorageConfig.from(global)
                        .resolve(root, CobbleConnectorStorageOptions.empty());
        try {
            StreamTableEnvironment catalogWriter = s3TableEnv(global);
            openS3Catalog(catalogWriter, root + "/catalog");
            catalogWriter.executeSql(
                    "CREATE TABLE s3_catalog.`default`.items (id BIGINT, name STRING, PRIMARY KEY(id) NOT ENFORCED)");
            catalogWriter
                    .executeSql(
                            "INSERT INTO s3_catalog.`default`.items VALUES (CAST(1 AS BIGINT), 'one'), (CAST(2 AS BIGINT), 'two')")
                    .await();
            java.util.Map<String, String> exported =
                    catalogWriter
                            .getCatalog("s3_catalog")
                            .get()
                            .getTable(
                                    new org.apache.flink.table.catalog.ObjectPath(
                                            "default", "items"))
                            .getOptions();
            for (String option : exported.keySet()) {
                assertTrue(!option.startsWith("s3.") && !option.startsWith("storage.option."));
            }
            catalogWriter.getCatalog("s3_catalog").get().close();
            // Typed Tables have their own family/key namespace; raw sources read DB column indexes.
            String rawRoot = root + "/raw";
            writeS3RawFixture(rawRoot, global);
            for (boolean explicit : new boolean[] {false, true}) {
                String path = root + (explicit ? "/explicit" : "/defaults");
                StreamTableEnvironment writer =
                        s3TableEnv(explicit ? incorrectS3Configuration() : global);
                writer.executeSql(tableDdl("s3_sink", path, explicit, true));
                writer.executeSql(
                                "INSERT INTO s3_sink VALUES (CAST(1 AS BIGINT), 'one'),"
                                        + " (CAST(2 AS BIGINT), 'two')")
                        .await();
                CobbleDynamicTableSink.SerializableConfig sink =
                        new CobbleDynamicTableSink.SerializableConfig(
                                path,
                                1,
                                2,
                                1,
                                false,
                                16L * 1024L * 1024L,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                runtime);
                Path localWriter = CobbleSinkPaths.tableRootPath(sink).toPath();
                if (Files.exists(localWriter)) {
                    Files.move(
                            localWriter,
                            tempDir.resolve(
                                    explicit ? "hidden-explicit-writer" : "hidden-default-writer"));
                }
                assertTrue(!Files.exists(localWriter));
                runColdS3Reader(path, rawRoot, false);
                if (!explicit) runColdS3Reader(path, rawRoot, true);
            }
        } finally {
            // Only this test's UUID prefix is removed; no bucket-wide cleanup.
            try (CobbleMetadataFileIO fileIO = CobbleMetadataFileIO.open(root, runtime)) {
                fileIO.delete("");
            }
        }
    }

    private void runColdS3Reader(String root, String rawRoot, boolean explicit) throws Exception {
        String classpath = System.getProperty("surefire.test.class.path");
        assertTrue(classpath != null && !classpath.isEmpty());
        Path log = tempDir.resolve("s3-reader-" + java.util.UUID.randomUUID() + ".log");
        Process process =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "java")
                                        .toString(),
                                "-cp",
                                classpath,
                                CobbleRawSourceSqlITTest.class.getName(),
                                "s3-cold-reader",
                                root,
                                rawRoot,
                                Boolean.toString(explicit))
                        .redirectErrorStream(true)
                        .redirectOutput(log.toFile())
                        .start();
        if (!process.waitFor(180L, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(10L, TimeUnit.SECONDS);
            throw new AssertionError("Independent S3 table reader timed out.");
        }
        String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
        Configuration credentials = s3Configuration();
        output =
                output.replace(credentials.getString("s3.access-key", ""), "<redacted>")
                        .replace(credentials.getString("s3.secret-key", ""), "<redacted>");
        assertEquals(0, process.exitValue(), "Independent S3 table reader failed: " + output);
    }

    /** Independent JVM intentionally has no access to the writer's local staging paths. */
    public static void main(String[] args) throws Exception {
        if (args.length != 4 || !"s3-cold-reader".equals(args[0])) {
            throw new IllegalArgumentException("Expected the S3 cold-reader scenario.");
        }
        boolean explicit = Boolean.parseBoolean(args[3]);
        Configuration global = explicit ? incorrectS3Configuration() : s3Configuration();
        FileSystem.initialize(global, PluginUtils.createPluginManagerFromRootFolder(global));
        StreamTableEnvironment reader = s3TableEnv(global);
        reader.executeSql(tableDdl("s3_typed", args[1], explicit, false));
        List<String> rows = new ArrayList<>();
        try (org.apache.flink.util.CloseableIterator<Row> iterator =
                reader.executeSql("SELECT id, name FROM s3_typed").collect()) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                rows.add(row.getField(0) + ":" + row.getField(1));
            }
        }
        Collections.sort(rows);
        assertEquals(Arrays.asList("1:one", "2:two"), rows);
        reader.executeSql(
                "CREATE TABLE s3_raw (`key` BYTES, `columns` ARRAY<BYTES>) WITH ("
                        + "'connector'='cobble', 'source.kind'='raw', 'path'='"
                        + args[2]
                        + "',"
                        + "'bucket'='1', 'raw.columns'='0', 'scan.mode'='batch'"
                        + explicitS3Clause(explicit)
                        + ")");
        List<String> rawKeys = new ArrayList<>();
        try (org.apache.flink.util.CloseableIterator<Row> iterator =
                reader.executeSql("SELECT `key`, `columns` FROM s3_raw").collect()) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                assertTrue(((byte[]) row.getField(0)).length > 0);
                rawKeys.add(new String((byte[]) row.getField(0), StandardCharsets.UTF_8));
            }
        }
        Collections.sort(rawKeys);
        assertEquals(Arrays.asList("raw-one", "raw-two"), rawKeys);
        if (!explicit) {
            openS3Catalog(reader, args[2].substring(0, args[2].lastIndexOf('/')) + "/catalog");
            List<String> catalogRows = new ArrayList<>();
            try (org.apache.flink.util.CloseableIterator<Row> iterator =
                    reader.executeSql("SELECT id, name FROM s3_catalog.`default`.items")
                            .collect()) {
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    catalogRows.add(row.getField(0) + ":" + row.getField(1));
                }
            }
            Collections.sort(catalogRows);
            assertEquals(Arrays.asList("1:one", "2:two"), catalogRows);
            reader.getCatalog("s3_catalog").get().close();
        }
    }

    private static void openS3Catalog(StreamTableEnvironment tables, String root) {
        tables.executeSql(
                "CREATE CATALOG s3_catalog WITH ('type'='cobble', 'path'='"
                        + root
                        + "', 'storage-id'='validation', 'buckets'='1')");
    }

    private static void writeS3RawFixture(String root, Configuration global) throws Exception {
        Config config = new Config().numColumns(1).totalBuckets(1);
        config.governanceMode = Config.GovernanceMode.NOOP;
        config.logConsole = false;
        config.snapshotRetention = null;
        Config.VolumeDescriptor volume = Config.VolumeDescriptor.singleVolume(root);
        // Every raw volume, including primary data, is remote; there is no local staging to hide.
        CobbleFlinkStorageConfig.from(global).register(config, volume);
        ShardSnapshot snapshot;
        try (Db writer = Db.open(config, 0, 0)) {
            writer.put(0, bytes("raw-one"), 0, bytes("one"));
            writer.put(0, bytes("raw-two"), 0, bytes("two"));
            snapshot = writer.snapshot();
        }
        try (DbCoordinator coordinator = DbCoordinator.open(config)) {
            coordinator.materializeGlobalSnapshot(1, 1L, Collections.singletonList(snapshot));
        }
    }

    private static String tableDdl(String table, String path, boolean explicit, boolean sink)
            throws Exception {
        return "CREATE TABLE "
                + table
                + " (id BIGINT, name STRING, PRIMARY KEY(id) NOT ENFORCED)"
                + " WITH ('connector'='cobble', 'path'='"
                + path
                + "', 'bucket'='1',"
                + (sink
                        ? "'sink.parallelism'='1', 'sink.writer-buffer-memory'='16 mb'"
                        : "'scan.mode'='batch'")
                + explicitS3Clause(explicit)
                + ")";
    }

    private static String explicitS3Clause(boolean explicit) throws Exception {
        if (!explicit) return "";
        Configuration config = s3Configuration();
        return ", 's3.endpoint'='"
                + config.getString("s3.endpoint", "")
                + "', 's3.access-key'='"
                + config.getString("s3.access-key", "")
                + "', 's3.secret-key'='"
                + config.getString("s3.secret-key", "")
                + "', 's3.path.style.access'='true', 's3.region'='us-east-1'";
    }

    private static Configuration s3Configuration() throws Exception {
        Configuration config = new Configuration();
        config.setString("s3.endpoint", System.getenv("COBBLE_TEST_S3_ENDPOINT"));
        config.setString(
                "s3.access-key",
                new String(
                                Files.readAllBytes(
                                        Paths.get(System.getenv("COBBLE_TEST_S3_ACCESS_KEY_FILE"))),
                                StandardCharsets.UTF_8)
                        .trim());
        config.setString(
                "s3.secret-key",
                new String(
                                Files.readAllBytes(
                                        Paths.get(System.getenv("COBBLE_TEST_S3_SECRET_KEY_FILE"))),
                                StandardCharsets.UTF_8)
                        .trim());
        config.setString("s3.path.style.access", "true");
        config.setString("s3.region", "us-east-1");
        return config;
    }

    private static Configuration incorrectS3Configuration() throws Exception {
        Configuration config = s3Configuration();
        config.setString("s3.endpoint", "http://127.0.0.1:1");
        config.setString("s3.access-key", "incorrect-access");
        config.setString("s3.secret-key", "incorrect-secret");
        return config;
    }

    private static StreamTableEnvironment s3TableEnv(Configuration config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().addConfiguration(config);
        return tableEnv;
    }

    @Test
    void rawSourceSqlQueryReturnsDirectWrittenBytes() throws Exception {
        Path tablePath = tempDir.resolve("raw-sql-e2e");
        int bucketCount = 2;
        int numColumns = 2;

        // Write data directly — no Flink sink, no inspect-schema sidecar.
        byte[][][] written = {
            {bytes("key-1"), bytes("val-1-a"), bytes("val-1-b")},
            {bytes("key-2"), bytes("val-2-a"), null},
            {bytes("key-3"), bytes("val-3-a"), bytes("val-3-b")},
        };

        Config writerConfig = new Config().numColumns(numColumns).totalBuckets(bucketCount);
        writerConfig.governanceMode = Config.GovernanceMode.NOOP;
        writerConfig.logConsole = false;
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = tablePath.toAbsolutePath().toString();
        volume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT);
        writerConfig.addVolume(volume);

        List<ShardSnapshot> shardSnapshots = new ArrayList<>();
        try (Db db = Db.open(writerConfig, 0, bucketCount - 1)) {
            for (byte[][] row : written) {
                int bucket = Math.floorMod(Arrays.hashCode(row[0]), bucketCount);
                db.put(bucket, row[0], 0, row[1]);
                if (row[2] != null) {
                    db.put(bucket, row[0], 1, row[2]);
                }
            }
            shardSnapshots.add(db.snapshot());
        }

        Config coordConfig = new Config().totalBuckets(bucketCount);
        coordConfig.governanceMode = Config.GovernanceMode.NOOP;
        coordConfig.logConsole = false;
        Config.VolumeDescriptor coordVolume = new Config.VolumeDescriptor();
        coordVolume.baseDir = tablePath.toAbsolutePath().toString();
        coordVolume.kinds =
                Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        coordConfig.addVolume(coordVolume);
        try (DbCoordinator coordinator = DbCoordinator.open(coordConfig)) {
            coordinator.materializeGlobalSnapshot(bucketCount, 1L, shardSnapshots);
        }

        // Read through the Flink SQL planner.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "CREATE TABLE raw_cobble ("
                        + "  `key` BYTES,"
                        + "  `columns` ARRAY<BYTES>"
                        + ") WITH ("
                        + "  'connector' = 'cobble',"
                        + "  'source.kind' = 'raw',"
                        + "  'path' = '"
                        + escape(tablePath)
                        + "',"
                        + "  'bucket' = '"
                        + bucketCount
                        + "',"
                        + "  'raw.columns' = '0,1',"
                        + "  'scan.checkpoint-id' = '1',"
                        + "  'scan.mode' = 'batch'"
                        + ")");

        List<Row> results = new ArrayList<>();
        try (org.apache.flink.util.CloseableIterator<Row> iter =
                tableEnv.executeSql("SELECT `key`, `columns` FROM raw_cobble").collect()) {
            while (iter.hasNext()) {
                results.add(iter.next());
            }
        }

        assertEquals(written.length, results.size());

        // Sort by key for deterministic comparison.
        results.sort(
                (r1, r2) -> {
                    byte[] k1 = (byte[]) r1.getField(0);
                    byte[] k2 = (byte[]) r2.getField(0);
                    return new String(k1, StandardCharsets.UTF_8)
                            .compareTo(new String(k2, StandardCharsets.UTF_8));
                });

        for (int i = 0; i < written.length; i++) {
            byte[] expectedKey = written[i][0];
            byte[] actualKey = (byte[]) results.get(i).getField(0);
            assertTrue(Arrays.equals(expectedKey, actualKey), "key mismatch at row " + i);

            byte[][] actualColumns = (byte[][]) results.get(i).getField(1);
            assertEquals(2, actualColumns.length, "columns array size at row " + i);
            assertTrue(
                    Arrays.equals(written[i][1], actualColumns[0]),
                    "column 0 mismatch at row " + i);
            if (written[i][2] == null) {
                assertTrue(actualColumns[1] == null, "column 1 should be null at row " + i);
            } else {
                assertTrue(
                        Arrays.equals(written[i][2], actualColumns[1]),
                        "column 1 mismatch at row " + i);
            }
        }
    }

    @Test
    void rawSourceRejectsLookupJoin() throws Exception {
        Path tablePath = tempDir.resolve("raw-lookup-reject");
        int bucketCount = 1;
        int numColumns = 1;

        Config writerConfig = new Config().numColumns(numColumns).totalBuckets(bucketCount);
        writerConfig.governanceMode = Config.GovernanceMode.NOOP;
        writerConfig.logConsole = false;
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = tablePath.toAbsolutePath().toString();
        volume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT);
        writerConfig.addVolume(volume);

        List<ShardSnapshot> shardSnapshots = new ArrayList<>();
        try (Db db = Db.open(writerConfig, 0, 0)) {
            db.put(0, bytes("k1"), 0, bytes("v1"));
            shardSnapshots.add(db.snapshot());
        }

        Config coordConfig = new Config().totalBuckets(bucketCount);
        coordConfig.governanceMode = Config.GovernanceMode.NOOP;
        coordConfig.logConsole = false;
        Config.VolumeDescriptor coordVolume = new Config.VolumeDescriptor();
        coordVolume.baseDir = tablePath.toAbsolutePath().toString();
        coordVolume.kinds =
                Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        coordConfig.addVolume(coordVolume);
        try (DbCoordinator coordinator = DbCoordinator.open(coordConfig)) {
            coordinator.materializeGlobalSnapshot(bucketCount, 1L, shardSnapshots);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "CREATE TABLE raw_cobble ("
                        + "  `key` BYTES,"
                        + "  `columns` ARRAY<BYTES>"
                        + ") WITH ("
                        + "  'connector' = 'cobble',"
                        + "  'source.kind' = 'raw',"
                        + "  'path' = '"
                        + escape(tablePath)
                        + "',"
                        + "  'bucket' = '1',"
                        + "  'raw.columns' = '0',"
                        + "  'scan.checkpoint-id' = '1',"
                        + "  'scan.mode' = 'batch'"
                        + ")");

        tableEnv.executeSql(
                "CREATE TABLE probe (k BYTES, proc_time AS PROCTIME()) WITH ('connector' = 'datagen', 'number-of-rows' = '1')");

        // A lookup join should fail because raw source does not support lookup. The planner may
        // wrap the ValidationException in a RuntimeException, so we check the full cause chain.
        Exception error =
                org.junit.jupiter.api.Assertions.assertThrows(
                        Exception.class,
                        () ->
                                tableEnv.executeSql(
                                                "SELECT * FROM probe LEFT JOIN raw_cobble FOR"
                                                        + " SYSTEM_TIME AS OF probe.proc_time ON"
                                                        + " probe.k = raw_cobble.`key`")
                                        .await(30, TimeUnit.SECONDS));
        boolean foundLookupMessage = false;
        Throwable current = error;
        while (current != null) {
            if (current.getMessage() != null
                    && current.getMessage().contains("lookup is not supported")) {
                foundLookupMessage = true;
                break;
            }
            current = current.getCause();
        }
        assertTrue(foundLookupMessage, "expected lookup-not-supported error, got: " + error);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String escape(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "\\\\");
    }
}
