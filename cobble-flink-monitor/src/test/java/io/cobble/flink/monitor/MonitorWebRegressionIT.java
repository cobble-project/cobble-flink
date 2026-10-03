package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.Db;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.DataField;
import io.cobble.table.LogicalTypes;
import io.cobble.table.Table;
import io.cobble.table.TableSchema;
import io.cobble.table.TableSnapshotCommitter;
import io.cobble.table.Value;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/** Runs the served UI against real HTTP and real, versioned Table data. */
class MonitorWebRegressionIT {
    @TempDir private Path tempDir;

    @Test
    void servedJavaScriptPreservesIntegersTracksAndPinnedSessionRecovery() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("table"))) {
            long first = fixture.publish(101, "first");
            ServerConfig config = new ServerConfig();
            config.port = 0;
            config.sessionIdleTimeoutSeconds = 2;
            try (CobbleFlinkMonitorServer.RunningServer server =
                    CobbleFlinkMonitorServer.start(config)) {
                String harness;
                try (InputStream input =
                        getClass().getResourceAsStream("/app-http-regression.js")) {
                    harness = new String(input.readAllBytes(), StandardCharsets.UTF_8);
                }
                Path ready = tempDir.resolve("ready");
                Path advanced = tempDir.resolve("advanced");
                Process process =
                        new ProcessBuilder(
                                        "node",
                                        "-",
                                        "http://127.0.0.1:" + server.address().getPort(),
                                        fixture.root.toUri().toString(),
                                        Long.toString(first),
                                        ready.toString(),
                                        advanced.toString())
                                .start();
                try {
                    process.getOutputStream().write(harness.getBytes(StandardCharsets.UTF_8));
                    process.getOutputStream().close();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                    while (!Files.exists(ready)
                            && process.isAlive()
                            && System.nanoTime() < deadline) {
                        Thread.sleep(20);
                    }
                    if (Files.exists(ready)) {
                        long second = fixture.publish(102, "second");
                        Files.writeString(advanced, Long.toString(second));
                    }
                    assertTrue(process.waitFor(40, TimeUnit.SECONDS), "Node HTTP probe timed out");
                    String stdout =
                            new String(
                                    process.getInputStream().readAllBytes(),
                                    StandardCharsets.UTF_8);
                    String stderr =
                            new String(
                                    process.getErrorStream().readAllBytes(),
                                    StandardCharsets.UTF_8);
                    assertEquals(0, process.exitValue(), stderr + stdout);
                    assertTrue(stdout.contains("SERVED_JS_HTTP_REGRESSION_PASS"));
                } finally {
                    if (process.isAlive()) {
                        process.destroyForcibly();
                        process.waitFor();
                    }
                }
            }
        }
    }

    static final class Fixture implements AutoCloseable {
        final Path root;
        private final Db db;
        private final Table table;
        private final TableSnapshotCommitter committer;

        Fixture(Path root) throws Exception {
            this.root = root;
            Config config = new Config().numColumns(2).totalBuckets(1);
            config.governanceMode = Config.GovernanceMode.NOOP;
            config.logConsole = false;
            config.walEnabled = false;
            config.snapshotOnlyTrack = true;
            config.snapshotDisableIncrementalBaseLink = true;
            config.logPath = root.resolveSibling("writer.log").toString();
            Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
            volume.baseDir = root.toAbsolutePath().toString();
            volume.kinds =
                    Arrays.asList(
                            Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                            Config.VolumeUsageKind.META,
                            Config.VolumeUsageKind.SNAPSHOT);
            config.addVolume(volume);
            TableSchema schema =
                    new TableSchema(
                            Arrays.asList(
                                    new DataField(0, "id", LogicalTypes.int64().notNull()),
                                    new DataField(1, "label", LogicalTypes.string()),
                                    new DataField(2, "blob", LogicalTypes.binary().nullable())),
                            Collections.singletonList(0L),
                            Collections.singletonList(0L));
            db = Db.open(config, 0, 0);
            table = Table.create(db, CobbleTableRowConverter.TABLE_NAME, schema);
            committer = TableSnapshotCommitter.open(config, 1, 10);
        }

        long publish(long commitId, String phase) throws Exception {
            long[] keys = {9007199254740992L, 9007199254740993L, Long.MIN_VALUE, Long.MAX_VALUE};
            String[] names = {"even", "odd", "min", "max"};
            for (int index = 0; index < keys.length; index++) {
                table.put(
                        Arrays.asList(
                                Value.int64(keys[index]),
                                Value.string(names[index] + "-" + phase),
                                index == 1
                                        ? Value.nullValue()
                                        : Value.binary(new byte[] {0, 1, (byte) index})));
            }
            ShardSnapshot shard = db.startAsyncSnapshot().future().get();
            return committer.commitBatch(commitId, Collections.singletonList(shard)).id;
        }

        @Override
        public void close() {
            committer.close();
            table.close();
            db.close();
        }
    }
}
