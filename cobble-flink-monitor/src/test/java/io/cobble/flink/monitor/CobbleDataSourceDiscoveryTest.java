package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cobble.flink.common.CobbleConnectorStorageOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

class CobbleDataSourceDiscoveryTest {

    @Test
    void discoversOnlyValidatedGlobalSnapshotManifests(@TempDir Path root) throws Exception {
        Path snapshots = root.resolve("snapshot");
        Files.createDirectories(snapshots);
        Files.write(
                snapshots.resolve("SNAPSHOT-7"),
                "{\"total_buckets\":16}".getBytes(StandardCharsets.UTF_8));
        Files.write(
                snapshots.resolve("SNAPSHOT-8"),
                "{\"shards\":[]}".getBytes(StandardCharsets.UTF_8));
        Files.write(snapshots.resolve("CURRENT"), new byte[0]);

        List<CheckpointEntry> discovered =
                CobbleDataSourceDiscovery.discover(
                        root.toUri().toString(), CobbleConnectorStorageOptions.empty());

        assertEquals(1, discovered.size());
        assertEquals(7L, discovered.get(0).id);
        assertEquals("sink", discovered.get(0).defaultOperator().operatorId);
        assertEquals(
                MonitorPathUtils.normalizeStorageDirectory(root.toUri().toString()),
                discovered.get(0).directory);
    }
}
