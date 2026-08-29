package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.Db;
import io.cobble.ExpandStorageMode;
import io.cobble.ShardSnapshot;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** End-to-end coverage for Cobble JNI calling Flink's optimized filesystem copy API. */
class CobbleFastCopyRestoreITTest {

    @Test
    void rescaleAdoptionUsesRegisteredFlinkFastCopyResolver(@TempDir Path tempDir)
            throws Exception {
        CobbleStateBackend.ensureCobbleLoaded();

        String firstSourceRoot = customUri(tempDir.resolve("source-0"));
        String secondSourceRoot = customUri(tempDir.resolve("source-1"));
        String sharedSnapshotRoot = customUri(tempDir.resolve("shared"));
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[256 * 1024];
        Arrays.fill(value, (byte) 7);

        try (Db firstSource =
                        Db.open(configWithVolumes(firstSourceRoot, sharedSnapshotRoot), 0, 0);
                Db secondSource =
                        Db.open(configWithVolumes(secondSourceRoot, sharedSnapshotRoot), 1, 1)) {
            ShardSnapshot firstSnapshot = snapshot(firstSource, 0, key, new byte[] {1});
            ShardSnapshot secondSnapshot = snapshot(secondSource, 1, key, value);

            Config targetConfig =
                    configWithVolumes(customUri(tempDir.resolve("target")), sharedSnapshotRoot);

            try (Db target = Db.restoreWithManifest(targetConfig, firstSnapshot.manifestPath)) {
                FastCopyTestFileSystem.resetFastCopyCount();
                target.expandBucket(
                        secondSnapshot.dbId,
                        secondSnapshot.snapshotId,
                        new int[] {1},
                        new int[] {1},
                        ExpandStorageMode.ADOPT_ASYNC);
                target.waitForExpandAdoption(TimeUnit.SECONDS.toMillis(10));
                waitForFastCopy();
                assertArrayEquals(value, target.get(1, key, 0));
            }
        }
    }

    private static ShardSnapshot snapshot(Db db, int bucket, byte[] key, byte[] value) {
        db.put(bucket, key, 0, value);
        db.switchMemtableType(Config.MemtableType.SKIPLIST, true);
        ShardSnapshot snapshot = db.snapshot();
        assertTrue(snapshot.dataSizeBytes > 0L);
        assertTrue(db.retainSnapshot(snapshot.snapshotId));
        return snapshot;
    }

    private static Config configWithVolumes(String primaryRoot, String sharedSnapshotRoot) {
        Config.VolumeDescriptor primary = new Config.VolumeDescriptor();
        primary.baseDir = primaryRoot;
        primary.kinds = Arrays.asList(Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH);

        Config.VolumeDescriptor shared = new Config.VolumeDescriptor();
        shared.baseDir = sharedSnapshotRoot;
        shared.kinds = Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        return new Config().addVolume(primary).addVolume(shared).numColumns(1).totalBuckets(2);
    }

    private static String customUri(Path path) {
        return FastCopyTestFileSystem.SCHEME + "://" + path.toUri().getPath();
    }

    private static void waitForFastCopy() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (FastCopyTestFileSystem.fastCopyCount() > 0) {
                return;
            }
            Thread.sleep(25L);
        }
        assertTrue(
                FastCopyTestFileSystem.fastCopyCount() > 0,
                "Cobble did not invoke Flink fast-copy while adopting imported snapshot files");
    }
}
