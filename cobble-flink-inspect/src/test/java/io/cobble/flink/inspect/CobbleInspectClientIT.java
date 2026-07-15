package io.cobble.flink.inspect;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.ShardSnapshot;
import io.cobble.structured.Db;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

class CobbleInspectClientIT {
    @TempDir private Path tempDir;

    @Test
    void discoversPinsScansPagesAndLooksUpRawDatasource() throws Exception {
        Path table = tempDir.resolve("table");
        int totalBuckets = 2;
        byte[] firstKey = bytes("alpha");
        byte[] secondKey = bytes("omega");
        writeTable(table, totalBuckets, 11L, firstKey, secondKey);

        try (CobbleInspectClient client =
                        CobbleInspectClient.builder().totalBuckets(totalBuckets).build();
                InspectSession session = client.openDataSource(table.toString())) {
            InspectCatalog catalog = session.catalog();
            assertEquals("data_source", catalog.sourceKind());
            assertEquals(11L, session.info().selection().checkpointId());
            assertFalse(session.info().selection().latest());
            assertEquals(1, session.targets().size());
            assertEquals(InspectTargetKind.RAW, session.targets().get(0).kind());

            String target = session.targets().get(0).id();
            InspectPage first = session.scan(new ScanRequest(target, 1, null));
            assertEquals(1, first.rows().size());
            assertNotNull(first.nextPageToken());
            InspectPage second = session.scan(new ScanRequest(target, 1, first.nextPageToken()));
            assertEquals(1, second.rows().size());
            assertFalse(
                    Arrays.equals(
                            first.rows().get(0).key().value(), second.rows().get(0).key().value()));

            int firstBucket = Math.floorMod(Arrays.hashCode(firstKey), totalBuckets);
            LookupResult lookup =
                    session.lookup(
                            new LookupRequest(
                                    target,
                                    Arrays.asList(
                                            new LookupKey(firstBucket, new RawBytes(firstKey)),
                                            new LookupKey(
                                                    firstBucket, new RawBytes(bytes("missing"))))));
            assertEquals(2, lookup.rows().size());
            assertTrue(lookup.rows().get(0).found());
            assertArrayEquals(bytes("one"), lookup.rows().get(0).value().value());
            assertFalse(lookup.rows().get(1).found());

            InspectException invalidProjection =
                    assertThrows(
                            InspectException.class,
                            () ->
                                    session.scan(
                                            new ScanRequest(
                                                    target, 10, null, null, null, new int[] {-1})));
            assertEquals(InspectErrorCode.INVALID_INPUT, invalidProjection.errorCode());

            InspectException nullPrefix =
                    assertThrows(
                            InspectException.class,
                            () ->
                                    session.scan(
                                            new ScanRequest(
                                                    target,
                                                    10,
                                                    null,
                                                    null,
                                                    new RawBytes(null),
                                                    null)));
            assertEquals(InspectErrorCode.INVALID_INPUT, nullPrefix.errorCode());

            session.close();
            InspectException closed = assertThrows(InspectException.class, () -> session.targets());
            assertEquals(InspectErrorCode.CLOSED, closed.errorCode());
        }
    }

    @Test
    void sessionsOwnIndependentReadersAndClientClosesRemainingSessions() throws Exception {
        Path first = tempDir.resolve("first-table");
        Path second = tempDir.resolve("second-table");
        writeTable(first, 1, 3L, bytes("a"), bytes("b"));
        writeTable(second, 1, 9L, bytes("x"), bytes("y"));

        CobbleInspectClient client = CobbleInspectClient.builder().totalBuckets(1).build();
        InspectSession firstSession = client.openDataSource(first.toString());
        InspectSession secondSession = client.openDataSource(second.toString());
        try {
            assertEquals(3L, firstSession.info().selection().checkpointId());
            assertEquals(9L, secondSession.info().selection().checkpointId());
            firstSession.close();
            assertEquals(
                    2,
                    secondSession
                            .scan(new ScanRequest(secondSession.targets().get(0).id(), 10, null))
                            .rows()
                            .size());

            client.close();
            InspectException closed = assertThrows(InspectException.class, secondSession::targets);
            assertEquals(InspectErrorCode.CLOSED, closed.errorCode());
        } finally {
            firstSession.close();
            secondSession.close();
            client.close();
        }
    }

    private void writeTable(
            Path table, int totalBuckets, long snapshotId, byte[] firstKey, byte[] secondKey)
            throws Exception {
        Config writer = config(table, totalBuckets, true);
        ShardSnapshot shard;
        try (Db db = Db.open(writer, 0, totalBuckets - 1)) {
            db.put(
                    Math.floorMod(Arrays.hashCode(firstKey), totalBuckets),
                    firstKey,
                    0,
                    bytes("one"));
            db.put(
                    Math.floorMod(Arrays.hashCode(secondKey), totalBuckets),
                    secondKey,
                    0,
                    bytes("two"));
            shard = db.snapshot();
        }
        try (DbCoordinator coordinator = DbCoordinator.open(config(table, totalBuckets, false))) {
            coordinator.materializeGlobalSnapshot(
                    totalBuckets, snapshotId, Collections.singletonList(shard));
        }
    }

    private Config config(Path table, int totalBuckets, boolean data) {
        Config config = new Config().numColumns(1).totalBuckets(totalBuckets);
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

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
