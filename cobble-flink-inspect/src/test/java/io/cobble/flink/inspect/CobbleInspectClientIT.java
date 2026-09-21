package io.cobble.flink.inspect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.inspect.internal.CobbleTableInspectTestData;
import io.cobble.table.Value;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

class CobbleInspectClientIT {
    @TempDir private Path tempDir;

    @Test
    void discoversPinsScansPagesAndLooksUpTable() throws Exception {
        Path table = tempDir.resolve("table");
        int totalBuckets = 2;
        byte[] firstKey = bytes("alpha");
        byte[] secondKey = bytes("omega");
        long snapshotId = writeTable(table, totalBuckets, 11L, firstKey, secondKey);

        try (CobbleInspectClient client =
                        CobbleInspectClient.builder().totalBuckets(totalBuckets).build();
                InspectSession session = client.openDataSource(table.toString())) {
            InspectCatalog catalog = session.catalog();
            assertEquals("data_source", catalog.sourceKind());
            assertEquals(snapshotId, session.info().selection().checkpointId());
            assertFalse(session.info().selection().latest());
            assertEquals(1, session.targets().size());
            assertEquals(InspectTargetKind.SINK, session.targets().get(0).kind());

            String target = session.targets().get(0).id();
            InspectPage first = session.scan(new ScanRequest(target, 1, null));
            assertEquals(1, first.rows().size());
            assertNotNull(first.nextPageToken());
            InspectPage second = session.scan(new ScanRequest(target, 1, first.nextPageToken()));
            assertEquals(1, second.rows().size());
            assertFalse(
                    Arrays.equals(
                            first.rows().get(0).key().value(), second.rows().get(0).key().value()));

            InspectRow firstRow = first.rows().get(0);
            int firstBucket = firstRow.bucket();
            LookupResult lookup =
                    session.lookup(
                            new LookupRequest(
                                    target,
                                    Arrays.asList(
                                            new LookupKey(firstBucket, firstRow.key()),
                                            new LookupKey(
                                                    firstBucket, new RawBytes(bytes("missing"))))));
            assertEquals(2, lookup.rows().size());
            assertTrue(lookup.rows().get(0).found());
            assertEquals(
                    firstRow.decodedColumns().get(0).fields().get(0).value().scalar(),
                    lookup.rows().get(0).decodedColumns().get(0).fields().get(0).value().scalar());
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
        long firstSnapshot = writeTable(first, 1, 3L, bytes("a"), bytes("b"));
        long secondSnapshot = writeTable(second, 1, 9L, bytes("x"), bytes("y"));

        CobbleInspectClient client = CobbleInspectClient.builder().totalBuckets(1).build();
        InspectSession firstSession = client.openDataSource(first.toString());
        InspectSession secondSession = client.openDataSource(second.toString());
        try {
            assertEquals(firstSnapshot, firstSession.info().selection().checkpointId());
            assertEquals(secondSnapshot, secondSession.info().selection().checkpointId());
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

    private long writeTable(
            Path table, int totalBuckets, long snapshotId, byte[] firstKey, byte[] secondKey)
            throws Exception {
        return CobbleTableInspectTestData.write(
                        table,
                        totalBuckets,
                        snapshotId,
                        CobbleTableInspectTestData.stringKeyValueSchema(),
                        Arrays.asList(
                                Arrays.asList(
                                        Value.string(new String(firstKey, StandardCharsets.UTF_8)),
                                        Value.string("one")),
                                Arrays.asList(
                                        Value.string(new String(secondKey, StandardCharsets.UTF_8)),
                                        Value.string("two"))))
                .id;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
