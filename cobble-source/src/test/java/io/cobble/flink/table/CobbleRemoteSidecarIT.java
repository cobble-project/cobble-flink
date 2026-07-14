package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleMetadataFileIO;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class CobbleRemoteSidecarIT {

    @Test
    void sinkWritesAndSourceResolvesSidecarThroughScopedS3Options() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("cobble.test.s3.enabled"));
        String endpoint = System.getProperty("cobble.test.s3.endpoint", "http://127.0.0.1:9000");
        String bucket = System.getProperty("cobble.test.s3.bucket", "cobble-test");
        String root = "s3://" + bucket + "/cobble-flink-sidecar-" + UUID.randomUUID();
        Map<String, String> values = new HashMap<>();
        values.put("s3.endpoint", endpoint);
        values.put("s3.access-key", System.getProperty("cobble.test.s3.access-key", "eeeeeeee"));
        values.put("s3.secret-key", System.getProperty("cobble.test.s3.secret-key", "eeeeeeee"));
        values.put("s3.path.style.access", "true");
        CobbleConnectorStorageOptions storageOptions = CobbleConnectorStorageOptions.from(values);
        CobbleDynamicTableSink.SerializableConfig sinkConfig =
                new CobbleDynamicTableSink.SerializableConfig(
                        root,
                        1,
                        1,
                        1,
                        false,
                        1024L * 1024L,
                        Collections.singletonList(
                                new CobbleDynamicTableSink.SerializableField(
                                        "id", "BIGINT", 0, -1)),
                        Collections.singletonList(
                                new CobbleDynamicTableSink.SerializableField(
                                        "name", "STRING", 1, 0)),
                        storageOptions);
        try {
            CobbleSinkInspectSchemaRegistry.writeForSnapshot(sinkConfig, 7L);

            CobbleResolvedSource detected =
                    CobbleSourceKindDetector.detect(
                            root, CobbleSourceKind.AUTO, true, storageOptions);
            assertEquals(CobbleSourceKind.SINK, detected.kind());
            ResolvedSchema ddl =
                    new ResolvedSchema(
                            Arrays.asList(
                                    Column.physical("id", DataTypes.BIGINT()),
                                    Column.physical("name", DataTypes.STRING())),
                            Collections.emptyList(),
                            UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));
            SinkSourceResolvedSchema resolved =
                    SinkSourceSchemaResolver.resolve(root, "latest", ddl, storageOptions);
            assertTrue(resolved.present());
            assertEquals(7L, resolved.schemaSnapshotId());
        } finally {
            deleteRemoteSidecar(root, storageOptions);
        }
    }

    private static void deleteRemoteSidecar(
            String root, CobbleConnectorStorageOptions storageOptions) throws Exception {
        try (CobbleMetadataFileIO fileIO = CobbleMetadataFileIO.open(root, storageOptions)) {
            for (String directory :
                    Arrays.asList("inspect-schema/events", "inspect-schema/blobs")) {
                for (String name : fileIO.list(directory)) {
                    fileIO.delete(directory + "/" + name);
                }
                fileIO.delete(directory);
            }
            fileIO.delete("inspect-schema");
        }
    }
}
