package io.cobble.flink.inspect.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleMetadataFileIO;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;
import io.cobble.flink.inspect.CobbleInspectClient;
import io.cobble.flink.inspect.InspectCatalog;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class CobbleRemoteDataSourceInspectIT {

    @Test
    void sdkDiscoversRemoteDataSourceAndSchemaWithConnectorOnlyStorageOptions() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("cobble.test.s3.enabled"));
        String endpoint = System.getProperty("cobble.test.s3.endpoint", "http://127.0.0.1:9000");
        String bucket = System.getProperty("cobble.test.s3.bucket", "cobble-test");
        String root = "s3://" + bucket + "/cobble-inspect-data-source-" + UUID.randomUUID();
        Map<String, String> values = new HashMap<>();
        values.put("s3.endpoint", endpoint);
        values.put("s3.access-key", System.getProperty("cobble.test.s3.access-key", "eeeeeeee"));
        values.put("s3.secret-key", System.getProperty("cobble.test.s3.secret-key", "eeeeeeee"));
        values.put("s3.path.style.access", "true");
        CobbleConnectorStorageOptions options = CobbleConnectorStorageOptions.from(values);
        byte[] schema = schema().toBytes();
        String hash = InspectSchemaRegistryLayout.sha256(schema);
        String event = InspectSchemaRegistryLayout.eventFileName(7L, hash);
        String blob = InspectSchemaRegistryLayout.blobFileName(hash);
        try {
            try (CobbleMetadataFileIO fileIO = CobbleMetadataFileIO.open(root, options)) {
                fileIO.mkdirs("snapshot");
                fileIO.write(
                        "snapshot/SNAPSHOT-7",
                        "{\"total_buckets\":1}".getBytes(StandardCharsets.UTF_8));
                fileIO.mkdirs("inspect-schema/events");
                fileIO.mkdirs("inspect-schema/blobs");
                fileIO.write("inspect-schema/events/" + event, new byte[0]);
                fileIO.write("inspect-schema/blobs/" + blob, schema);
            }

            try (CobbleInspectClient client =
                    CobbleInspectClient.builder().storageOptions(options).totalBuckets(1).build()) {
                InspectCatalog catalog = client.discover(root);
                assertEquals("data_source", catalog.sourceKind());
                assertEquals(1, catalog.checkpoints().size());
                assertEquals(7L, catalog.checkpoints().get(0).checkpointId());
            }
            SinkSchemaResolveResult resolved = SinkInspectSchemaResolver.resolve(root, 7L, options);
            assertEquals(SinkSchemaResolveResult.STATUS_AVAILABLE, resolved.status);
            assertEquals("name", resolved.store.schema().valueFields().get(0).name());
        } finally {
            cleanup(root, options, event, blob);
        }
    }

    private static SinkInspectSchemaStore schema() {
        return SinkInspectSchemaStore.of(
                new SinkInspectSchema(
                        Collections.singletonList(SinkInspectField.key("id", "BIGINT", 0, -1)),
                        Collections.singletonList(
                                SinkInspectField.value("name", "VARCHAR(2147483647)", 1, 0))));
    }

    private static void cleanup(
            String root, CobbleConnectorStorageOptions options, String event, String blob)
            throws Exception {
        try (CobbleMetadataFileIO fileIO = CobbleMetadataFileIO.open(root, options)) {
            for (String path :
                    Arrays.asList(
                            "snapshot/SNAPSHOT-7",
                            "inspect-schema/events/" + event,
                            "inspect-schema/blobs/" + blob)) {
                fileIO.delete(path);
            }
            for (String directory :
                    Arrays.asList(
                            "snapshot",
                            "inspect-schema/events",
                            "inspect-schema/blobs",
                            "inspect-schema")) {
                fileIO.delete(directory);
            }
        }
    }
}
