package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class CobbleRemoteMetadataFileIOIT {

    @Test
    void roundTripsSidecarThroughConnectorScopedS3Options() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("cobble.test.s3.enabled"));
        String endpoint = System.getProperty("cobble.test.s3.endpoint", "http://127.0.0.1:9000");
        String bucket = System.getProperty("cobble.test.s3.bucket", "cobble-test");
        String access = System.getProperty("cobble.test.s3.access-key", "eeeeeeee");
        String secret = System.getProperty("cobble.test.s3.secret-key", "eeeeeeee");
        String root = "s3://" + bucket + "/cobble-flink-metadata-" + UUID.randomUUID();
        Map<String, String> values = new HashMap<>();
        values.put("s3.endpoint", endpoint);
        values.put("s3.access-key", access);
        values.put("s3.secret-key", secret);
        values.put("s3.path.style.access", "true");

        CobbleConnectorStorageOptions storageOptions = CobbleConnectorStorageOptions.from(values);
        try {
            try (CobbleMetadataFileIO fileIO = CobbleMetadataFileIO.open(root, storageOptions)) {
                fileIO.mkdirs("inspect-schema/blobs");
                fileIO.writeIfAbsent("inspect-schema/blobs/test.csch", new byte[] {1, 2, 3, 4});
                assertTrue(fileIO.list("inspect-schema/blobs").contains("test.csch"));
                assertArrayEquals(
                        new byte[] {1, 2, 3, 4}, fileIO.read("inspect-schema/blobs/test.csch"));
            }
        } finally {
            try (CobbleMetadataFileIO fileIO = CobbleMetadataFileIO.open(root, storageOptions)) {
                fileIO.delete("inspect-schema/blobs/test.csch");
                fileIO.delete("inspect-schema/blobs");
                fileIO.delete("inspect-schema");
            }
        }
    }
}
