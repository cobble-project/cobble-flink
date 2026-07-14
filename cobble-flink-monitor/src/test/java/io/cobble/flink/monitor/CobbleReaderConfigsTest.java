package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.cobble.Config;
import io.cobble.flink.common.CobbleConnectorStorageOptions;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class CobbleReaderConfigsTest {

    @Test
    void appliesGenericRootProviderOptionsToRemoteVolumes() {
        Map<String, String> values = new HashMap<>();
        values.put("storage.option.endpoint", "https://oss.example.com");
        values.put("storage.option.access_key_id", "oss-access");
        values.put("storage.option.access_key_secret", "oss-secret");
        CobbleConnectorStorageOptions options =
                CobbleConnectorStorageOptions.fromStorageOptions(values);
        Config config = CobbleReaderConfigs.base(32);

        CobbleReaderConfigs.addVolume(config, "oss://bucket/root", options);
        CobbleReaderConfigs.addVolume(config, "azblob://container/data", options);

        assertEquals(
                "https://oss.example.com", config.volumes.get(0).customOptions.get("endpoint"));
        assertEquals("oss-access", config.volumes.get(0).customOptions.get("access_key_id"));
        assertEquals("oss-secret", config.volumes.get(0).customOptions.get("access_key_secret"));
        assertEquals(
                "https://oss.example.com", config.volumes.get(1).customOptions.get("endpoint"));
    }

    @Test
    void dataSourceAndCheckpointUseOnlyTheirSingleConfiguredOrDiscoveredVolume() {
        CobbleConnectorStorageOptions options = CobbleConnectorStorageOptions.empty();

        Config dataSource = CobbleReaderConfigs.dataSource(32, "s3://bucket/table", options);
        Config checkpoint =
                CobbleReaderConfigs.checkpoint(
                        32, Collections.singletonList("s3://bucket/manifest-volume"), options);

        assertEquals(1, dataSource.volumes.size());
        assertEquals("s3://bucket/table", dataSource.volumes.get(0).baseDir);
        assertEquals(1, checkpoint.volumes.size());
        assertEquals("s3://bucket/manifest-volume", checkpoint.volumes.get(0).baseDir);
    }

    @Test
    void leavesHdfsDescriptorForRegisteredFlinkFallback() {
        Config config = CobbleReaderConfigs.base(32);

        CobbleReaderConfigs.addVolume(
                config, "hdfs://namenode:8020/table", CobbleConnectorStorageOptions.empty());

        Config.VolumeDescriptor volume = config.volumes.get(0);
        assertEquals("hdfs://namenode:8020/table", volume.baseDir);
        assertNull(volume.accessId);
        assertNull(volume.secretKey);
        assertNull(volume.customOptions);
    }
}
