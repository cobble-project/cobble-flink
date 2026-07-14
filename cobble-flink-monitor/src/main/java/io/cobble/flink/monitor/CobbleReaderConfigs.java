package io.cobble.flink.monitor;

import io.cobble.Config;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleLoader;

import java.util.List;

final class CobbleReaderConfigs {
    private CobbleReaderConfigs() {}

    static Config base(int totalBuckets) {
        Config cobbleConfig = new Config().numColumns(1).totalBuckets(totalBuckets);
        cobbleConfig.snapshotRetention = null;
        return cobbleConfig;
    }

    static void addVolume(
            Config cobbleConfig,
            String volumeDirectory,
            CobbleConnectorStorageOptions storageOptions) {
        CobbleLoader.ensureCobbleLoaded();
        Config.VolumeDescriptor volume = Config.VolumeDescriptor.singleVolume(volumeDirectory);
        storageOptions.applyTo(volume);
        cobbleConfig.addVolume(volume);
    }

    static Config dataSource(
            int totalBuckets, String rootDirectory, CobbleConnectorStorageOptions storageOptions) {
        Config config = base(totalBuckets);
        addVolume(config, rootDirectory, storageOptions);
        return config;
    }

    static Config checkpoint(
            int totalBuckets,
            List<String> discoveredVolumes,
            CobbleConnectorStorageOptions storageOptions) {
        Config config = base(totalBuckets);
        for (String volumeDirectory : discoveredVolumes) {
            addVolume(config, volumeDirectory, storageOptions);
        }
        return config;
    }
}
