package io.cobble.flink.inspect.internal;

import io.cobble.Config;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleFlinkFileSystemResolver;
import io.cobble.flink.common.CobbleFlinkStorageConfig;
import io.cobble.flink.common.CobbleLoader;

import java.util.List;

public final class CobbleReaderConfigs {
    private CobbleReaderConfigs() {}

    public static Config base(int totalBuckets) {
        Config cobbleConfig = new Config().numColumns(1).totalBuckets(totalBuckets);
        cobbleConfig.snapshotRetention = null;
        return cobbleConfig;
    }

    public static void addVolume(
            Config cobbleConfig,
            String volumeDirectory,
            CobbleConnectorStorageOptions storageOptions) {
        CobbleLoader.ensureCobbleLoaded();
        Config.VolumeDescriptor volume = Config.VolumeDescriptor.singleVolume(volumeDirectory);
        CobbleFlinkFileSystemResolver.applyTo(volume, storageOptions);
        CobbleFlinkStorageConfig.empty().register(cobbleConfig, volume);
    }

    public static Config dataSource(
            int totalBuckets, String rootDirectory, CobbleConnectorStorageOptions storageOptions) {
        Config config = base(totalBuckets);
        addVolume(config, rootDirectory, storageOptions);
        return config;
    }

    public static Config checkpoint(
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
