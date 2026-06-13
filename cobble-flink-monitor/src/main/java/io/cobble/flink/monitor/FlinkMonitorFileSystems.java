package io.cobble.flink.monitor;

import io.cobble.flink.common.CobbleFlinkFileSystems;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;

import java.io.File;

final class FlinkMonitorFileSystems {
    private FlinkMonitorFileSystems() {}

    static Configuration initialize(String flinkConfPath) {
        Configuration flinkConfiguration;
        if (flinkConfPath == null) {
            flinkConfiguration = new Configuration();
        } else {
            File path = new File(flinkConfPath);
            String configDirectory = path.isFile() ? path.getParent() : flinkConfPath;
            flinkConfiguration = GlobalConfiguration.loadConfiguration(configDirectory);
        }
        FileSystem.initialize(flinkConfiguration);
        CobbleFlinkFileSystems.ensureRegistered();
        return flinkConfiguration;
    }
}
