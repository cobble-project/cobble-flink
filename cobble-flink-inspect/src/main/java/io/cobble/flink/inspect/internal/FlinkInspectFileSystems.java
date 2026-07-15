package io.cobble.flink.inspect.internal;

import io.cobble.flink.common.CobbleLoader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;

import java.io.File;

public final class FlinkInspectFileSystems {
    private FlinkInspectFileSystems() {}

    public static Configuration initialize(String flinkConfPath) {
        Configuration flinkConfiguration;
        if (flinkConfPath == null) {
            flinkConfiguration = new Configuration();
        } else {
            File path = new File(flinkConfPath);
            String configDirectory = path.isFile() ? path.getParent() : flinkConfPath;
            flinkConfiguration = GlobalConfiguration.loadConfiguration(configDirectory);
        }
        initialize(flinkConfiguration);
        return flinkConfiguration;
    }

    public static void initialize(Configuration flinkConfiguration) {
        FileSystem.initialize(
                flinkConfiguration == null ? new Configuration() : flinkConfiguration);
        CobbleLoader.ensureCobbleLoaded();
    }
}
