package io.cobble.flink.inspect.internal;

import io.cobble.flink.common.CobbleLoader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;

import java.io.File;

public final class FlinkInspectFileSystems {
    private FlinkInspectFileSystems() {}

    public static void initialize(String flinkConfPath) {
        if (flinkConfPath == null) {
            CobbleLoader.ensureCobbleLoaded();
            return;
        }
        File path = new File(flinkConfPath);
        String configDirectory = path.isFile() ? path.getParent() : flinkConfPath;
        Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration(configDirectory);
        FileSystem.initialize(flinkConfiguration);
        CobbleLoader.ensureCobbleLoaded();
    }
}
