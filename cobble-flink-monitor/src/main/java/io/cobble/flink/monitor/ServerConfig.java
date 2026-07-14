package io.cobble.flink.monitor;

import io.cobble.flink.common.CobbleConnectorStorageOptions;

import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

final class ServerConfig {
    private static final int DEFAULT_PORT = 8088;
    private static final int DEFAULT_TOTAL_BUCKETS = 32768;
    private static final int DEFAULT_INSPECT_LIMIT = 100;
    private static final int DEFAULT_INSPECT_MAX_LIMIT = 1000;

    String bindAddress = "127.0.0.1";
    int port = DEFAULT_PORT;
    String checkpointRoot;
    int totalBuckets = DEFAULT_TOTAL_BUCKETS;
    int inspectDefaultLimit = DEFAULT_INSPECT_LIMIT;
    int inspectMaxLimit = DEFAULT_INSPECT_MAX_LIMIT;
    String flinkConfPath;
    Configuration flinkConfiguration = new Configuration();
    List<String> userJars = new ArrayList<>();
    CobbleConnectorStorageOptions storageOptions = CobbleConnectorStorageOptions.empty();
    int storageOptionCount;

    private ServerConfig() {}

    static ServerConfig parse(String[] args) {
        ServerConfig config = new ServerConfig();
        Map<String, List<String>> values = parseArgs(args);
        config.bindAddress = lastOr(values, "bind", config.bindAddress);
        if (values.containsKey("port")) {
            config.port = parsePositiveInt(last(values, "port"), "port");
        }
        if (values.containsKey("total-buckets")) {
            config.totalBuckets = parsePositiveInt(last(values, "total-buckets"), "total-buckets");
        }
        if (values.containsKey("inspect-default-limit")) {
            config.inspectDefaultLimit =
                    parsePositiveInt(
                            last(values, "inspect-default-limit"), "inspect-default-limit");
        }
        if (values.containsKey("inspect-max-limit")) {
            config.inspectMaxLimit =
                    parsePositiveInt(last(values, "inspect-max-limit"), "inspect-max-limit");
        }
        config.flinkConfPath = blankToNull(last(values, "flink-conf"));
        if (config.inspectDefaultLimit > config.inspectMaxLimit) {
            throw new InputException("--inspect-default-limit must be <= --inspect-max-limit");
        }
        config.checkpointRoot = blankToNull(last(values, "checkpoint"));
        config.userJars = collectUserJars(values);
        Map<String, String> storageValues =
                loadStorageOptionsFile(last(values, "storage-options-file"));
        for (String raw : values.getOrDefault("storage-option", new ArrayList<>())) {
            addStorageOption(storageValues, raw);
        }
        try {
            config.storageOptions = CobbleConnectorStorageOptions.fromStorageOptions(storageValues);
        } catch (IllegalArgumentException e) {
            throw new InputException(e.getMessage());
        }
        config.storageOptionCount = storageValues.size();
        return config;
    }

    private static Map<String, String> loadStorageOptionsFile(String path) {
        Map<String, String> result = new LinkedHashMap<>();
        String normalizedPath = blankToNull(path);
        if (normalizedPath == null) {
            return result;
        }
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream(normalizedPath)) {
            properties.load(input);
        } catch (IOException e) {
            throw new InputException("failed to read --storage-options-file");
        }
        for (String name : properties.stringPropertyNames()) {
            result.put(name, properties.getProperty(name));
        }
        return result;
    }

    private static void addStorageOption(Map<String, String> output, String raw) {
        int separator = raw == null ? -1 : raw.indexOf('=');
        if (separator <= 0) {
            throw new InputException("--storage-option must use KEY=VALUE syntax");
        }
        String key = raw.substring(0, separator).trim();
        if (key.isEmpty()) {
            throw new InputException("--storage-option must contain a non-empty key");
        }
        output.put(key, raw.substring(separator + 1).trim());
    }

    private static List<String> collectUserJars(Map<String, List<String>> values) {
        List<String> jars = new ArrayList<>(values.getOrDefault("user-jar", new ArrayList<>()));
        List<String> classpath = values.getOrDefault("user-classpath", new ArrayList<>());
        for (String entry : classpath) {
            for (String token : entry.split(File.pathSeparator)) {
                String trimmed = token.trim();
                if (!trimmed.isEmpty()) {
                    jars.add(trimmed);
                }
            }
        }
        return jars;
    }

    private static String last(Map<String, List<String>> values, String key) {
        List<String> list = values.get(key);
        return list == null || list.isEmpty() ? null : list.get(list.size() - 1);
    }

    private static String lastOr(Map<String, List<String>> values, String key, String fallback) {
        String value = last(values, key);
        return value == null ? fallback : value;
    }

    private static Map<String, List<String>> parseArgs(String[] args) {
        Map<String, List<String>> output = new LinkedHashMap<>();
        for (int index = 0; index < args.length; index++) {
            String arg = args[index];
            if ("--help".equals(arg) || "-h".equals(arg)) {
                printUsageAndExit();
            }
            if (!arg.startsWith("--")) {
                throw new InputException("unexpected argument: " + arg);
            }
            String key = arg.substring(2);
            String value;
            int separator = key.indexOf('=');
            if (separator >= 0) {
                value = key.substring(separator + 1);
                key = key.substring(0, separator);
            } else {
                if (index + 1 >= args.length) {
                    throw new InputException("missing value for " + arg);
                }
                value = args[++index];
            }
            output.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        }
        return output;
    }

    private static void printUsageAndExit() {
        System.out.println(
                "Usage: cobble-flink-monitor [--checkpoint PATH] [options]\n"
                        + "Options:\n"
                        + "  --checkpoint PATH              optional initial checkpoint root or Cobble data source\n"
                        + "  --bind ADDRESS                 default 127.0.0.1\n"
                        + "  --port PORT                    default 8088\n"
                        + "  --flink-conf PATH              optional Flink conf dir or flink-conf.yaml\n"
                        + "  --storage-options-file PATH    Java properties file with connector storage options\n"
                        + "  --storage-option KEY=VALUE     connector storage option (repeatable; last wins)\n"
                        + "  --total-buckets N              default 32768\n"
                        + "  --inspect-default-limit N      default 100\n"
                        + "  --inspect-max-limit N          default 1000\n"
                        + "  --user-jar PATH                user job jar (repeatable) for live serializer restore\n"
                        + "  --user-classpath PATHS         path-separator-joined user jars (alias for --user-jar)");
        System.exit(0);
    }

    private static int parsePositiveInt(String raw, String field) {
        try {
            int value = Integer.parseInt(raw);
            if (value <= 0) {
                throw new InputException("`" + field + "` must be greater than 0");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new InputException("invalid integer for `" + field + "`: " + raw);
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value;
    }
}
