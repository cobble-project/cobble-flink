package io.cobble.flink.monitor;

import org.apache.flink.configuration.Configuration;

import java.util.LinkedHashMap;
import java.util.Map;

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

    private ServerConfig() {}

    static ServerConfig parse(String[] args) {
        ServerConfig config = new ServerConfig();
        Map<String, String> values = parseArgs(args);
        config.bindAddress = values.getOrDefault("bind", config.bindAddress);
        if (values.containsKey("port")) {
            config.port = parsePositiveInt(values.get("port"), "port");
        }
        if (values.containsKey("total-buckets")) {
            config.totalBuckets = parsePositiveInt(values.get("total-buckets"), "total-buckets");
        }
        if (values.containsKey("inspect-default-limit")) {
            config.inspectDefaultLimit =
                    parsePositiveInt(values.get("inspect-default-limit"), "inspect-default-limit");
        }
        if (values.containsKey("inspect-max-limit")) {
            config.inspectMaxLimit =
                    parsePositiveInt(values.get("inspect-max-limit"), "inspect-max-limit");
        }
        config.flinkConfPath = blankToNull(values.get("flink-conf"));
        if (config.inspectDefaultLimit > config.inspectMaxLimit) {
            throw new InputException("--inspect-default-limit must be <= --inspect-max-limit");
        }
        config.checkpointRoot = blankToNull(values.get("checkpoint"));
        return config;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> output = new LinkedHashMap<>();
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
            output.put(key, value);
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
                        + "  --total-buckets N              default 32768\n"
                        + "  --inspect-default-limit N      default 100\n"
                        + "  --inspect-max-limit N          default 1000");
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
