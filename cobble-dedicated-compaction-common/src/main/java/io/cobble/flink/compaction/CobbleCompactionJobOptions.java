package io.cobble.flink.compaction;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Validated command-line options shared by the Flink 1.x and 2.x job entry points. */
public final class CobbleCompactionJobOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final long DEFAULT_POLL_INTERVAL_MILLIS = 1_000L;
    private static final int DEFAULT_PARALLELISM = 1;

    private final String configPath;
    private final String path;
    private final long pollIntervalMillis;
    private final int parallelism;

    private CobbleCompactionJobOptions(
            String configPath,
            String path,
            long pollIntervalMillis,
            int parallelism) {
        this.configPath = configPath;
        this.path = path;
        this.pollIntervalMillis = pollIntervalMillis;
        this.parallelism = parallelism;
    }

    public static CobbleCompactionJobOptions parse(String[] args) {
        String configPath = null;
        String path = null;
        long pollIntervalMillis = DEFAULT_POLL_INTERVAL_MILLIS;
        int parallelism = DEFAULT_PARALLELISM;
        for (int index = 0; index < args.length; index++) {
            String option = args[index];
            if ("--config".equals(option)) {
                configPath = requireValue(args, ++index, option);
            } else if ("--path".equals(option)) {
                if (path != null) {
                    throw new IllegalArgumentException("--path may only be specified once");
                }
                path = validatePath(requireValue(args, ++index, option));
            } else if ("--poll-interval-ms".equals(option)) {
                pollIntervalMillis = parsePositiveLong(requireValue(args, ++index, option), option);
            } else if ("--parallelism".equals(option)) {
                parallelism = parsePositiveInt(requireValue(args, ++index, option), option);
            } else {
                throw new IllegalArgumentException("Unknown option: " + option);
            }
        }
        if (isBlank(configPath)) {
            throw new IllegalArgumentException("--config is required");
        }
        if (path == null) {
            throw new IllegalArgumentException("--path is required");
        }
        return new CobbleCompactionJobOptions(
                configPath, path, pollIntervalMillis, parallelism);
    }

    public String configPath() {
        return configPath;
    }

    public long pollIntervalMillis() {
        return pollIntervalMillis;
    }

    public int parallelism() {
        return parallelism;
    }

    CobbleCompactionSource source() {
        return CobbleCompactionSource.scan(configPath, path, pollIntervalMillis);
    }

    private static String validatePath(String value) {
        URI uri;
        try {
            uri = new URI(value);
        } catch (URISyntaxException error) {
            throw new IllegalArgumentException("Invalid --path: " + value, error);
        }
        if (uri.getScheme() == null) {
            Path path = Paths.get(value);
            if (!path.isAbsolute()) {
                throw new IllegalArgumentException("--path must be absolute: " + value);
            }
        } else if (uri.getRawAuthority() == null && !"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException(
                    "--path must be an absolute storage URL: " + value);
        }
        if (uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null) {
            throw new IllegalArgumentException(
                    "--path must not contain credentials, query options, or fragments; "
                            + "configure storage access in --config: "
                            + value);
        }
        return value;
    }

    private static String requireValue(String[] args, int index, String option) {
        if (index >= args.length || isBlank(args[index])) {
            throw new IllegalArgumentException(option + " requires a value");
        }
        return args[index];
    }

    private static int parsePositiveInt(String value, String option) {
        long parsed = parsePositiveLong(value, option);
        if (parsed > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(option + " is too large: " + value);
        }
        return (int) parsed;
    }

    private static long parsePositiveLong(String value, String option) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException(option + " must be greater than zero");
            }
            return parsed;
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(option + " must be an integer: " + value, error);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
