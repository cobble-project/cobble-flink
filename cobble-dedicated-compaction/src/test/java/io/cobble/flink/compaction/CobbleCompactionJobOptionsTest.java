package io.cobble.flink.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class CobbleCompactionJobOptionsTest {
    @Test
    void parsesPathMonitor() {
        CobbleCompactionJobOptions options =
                CobbleCompactionJobOptions.parse(
                        new String[] {
                            "--config",
                            "compactor.yaml",
                            "--path",
                            "s3://bucket/checkpoints/job-id",
                            "--parallelism",
                            "4",
                            "--poll-interval-ms",
                            "2500"
                        });

        assertEquals("compactor.yaml", options.configPath());
        assertEquals(4, options.parallelism());
        assertEquals(2500L, options.pollIntervalMillis());
    }

    @Test
    void parsesExactDatabasePath() {
        CobbleCompactionJobOptions options =
                CobbleCompactionJobOptions.parse(
                        new String[] {"--config", "compactor.yaml", "--path", "s3://bucket/db-a"});

        assertEquals(1, options.parallelism());
        assertEquals(1000L, options.pollIntervalMillis());
    }

    @Test
    void requiresExactlyOnePath() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CobbleCompactionJobOptions.parse(new String[] {"--config", "c.yaml"}));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        CobbleCompactionJobOptions.parse(
                                new String[] {
                                    "--config", "c.yaml", "--path", "/checkpoints", "--path", "/db"
                                }));
    }

    @Test
    void rejectsInvalidNumbersAndUnknownOptions() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        CobbleCompactionJobOptions.parse(
                                new String[] {
                                    "--config",
                                    "c.yaml",
                                    "--path",
                                    "/checkpoints",
                                    "--parallelism",
                                    "0"
                                }));
        assertThrows(
                IllegalArgumentException.class,
                () -> CobbleCompactionJobOptions.parse(new String[] {"--unknown"}));
    }

    @Test
    void rejectsRelativeAndCredentialBearingPaths() {
        assertInvalidPath("checkpoints/job-id");
        assertInvalidPath("s3://access:secret@bucket/checkpoints/job-id");
        assertInvalidPath("s3://bucket/checkpoints?endpoint=localhost");
    }

    private static void assertInvalidPath(String path) {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        CobbleCompactionJobOptions.parse(
                                new String[] {"--config", "c.yaml", "--path", path}));
    }
}
