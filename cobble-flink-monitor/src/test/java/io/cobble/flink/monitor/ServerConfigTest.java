package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class ServerConfigTest {

    @Test
    void parsesNoUserJar() {
        ServerConfig config = ServerConfig.parse(new String[] {"--checkpoint", "/tmp/ckpt"});
        assertTrue(config.userJars.isEmpty());
    }

    @Test
    void parsesSingleUserJar(@TempDir Path tempDir) throws IOException {
        Path jar = tempDir.resolve("a.jar");
        Files.createFile(jar);
        ServerConfig config = ServerConfig.parse(new String[] {"--user-jar", jar.toString()});
        assertEquals(Collections.singletonList(jar.toString()), config.userJars);
    }

    @Test
    void parsesRepeatedUserJar(@TempDir Path tempDir) throws IOException {
        Path jarA = tempDir.resolve("a.jar");
        Path jarB = tempDir.resolve("b.jar");
        Files.createFile(jarA);
        Files.createFile(jarB);
        ServerConfig config =
                ServerConfig.parse(
                        new String[] {
                            "--user-jar", jarA.toString(), "--user-jar", jarB.toString(),
                        });
        assertEquals(Arrays.asList(jarA.toString(), jarB.toString()), config.userJars);
    }

    @Test
    void parsesUserClasspathAlias(@TempDir Path tempDir) throws IOException {
        Path jarA = tempDir.resolve("a.jar");
        Path jarB = tempDir.resolve("b.jar");
        Files.createFile(jarA);
        Files.createFile(jarB);
        String joined = jarA.toString() + System.getProperty("path.separator") + jarB.toString();
        ServerConfig config = ServerConfig.parse(new String[] {"--user-classpath", joined});
        assertEquals(Arrays.asList(jarA.toString(), jarB.toString()), config.userJars);
    }

    @Test
    void parsesEqualsFormUserJar(@TempDir Path tempDir) throws IOException {
        Path jar = tempDir.resolve("a.jar");
        Files.createFile(jar);
        ServerConfig config = ServerConfig.parse(new String[] {"--user-jar=" + jar.toString()});
        assertEquals(Collections.singletonList(jar.toString()), config.userJars);
    }

    @Test
    void userClasspathAndUserJarCombine(@TempDir Path tempDir) throws IOException {
        Path jarA = tempDir.resolve("a.jar");
        Path jarB = tempDir.resolve("b.jar");
        Path jarC = tempDir.resolve("c.jar");
        Files.createFile(jarA);
        Files.createFile(jarB);
        Files.createFile(jarC);
        String classpath = jarB.toString() + System.getProperty("path.separator") + jarC.toString();
        ServerConfig config =
                ServerConfig.parse(
                        new String[] {
                            "--user-jar", jarA.toString(), "--user-classpath", classpath,
                        });
        assertEquals(
                Arrays.asList(jarA.toString(), jarB.toString(), jarC.toString()), config.userJars);
    }

    @Test
    void missingUserJarPathThrowsInputException() {
        // ServerConfig.parse collects the paths; UserClasspath.create validates them.
        ServerConfig config =
                ServerConfig.parse(new String[] {"--user-jar", "/nonexistent/path.jar"});
        assertEquals(Collections.singletonList("/nonexistent/path.jar"), config.userJars);
        InputException e =
                assertThrows(InputException.class, () -> UserClasspath.create(config.userJars));
        assertTrue(e.getMessage().contains("does not exist"));
    }

    @Test
    void retainsExistingScalarOptions() {
        ServerConfig config =
                ServerConfig.parse(
                        new String[] {
                            "--port", "9090",
                            "--bind", "0.0.0.0",
                            "--total-buckets", "128",
                            "--inspect-default-limit", "5",
                            "--inspect-max-limit", "50",
                        });
        assertEquals(9090, config.port);
        assertEquals("0.0.0.0", config.bindAddress);
        assertEquals(128, config.totalBuckets);
        assertEquals(5, config.inspectDefaultLimit);
        assertEquals(50, config.inspectMaxLimit);
    }

    @Test
    void duplicateScalarOptionUsesLastValue() {
        // Scalar options preserve the legacy "last wins" semantics (the old Map.put overwrote
        // duplicates). Only --user-jar / --user-classpath are list-valued.
        ServerConfig config = ServerConfig.parse(new String[] {"--port", "9090", "--port", "9191"});
        assertEquals(9191, config.port);
    }

    @Test
    void userClasspathSkipsBlankTokens(@TempDir Path tempDir) throws IOException {
        Path jar = tempDir.resolve("a.jar");
        Files.createFile(jar);
        String pathSep = System.getProperty("path.separator");
        String joined = pathSep + jar.toString() + pathSep;
        ServerConfig config = ServerConfig.parse(new String[] {"--user-classpath", joined});
        List<String> expected = Collections.singletonList(jar.toString());
        assertEquals(expected, config.userJars);
    }
}
