package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

class UserClasspathTest {

    @Test
    void emptyHasNoEntries() {
        UserClasspath uc = UserClasspath.empty();
        assertTrue(uc.entries().isEmpty());
        assertNotNull(uc.classLoader());
        uc.close();
    }

    @Test
    void loadsClassFromSingleJar(@TempDir Path tempDir) throws Exception {
        Path jar = writeMarkerJar(tempDir, "payload.jar", "Marker");

        UserClasspath uc = UserClasspath.create(Collections.singletonList(jar.toString()));
        try {
            assertEquals(1, uc.entries().size());
            assertEquals(jar.toRealPath().toString(), uc.entries().get(0));
            Class<?> marker = Class.forName("io.cobble.test.Marker", false, uc.classLoader());
            assertNotNull(marker);
            assertEquals("io.cobble.test.Marker", marker.getName());
        } finally {
            uc.close();
        }
    }

    @Test
    void directoryExpandsJarsSortedByName(@TempDir Path tempDir) throws Exception {
        Path dir = tempDir.resolve("jars");
        Files.createDirectories(dir);
        writeMarkerJar(dir, "z.jar", "Z");
        writeMarkerJar(dir, "a.jar", "A");
        Files.createFile(dir.resolve("not-a-jar.txt"));

        UserClasspath uc = UserClasspath.create(Collections.singletonList(dir.toString()));
        try {
            // Only .jar files, sorted by filename: a.jar before z.jar.
            assertEquals(2, uc.entries().size());
            assertTrue(uc.entries().get(0).endsWith("a.jar"));
            assertTrue(uc.entries().get(1).endsWith("z.jar"));
            assertNotNull(Class.forName("io.cobble.test.A", false, uc.classLoader()));
            assertNotNull(Class.forName("io.cobble.test.Z", false, uc.classLoader()));
        } finally {
            uc.close();
        }
    }

    @Test
    void classpathOrderIsDeterministic(@TempDir Path tempDir) throws Exception {
        Path jarA = writeMarkerJar(tempDir, "a.jar", "A");
        Path jarB = writeMarkerJar(tempDir, "b.jar", "B");

        UserClasspath uc1 = UserClasspath.create(Arrays.asList(jarA.toString(), jarB.toString()));
        List<String> entries1 = uc1.entries();
        uc1.close();

        UserClasspath uc2 = UserClasspath.create(Arrays.asList(jarA.toString(), jarB.toString()));
        List<String> entries2 = uc2.entries();
        uc2.close();

        assertEquals(entries1, entries2);
    }

    @Test
    void duplicateCanonicalPathDeduplicated(@TempDir Path tempDir) throws Exception {
        Path jar = writeMarkerJar(tempDir, "dup.jar", "Dup");

        UserClasspath uc = UserClasspath.create(Arrays.asList(jar.toString(), jar.toString()));
        try {
            assertEquals(1, uc.entries().size());
        } finally {
            uc.close();
        }
    }

    @Test
    void missingPathThrowsInputException() {
        InputException e =
                assertThrows(
                        InputException.class,
                        () ->
                                UserClasspath.create(
                                        Collections.singletonList("/nonexistent/path.jar")));
        assertTrue(e.getMessage().contains("does not exist"));
    }

    @Test
    void closeIsIdempotent(@TempDir Path tempDir) throws Exception {
        Path jar = writeMarkerJar(tempDir, "a.jar", "A");
        UserClasspath uc = UserClasspath.create(Collections.singletonList(jar.toString()));
        uc.close();
        // Second close should not throw.
        uc.close();
    }

    @Test
    void emptyPathListReturnsEmptyUserClasspath() {
        UserClasspath uc = UserClasspath.create(Collections.emptyList());
        try {
            assertTrue(uc.entries().isEmpty());
        } finally {
            uc.close();
        }
    }

    @Test
    void blankPathTokensAreSkipped(@TempDir Path tempDir) throws Exception {
        // Although ServerConfig splits --user-classpath tokens, UserClasspath itself should
        // tolerate blank entries gracefully by skipping them.
        Path jar = writeMarkerJar(tempDir, "a.jar", "A");
        UserClasspath uc = UserClasspath.create(Arrays.asList("", jar.toString(), "  "));
        try {
            assertEquals(1, uc.entries().size());
        } finally {
            uc.close();
        }
    }

    // --- helpers ---

    /** Compile a trivial marker class and pack it into a jar. */
    private static Path writeMarkerJar(Path dir, String jarName, String simpleClassName)
            throws Exception {
        javax.tools.JavaCompiler compiler = javax.tools.ToolProvider.getSystemJavaCompiler();
        assumeTrue(compiler != null, "JDK compiler required for this test");

        String fullClassName = "io.cobble.test." + simpleClassName;
        String internalName = fullClassName.replace('.', '/');
        Path srcDir = dir.resolve("src-" + simpleClassName);
        Path classesDir = dir.resolve("classes-" + simpleClassName);
        Files.createDirectories(srcDir);
        Files.createDirectories(classesDir);
        Path sourceFile = srcDir.resolve(internalName + ".java");
        Files.createDirectories(sourceFile.getParent());
        Files.write(
                sourceFile,
                ("package io.cobble.test; public class " + simpleClassName + " {}")
                        .getBytes(StandardCharsets.UTF_8));

        compiler.run(
                null,
                null,
                null,
                "-d",
                classesDir.toAbsolutePath().toString(),
                sourceFile.toAbsolutePath().toString());

        Path jarPath = dir.resolve(jarName);
        String classFile = internalName + ".class";
        Path compiledClass = classesDir.resolve(classFile);
        assertTrue(Files.exists(compiledClass), "expected compiled class: " + classFile);
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jarPath))) {
            out.putNextEntry(new JarEntry(classFile));
            Files.copy(compiledClass, out);
            out.closeEntry();
        }
        return jarPath;
    }
}
