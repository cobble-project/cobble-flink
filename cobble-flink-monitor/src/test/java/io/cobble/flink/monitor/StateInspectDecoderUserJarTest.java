package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.cobble.flink.common.inspect.SerializerInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Proves that {@link StateInspectDecoder} restores user serializers via the thread context
 * classloader. A non-portable serializer (compiled into a separate jar so it is absent from the
 * test classpath) is used: decode fails without the user classloader and succeeds with it.
 */
class StateInspectDecoderUserJarTest {

    private static final String USER_PACKAGE = "io.cobble.test";
    private static final String USER_SERIALIZER_CLASS = USER_PACKAGE + ".UserStringSerializer";
    private static final String USER_SERIALIZER_INTERNAL = USER_SERIALIZER_CLASS.replace('.', '/');

    /**
     * Source for a non-portable TypeSerializer&lt;String&gt; whose {@code snapshotConfiguration()}
     * throws, forcing {@code SerializerInspectSchema.fromSerializer} to persist {@code
     * serializedSerializerBytes} (the Java-serialized live serializer). Restore then requires the
     * user classloader to deserialize those bytes.
     */
    private static final String USER_SERIALIZER_SOURCE =
            "package "
                    + USER_PACKAGE
                    + ";\n"
                    + "import org.apache.flink.api.common.typeutils.TypeSerializer;\n"
                    + "import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;\n"
                    + "import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;\n"
                    + "import org.apache.flink.api.common.typeutils.base.StringSerializer;\n"
                    + "import org.apache.flink.core.memory.DataInputView;\n"
                    + "import org.apache.flink.core.memory.DataOutputView;\n"
                    + "public final class UserStringSerializer extends TypeSerializer<String> {\n"
                    + "  private static final long serialVersionUID = 1L;\n"
                    + "  public boolean isImmutableType() { return true; }\n"
                    + "  public TypeSerializer<String> duplicate() { return this; }\n"
                    + "  public String createInstance() { return \"\"; }\n"
                    + "  public String copy(String from) { return from; }\n"
                    + "  public String copy(String from, String reuse) { return from; }\n"
                    + "  public int getLength() { return -1; }\n"
                    + "  public void serialize(String record, DataOutputView target) throws java.io.IOException { StringSerializer.INSTANCE.serialize(record, target); }\n"
                    + "  public String deserialize(DataInputView source) throws java.io.IOException { return StringSerializer.INSTANCE.deserialize(source); }\n"
                    + "  public String deserialize(String reuse, DataInputView source) throws java.io.IOException { return deserialize(source); }\n"
                    + "  public void copy(DataInputView source, DataOutputView target) throws java.io.IOException { StringSerializer.INSTANCE.copy(source, target); }\n"
                    + "  public boolean equals(Object obj) { return obj instanceof UserStringSerializer; }\n"
                    + "  public int hashCode() { return UserStringSerializer.class.hashCode(); }\n"
                    + "  public TypeSerializerSnapshot<String> snapshotConfiguration() {\n"
                    + "    throw new RuntimeException(\"expected snapshot failure - non-portable\");\n"
                    + "  }\n"
                    + "}\n";

    @Test
    void restoreFailsWithoutUserJarAndSucceedsWithUserJar(@TempDir Path tempDir) throws Exception {
        // Arrange: compile the user serializer into a jar (absent from test classpath).
        Path userJar = compileUserJar(tempDir);
        URLClassLoader userLoader =
                new URLClassLoader(
                        new URL[] {userJar.toUri().toURL()}, getClass().getClassLoader());

        // Sanity: the user serializer class is NOT visible to the test/system classloader,
        // but IS visible to the user loader.
        assertClassNotFound(USER_SERIALIZER_CLASS, getClass().getClassLoader());
        Class<?> userSerializerClass = Class.forName(USER_SERIALIZER_CLASS, false, userLoader);
        assertNotNull(userSerializerClass);

        // Build a StateInspectSchema.forValue with the user serializer as the value serializer.
        // forValue internally calls SerializerInspectSchema.fromSerializer(serializer), which
        // captures serializedSerializerBytes (because snapshotConfiguration() throws).
        TypeSerializer<?> userSerializer = instantiateSerializer(userLoader);
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "user-value-state",
                        "cf-user-value",
                        false,
                        StringSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        userSerializer);
        // Verify the value serializer schema has serialized fallback bytes (non-portable).
        SerializerInspectSchema valueSchema = schema.valueSerializer();
        assertNotNull(valueSchema.serializedSerializerBytes());
        assertNull(valueSchema.snapshotBytes());

        // Build a semantic target so both raw and semantic decode paths are exercised.
        InspectTarget target =
                semanticTarget(
                        schema,
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("VARCHAR(2147483647)"),
                                StateInspectType.unknown(),
                                StateInspectType.scalar("VARCHAR(2147483647)")));

        byte[] rowKey = keyWithVoidNamespace(serializeString("k"));
        byte[][] columns = new byte[][] {serializeString("hello")};

        ClassLoader originalTccl = Thread.currentThread().getContextClassLoader();
        try {
            // --- Without user jar: monitor's own classloader cannot resolve UserStringSerializer.
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            StateInspectDecoder.DecodedRow row =
                    StateInspectDecoder.decode(target, rowKey, columns);
            // Value decode should fail -> decode_error present.
            assertNotNull(row.decodeError, "decode error expected without user jar");
            assertTrue(
                    row.decodeError.contains("UserStringSerializer")
                            || row.decodeError.contains("Failed to restore serializer"),
                    "decode error should mention the user serializer: " + row.decodeError);

            // --- With user jar: TCCL can resolve UserStringSerializer -> restore succeeds.
            Thread.currentThread().setContextClassLoader(userLoader);
            StateInspectDecoder.DecodedRow rowOk =
                    StateInspectDecoder.decode(target, rowKey, columns);
            // Key still decodes (StringSerializer is portable).
            assertEquals("k", rowOk.decodedKey.get("key"));
            // Value decodes to "hello" (the user serializer delegates to StringSerializer).
            assertEquals("hello", rowOk.decodedValue, "value should decode with user jar");
        } finally {
            Thread.currentThread().setContextClassLoader(originalTccl);
            userLoader.close();
        }
    }

    // --- helpers ---

    /** Compile the user serializer source into a jar and return the jar path. */
    private Path compileUserJar(Path tempDir) throws Exception {
        javax.tools.JavaCompiler compiler = javax.tools.ToolProvider.getSystemJavaCompiler();
        assumeTrue(compiler != null, "JDK compiler required for this test");

        Path srcDir = tempDir.resolve("src");
        Path classesDir = tempDir.resolve("classes");
        Files.createDirectories(srcDir);
        Files.createDirectories(classesDir);
        Path sourceFile = srcDir.resolve(USER_SERIALIZER_INTERNAL + ".java");
        Files.createDirectories(sourceFile.getParent());
        Files.write(sourceFile, USER_SERIALIZER_SOURCE.getBytes(StandardCharsets.UTF_8));

        // Compile against the Flink API classes on the test classpath.
        String classpath = System.getProperty("java.class.path");
        compiler.run(
                null,
                null,
                null,
                "-classpath",
                classpath,
                "-d",
                classesDir.toAbsolutePath().toString(),
                sourceFile.toAbsolutePath().toString());

        // Pack the compiled .class file into a jar.
        Path jarPath = tempDir.resolve("user-serializer.jar");
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jarPath))) {
            String classFile = USER_SERIALIZER_INTERNAL + ".class";
            Path compiledClass = classesDir.resolve(classFile);
            assertTrue(Files.exists(compiledClass), "expected compiled class: " + classFile);
            out.putNextEntry(new JarEntry(classFile));
            Files.copy(compiledClass, out);
            out.closeEntry();
        }
        return jarPath;
    }

    /** Instantiate the user serializer via the user classloader. */
    @SuppressWarnings("unchecked")
    private static TypeSerializer<?> instantiateSerializer(ClassLoader userLoader)
            throws Exception {
        Class<?> serializerClass = Class.forName(USER_SERIALIZER_CLASS, true, userLoader);
        Constructor<?> ctor = serializerClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        return (TypeSerializer<?>) ctor.newInstance();
    }

    private static InspectTarget target(StateInspectSchema schema) {
        return new InspectTarget(
                schema.stateName(),
                schema.stateName(),
                "state",
                schema.columnFamily(),
                false,
                schema.stateKind().name(),
                java.util.Collections.emptyMap(),
                schema,
                null);
    }

    private static InspectTarget semanticTarget(
            StateInspectSchema schema, StateInspectSemanticSchema semanticSchema) {
        return new InspectTarget(
                schema.stateName(),
                schema.stateName(),
                "state",
                schema.columnFamily(),
                false,
                schema.stateKind().name(),
                java.util.Collections.emptyMap(),
                schema,
                semanticSchema,
                null);
    }

    private static byte[] serializeString(String value) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(32);
        StringSerializer.INSTANCE.serialize(value, out);
        return out.getCopyOfBuffer();
    }

    private static byte[] keyWithVoidNamespace(byte[] keyBytes) throws IOException {
        // VoidNamespace layout: key bytes + single 0x00 byte (VoidNamespaceSerializer writes one
        // byte). Mirrors StateInspectDecoderTest.keyWithVoidNamespace.
        DataOutputSerializer out = new DataOutputSerializer(keyBytes.length + 1);
        out.write(keyBytes);
        out.writeByte(0);
        return out.getCopyOfBuffer();
    }

    private static void assertClassNotFound(String className, ClassLoader loader) {
        try {
            Class.forName(className, false, loader);
            throw new AssertionError(
                    "Expected ClassNotFoundException for "
                            + className
                            + " with loader "
                            + loader
                            + " - the class must NOT be on the test classpath");
        } catch (ClassNotFoundException expected) {
            // pass
        }
    }
}
