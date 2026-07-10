package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.cobble.flink.common.inspect.DescriptorCapability;
import io.cobble.flink.common.inspect.InspectDecoderDescriptor;
import io.cobble.flink.common.inspect.SerializerInspectSchema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Proves that the monitor's classless Avro path works without {@code flink-avro} on the runtime
 * classpath. The test:
 *
 * <ol>
 *   <li>Serializes fixtures using the test-scoped {@link AvroSerializer} (before isolation).
 *   <li>Executes the monitor classless path through an isolated classloader whose parent has the
 *       monitor production runtime and Apache Avro but explicitly cannot load {@code
 *       org.apache.flink.formats.avro.*}.
 *   <li>Asserts classless semantic JSON output is correct.
 *   <li>Asserts direct loading of Flink's Avro helper from that loader fails.
 *   <li>Inspects the packaged monitor jar and asserts it contains no Flink Avro classes.
 * </ol>
 */
class NoFlinkAvroIsolationTest {

    @Test
    void classlessDecodeWorksInIsolatedLoaderWithoutFlinkAvro() throws Exception {
        // Step 1: Build fixture in the test classloader (which has flink-avro).
        Schema schema = Schema.createRecord("Person", null, null, false);
        schema.setFields(
                Arrays.asList(
                        new Schema.Field("id", Schema.create(Schema.Type.INT), null, null),
                        new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

        AvroSerializer<GenericRecord> avroSerializer =
                new AvroSerializer<>(GenericRecord.class, schema);
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", 42);
        record.put("name", "isolated-test");

        DataOutputSerializer out = new DataOutputSerializer(64);
        avroSerializer.serialize(record, out);
        byte[] valueBytes = out.getCopyOfBuffer();

        SerializerInspectSchema serializerSchema =
                SerializerInspectSchema.fromSerializer(avroSerializer);
        InspectDecoderDescriptor descriptor = serializerSchema.decoderDescriptor();
        assertNotNull(descriptor);
        assertTrue(descriptor.isAvro());
        assertEquals(DescriptorCapability.FULLY_CLASSLESS, descriptor.capability());

        String schemaJson = descriptor.avroWriterSchemaJson();
        String wireFormat = descriptor.avroWireFormat();

        // Step 2: Build an isolated classloader that has the monitor + Avro classes but NOT
        // flink-avro classes. We filter out any classpath URL that contains flink-avro.
        URL[] filteredUrls = buildClasspathWithoutFlinkAvro();
        URLClassLoader isolatedLoader =
                new URLClassLoader(filteredUrls, ClassLoader.getSystemClassLoader().getParent());

        try {
            // Step 3: Verify flink-avro is NOT loadable from the isolated loader.
            try {
                isolatedLoader.loadClass("org.apache.flink.formats.avro.typeutils.AvroSerializer");
                fail(
                        "flink-avro AvroSerializer should not be loadable from the isolated classloader");
            } catch (ClassNotFoundException e) {
                // Expected - flink-avro is not on the isolated classpath.
            }

            // Step 4: Verify monitor classes ARE loadable.
            Class<?> decoderClass =
                    isolatedLoader.loadClass("io.cobble.flink.monitor.AvroClasslessDecoder");
            Class<?> descriptorClass =
                    isolatedLoader.loadClass(
                            "io.cobble.flink.common.inspect.InspectDecoderDescriptor");

            // Step 5: Build a descriptor in the isolated loader.
            Object isolatedDescriptor = buildAvroDescriptor(isolatedLoader, schemaJson, wireFormat);
            assertNotNull(isolatedDescriptor);

            // Step 6: Invoke decode and verify the result through reflection (the returned
            // GenericRecord is loaded by the isolated loader, so we can't use instanceof).
            Method decodeMethod =
                    decoderClass.getDeclaredMethod("decode", descriptorClass, byte[].class);
            decodeMethod.setAccessible(true);
            Object result = decodeMethod.invoke(null, isolatedDescriptor, valueBytes);
            assertNotNull(result);

            // The result is a GenericRecord loaded by the isolated loader. Use reflection to
            // verify field values.
            Method getMethod = result.getClass().getMethod("get", String.class);
            assertEquals(42, getMethod.invoke(result, "id"));
            assertEquals("isolated-test", getMethod.invoke(result, "name").toString());
        } finally {
            isolatedLoader.close();
        }
    }

    /**
     * Asserts that the packaged monitor jar contains Apache Avro classes but no flink-avro classes.
     *
     * <p>This test requires the jar to be built first ({@code mvn package}). When the jar is not
     * present (e.g. during {@code mvn test}), the test is skipped via {@code assumeTrue} rather
     * than passing silently. The Phase 2 report's validation section includes an explicit
     * post-package run of this test.
     */
    @Test
    void monitorJarContainsNoFlinkAvroClasses() throws Exception {
        File jarFile = findMonitorJar();
        org.junit.jupiter.api.Assumptions.assumeTrue(
                jarFile != null,
                "Monitor jar not found; run `mvn package` first. This test is part of the"
                        + " post-package validation step.");
        try (JarFile jar = new JarFile(jarFile)) {
            List<String> flinkAvroEntries = new ArrayList<>();
            List<String> avroEntries = new ArrayList<>();
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (name.startsWith("org/apache/flink/formats/avro/")) {
                    flinkAvroEntries.add(name);
                }
                if (name.startsWith("org/apache/avro/") && name.endsWith(".class")) {
                    avroEntries.add(name);
                }
            }
            assertFalse(
                    avroEntries.isEmpty(),
                    "Monitor jar should contain Apache Avro classes, found none");
            assertTrue(
                    flinkAvroEntries.isEmpty(),
                    "Monitor jar must NOT contain flink-avro classes, but found: "
                            + flinkAvroEntries);
        }
    }

    // ---- Helpers ----

    /**
     * Builds a classpath that excludes any JAR or directory containing {@code flink-avro}. The
     * monitor production classes, cobble-common, and Apache Avro remain.
     */
    private static URL[] buildClasspathWithoutFlinkAvro() {
        String classpath = System.getProperty("java.class.path");
        String[] paths = classpath.split(File.pathSeparator);
        List<URL> urls = new ArrayList<>();
        for (String path : paths) {
            if (path.contains("flink-avro")) {
                continue;
            }
            try {
                urls.add(new File(path).toURI().toURL());
            } catch (Exception e) {
                // Skip malformed paths.
            }
        }
        return urls.toArray(new URL[0]);
    }

    /**
     * Constructs an AVRO {@link InspectDecoderDescriptor} in the isolated classloader's class
     * space. We use the public factory method if available, otherwise fall back to reflection on
     * the inner {@code AvroDescriptor} class.
     */
    @SuppressWarnings("unchecked")
    private static Object buildAvroDescriptor(
            ClassLoader loader, String schemaJson, String wireFormat) throws Exception {
        Class<?> descriptorClass =
                loader.loadClass("io.cobble.flink.common.inspect.InspectDecoderDescriptor");
        Class<? extends Enum> capClass =
                (Class<? extends Enum>)
                        loader.loadClass("io.cobble.flink.common.inspect.DescriptorCapability");
        Object fullyCap = Enum.valueOf(capClass, "FULLY_CLASSLESS");

        // Use the public factory: InspectDecoderDescriptor.avro(schemaJson, wireFormat, capability)
        Method factory =
                descriptorClass.getDeclaredMethod("avro", String.class, String.class, capClass);
        factory.setAccessible(true);
        return factory.invoke(null, schemaJson, wireFormat, fullyCap);
    }

    private static File findMonitorJar() {
        // Look relative to the test's class output directory (target/).
        String classDir =
                NoFlinkAvroIsolationTest.class
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation()
                        .getPath();
        File targetDir = new File(classDir).getParentFile();
        if (targetDir == null || !targetDir.isDirectory()) {
            return null;
        }
        File[] jars =
                targetDir.listFiles(
                        (dir, name) ->
                                name.startsWith("cobble-flink-monitor")
                                        && name.endsWith(".jar")
                                        && !name.contains("sources")
                                        && !name.contains("javadoc")
                                        && !name.contains("tests"));
        if (jars != null && jars.length > 0) {
            return jars[0];
        }
        return null;
    }
}
