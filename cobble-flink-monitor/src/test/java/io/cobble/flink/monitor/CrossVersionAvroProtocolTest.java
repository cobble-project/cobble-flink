package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.DescriptorCapability;
import io.cobble.flink.common.inspect.InspectDecoderDescriptor;
import io.cobble.flink.common.inspect.SerializerInspectSchema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Cross-version protocol verification: produces real GenericRecord bytes from Flink 1.17, 1.19, and
 * 2.0 {@code AvroSerializer}s, then decodes each with the monitor build. Asserts identical semantic
 * JSON across all writer versions.
 *
 * <p>This is a Phase 2 acceptance gate. The test is only enabled when the Flink version jars are
 * available in the local Maven repository.
 */
class CrossVersionAvroProtocolTest {

    private static final String TEST_SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"CrossVersionRecord\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"int\"},"
                    + "{\"name\":\"label\",\"type\":\"string\"},"
                    + "{\"name\":\"active\",\"type\":\"boolean\"}"
                    + "]}";

    /** Schema JSON matching {@link TestSpecificEvent#SCHEMA$}. */
    private static final String SPECIFIC_SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"TestSpecificEvent\","
                    + "\"namespace\":\"io.cobble.flink.monitor\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"int\"},"
                    + "{\"name\":\"label\",\"type\":\"string\"},"
                    + "{\"name\":\"active\",\"type\":\"boolean\"}"
                    + "]}";

    private static final int EXPECTED_ID = 123;
    private static final String EXPECTED_LABEL = "cross-version";
    private static final boolean EXPECTED_ACTIVE = true;

    /**
     * Verifies that bytes written by Flink 1.17.2's AvroSerializer decode correctly via the
     * monitor's classless path.
     */
    @Test
    @EnabledIf("isFlink117Available")
    void decodesFlink117GenericRecordBytes() throws Exception {
        byte[] bytes = serializeGenericWithFlinkVersion("1.17.2");
        GenericRecord decoded = decodeWithMonitor(bytes, TEST_SCHEMA_JSON);
        assertRecordValues(decoded);
    }

    /**
     * Verifies that bytes written by Flink 1.19.3's AvroSerializer decode correctly. The 1.19
     * fixture is required by the Phase 2 plan.
     */
    @Test
    @EnabledIf("isFlink119Available")
    void decodesFlink119GenericRecordBytes() throws Exception {
        byte[] bytes = serializeGenericWithFlinkVersion("1.19.3");
        GenericRecord decoded = decodeWithMonitor(bytes, TEST_SCHEMA_JSON);
        assertRecordValues(decoded);
    }

    /** Verifies that bytes written by Flink 2.0.0's AvroSerializer decode correctly. */
    @Test
    @EnabledIf("isFlink200Available")
    void decodesFlink200GenericRecordBytes() throws Exception {
        byte[] bytes = serializeGenericWithFlinkVersion("2.0.0");
        GenericRecord decoded = decodeWithMonitor(bytes, TEST_SCHEMA_JSON);
        assertRecordValues(decoded);
    }

    /**
     * Verifies that SpecificRecord bytes written by Flink 1.17.2's AvroSerializer decode correctly.
     */
    @Test
    @EnabledIf("isFlink117Available")
    void decodesFlink117SpecificRecordBytes() throws Exception {
        byte[] bytes = serializeSpecificWithFlinkVersion("1.17.2");
        GenericRecord decoded = decodeWithMonitor(bytes, SPECIFIC_SCHEMA_JSON);
        assertRecordValues(decoded);
    }

    /**
     * Verifies that SpecificRecord bytes written by Flink 1.19.3's AvroSerializer decode correctly.
     */
    @Test
    @EnabledIf("isFlink119Available")
    void decodesFlink119SpecificRecordBytes() throws Exception {
        byte[] bytes = serializeSpecificWithFlinkVersion("1.19.3");
        GenericRecord decoded = decodeWithMonitor(bytes, SPECIFIC_SCHEMA_JSON);
        assertRecordValues(decoded);
    }

    /**
     * Verifies that SpecificRecord bytes written by Flink 2.0.0's AvroSerializer decode correctly.
     */
    @Test
    @EnabledIf("isFlink200Available")
    void decodesFlink200SpecificRecordBytes() throws Exception {
        byte[] bytes = serializeSpecificWithFlinkVersion("2.0.0");
        GenericRecord decoded = decodeWithMonitor(bytes, SPECIFIC_SCHEMA_JSON);
        assertRecordValues(decoded);
    }

    /**
     * Verifies that all three versions produce identical bytes for the same GenericRecord input.
     * This is the strongest form of protocol compatibility: if the bytes are identical, the
     * semantic JSON will be identical by construction.
     */
    @Test
    @EnabledIf("allVersionsAvailable")
    void allVersionsProduceIdenticalGenericBytes() throws Exception {
        byte[] bytes117 = serializeGenericWithFlinkVersion("1.17.2");
        byte[] bytes119 = serializeGenericWithFlinkVersion("1.19.3");
        byte[] bytes200 = serializeGenericWithFlinkVersion("2.0.0");

        assertEquals(
                Arrays.toString(bytes117),
                Arrays.toString(bytes119),
                "1.17 and 1.19 GenericRecord bytes differ");
        assertEquals(
                Arrays.toString(bytes117),
                Arrays.toString(bytes200),
                "1.17 and 2.0 GenericRecord bytes differ");
    }

    /**
     * Verifies that SpecificRecord bytes are also identical across all three versions, and that
     * they match the GenericRecord bytes (since the wire format is the same).
     */
    @Test
    @EnabledIf("allVersionsAvailable")
    void allVersionsProduceIdenticalSpecificBytes() throws Exception {
        byte[] bytes117 = serializeSpecificWithFlinkVersion("1.17.2");
        byte[] bytes119 = serializeSpecificWithFlinkVersion("1.19.3");
        byte[] bytes200 = serializeSpecificWithFlinkVersion("2.0.0");

        assertEquals(
                Arrays.toString(bytes117),
                Arrays.toString(bytes119),
                "1.17 and 1.19 SpecificRecord bytes differ");
        assertEquals(
                Arrays.toString(bytes117),
                Arrays.toString(bytes200),
                "1.17 and 2.0 SpecificRecord bytes differ");

        // SpecificRecord and GenericRecord should produce identical bytes for the same data.
        byte[] generic117 = serializeGenericWithFlinkVersion("1.17.2");
        assertEquals(
                Arrays.toString(bytes117),
                Arrays.toString(generic117),
                "SpecificRecord and GenericRecord bytes differ for 1.17");
    }

    // ---- Helpers ----

    /**
     * Serializes a fixed GenericRecord using the specified Flink version's AvroSerializer, loaded
     * from the local Maven repository via an isolated URLClassLoader.
     */
    @SuppressWarnings("unchecked")
    private static byte[] serializeGenericWithFlinkVersion(String flinkVersion) throws Exception {
        URL[] urls = buildVersionClasspath(flinkVersion);
        try (URLClassLoader loader =
                new URLClassLoader(urls, ClassLoader.getSystemClassLoader().getParent())) {
            Class<?> schemaClass = loader.loadClass("org.apache.avro.Schema");
            Class<?> recordClass = loader.loadClass("org.apache.avro.generic.GenericData$Record");
            Class<?> genericRecordClass = loader.loadClass("org.apache.avro.generic.GenericRecord");
            Class<?> avroSerializerClass =
                    loader.loadClass("org.apache.flink.formats.avro.typeutils.AvroSerializer");
            Class<?> dataOutputSerializerClass =
                    loader.loadClass("org.apache.flink.core.memory.DataOutputSerializer");

            // Parse the schema.
            Method parseMethod = schemaClass.getMethod("parse", String.class);
            Object schema = parseMethod.invoke(null, TEST_SCHEMA_JSON);

            // Create AvroSerializer(GenericRecord.class, schema).
            Constructor<?> avroSerializerCtor =
                    avroSerializerClass.getConstructor(Class.class, schemaClass);
            Object avroSerializer = avroSerializerCtor.newInstance(genericRecordClass, schema);

            // Create GenericData.Record(schema).
            Constructor<?> recordCtor = recordClass.getConstructor(schemaClass);
            Object record = recordCtor.newInstance(schema);

            // Set field values.
            Method putMethod = record.getClass().getMethod("put", String.class, Object.class);
            putMethod.invoke(record, "id", EXPECTED_ID);
            putMethod.invoke(record, "label", EXPECTED_LABEL);
            putMethod.invoke(record, "active", EXPECTED_ACTIVE);

            return serializeRecord(
                    loader, avroSerializerClass, avroSerializer, record, dataOutputSerializerClass);
        }
    }

    /**
     * Serializes a fixed SpecificRecord using the specified Flink version's AvroSerializer. The
     * {@link TestSpecificEvent} class is loaded from the test classpath (which is included in the
     * isolated loader's URLs).
     */
    @SuppressWarnings("unchecked")
    private static byte[] serializeSpecificWithFlinkVersion(String flinkVersion) throws Exception {
        URL[] urls = buildVersionClasspath(flinkVersion);
        try (URLClassLoader loader =
                new URLClassLoader(urls, ClassLoader.getSystemClassLoader().getParent())) {
            Class<?> avroSerializerClass =
                    loader.loadClass("org.apache.flink.formats.avro.typeutils.AvroSerializer");
            Class<?> dataOutputSerializerClass =
                    loader.loadClass("org.apache.flink.core.memory.DataOutputSerializer");

            // Load TestSpecificEvent from the test classpath.
            Class<?> specificRecordClass =
                    loader.loadClass("io.cobble.flink.monitor.TestSpecificEvent");

            // Create AvroSerializer(TestSpecificEvent.class) - single-arg constructor uses SCHEMA$.
            Constructor<?> avroSerializerCtor = avroSerializerClass.getConstructor(Class.class);
            Object avroSerializer = avroSerializerCtor.newInstance(specificRecordClass);

            // Create and populate the SpecificRecord instance.
            Object record = specificRecordClass.getDeclaredConstructor().newInstance();
            Method setId = specificRecordClass.getMethod("setId", int.class);
            setId.invoke(record, EXPECTED_ID);
            Method setLabel = specificRecordClass.getMethod("setLabel", CharSequence.class);
            setLabel.invoke(record, EXPECTED_LABEL);
            Method setActive = specificRecordClass.getMethod("setActive", boolean.class);
            setActive.invoke(record, EXPECTED_ACTIVE);

            return serializeRecord(
                    loader, avroSerializerClass, avroSerializer, record, dataOutputSerializerClass);
        }
    }

    private static byte[] serializeRecord(
            URLClassLoader loader,
            Class<?> avroSerializerClass,
            Object avroSerializer,
            Object record,
            Class<?> dataOutputSerializerClass)
            throws Exception {
        // Create DataOutputSerializer.
        Constructor<?> dataOutputCtor = dataOutputSerializerClass.getConstructor(int.class);
        Object dataOutput = dataOutputCtor.newInstance(128);

        // Serialize. DataOutputSerializer implements DataOutputView.
        Class<?> dataOutputViewClass =
                loader.loadClass("org.apache.flink.core.memory.DataOutputView");
        Method serializeMethod =
                avroSerializerClass.getMethod("serialize", Object.class, dataOutputViewClass);
        serializeMethod.invoke(avroSerializer, record, dataOutput);

        // Get bytes.
        Method getCopyOfBuffer = dataOutput.getClass().getMethod("getCopyOfBuffer");
        return (byte[]) getCopyOfBuffer.invoke(dataOutput);
    }

    /**
     * Decodes the bytes using the monitor's classless Avro decoder (which uses the current build's
     * Avro 1.11.1 and FlinkDataInputDecoder).
     */
    private static GenericRecord decodeWithMonitor(byte[] bytes, String schemaJson)
            throws Exception {
        // Build a descriptor for the test schema.
        Schema schema = new Schema.Parser().parse(schemaJson);
        // Use the current build's AvroSerializer to extract the descriptor.
        AvroSerializer<GenericRecord> avroSerializer =
                new AvroSerializer<>(GenericRecord.class, schema);
        SerializerInspectSchema serializerSchema =
                SerializerInspectSchema.fromSerializer(avroSerializer);
        InspectDecoderDescriptor descriptor = serializerSchema.decoderDescriptor();

        assertNotNull(descriptor);
        assertTrue(descriptor.isAvro());
        assertEquals(DescriptorCapability.FULLY_CLASSLESS, descriptor.capability());

        // Decode using the monitor's classless path.
        return AvroClasslessDecoder.decode(descriptor, bytes);
    }

    private static void assertRecordValues(GenericRecord record) {
        assertNotNull(record);
        assertEquals(EXPECTED_ID, record.get("id"));
        assertEquals(EXPECTED_LABEL, record.get("label").toString());
        assertEquals(EXPECTED_ACTIVE, record.get("active"));
    }

    /**
     * Builds a classpath for the specified Flink version. Uses the current test classpath (minus
     * the default flink-avro) as a base, then prepends the version-specific flink-avro, flink-core,
     * and Avro jars. This ensures all transitive dependencies (Jackson, etc.) are available while
     * the version-specific AvroSerializer is loaded.
     */
    private static URL[] buildVersionClasspath(String flinkVersion) throws Exception {
        String m2Home = System.getProperty("user.home") + "/.m2/repository";

        // Avro version: 1.17.2 uses 1.11.1, 1.19.3 uses 1.11.4, 2.0.0 uses 1.11.4.
        String avroVersion;
        switch (flinkVersion) {
            case "1.17.2":
                avroVersion = "1.11.1";
                break;
            case "1.19.3":
            case "2.0.0":
                avroVersion = "1.11.4";
                break;
            default:
                avroVersion = "1.11.1";
        }

        // Start with the version-specific jars (these take precedence).
        String[][] versionArtifacts = {
            {"org/apache/flink/flink-avro", "flink-avro", flinkVersion},
            {"org/apache/flink/flink-core", "flink-core", flinkVersion},
            {"org/apache/avro/avro", "avro", avroVersion},
        };

        java.util.List<URL> urls = new java.util.ArrayList<>();
        for (String[] art : versionArtifacts) {
            Path jarPath = Paths.get(m2Home, art[0], art[2], art[1] + "-" + art[2] + ".jar");
            if (Files.exists(jarPath)) {
                urls.add(jarPath.toUri().toURL());
            }
        }

        // Add the full test classpath, EXCLUDING the default flink-avro and avro jars (which
        // would conflict with the version-specific ones above). Since URLClassLoader checks
        // its own URLs first, the version-specific jars will be loaded before the test
        // classpath's default versions.
        String classpath = System.getProperty("java.class.path");
        for (String path : classpath.split(File.pathSeparator)) {
            // Skip flink-avro and avro from the default test classpath.
            if (path.contains("flink-avro") || path.contains("org/apache/avro/avro")) {
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

    // ---- Availability checks ----

    @SuppressWarnings("unused")
    static boolean isFlink117Available() {
        return jarExists("org/apache/flink/flink-avro/1.17.2/flink-avro-1.17.2.jar");
    }

    @SuppressWarnings("unused")
    static boolean isFlink119Available() {
        return jarExists("org/apache/flink/flink-avro/1.19.3/flink-avro-1.19.3.jar");
    }

    @SuppressWarnings("unused")
    static boolean isFlink200Available() {
        return jarExists("org/apache/flink/flink-avro/2.0.0/flink-avro-2.0.0.jar");
    }

    @SuppressWarnings("unused")
    static boolean allVersionsAvailable() {
        return isFlink117Available() && isFlink119Available() && isFlink200Available();
    }

    private static boolean jarExists(String relativePath) {
        String m2Home = System.getProperty("user.home") + "/.m2/repository";
        return new File(m2Home, relativePath).exists();
    }
}
