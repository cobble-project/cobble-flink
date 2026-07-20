package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.cobble.flink.common.inspect.DescriptorCapability;
import io.cobble.flink.common.inspect.InspectDecoderDescriptor;
import io.cobble.flink.common.inspect.SerializerInspectSchema;
import io.cobble.flink.common.inspect.decode.ClasslessPojoValue;
import io.cobble.flink.common.inspect.decode.PojoInspectDecoder;
import io.cobble.flink.inspect.internal.*;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Cross-version POJO protocol validation. Produces real POJO bytes from Flink 1.17.2, 1.19.3, and
 * 2.0.0 {@code PojoSerializer}s, then decodes each with the monitor's classless path.
 *
 * <p><strong>This class is NOT named {@code *Test}</strong>, so Surefire's default pattern does not
 * pick it up during normal {@code mvn test}. It is intended to be run explicitly via:
 *
 * <pre>{@code
 * mvn test -pl cobble-flink-monitor -am -Dtest=CrossVersionPojoProtocolValidation
 *     -Dsurefire.failIfNoSpecifiedTests=false
 * }</pre>
 *
 * <p>The companion script {@code scripts/cross-version-pojo-protocol.sh} pre-checks jar
 * availability and runs this validation.
 *
 * <p>Each test <strong>hard-fails</strong> (no {@code @EnabledIf}, no {@code assumeTrue}) if any
 * required jar is missing. Each isolated {@code URLClassLoader} independently loads the POJO
 * fixture class from the test-classes directory, so the parent loader cannot leak the current
 * build's class.
 */
class CrossVersionPojoProtocolValidation {

    private static final Path MAVEN_REPOSITORY =
            Paths.get(
                    System.getProperty(
                            "cross.version.maven.repo",
                            Paths.get(System.getProperty("user.home"), ".m2", "repository")
                                    .toString()));

    private static final String[] REQUIRED_VERSIONS = {"1.17.2", "1.19.3", "2.0.0"};

    // ---- Tests ----

    @Test
    void allVersionsProduceIdenticalBasePojoBytes() throws Exception {
        requireAllJarsAvailable();

        byte[] bytes117 = serializeBasePojoWithFlinkVersion("1.17.2");
        byte[] bytes119 = serializeBasePojoWithFlinkVersion("1.19.3");
        byte[] bytes200 = serializeBasePojoWithFlinkVersion("2.0.0");

        assertArrayEquals(bytes117, bytes119, "1.17.2 vs 1.19.3 base POJO bytes differ");
        assertArrayEquals(bytes117, bytes200, "1.17.2 vs 2.0.0 base POJO bytes differ");
    }

    @Test
    void allVersionsProduceIdenticalNestedPojoBytes() throws Exception {
        requireAllJarsAvailable();

        byte[] bytes117 = serializeNestedPojoWithFlinkVersion("1.17.2");
        byte[] bytes119 = serializeNestedPojoWithFlinkVersion("1.19.3");
        byte[] bytes200 = serializeNestedPojoWithFlinkVersion("2.0.0");

        assertArrayEquals(bytes117, bytes119, "1.17.2 vs 1.19.3 nested POJO bytes differ");
        assertArrayEquals(bytes117, bytes200, "1.17.2 vs 2.0.0 nested POJO bytes differ");
    }

    @Test
    void allVersionsProduceIdenticalRegisteredSubclassBytes() throws Exception {
        requireAllJarsAvailable();

        byte[] bytes117 = serializeRegisteredSubclassWithFlinkVersion("1.17.2", 0);
        byte[] bytes119 = serializeRegisteredSubclassWithFlinkVersion("1.19.3", 0);
        byte[] bytes200 = serializeRegisteredSubclassWithFlinkVersion("2.0.0", 0);

        assertArrayEquals(bytes117, bytes119, "1.17.2 vs 1.19.3 subclass tag 0 bytes differ");
        assertArrayEquals(bytes117, bytes200, "1.17.2 vs 2.0.0 subclass tag 0 bytes differ");
    }

    @Test
    void allVersionsProduceIdenticalRegisteredSubclassTag1Bytes() throws Exception {
        requireAllJarsAvailable();

        byte[] bytes117 = serializeRegisteredSubclassWithFlinkVersion("1.17.2", 1);
        byte[] bytes119 = serializeRegisteredSubclassWithFlinkVersion("1.19.3", 1);
        byte[] bytes200 = serializeRegisteredSubclassWithFlinkVersion("2.0.0", 1);

        assertArrayEquals(bytes117, bytes119, "1.17.2 vs 1.19.3 subclass tag 1 bytes differ");
        assertArrayEquals(bytes117, bytes200, "1.17.2 vs 2.0.0 subclass tag 1 bytes differ");
    }

    @Test
    void monitorDecodesCrossVersionBasePojoBytes() throws Exception {
        requireAllJarsAvailable();

        for (String version : REQUIRED_VERSIONS) {
            byte[] bytes = serializeBasePojoWithFlinkVersion(version);
            Object result = decodeWithMonitor(bytes, SimplePojoFixture.class);
            assertTrue(result instanceof ClasslessPojoValue, "version " + version);
            ClasslessPojoValue value = (ClasslessPojoValue) result;
            assertEquals(123, value.fields().get("id"), "version " + version);
            assertEquals("cross-version", value.fields().get("name"), "version " + version);
        }
    }

    @Test
    void monitorDecodesCrossVersionNestedPojoBytes() throws Exception {
        requireAllJarsAvailable();

        for (String version : REQUIRED_VERSIONS) {
            byte[] bytes = serializeNestedPojoWithFlinkVersion(version);
            Object result = decodeWithMonitor(bytes, NestedPojoFixture.class);
            assertTrue(result instanceof ClasslessPojoValue, "version " + version);
            ClasslessPojoValue value = (ClasslessPojoValue) result;
            assertEquals(7, value.fields().get("id"), "version " + version);
            Object inner = value.fields().get("inner");
            assertTrue(inner instanceof ClasslessPojoValue, "version " + version);
            assertEquals(42, ((ClasslessPojoValue) inner).fields().get("x"), "version " + version);
            assertEquals(
                    "nested", ((ClasslessPojoValue) inner).fields().get("y"), "version " + version);
        }
    }

    @Test
    void monitorDecodesCrossVersionRegisteredSubclassBytes() throws Exception {
        requireAllJarsAvailable();

        for (String version : REQUIRED_VERSIONS) {
            byte[] bytes = serializeRegisteredSubclassWithFlinkVersion(version, 0);
            Object result =
                    decodeWithMonitor(
                            bytes,
                            PojoBaseFixture.class,
                            PojoSubclassAFixture.class,
                            PojoSubclassBFixture.class);
            assertTrue(result instanceof ClasslessPojoValue, "version " + version);
            ClasslessPojoValue value = (ClasslessPojoValue) result;
            assertEquals(1, value.fields().get("id"), "version " + version);
            assertEquals("base", value.fields().get("name"), "version " + version);
            assertEquals("extraA", value.fields().get("extraA"), "version " + version);
        }
    }

    @Test
    void monitorDecodesCrossVersionRegisteredSubclassTag1Bytes() throws Exception {
        requireAllJarsAvailable();

        for (String version : REQUIRED_VERSIONS) {
            byte[] bytes = serializeRegisteredSubclassWithFlinkVersion(version, 1);
            Object result =
                    decodeWithMonitor(
                            bytes,
                            PojoBaseFixture.class,
                            PojoSubclassAFixture.class,
                            PojoSubclassBFixture.class);
            assertTrue(result instanceof ClasslessPojoValue, "version " + version);
            ClasslessPojoValue value = (ClasslessPojoValue) result;
            assertEquals(2, value.fields().get("id"), "version " + version);
            assertEquals("base2", value.fields().get("name"), "version " + version);
            assertEquals(99, value.fields().get("extraB"), "version " + version);
        }
    }

    // ---- Serialization (via isolated URLClassLoader per Flink version) ----

    /**
     * Serializes a base POJO ({@code id=123, name="cross-version"}) using the specified Flink
     * version's {@code PojoSerializer}.
     */
    private static byte[] serializeBasePojoWithFlinkVersion(String flinkVersion) throws Exception {
        URL[] urls = buildVersionClasspath(flinkVersion);
        ClassLoader parent = ClassLoader.getSystemClassLoader().getParent();
        try (URLClassLoader loader = new URLClassLoader(urls, parent)) {
            Class<?> pojoClass = loader.loadClass(SimplePojoFixture.class.getName());
            Object pojo =
                    pojoClass
                            .getConstructor(int.class, String.class)
                            .newInstance(123, "cross-version");

            ClassLoader originalTccl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(loader);
            try {
                Object serializer = createPojoSerializer(loader, pojoClass, null);
                return serializeViaReflection(loader, serializer, pojo);
            } finally {
                Thread.currentThread().setContextClassLoader(originalTccl);
            }
        }
    }

    private static byte[] serializeNestedPojoWithFlinkVersion(String flinkVersion)
            throws Exception {
        URL[] urls = buildVersionClasspath(flinkVersion);
        ClassLoader parent = ClassLoader.getSystemClassLoader().getParent();
        try (URLClassLoader loader = new URLClassLoader(urls, parent)) {
            Class<?> innerClass = loader.loadClass(InnerPojoFixture.class.getName());
            Class<?> nestedClass = loader.loadClass(NestedPojoFixture.class.getName());
            Object inner =
                    innerClass.getConstructor(int.class, String.class).newInstance(42, "nested");
            Object nested = nestedClass.getDeclaredConstructor().newInstance();
            nestedClass.getField("id").setInt(nested, 7);
            nestedClass.getField("inner").set(nested, inner);

            ClassLoader originalTccl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(loader);
            try {
                Object serializer = createPojoSerializer(loader, nestedClass, null);
                return serializeViaReflection(loader, serializer, nested);
            } finally {
                Thread.currentThread().setContextClassLoader(originalTccl);
            }
        }
    }

    /**
     * Serializes a registered subclass instance using the specified Flink version's {@code
     * PojoSerializer}. Both {@code PojoSubclassAFixture} (tag 0) and {@code PojoSubclassBFixture}
     * (tag 1) are registered, then the instance of the subclass corresponding to {@code tag} is
     * serialized.
     *
     * @param tag 0 for PojoSubclassAFixture, 1 for PojoSubclassBFixture.
     */
    private static byte[] serializeRegisteredSubclassWithFlinkVersion(String flinkVersion, int tag)
            throws Exception {
        URL[] urls = buildVersionClasspath(flinkVersion);
        ClassLoader parent = ClassLoader.getSystemClassLoader().getParent();
        try (URLClassLoader loader = new URLClassLoader(urls, parent)) {
            Class<?> baseClass = loader.loadClass(PojoBaseFixture.class.getName());
            Class<?> subclassAClass = loader.loadClass(PojoSubclassAFixture.class.getName());
            Class<?> subclassBClass = loader.loadClass(PojoSubclassBFixture.class.getName());

            Class<?> targetClass;
            Object subclass;
            if (tag == 0) {
                targetClass = subclassAClass;
                subclass = subclassAClass.getDeclaredConstructor().newInstance();
                subclassAClass.getField("id").setInt(subclass, 1);
                subclassAClass.getField("name").set(subclass, "base");
                subclassAClass.getField("extraA").set(subclass, "extraA");
            } else {
                targetClass = subclassBClass;
                subclass = subclassBClass.getDeclaredConstructor().newInstance();
                subclassBClass.getField("id").setInt(subclass, 2);
                subclassBClass.getField("name").set(subclass, "base2");
                subclassBClass.getField("extraB").setInt(subclass, 99);
            }

            ClassLoader originalTccl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(loader);
            try {
                // Register both subclasses so tags are deterministic: A=0, B=1.
                Object serializer =
                        createPojoSerializer(loader, baseClass, subclassAClass, subclassBClass);
                return serializeViaReflection(loader, serializer, subclass);
            } finally {
                Thread.currentThread().setContextClassLoader(originalTccl);
            }
        }
    }

    /**
     * Creates a {@code PojoSerializer} for the given POJO class using the Flink version loaded in
     * the isolated loader. The public 4-argument {@code PojoSerializer} constructor is used
     * directly, with field serializers built manually, to avoid the heavy transitive dependency
     * chain of {@code TypeInformation.of()} (which invokes {@code TypeExtractor} internally).
     *
     * <p>The config API differs across Flink versions:
     *
     * <ul>
     *   <li>1.17.x: constructor takes {@code ExecutionConfig} directly.
     *   <li>1.19.x: constructor takes {@code SerializerConfig}; {@code ExecutionConfig} exposes it
     *       via {@code getSerializerConfig()}.
     *   <li>2.0.x: same as 1.19.x ({@code SerializerConfig} via {@code getSerializerConfig()}).
     * </ul>
     *
     * <p>To stay version-agnostic, the method first locates the public 4-argument constructor, then
     * builds a config object that is an instance of the constructor's 4th parameter type. Field
     * serializers are built by matching field types to well-known Flink primitives; for nested POJO
     * fields the method recurses.
     */
    @SuppressWarnings("unchecked")
    private static Object createPojoSerializer(
            URLClassLoader loader, Class<?> pojoClass, Class<?>... registeredSubclasses)
            throws Exception {
        Class<?> pojoSerializerClass =
                loader.loadClass("org.apache.flink.api.java.typeutils.runtime.PojoSerializer");
        Class<?> typeSerializerClass =
                loader.loadClass("org.apache.flink.api.common.typeutils.TypeSerializer");

        // Locate the public 4-argument constructor: (Class, TypeSerializer[], Field[], config).
        Constructor<?> matchedCtor = null;
        for (Constructor<?> ctor : pojoSerializerClass.getConstructors()) {
            Class<?>[] params = ctor.getParameterTypes();
            if (params.length == 4
                    && params[0] == Class.class
                    && params[1].isArray()
                    && params[2] == Field[].class) {
                matchedCtor = ctor;
                break;
            }
        }
        if (matchedCtor == null) {
            throw new IllegalStateException(
                    "Public 4-argument PojoSerializer constructor not found in "
                            + pojoSerializerClass);
        }
        Class<?> configParamType = matchedCtor.getParameterTypes()[3];

        // Build a config object assignable to the constructor's 4th parameter type.
        Object config = buildConfigObject(loader, configParamType);

        // Register subclass types if provided. registerPojoType exists on ExecutionConfig (1.x)
        // and on SerializerConfig/SerializerConfigImpl (1.19+, 2.x). Registration order
        // determines the tag: first registered = tag 0, second = tag 1, etc.
        if (registeredSubclasses != null) {
            Method registerMethod = findRegisterPojoType(config.getClass());
            for (Class<?> subclass : registeredSubclasses) {
                if (subclass != null) {
                    registerMethod.invoke(config, subclass);
                }
            }
        }

        // Get POJO fields and build field serializers (non-static, non-transient public fields).
        List<Field> pojoFieldList = new ArrayList<>();
        for (Field f : pojoClass.getFields()) {
            int mods = f.getModifiers();
            if (java.lang.reflect.Modifier.isStatic(mods)
                    || java.lang.reflect.Modifier.isTransient(mods)) {
                continue;
            }
            pojoFieldList.add(f);
        }
        Field[] fields = pojoFieldList.toArray(new Field[0]);

        // Build a properly-typed TypeSerializer[] array.
        Object typeSerializerArray =
                java.lang.reflect.Array.newInstance(typeSerializerClass, fields.length);
        for (int i = 0; i < fields.length; i++) {
            Object fieldSerializer = createFieldSerializer(loader, fields[i].getType(), config);
            java.lang.reflect.Array.set(typeSerializerArray, i, fieldSerializer);
        }

        return matchedCtor.newInstance(pojoClass, typeSerializerArray, fields, config);
    }

    /**
     * Builds a config object assignable to {@code configParamType}. Handles the three config API
     * shapes across Flink 1.17.x / 1.19.x / 2.0.x.
     */
    private static Object buildConfigObject(URLClassLoader loader, Class<?> configParamType)
            throws Exception {
        // Case 1: constructor takes ExecutionConfig (1.17.x).
        Class<?> executionConfigClass;
        try {
            executionConfigClass = loader.loadClass("org.apache.flink.api.common.ExecutionConfig");
        } catch (ClassNotFoundException e) {
            executionConfigClass = null;
        }
        if (executionConfigClass != null
                && configParamType.isAssignableFrom(executionConfigClass)) {
            return executionConfigClass.getDeclaredConstructor().newInstance();
        }

        // Case 2: constructor takes SerializerConfig (1.19.x, 2.x). ExecutionConfig exposes it
        // via getSerializerConfig().
        if (executionConfigClass != null) {
            try {
                Method getSerializerConfig = executionConfigClass.getMethod("getSerializerConfig");
                Object ec = executionConfigClass.getDeclaredConstructor().newInstance();
                Object sc = getSerializerConfig.invoke(ec);
                if (sc != null && configParamType.isInstance(sc)) {
                    return sc;
                }
            } catch (NoSuchMethodException e) {
                // getSerializerConfig() not present; fall through to direct SerializerConfigImpl.
            }
        }

        // Case 3: construct SerializerConfigImpl directly.
        Class<?> implClass =
                loader.loadClass("org.apache.flink.api.common.serialization.SerializerConfigImpl");
        Object impl = implClass.getDeclaredConstructor().newInstance();
        if (configParamType.isInstance(impl)) {
            return impl;
        }
        throw new IllegalStateException(
                "Cannot build a config object assignable to " + configParamType.getName());
    }

    /**
     * Finds the {@code registerPojoType(Class)} method, walking up the class hierarchy and
     * interfaces so it works for both {@code ExecutionConfig} (1.x) and {@code
     * SerializerConfigImpl} (1.19+, 2.x).
     */
    private static Method findRegisterPojoType(Class<?> configClass) throws NoSuchMethodException {
        for (Class<?> c = configClass; c != null; c = c.getSuperclass()) {
            try {
                return c.getMethod("registerPojoType", Class.class);
            } catch (NoSuchMethodException e) {
                // Check interfaces on this level.
            }
            for (Class<?> iface : c.getInterfaces()) {
                try {
                    return iface.getMethod("registerPojoType", Class.class);
                } catch (NoSuchMethodException e) {
                    // Continue.
                }
            }
        }
        throw new NoSuchMethodException(
                "registerPojoType(Class) not found on " + configClass.getName());
    }

    /**
     * Creates a field serializer for the given field type. Handles primitive types (int, String)
     * and nested POJOs. Throws for unsupported types.
     */
    private static Object createFieldSerializer(
            URLClassLoader loader, Class<?> fieldType, Object config) throws Exception {
        if (fieldType == int.class || fieldType == Integer.class) {
            return loader.loadClass("org.apache.flink.api.common.typeutils.base.IntSerializer")
                    .getField("INSTANCE")
                    .get(null);
        }
        if (fieldType == String.class) {
            return loader.loadClass("org.apache.flink.api.common.typeutils.base.StringSerializer")
                    .getField("INSTANCE")
                    .get(null);
        }
        if (fieldType == boolean.class || fieldType == Boolean.class) {
            return loader.loadClass("org.apache.flink.api.common.typeutils.base.BooleanSerializer")
                    .getField("INSTANCE")
                    .get(null);
        }
        if (fieldType == long.class || fieldType == Long.class) {
            return loader.loadClass("org.apache.flink.api.common.typeutils.base.LongSerializer")
                    .getField("INSTANCE")
                    .get(null);
        }
        // For nested POJOs, recurse.
        if (!fieldType.getName().startsWith("java.")) {
            return createPojoSerializer(loader, fieldType, null);
        }
        throw new IllegalArgumentException(
                "Unsupported field type for cross-version test: " + fieldType);
    }

    @SuppressWarnings("unchecked")
    private static byte[] serializeViaReflection(
            URLClassLoader loader, Object serializer, Object value) throws Exception {
        Class<?> dataOutputSerializerClass =
                loader.loadClass("org.apache.flink.core.memory.DataOutputSerializer");
        Constructor<?> ctor = dataOutputSerializerClass.getConstructor(int.class);
        Object dataOutput = ctor.newInstance(128);
        Class<?> dataOutputViewClass =
                loader.loadClass("org.apache.flink.core.memory.DataOutputView");
        Method serializeMethod =
                serializer.getClass().getMethod("serialize", Object.class, dataOutputViewClass);
        serializeMethod.invoke(serializer, value, dataOutput);
        Method getCopyOfBuffer = dataOutput.getClass().getMethod("getCopyOfBuffer");
        return (byte[]) getCopyOfBuffer.invoke(dataOutput);
    }

    // ---- Monitor decode ----

    private static Object decodeWithMonitor(byte[] bytes, Class<?> pojoClass) throws Exception {
        return decodeWithMonitor(bytes, pojoClass, (Class<?>[]) null);
    }

    private static Object decodeWithMonitor(
            byte[] bytes, Class<?> pojoClass, Class<?>... registeredSubclasses) throws Exception {
        org.apache.flink.api.common.ExecutionConfig config =
                new org.apache.flink.api.common.ExecutionConfig();
        if (registeredSubclasses != null) {
            for (Class<?> subclass : registeredSubclasses) {
                if (subclass != null) {
                    config.registerPojoType(subclass);
                }
            }
        }
        TypeSerializer<?> pojoSerializer = TypeInformation.of(pojoClass).createSerializer(config);
        SerializerInspectSchema schema = SerializerInspectSchema.fromSerializer(pojoSerializer);
        InspectDecoderDescriptor descriptor = schema.decoderDescriptor();
        assertNotNull(descriptor);
        assertTrue(descriptor.isPojo());
        assertEquals(DescriptorCapability.FULLY_CLASSLESS, descriptor.capability());
        return PojoInspectDecoder.decode(descriptor, bytes);
    }

    // ---- Classpath building ----

    /**
     * Builds the classpath for an isolated {@code URLClassLoader} targeting the specified Flink
     * version.
     *
     * <p>The isolated loader's parent is the platform classloader (no Flink classes). To produce a
     * self-contained classpath, this method:
     *
     * <ol>
     *   <li>Scans the configured local Maven repository for <em>all</em> Flink jars of the target
     *       version and adds them first.
     *   <li>Adds the test classpath's non-Flink jars (transitive deps like Kryo, Jackson, etc.).
     *   <li>Skips <em>all</em> Flink jars from the test classpath, regardless of version, to avoid
     *       cross-version contamination. The version-specific jars from step 1 take their place.
     * </ol>
     *
     * <p>This is necessary because Flink 2.0 split {@code flink-core} into multiple modules ({@code
     * flink-core-api}, {@code flink-core}, etc.), and {@code PojoSerializer} transitively
     * references classes (e.g. {@code Function}) that moved between modules. Adding all
     * version-specific jars ensures the isolated loader has a consistent set.
     */
    private static URL[] buildVersionClasspath(String flinkVersion) throws Exception {
        List<URL> urls = new ArrayList<>();

        // 1. Add all version-specific Flink jars from the local Maven repository.
        Path flinkM2Root = MAVEN_REPOSITORY.resolve("org/apache/flink");
        if (Files.isDirectory(flinkM2Root)) {
            try (Stream<Path> modules = Files.list(flinkM2Root)) {
                List<Path> moduleDirs = new ArrayList<>();
                modules.forEach(moduleDirs::add);
                moduleDirs.sort(Comparator.comparing(p -> p.getFileName().toString()));
                for (Path moduleDir : moduleDirs) {
                    Path versionDir = moduleDir.resolve(flinkVersion);
                    if (!Files.isDirectory(versionDir)) {
                        continue;
                    }
                    try (Stream<Path> jars =
                            Files.list(versionDir).filter(p -> p.toString().endsWith(".jar"))) {
                        List<Path> jarList = new ArrayList<>();
                        jars.forEach(jarList::add);
                        jarList.sort(Comparator.comparing(p -> p.getFileName().toString()));
                        for (Path jar : jarList) {
                            // Skip sources/javadoc/test jars.
                            String name = jar.getFileName().toString();
                            if (name.contains("sources")
                                    || name.contains("javadoc")
                                    || name.contains("tests")) {
                                continue;
                            }
                            urls.add(jar.toUri().toURL());
                        }
                    }
                }
            }
        }

        // 2. Add non-Flink jars from the test classpath (transitive deps like Kryo, Jackson).
        String classpath = System.getProperty("java.class.path");
        for (String path : classpath.split(File.pathSeparator)) {
            // Skip all Flink jars - the version-specific ones above replace them.
            if (path.contains("/org/apache/flink/")) {
                continue;
            }
            // Keep test-classes (for fixture classes) and all non-Flink transitive deps (Kryo,
            // Jackson, etc.). Monitor/cobble-common production classes are on the classpath but
            // are never loaded by the isolated loader (only fixture + Flink classes are loaded).
            try {
                urls.add(new File(path).toURI().toURL());
            } catch (Exception e) {
                // Skip malformed paths.
            }
        }
        return urls.toArray(new URL[0]);
    }

    // ---- Jar availability ----

    /**
     * Verifies that the required Flink artifacts for each version exist in the local Maven
     * repository. The required set mirrors what {@link #buildVersionClasspath} depends on:
     *
     * <ul>
     *   <li>All versions: {@code flink-core} (contains {@code PojoSerializer}, {@code
     *       TypeExtractor}, {@code ExecutionConfig}, primitive serializers).
     *   <li>2.0+: {@code flink-core-api} (contains {@code Function}, which moved out of {@code
     *       flink-core} in the 2.0 module split).
     * </ul>
     *
     * <p>Hard-fails (no {@code assumeTrue}) so missing jars produce a clear message instead of a
     * confusing {@code ClassNotFoundException} from the isolated classloader.
     */
    private static void requireAllJarsAvailable() {
        // Required modules per version. Each entry is (artifactId, jarNamePrefix).
        Map<String, String[][]> requiredByVersion = new LinkedHashMap<>();
        for (String version : REQUIRED_VERSIONS) {
            List<String[]> modules = new ArrayList<>();
            modules.add(new String[] {"flink-core", "flink-core-" + version + ".jar"});
            if (version.startsWith("2.")) {
                modules.add(new String[] {"flink-core-api", "flink-core-api-" + version + ".jar"});
            }
            requiredByVersion.put(version, modules.toArray(new String[0][]));
        }

        List<String> missing = new ArrayList<>();
        for (String version : REQUIRED_VERSIONS) {
            for (String[] module : requiredByVersion.get(version)) {
                Path jar =
                        MAVEN_REPOSITORY
                                .resolve("org/apache/flink")
                                .resolve(module[0])
                                .resolve(version)
                                .resolve(module[1]);
                if (!Files.exists(jar)) {
                    missing.add(jar.toString());
                }
            }
        }
        if (!missing.isEmpty()) {
            fail(
                    "Required Flink jars are missing from the local Maven repository.\n"
                            + "Cross-version POJO protocol validation cannot proceed.\n"
                            + "Missing jars:\n  "
                            + String.join("\n  ", missing)
                            + "\nInstall them or run: mvn dependency:resolve");
        }
    }

    // ---- POJO fixtures (must be loadable by the isolated URLClassLoader) ----

    public static final class SimplePojoFixture implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public String name;

        public SimplePojoFixture() {}

        public SimplePojoFixture(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public static final class NestedPojoFixture implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public InnerPojoFixture inner;
    }

    public static final class InnerPojoFixture implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int x;
        public String y;

        public InnerPojoFixture() {}

        public InnerPojoFixture(int x, String y) {
            this.x = x;
            this.y = y;
        }
    }

    public static class PojoBaseFixture implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public String name;
    }

    public static class PojoSubclassAFixture extends PojoBaseFixture {
        private static final long serialVersionUID = 1L;
        public String extraA;
    }

    public static class PojoSubclassBFixture extends PojoBaseFixture {
        private static final long serialVersionUID = 1L;
        public int extraB;
    }
}
