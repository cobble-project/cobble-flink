package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Proves that {@link StateInspectDecoder#decode} produces correct semantic output for POJO
 * serializers when the writer's POJO class is absent from the classpath.
 *
 * <p>Each test serializes fixtures while the normal test loader can see the POJO class, then
 * decodes with a thread context classloader that rejects the POJO class. Asserts decoded fields are
 * present and {@code decode_error} is null for FULLY_CLASSLESS cases.
 */
class StateInspectDecoderNoPojoClassTest {

    // ---- Test fixtures ----

    public static final class SimplePojo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public String name;

        public SimplePojo() {}

        public SimplePojo(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public static final class PojoWithNested implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public PojoInspectDecoderTest.InnerPojo inner;
    }

    public static class PojoBase implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public String name;
    }

    public static class PojoSubclassA extends PojoBase {
        private static final long serialVersionUID = 1L;
        public String extraA;
    }

    // ---- Tests ----

    @Test
    void valueStatePojoDecodesWithoutPojoClass() throws Exception {
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "pojo-no-class",
                        "cf-pojo-no-class",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);
        assertNull(schema.valueSerializer().serializedSerializerBytes());

        StateInspectType pojoRowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), pojoRowType);

        SimplePojo pojo = new SimplePojo(55, "carol");
        byte[] valueBytes = serialize(pojoSerializer, pojo);
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row =
                decodeWithBlockedPojoClass(
                        schema, semanticSchema, rowKey, valueBytes, "SimplePojo");

        assertNull(row.decodeError, "decode_error should be null: " + row.decodeError);
        assertNotNull(row.decodedParts);
        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        assertNotNull(valuePart);
        assertEquals("ROW", valuePart.get("kind"));
        Map<String, Object> byName = semanticFieldsByName((List<?>) valuePart.get("fields"));
        assertEquals(55, ((Map<?, ?>) byName.get("id")).get("value"));
        assertEquals("carol", ((Map<?, ?>) byName.get("name")).get("value"));
    }

    @Test
    void listStatePojoDecodesWithoutPojoClass() throws Exception {
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        StateInspectSchema schema =
                StateInspectSchema.forList(
                        "pojo-list-no-class",
                        "cf-pojo-list-no-class",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);
        assertNull(schema.listElementSerializer().serializedSerializerBytes());

        StateInspectType elementType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forList(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), elementType);

        SimplePojo p1 = new SimplePojo(10, "a");
        SimplePojo p2 = new SimplePojo(20, "b");
        byte[] listBytes =
                buildListPayload(serialize(pojoSerializer, p1), serialize(pojoSerializer, p2));
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row =
                decodeWithBlockedPojoClass(schema, semanticSchema, rowKey, listBytes, "SimplePojo");

        assertNull(row.decodeError, "decode_error should be null: " + row.decodeError);
        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        assertNotNull(valuePart);
        assertEquals("LIST", valuePart.get("kind"));
        List<?> values = (List<?>) valuePart.get("values");
        assertEquals(2, values.size());
    }

    @Test
    void mapStatePojoKeyAndValueDecodeWithoutPojoClass() throws Exception {
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "pojo-map-no-class",
                        "cf-pojo-map-no-class",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer,
                        pojoSerializer);
        assertNull(schema.mapUserKeySerializer().serializedSerializerBytes());
        assertNull(schema.mapUserValueSerializer().serializedSerializerBytes());

        StateInspectType pojoRowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forMap(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        pojoRowType,
                        pojoRowType);

        SimplePojo keyPojo = new SimplePojo(5, "key");
        SimplePojo valuePojo = new SimplePojo(6, "val");
        byte[] mapKeyBytes = serialize(pojoSerializer, keyPojo);
        byte[] rowKey =
                mapKeyWithVoidNamespace(schema, serialize(IntSerializer.INSTANCE, 1), mapKeyBytes);
        byte[] mapValueColumn = mapValueBytes(serialize(pojoSerializer, valuePojo));

        StateInspectDecoder.DecodedRow row =
                decodeWithBlockedPojoClass(
                        schema, semanticSchema, rowKey, mapValueColumn, "SimplePojo");

        assertNull(row.decodeError, "decode_error should be null: " + row.decodeError);
        Map<?, ?> keyPart = (Map<?, ?>) row.decodedParts.get("map_key");
        assertNotNull(keyPart);
        assertEquals("ROW", keyPart.get("kind"));
        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("map_value");
        assertNotNull(valuePart);
        assertEquals("ROW", valuePart.get("kind"));
    }

    @Test
    void nestedPojoDecodesWithoutPojoClass() throws Exception {
        TypeSerializer<PojoWithNested> pojoSerializer =
                TypeInformation.of(PojoWithNested.class).createSerializer(new ExecutionConfig());
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "pojo-nested-no-class",
                        "cf-pojo-nested-no-class",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);
        assertNull(schema.valueSerializer().serializedSerializerBytes());

        StateInspectType innerRowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("x", StateInspectType.scalar("INT")),
                                new StateInspectField("y", StateInspectType.scalar("VARCHAR"))));
        StateInspectType pojoRowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("inner", innerRowType)));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), pojoRowType);

        PojoWithNested pojo = new PojoWithNested();
        pojo.id = 7;
        pojo.inner = new PojoInspectDecoderTest.InnerPojo(3, "nested");
        byte[] valueBytes = serialize(pojoSerializer, pojo);
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row =
                decodeWithBlockedPojoClass(
                        schema, semanticSchema, rowKey, valueBytes, "PojoWithNested");

        assertNull(row.decodeError, "decode_error should be null: " + row.decodeError);
        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        assertNotNull(valuePart);
        assertEquals("ROW", valuePart.get("kind"));
        Map<String, Object> byName = semanticFieldsByName((List<?>) valuePart.get("fields"));
        assertEquals(7, ((Map<?, ?>) byName.get("id")).get("value"));
        Map<?, ?> innerPart = (Map<?, ?>) byName.get("inner");
        assertNotNull(innerPart);
        assertEquals("ROW", innerPart.get("kind"));
    }

    @Test
    void registeredSubclassDecodesWithoutPojoClass() throws Exception {
        ExecutionConfig config = new ExecutionConfig();
        config.registerPojoType(PojoSubclassA.class);
        TypeSerializer<PojoBase> pojoSerializer =
                TypeInformation.of(PojoBase.class).createSerializer(config);
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "pojo-subclass-no-class",
                        "cf-pojo-subclass-no-class",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);
        assertNull(schema.valueSerializer().serializedSerializerBytes());

        StateInspectType pojoRowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR")),
                                new StateInspectField(
                                        "extraA", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), pojoRowType);

        PojoSubclassA a = new PojoSubclassA();
        a.id = 1;
        a.name = "base";
        a.extraA = "extra";
        byte[] valueBytes = serialize(pojoSerializer, a);
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row =
                decodeWithBlockedPojoClass(schema, semanticSchema, rowKey, valueBytes, "PojoBase");

        assertNull(row.decodeError, "decode_error should be null: " + row.decodeError);
        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        assertNotNull(valuePart);
        assertEquals("ROW", valuePart.get("kind"));
        Map<String, Object> byName = semanticFieldsByName((List<?>) valuePart.get("fields"));
        assertEquals(1, ((Map<?, ?>) byName.get("id")).get("value"));
        assertEquals("base", ((Map<?, ?>) byName.get("name")).get("value"));
        assertEquals("extra", ((Map<?, ?>) byName.get("extraA")).get("value"));
    }

    // ---- Helpers ----

    private static StateInspectDecoder.DecodedRow decodeWithBlockedPojoClass(
            StateInspectSchema schema,
            StateInspectSemanticSchema semanticSchema,
            byte[] rowKey,
            byte[] valueColumn,
            String blockedClassNameFragment) {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader blockingLoader =
                new ClassLoader(ClassLoader.getSystemClassLoader()) {
                    @Override
                    public Class<?> loadClass(String name, boolean resolve)
                            throws ClassNotFoundException {
                        if (name.contains(blockedClassNameFragment)) {
                            throw new ClassNotFoundException("Blocked (no POJO class): " + name);
                        }
                        return super.loadClass(name, resolve);
                    }
                };
        InspectTarget target =
                new InspectTarget(
                        schema.stateName(),
                        schema.stateName(),
                        "state",
                        schema.columnFamily(),
                        false,
                        schema.stateKind().name(),
                        Collections.emptyMap(),
                        schema,
                        semanticSchema,
                        null);
        Thread.currentThread().setContextClassLoader(blockingLoader);
        try {
            return StateInspectDecoder.decode(target, rowKey, new byte[][] {valueColumn});
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private static <T> byte[] serialize(TypeSerializer<T> serializer, T value) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);
        return out.getCopyOfBuffer();
    }

    private static byte[] keyWithVoidNamespace(byte[] stateKeyBytes) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(stateKeyBytes.length + 1);
        output.write(stateKeyBytes);
        output.writeByte(0);
        return output.getCopyOfBuffer();
    }

    private static byte[] mapKeyWithVoidNamespace(
            StateInspectSchema schema, byte[] stateKeyBytes, byte[] mapKeyBytes) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(128);
        output.write(stateKeyBytes);
        output.writeByte(0);
        output.writeByte(0);
        output.write(mapKeyBytes);
        if (schema.mapKeyLengthStored()) {
            output.writeInt(stateKeyBytes.length);
        }
        if (schema.mapNamespaceLengthStored()) {
            output.writeInt(1);
        }
        return output.getCopyOfBuffer();
    }

    private static byte[] mapValueBytes(byte[] payload) {
        byte[] row = new byte[payload.length + 1];
        row[0] = 0x00;
        System.arraycopy(payload, 0, row, 1, payload.length);
        return row;
    }

    private static byte[] buildListPayload(byte[]... elements) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] element : elements) {
            out.write(element);
            out.write(',');
        }
        return out.toByteArray();
    }

    private static Map<String, Object> semanticFieldsByName(List<?> fields) {
        Map<String, Object> byName = new LinkedHashMap<>();
        for (Object field : fields) {
            Map<?, ?> f = (Map<?, ?>) field;
            byName.put((String) f.get("name"), f);
        }
        return byName;
    }
}
