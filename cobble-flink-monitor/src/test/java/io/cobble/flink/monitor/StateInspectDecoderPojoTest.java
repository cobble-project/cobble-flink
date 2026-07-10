package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.DescriptorCapability;
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
 * Full-path integration tests for classless POJO decoding through {@link
 * StateInspectDecoder#decode}.
 *
 * <p>Tests cover ValueState, ListState, and MapState with FULLY_CLASSLESS POJOs, nested POJO
 * rendering, PARTIALLY_CLASSLESS fallback behavior, and user-jar regression.
 */
class StateInspectDecoderPojoTest {

    // ---- Test fixtures (shared with PojoInspectDecoderTest) ----

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

    /**
     * Non-registered subclass of {@link PojoBase}. Used to pollute the serializer snapshot so the
     */
    /** descriptor becomes PARTIALLY_CLASSLESS (hasNonRegisteredSubclasses=true). */
    public static class PojoSubclassB extends PojoBase {
        private static final long serialVersionUID = 1L;
        public int extraB;
    }

    // ---- Tests: ValueState ----

    @Test
    void valueStatePojoDecodesAsSemanticRow() throws Exception {
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "pojo-val",
                        "cf-pojo-val",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);

        StateInspectType pojoRowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), pojoRowType);

        SimplePojo pojo = new SimplePojo(77, "alice");
        byte[] valueBytes = serialize(pojoSerializer, pojo);
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row = decode(schema, semanticSchema, rowKey, valueBytes);

        assertNull(row.decodeError, "decode_error should be null: " + row.decodeError);
        assertNotNull(row.decodedParts);
        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        assertNotNull(valuePart);
        assertEquals("ROW", valuePart.get("kind"));
        Map<String, Object> byName = semanticFieldsByName((List<?>) valuePart.get("fields"));
        assertEquals(77, ((Map<?, ?>) byName.get("id")).get("value"));
        assertEquals("alice", ((Map<?, ?>) byName.get("name")).get("value"));
    }

    @Test
    void valueStatePojoStateKeyDecodesAsSemanticRow() throws Exception {
        TypeSerializer<SimplePojo> keySerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "pojo-key",
                        "cf-pojo-key",
                        false,
                        keySerializer,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);

        StateInspectType keyRowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        keyRowType, StateInspectType.unknown(), StateInspectType.scalar("INT"));

        SimplePojo key = new SimplePojo(3, "key-pojo");
        byte[] rowKey = keyWithVoidNamespace(serialize(keySerializer, key));
        byte[] valueBytes = serialize(IntSerializer.INSTANCE, 99);

        StateInspectDecoder.DecodedRow row = decode(schema, semanticSchema, rowKey, valueBytes);

        assertNull(row.decodeError, "decode_error should be null: " + row.decodeError);
        assertNotNull(row.decodedParts);
        Map<?, ?> keyPart = (Map<?, ?>) row.decodedParts.get("state_key");
        assertNotNull(keyPart);
        assertEquals("ROW", keyPart.get("kind"));
    }

    // ---- Tests: ListState ----

    @Test
    void listStatePojoElementsDecode() throws Exception {
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        StateInspectSchema schema =
                StateInspectSchema.forList(
                        "pojo-list",
                        "cf-pojo-list",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);

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

        StateInspectDecoder.DecodedRow row = decode(schema, semanticSchema, rowKey, listBytes);

        assertNull(row.decodeError, "decode_error should be null: " + row.decodeError);
        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        assertNotNull(valuePart);
        assertEquals("LIST", valuePart.get("kind"));
        List<?> values = (List<?>) valuePart.get("values");
        assertEquals(2, values.size());
    }

    @Test
    void listStatePartiallyClasslessPojoFallbackRendersBaseFields() throws Exception {
        // PARTIALLY_CLASSLESS via non-registered subclass.
        //
        // PojoBase has PojoSubclassA registered. We write one PojoSubclassB (non-registered)
        // instance through the serializer before creating the schema. This pollutes the
        // PojoSerializerSnapshot's nonRegisteredSubclassSerializerSnapshots, making the descriptor
        // PARTIALLY_CLASSLESS (hasNonRegisteredSubclasses=true).
        //
        // The list elements are PojoSubclassB instances (wire flag 0x04 IS_SUBCLASS). The classless
        // decoder rejects flag 0x04, triggering the PARTIALLY_CLASSLESS fallback: the local
        // classless results are discarded, and the original complete list bytes are re-decoded via
        // the restored live PojoSerializer. With the subclass class available, the fallback
        // succeeds and the elements are rendered as ROWs with the base fields (id, name).
        ExecutionConfig config = new ExecutionConfig();
        config.registerPojoType(PojoSubclassA.class);
        TypeSerializer<PojoBase> pojoSerializer =
                TypeInformation.of(PojoBase.class).createSerializer(config);

        // Write a non-registered subclass instance to pollute the snapshot.
        PojoSubclassB polluter = new PojoSubclassB();
        polluter.id = 0;
        polluter.name = "polluter";
        polluter.extraB = 0;
        serialize(pojoSerializer, polluter);

        // Create schema AFTER the polluting write -> descriptor is PARTIALLY_CLASSLESS.
        StateInspectSchema schema =
                StateInspectSchema.forList(
                        "pojo-partial-list",
                        "cf-pojo-partial-list",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);
        // Assert the descriptor is actually PARTIALLY_CLASSLESS (not FULLY_CLASSLESS).
        assertEquals(
                DescriptorCapability.PARTIALLY_CLASSLESS,
                schema.listElementSerializer().decoderDescriptor().capability(),
                "descriptor should be PARTIALLY_CLASSLESS after non-registered subclass write");
        assertTrue(
                schema.listElementSerializer().decoderDescriptor().pojoHasNonRegisteredSubclasses(),
                "hasNonRegisteredSubclasses should be true");

        // Semantic schema: only base fields (id, name). Subclass-only fields (extraB) are excluded.
        StateInspectType elementType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forList(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), elementType);

        // List elements: two PojoSubclassB instances (flag 0x04, non-registered subclass).
        PojoSubclassB b1 = new PojoSubclassB();
        b1.id = 10;
        b1.name = "first";
        b1.extraB = 100;
        PojoSubclassB b2 = new PojoSubclassB();
        b2.id = 20;
        b2.name = "second";
        b2.extraB = 200;
        byte[] listBytes =
                buildListPayload(serialize(pojoSerializer, b1), serialize(pojoSerializer, b2));
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        // Decode with the subclass class available -> fallback succeeds.
        StateInspectDecoder.DecodedRow row = decode(schema, semanticSchema, rowKey, listBytes);

        assertNull(row.decodeError, "decode_error should be null: " + row.decodeError);
        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("value");
        assertNotNull(valuePart, "value part should be present");
        assertEquals("LIST", valuePart.get("kind"));
        List<?> values = (List<?>) valuePart.get("values");
        assertEquals(2, values.size(), "should have 2 elements via fallback");
        // Verify each element renders as a ROW with base fields.
        for (int i = 0; i < values.size(); i++) {
            Map<?, ?> element = (Map<?, ?>) values.get(i);
            assertEquals("ROW", element.get("kind"), "element " + i);
            Map<String, Object> byName = semanticFieldsByName((List<?>) element.get("fields"));
            assertTrue(byName.containsKey("id"), "element " + i + " should have id");
            assertTrue(byName.containsKey("name"), "element " + i + " should have name");
        }
        // Verify specific values.
        Map<?, ?> elem0 = (Map<?, ?>) values.get(0);
        Map<String, Object> byName0 = semanticFieldsByName((List<?>) elem0.get("fields"));
        assertEquals(10, ((Map<?, ?>) byName0.get("id")).get("value"));
        assertEquals("first", ((Map<?, ?>) byName0.get("name")).get("value"));
    }

    @Test
    void listStatePartiallyClasslessPojoFallbackFailsWhenSubclassBlocked() throws Exception {
        // Same setup as above, but the non-registered subclass class is blocked from the TCCL.
        // The classless path fails (flag 0x04), and the live serializer fallback also fails
        // because it cannot resolve the subclass class. The error should reference "value".
        ExecutionConfig config = new ExecutionConfig();
        config.registerPojoType(PojoSubclassA.class);
        TypeSerializer<PojoBase> pojoSerializer =
                TypeInformation.of(PojoBase.class).createSerializer(config);

        // Write a non-registered subclass instance to pollute the snapshot.
        PojoSubclassB polluter = new PojoSubclassB();
        polluter.id = 0;
        polluter.name = "polluter";
        polluter.extraB = 0;
        serialize(pojoSerializer, polluter);

        StateInspectSchema schema =
                StateInspectSchema.forList(
                        "pojo-partial-blocked",
                        "cf-pojo-partial-blocked",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);
        assertEquals(
                DescriptorCapability.PARTIALLY_CLASSLESS,
                schema.listElementSerializer().decoderDescriptor().capability());

        StateInspectType elementType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forList(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), elementType);

        PojoSubclassB b = new PojoSubclassB();
        b.id = 1;
        b.name = "blocked";
        b.extraB = 99;
        byte[] listBytes = buildListPayload(serialize(pojoSerializer, b));
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        // Decode with TCCL blocking the subclass class.
        // The classless path fails (flag 0x04). The fallback also fails because the restored
        // live serializer cannot resolve PojoSubclassB. The error should reference "value".
        StateInspectDecoder.DecodedRow row =
                decodeWithBlockedClass(schema, semanticSchema, rowKey, listBytes, "PojoSubclassB");

        assertNotNull(row.decodeError, "expected decode_error when both paths fail");
        assertTrue(
                row.decodeError.contains("value"),
                "error should reference value: " + row.decodeError);
    }

    // ---- Tests: MapState ----

    @Test
    void mapStatePojoKeyAndValueDecode() throws Exception {
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "pojo-map",
                        "cf-pojo-map",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer,
                        pojoSerializer);

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

        StateInspectDecoder.DecodedRow row = decode(schema, semanticSchema, rowKey, mapValueColumn);

        assertNull(row.decodeError, "decode_error should be null: " + row.decodeError);
        Map<?, ?> keyPart = (Map<?, ?>) row.decodedParts.get("map_key");
        assertNotNull(keyPart);
        assertEquals("ROW", keyPart.get("kind"));
        Map<?, ?> valuePart = (Map<?, ?>) row.decodedParts.get("map_value");
        assertNotNull(valuePart);
        assertEquals("ROW", valuePart.get("kind"));
    }

    // ---- Tests: nested POJO ----

    @Test
    void nestedPojoRendersAsNestedSemanticRow() throws Exception {
        TypeSerializer<PojoWithNested> pojoSerializer =
                TypeInformation.of(PojoWithNested.class).createSerializer(new ExecutionConfig());
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "pojo-nested",
                        "cf-pojo-nested",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);

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

        StateInspectDecoder.DecodedRow row = decode(schema, semanticSchema, rowKey, valueBytes);

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

    // ---- Tests: registered subclass ----

    @Test
    void registeredSubclassDecodesThroughStateInspectDecoder() throws Exception {
        ExecutionConfig config = new ExecutionConfig();
        config.registerPojoType(PojoSubclassA.class);
        TypeSerializer<PojoBase> pojoSerializer =
                TypeInformation.of(PojoBase.class).createSerializer(config);
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "pojo-subclass",
                        "cf-pojo-subclass",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);

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

        StateInspectDecoder.DecodedRow row = decode(schema, semanticSchema, rowKey, valueBytes);

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

    private static StateInspectDecoder.DecodedRow decode(
            StateInspectSchema schema,
            StateInspectSemanticSchema semanticSchema,
            byte[] rowKey,
            byte[] valueColumn) {
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
        return StateInspectDecoder.decode(target, rowKey, new byte[][] {valueColumn});
    }

    private static StateInspectDecoder.DecodedRow decodeWithBlockedClass(
            StateInspectSchema schema,
            StateInspectSemanticSchema semanticSchema,
            byte[] rowKey,
            byte[] valueColumn,
            String blockedClassName) {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader blockingLoader =
                new ClassLoader(ClassLoader.getSystemClassLoader()) {
                    @Override
                    public Class<?> loadClass(String name, boolean resolve)
                            throws ClassNotFoundException {
                        if (name.contains(blockedClassName)) {
                            throw new ClassNotFoundException("Blocked: " + name);
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
