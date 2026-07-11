package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.StateRowKeyLayout;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Unit coverage for the state lookup row-key encoder and key-group computation. */
class CobbleStateLookupKeyEncoderTest {

    private static final int TOTAL_KEY_GROUPS = 16;

    @Test
    void rejectsClasslessPojoStructuredStateKeyForExactLookup() throws Exception {
        TypeSerializer<LookupPojo> pojoSerializer =
                TypeInformation.of(LookupPojo.class).createSerializer(new ExecutionConfig());
        StateInspectType pojoKey =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField(
                                        "name", StateInspectType.scalar("VARCHAR(2147483647)"))));
        StateSourceConfig config =
                valueConfig(
                        fields(
                                field("id", "INT", StateSourceField.Group.STATE_KEY, 0),
                                field(
                                        "name",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.STATE_KEY,
                                        1),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(
                                fields(
                                        field("id", "INT", StateSourceField.Group.STATE_KEY, 0),
                                        field(
                                                "name",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.STATE_KEY,
                                                1))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        StateInspectSchema.forValue(
                                "orders",
                                "cf",
                                false,
                                pojoSerializer,
                                VoidNamespaceSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forValue(
                                pojoKey,
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT")));

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new CobbleStateLookupKeyEncoder(
                                        config, runtimeSchema, new int[] {0, 1}));
        assertTrue(
                error.getMessage().contains("exact state key reconstruction"), error.getMessage());
        assertTrue(error.getMessage().contains("classless POJO"), error.getMessage());
        assertTrue(error.getMessage().contains("scan mode"), error.getMessage());
    }

    public static final class LookupPojo {
        public int id;
        public String name;

        public LookupPojo() {}
    }

    // ------------------------------------------------------------------------------------------
    //  Byte-layout + key-group tests
    // ------------------------------------------------------------------------------------------

    @Test
    void encodesScalarValueStateWithVoidNamespace() throws Exception {
        StateSourceConfig config =
                valueConfig(
                        fields(
                                field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(fields(field("key", "INT", StateSourceField.Group.STATE_KEY, 0))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        StateInspectSchema.forValue(
                                "orders",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT")));

        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0});

        RowData keyRow = singleIntRow(12);
        CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                encoder.encode(keyRow, TOTAL_KEY_GROUPS);

        // VoidNamespaceSerializer writes exactly one 0x00 byte despite getLength()==0.
        byte[] expected = concat(serialize(IntSerializer.INSTANCE, 12), namespaceBytes());
        assertArrayEquals(expected, encoded.rowKey());
        assertEquals(
                KeyGroupRangeAssignment.assignToKeyGroup(12, TOTAL_KEY_GROUPS), encoded.keyGroup());
    }

    @Test
    void snapshotOnlySerializerEncodesLookupKey() throws Exception {
        // IntSerializer is monitor-portable: the sidecar has snapshot bytes but NO
        // serializedSerializerBytes. The lookup encoder must restore from snapshot alone.
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "orders",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        assertNull(schema.keySerializer().serializedSerializerBytes());
        assertNotNull(schema.keySerializer().snapshotBytes());

        StateSourceConfig config =
                valueConfig(
                        fields(
                                field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(fields(field("key", "INT", StateSourceField.Group.STATE_KEY, 0))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        schema,
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT")));

        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0});

        RowData keyRow = singleIntRow(7);
        CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                encoder.encode(keyRow, TOTAL_KEY_GROUPS);

        byte[] expected = concat(serialize(IntSerializer.INSTANCE, 7), namespaceBytes());
        assertArrayEquals(expected, encoded.rowKey());
        assertEquals(
                KeyGroupRangeAssignment.assignToKeyGroup(7, TOTAL_KEY_GROUPS), encoded.keyGroup());
    }

    @Test
    void encodesScalarValueStateWithStringNamespace() throws Exception {
        StateSourceConfig config =
                valueConfig(
                        fields(
                                field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                field(
                                        "namespace",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.NAMESPACE,
                                        0),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(
                                fields(
                                        field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                        field(
                                                "namespace",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.NAMESPACE,
                                                0))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        StateInspectSchema.forValue(
                                "orders",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.scalar("VARCHAR(2147483647)"),
                                StateInspectType.scalar("INT")));

        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0, 1});

        GenericRowData keyRow = new GenericRowData(2);
        keyRow.setField(0, 5);
        keyRow.setField(1, StringData.fromString("ns"));
        CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                encoder.encode(keyRow, TOTAL_KEY_GROUPS);

        // INT key is fixed-length, STRING namespace is variable => no key-length suffix.
        byte[] expected =
                concat(
                        serialize(IntSerializer.INSTANCE, 5),
                        serialize(StringSerializer.INSTANCE, "ns"));
        assertArrayEquals(expected, encoded.rowKey());
        assertEquals(
                KeyGroupRangeAssignment.assignToKeyGroup(5, TOTAL_KEY_GROUPS), encoded.keyGroup());

        // Decoder round-trip: key at position 0, namespace at position 1, value at position 2.
        assertDecoderRoundTripValue(
                runtimeSchema, config, encoded.rowKey(), new Object[] {5, "ns", 1});
    }

    @Test
    void encodesVariableKeyAndVariableNamespaceWithKeyLengthSuffix() throws Exception {
        StateSourceConfig config =
                valueConfig(
                        fields(
                                field(
                                        "key",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.STATE_KEY,
                                        0),
                                field(
                                        "namespace",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.NAMESPACE,
                                        0),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(
                                fields(
                                        field(
                                                "key",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.STATE_KEY,
                                                0),
                                        field(
                                                "namespace",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.NAMESPACE,
                                                0))));
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "orders",
                        "cf",
                        false,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        schema,
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("VARCHAR(2147483647)"),
                                StateInspectType.scalar("VARCHAR(2147483647)"),
                                StateInspectType.scalar("INT")));

        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0, 1});

        GenericRowData keyRow = new GenericRowData(2);
        keyRow.setField(0, StringData.fromString("customer"));
        keyRow.setField(1, StringData.fromString("tenant"));
        CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                encoder.encode(keyRow, TOTAL_KEY_GROUPS);

        byte[] keyBytes = serialize(StringSerializer.INSTANCE, "customer");
        byte[] namespaceBytes = serialize(StringSerializer.INSTANCE, "tenant");
        // Both variable => key-length suffix present.
        assertTrue(schema.keyLengthStored());
        byte[] expected = concat(keyBytes, namespaceBytes, intBytes(keyBytes.length));
        assertArrayEquals(expected, encoded.rowKey());
        // Last 4 bytes are the key length.
        int suffix = readInt(encoded.rowKey(), encoded.rowKey().length - Integer.BYTES);
        assertEquals(keyBytes.length, suffix);

        assertDecoderRoundTripValue(
                runtimeSchema, config, encoded.rowKey(), new Object[] {"customer", "tenant", 1});
    }

    @Test
    void encodesMapStateWithVoidNamespace() throws Exception {
        StateSourceConfig config =
                mapConfig(
                        fields(
                                field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                field("map_key", "INT", StateSourceField.Group.MAP_KEY, 0),
                                field("map_value", "INT", StateSourceField.Group.MAP_VALUE, 0)),
                        contract(
                                fields(
                                        field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                        field(
                                                "map_key",
                                                "INT",
                                                StateSourceField.Group.MAP_KEY,
                                                0))));
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "counters",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        schema,
                        StateInspectSemanticSchema.forMap(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT"),
                                StateInspectType.scalar("INT")));

        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0, 1});

        GenericRowData keyRow = new GenericRowData(2);
        keyRow.setField(0, 7);
        keyRow.setField(1, 99);
        CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                encoder.encode(keyRow, TOTAL_KEY_GROUPS);

        byte[] expected =
                concat(
                        serialize(IntSerializer.INSTANCE, 7),
                        namespaceBytes(),
                        new byte[] {0x00},
                        serialize(IntSerializer.INSTANCE, 99));
        assertArrayEquals(expected, encoded.rowKey());
        // VoidNamespace byte at position 4, separator at position 5, then the 4-byte INT map key.
        assertEquals(10, encoded.rowKey().length);
        assertEquals(
                KeyGroupRangeAssignment.assignToKeyGroup(7, TOTAL_KEY_GROUPS), encoded.keyGroup());
    }

    @Test
    void encodesMapStateWithVariableKeyNamespaceAndMapKeySuffixes() throws Exception {
        StateSourceConfig config =
                mapConfig(
                        fields(
                                field(
                                        "key",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.STATE_KEY,
                                        0),
                                field(
                                        "namespace",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.NAMESPACE,
                                        0),
                                field(
                                        "map_key",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.MAP_KEY,
                                        0),
                                field("map_value", "INT", StateSourceField.Group.MAP_VALUE, 0)),
                        contract(
                                fields(
                                        field(
                                                "key",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.STATE_KEY,
                                                0),
                                        field(
                                                "namespace",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.NAMESPACE,
                                                0),
                                        field(
                                                "map_key",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.MAP_KEY,
                                                0))));
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "counters",
                        "cf",
                        false,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        schema,
                        StateInspectSemanticSchema.forMap(
                                StateInspectType.scalar("VARCHAR(2147483647)"),
                                StateInspectType.scalar("VARCHAR(2147483647)"),
                                StateInspectType.scalar("VARCHAR(2147483647)"),
                                StateInspectType.scalar("INT")));

        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0, 1, 2});

        GenericRowData keyRow = new GenericRowData(3);
        keyRow.setField(0, StringData.fromString("k"));
        keyRow.setField(1, StringData.fromString("ns"));
        keyRow.setField(2, StringData.fromString("mk"));
        CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                encoder.encode(keyRow, TOTAL_KEY_GROUPS);

        byte[] keyBytes = serialize(StringSerializer.INSTANCE, "k");
        byte[] namespaceBytes = serialize(StringSerializer.INSTANCE, "ns");
        byte[] mapKeyBytes = serialize(StringSerializer.INSTANCE, "mk");
        // Three variable parts => 2 suffixes: key length first, then namespace length.
        assertTrue(schema.mapKeyLengthStored());
        assertTrue(schema.mapNamespaceLengthStored());
        byte[] expected =
                concat(
                        keyBytes,
                        namespaceBytes,
                        new byte[] {0x00},
                        mapKeyBytes,
                        intBytes(keyBytes.length),
                        intBytes(namespaceBytes.length));
        assertArrayEquals(expected, encoded.rowKey());

        assertDecoderRoundTripMap(
                runtimeSchema, config, encoded.rowKey(), new Object[] {"k", "ns", "mk"});
    }

    // ------------------------------------------------------------------------------------------
    //  Null rejection
    // ------------------------------------------------------------------------------------------

    @Test
    void rejectsNullStateKeyField() throws Exception {
        StateSourceConfig config =
                valueConfig(
                        fields(
                                field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(fields(field("key", "INT", StateSourceField.Group.STATE_KEY, 0))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        StateInspectSchema.forValue(
                                "orders",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT")));
        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0});

        GenericRowData keyRow = new GenericRowData(1);
        keyRow.setField(0, null);
        IOException error =
                assertThrows(IOException.class, () -> encoder.encode(keyRow, TOTAL_KEY_GROUPS));
        assertTrue(error.getMessage().contains("'key'"), error.getMessage());
        assertTrue(error.getMessage().contains("state key"), error.getMessage());
    }

    @Test
    void rejectsNullNamespaceFieldWhenNamespaceRequired() throws Exception {
        StateSourceConfig config =
                valueConfig(
                        fields(
                                field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                field(
                                        "namespace",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.NAMESPACE,
                                        0),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(
                                fields(
                                        field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                        field(
                                                "namespace",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.NAMESPACE,
                                                0))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        StateInspectSchema.forValue(
                                "orders",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.scalar("VARCHAR(2147483647)"),
                                StateInspectType.scalar("INT")));
        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0, 1});

        GenericRowData keyRow = new GenericRowData(2);
        keyRow.setField(0, 5);
        keyRow.setField(1, null);
        IOException error =
                assertThrows(IOException.class, () -> encoder.encode(keyRow, TOTAL_KEY_GROUPS));
        assertTrue(error.getMessage().contains("'namespace'"), error.getMessage());
        assertTrue(error.getMessage().contains("namespace"), error.getMessage());
    }

    @Test
    void rejectsNullMapKeyField() throws Exception {
        StateSourceConfig config =
                mapConfig(
                        fields(
                                field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                field("map_key", "INT", StateSourceField.Group.MAP_KEY, 0),
                                field("map_value", "INT", StateSourceField.Group.MAP_VALUE, 0)),
                        contract(
                                fields(
                                        field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                        field(
                                                "map_key",
                                                "INT",
                                                StateSourceField.Group.MAP_KEY,
                                                0))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        StateInspectSchema.forMap(
                                "counters",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                IntSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forMap(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT"),
                                StateInspectType.scalar("INT")));
        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0, 1});

        GenericRowData keyRow = new GenericRowData(2);
        keyRow.setField(0, 7);
        keyRow.setField(1, null);
        IOException error =
                assertThrows(IOException.class, () -> encoder.encode(keyRow, TOTAL_KEY_GROUPS));
        assertTrue(error.getMessage().contains("'map_key'"), error.getMessage());
        assertTrue(error.getMessage().contains("map key"), error.getMessage());
    }

    // ------------------------------------------------------------------------------------------
    //  Serializer-restoration failure + unsupported shapes
    // ------------------------------------------------------------------------------------------

    @Test
    void failsWhenSerializerCannotBeRestored() {
        // A custom TypeSerializer whose snapshotConfiguration() fails and that is not Java-
        // Serializable produces a SerializerInspectSchema with null snapshot and null serialized
        // bytes. restoreSerializer returns null, and GroupLookupEncoder.create must fail. The
        // encoder constructor wraps that IOException into IllegalArgumentException.
        UnrestorableIntSerializer bogusSerializer = new UnrestorableIntSerializer();
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "orders",
                        "cf",
                        false,
                        bogusSerializer,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        StateSourceConfig config =
                valueConfig(
                        fields(
                                field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(fields(field("key", "INT", StateSourceField.Group.STATE_KEY, 0))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        schema,
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT")));

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new CobbleStateLookupKeyEncoder(
                                        config, runtimeSchema, new int[] {0}));
        assertTrue(error.getMessage().contains("state key"), error.getMessage());
    }

    @Test
    void failsForUnsupportedTupleObjectShape() {
        // A TUPLE semantic type is accepted at construction but rejected at encode time when
        // buildObject encounters the TUPLE kind.
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "orders",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        StateInspectType tupleType =
                StateInspectType.tuple(
                        Collections.singletonList(
                                new StateInspectField("f0", StateInspectType.scalar("INT"))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        schema,
                        StateInspectSemanticSchema.forValue(
                                tupleType,
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT")));
        StateSourceConfig config =
                valueConfig(
                        fields(
                                field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(fields(field("key", "INT", StateSourceField.Group.STATE_KEY, 0))));

        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0});
        IOException error =
                assertThrows(
                        IOException.class, () -> encoder.encode(singleIntRow(1), TOTAL_KEY_GROUPS));
        assertTrue(
                error.getMessage().contains("tuple")
                        || error.getMessage().contains("not implemented"),
                error.getMessage());
    }

    @Test
    void failsForUnsupportedUnknownSemanticShape() {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "orders",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        schema,
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.unknown(),
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT")));
        StateSourceConfig config =
                valueConfig(
                        fields(
                                field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(fields(field("key", "INT", StateSourceField.Group.STATE_KEY, 0))));

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new CobbleStateLookupKeyEncoder(
                                        config, runtimeSchema, new int[] {0}));
        assertTrue(error.getMessage().contains("state key"), error.getMessage());
    }

    // ------------------------------------------------------------------------------------------
    //  Parity with backend byte layout (independent reference builder)
    // ------------------------------------------------------------------------------------------

    @Test
    void valueStateRowKeyParityWithBackendLayout() throws Exception {
        // Reference: replicate CanonicalSavepointRestoreOperation.buildKeyAndNamespace layout.
        byte[] keyBytes = serialize(StringSerializer.INSTANCE, "customer");
        byte[] namespaceBytes = serialize(StringSerializer.INSTANCE, "tenant");
        boolean storeKeyLength =
                StateRowKeyLayout.shouldStoreKeyLengthForKeyNamespace(
                        StringSerializer.INSTANCE.getLength(),
                        StringSerializer.INSTANCE.getLength());
        byte[] reference =
                storeKeyLength
                        ? concat(keyBytes, namespaceBytes, intBytes(keyBytes.length))
                        : concat(keyBytes, namespaceBytes);

        StateSourceConfig config =
                valueConfig(
                        fields(
                                field(
                                        "key",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.STATE_KEY,
                                        0),
                                field(
                                        "namespace",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.NAMESPACE,
                                        0),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(
                                fields(
                                        field(
                                                "key",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.STATE_KEY,
                                                0),
                                        field(
                                                "namespace",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.NAMESPACE,
                                                0))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        StateInspectSchema.forValue(
                                "orders",
                                "cf",
                                false,
                                StringSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("VARCHAR(2147483647)"),
                                StateInspectType.scalar("VARCHAR(2147483647)"),
                                StateInspectType.scalar("INT")));
        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0, 1});

        GenericRowData keyRow = new GenericRowData(2);
        keyRow.setField(0, StringData.fromString("customer"));
        keyRow.setField(1, StringData.fromString("tenant"));
        CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                encoder.encode(keyRow, TOTAL_KEY_GROUPS);

        assertArrayEquals(reference, encoded.rowKey());
    }

    @Test
    void mapStateRowKeyParityWithBackendLayout() throws Exception {
        byte[] keyBytes = serialize(StringSerializer.INSTANCE, "k");
        byte[] namespaceBytes = serialize(StringSerializer.INSTANCE, "ns");
        byte[] mapKeyBytes = serialize(StringSerializer.INSTANCE, "mk");
        boolean storeMapKeyLength =
                StateRowKeyLayout.shouldStoreMapKeyLength(
                        StringSerializer.INSTANCE.getLength(),
                        StringSerializer.INSTANCE.getLength(),
                        StringSerializer.INSTANCE.getLength());
        boolean storeMapNsLength =
                StateRowKeyLayout.shouldStoreMapNamespaceLength(
                        StringSerializer.INSTANCE.getLength(),
                        StringSerializer.INSTANCE.getLength(),
                        StringSerializer.INSTANCE.getLength());
        List<byte[]> parts = new ArrayList<>();
        parts.add(keyBytes);
        parts.add(namespaceBytes);
        parts.add(new byte[] {0x00});
        parts.add(mapKeyBytes);
        if (storeMapKeyLength) {
            parts.add(intBytes(keyBytes.length));
        }
        if (storeMapNsLength) {
            parts.add(intBytes(namespaceBytes.length));
        }
        byte[] reference = concat(parts.toArray(new byte[0][]));

        StateSourceConfig config =
                mapConfig(
                        fields(
                                field(
                                        "key",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.STATE_KEY,
                                        0),
                                field(
                                        "namespace",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.NAMESPACE,
                                        0),
                                field(
                                        "map_key",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.MAP_KEY,
                                        0),
                                field("map_value", "INT", StateSourceField.Group.MAP_VALUE, 0)),
                        contract(
                                fields(
                                        field(
                                                "key",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.STATE_KEY,
                                                0),
                                        field(
                                                "namespace",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.NAMESPACE,
                                                0),
                                        field(
                                                "map_key",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.MAP_KEY,
                                                0))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        StateInspectSchema.forMap(
                                "counters",
                                "cf",
                                false,
                                StringSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forMap(
                                StateInspectType.scalar("VARCHAR(2147483647)"),
                                StateInspectType.scalar("VARCHAR(2147483647)"),
                                StateInspectType.scalar("VARCHAR(2147483647)"),
                                StateInspectType.scalar("INT")));
        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0, 1, 2});

        GenericRowData keyRow = new GenericRowData(3);
        keyRow.setField(0, StringData.fromString("k"));
        keyRow.setField(1, StringData.fromString("ns"));
        keyRow.setField(2, StringData.fromString("mk"));
        CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                encoder.encode(keyRow, TOTAL_KEY_GROUPS);

        assertArrayEquals(reference, encoded.rowKey());
    }

    @Test
    void valueStateRowKeyParityWithVoidNamespace() throws Exception {
        // Reference: key bytes + 1-byte VoidNamespace, no length suffix.
        byte[] keyBytes = serialize(IntSerializer.INSTANCE, 42);
        byte[] reference = concat(keyBytes, namespaceBytes());

        StateSourceConfig config =
                valueConfig(
                        fields(
                                field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(fields(field("key", "INT", StateSourceField.Group.STATE_KEY, 0))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        StateInspectSchema.forValue(
                                "orders",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT")));
        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0});

        CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                encoder.encode(singleIntRow(42), TOTAL_KEY_GROUPS);
        assertArrayEquals(reference, encoded.rowKey());
    }

    @Test
    void keyGroupMatchesBackendAssignmentForScalarKey() throws Exception {
        StateSourceConfig config =
                valueConfig(
                        fields(
                                field("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(fields(field("key", "INT", StateSourceField.Group.STATE_KEY, 0))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        StateInspectSchema.forValue(
                                "orders",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT")));
        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0});

        for (int key = 0; key < 100; key++) {
            CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                    encoder.encode(singleIntRow(key), TOTAL_KEY_GROUPS);
            assertEquals(
                    KeyGroupRangeAssignment.assignToKeyGroup(key, TOTAL_KEY_GROUPS),
                    encoded.keyGroup());
        }
    }

    // ------------------------------------------------------------------------------------------
    //  Structured ROW key: byte parity + key-group parity
    // ------------------------------------------------------------------------------------------

    @Test
    void encodesRowKeyWithBigIntAndVarCharAndMatchesBackend() throws Exception {
        // A structured ROW<BIGINT, VARCHAR> state key must keep internal field values (StringData,
        // not Java String) in the GenericRowData so RowDataSerializer serializes correctly and
        // GenericRowData.hashCode() matches the backend's key-group assignment.
        BigIntType bigIntType = new BigIntType();
        VarCharType varCharType = new VarCharType(VarCharType.MAX_LENGTH);
        RowDataSerializer keySerializer = new RowDataSerializer(bigIntType, varCharType);

        StateInspectType keyType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("BIGINT")),
                                new StateInspectField(
                                        "name", StateInspectType.scalar("VARCHAR(2147483647)"))));

        StateSourceConfig config =
                valueConfig(
                        fields(
                                field("id", "BIGINT", StateSourceField.Group.STATE_KEY, 0),
                                field(
                                        "name",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.STATE_KEY,
                                        1),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(
                                fields(
                                        field("id", "BIGINT", StateSourceField.Group.STATE_KEY, 0),
                                        field(
                                                "name",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.STATE_KEY,
                                                1))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        StateInspectSchema.forValue(
                                "orders",
                                "cf",
                                false,
                                keySerializer,
                                VoidNamespaceSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forValue(
                                keyType,
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT")));
        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0, 1});

        // Build the lookup key row: BIGINT=42, VARCHAR="alice".
        GenericRowData lookupRow = new GenericRowData(2);
        lookupRow.setField(0, 42L);
        lookupRow.setField(1, StringData.fromString("alice"));

        CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                encoder.encode(lookupRow, TOTAL_KEY_GROUPS);

        // Reference row key: serialize a GenericRowData with the SAME internal values (no
        // toExternal conversion), then append the 1-byte VoidNamespace.
        GenericRowData referenceKey = new GenericRowData(2);
        referenceKey.setField(0, 42L);
        referenceKey.setField(1, StringData.fromString("alice"));
        byte[] expectedKeyBytes = serialize(keySerializer, referenceKey);
        byte[] expected = concat(expectedKeyBytes, namespaceBytes());
        assertArrayEquals(expected, encoded.rowKey());

        // Key-group parity: the backend passes the RowData key object to
        // assignToKeyGroup. The encoder's buildObject returns a GenericRowData with internal
        // values, so hashCode() must match.
        assertEquals(
                KeyGroupRangeAssignment.assignToKeyGroup(referenceKey, TOTAL_KEY_GROUPS),
                encoded.keyGroup());

        // Decoder round-trip: the decoder reads the RowData key back as individual fields.
        assertDecoderRoundTripRowKey(
                runtimeSchema, config, encoded.rowKey(), new Object[] {42L, "alice", 1});
    }

    @Test
    void rowKeyGroupMatchesBackendForMultipleVarCharValues() throws Exception {
        // Verify key-group parity across multiple VARCHAR values — if the encoder accidentally
        // converted StringData to Java String, GenericRowData.hashCode() would differ from the
        // backend's and these assertions would fail.
        BigIntType bigIntType = new BigIntType();
        VarCharType varCharType = new VarCharType(VarCharType.MAX_LENGTH);
        RowDataSerializer keySerializer = new RowDataSerializer(bigIntType, varCharType);
        StateInspectType keyType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("BIGINT")),
                                new StateInspectField(
                                        "name", StateInspectType.scalar("VARCHAR(2147483647)"))));

        StateSourceConfig config =
                valueConfig(
                        fields(
                                field("id", "BIGINT", StateSourceField.Group.STATE_KEY, 0),
                                field(
                                        "name",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.STATE_KEY,
                                        1),
                                field("value", "INT", StateSourceField.Group.VALUE, 0)),
                        contract(
                                fields(
                                        field("id", "BIGINT", StateSourceField.Group.STATE_KEY, 0),
                                        field(
                                                "name",
                                                "VARCHAR(2147483647)",
                                                StateSourceField.Group.STATE_KEY,
                                                1))));
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                runtimeSchema(
                        StateInspectSchema.forValue(
                                "orders",
                                "cf",
                                false,
                                keySerializer,
                                VoidNamespaceSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forValue(
                                keyType,
                                StateInspectType.unknown(),
                                StateInspectType.scalar("INT")));
        CobbleStateLookupKeyEncoder encoder =
                new CobbleStateLookupKeyEncoder(config, runtimeSchema, new int[] {0, 1});

        String[] names = {"alice", "bob", "charlie", "", "x", "a-very-long-name-for-hashing"};
        for (String name : names) {
            GenericRowData lookupRow = new GenericRowData(2);
            lookupRow.setField(0, 1L);
            lookupRow.setField(1, StringData.fromString(name));

            GenericRowData referenceKey = new GenericRowData(2);
            referenceKey.setField(0, 1L);
            referenceKey.setField(1, StringData.fromString(name));

            CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                    encoder.encode(lookupRow, TOTAL_KEY_GROUPS);
            assertEquals(
                    KeyGroupRangeAssignment.assignToKeyGroup(referenceKey, TOTAL_KEY_GROUPS),
                    encoded.keyGroup(),
                    "key-group mismatch for name='" + name + "'");
        }
    }

    // ------------------------------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------------------------------

    private static CobbleStateSourceRuntime.RuntimeSchema runtimeSchema(
            StateInspectSchema schema, StateInspectSemanticSchema semantic) {
        return new CobbleStateSourceRuntime.RuntimeSchema(schema, semantic);
    }

    private static StateSourceConfig valueConfig(
            List<StateSourceField> outputFields, StateSourceLookupKeyContract contract) {
        return new StateSourceConfig(
                "file:///tmp/checkpoints",
                StateSourceConfig.Layout.CHECKPOINT_ROOT,
                "operator-1",
                "orders",
                "value",
                "latest",
                "batch",
                7L,
                -1,
                0L,
                outputFields,
                contract);
    }

    private static StateSourceConfig mapConfig(
            List<StateSourceField> outputFields, StateSourceLookupKeyContract contract) {
        return new StateSourceConfig(
                "file:///tmp/checkpoints",
                StateSourceConfig.Layout.CHECKPOINT_ROOT,
                "operator-1",
                "counters",
                "map",
                "latest",
                "batch",
                7L,
                -1,
                0L,
                outputFields,
                contract);
    }

    private static StateSourceLookupKeyContract contract(List<StateSourceField> requiredFields) {
        int[] positions = new int[requiredFields.size()];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = i;
        }
        return StateSourceLookupKeyContract.present(requiredFields, positions);
    }

    private static List<StateSourceField> fields(StateSourceField... fields) {
        return Arrays.asList(fields);
    }

    private static StateSourceField field(
            String name, String logicalType, StateSourceField.Group group, int index) {
        return new StateSourceField(name, logicalType, group, index);
    }

    private static RowData singleIntRow(int value) {
        GenericRowData row = new GenericRowData(1);
        row.setField(0, value);
        return row;
    }

    private static byte[] namespaceBytes() throws Exception {
        return serialize(VoidNamespaceSerializer.INSTANCE, VoidNamespace.INSTANCE);
    }

    private static <T> byte[] serialize(TypeSerializer<T> serializer, T value) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(32);
        serializer.serialize(value, out);
        return out.getCopyOfBuffer();
    }

    private static byte[] intBytes(int value) {
        return new byte[] {
            (byte) ((value >> 24) & 0xFF),
            (byte) ((value >> 16) & 0xFF),
            (byte) ((value >> 8) & 0xFF),
            (byte) (value & 0xFF)
        };
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }

    private static byte[] concat(byte[]... chunks) {
        int length = 0;
        for (byte[] chunk : chunks) {
            length += chunk.length;
        }
        byte[] out = new byte[length];
        int offset = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, out, offset, chunk.length);
            offset += chunk.length;
        }
        return out;
    }

    /**
     * Round-trips encoder bytes through the value-state decoder.
     *
     * @param expected key, namespace, value (in output-column order)
     */
    private void assertDecoderRoundTripValue(
            CobbleStateSourceRuntime.RuntimeSchema runtimeSchema,
            StateSourceConfig config,
            byte[] rowKey,
            Object[] expected)
            throws Exception {
        CobbleStateRowDecoder decoder = new CobbleStateRowDecoder(config, runtimeSchema);
        byte[] valueColumn =
                serialize(IntSerializer.INSTANCE, (Integer) expected[expected.length - 1]);
        List<RowData> rows = decoder.decode(rowKey, new byte[][] {valueColumn}, "lk", 0);
        assertEquals(1, rows.size());
        RowData row = rows.get(0);
        int pos = 0;
        for (Object want : expected) {
            if (want instanceof Integer) {
                assertEquals(want, row.getInt(pos), "column " + pos);
            } else if (want instanceof String) {
                assertEquals(want, row.getString(pos).toString(), "column " + pos);
            }
            pos++;
        }
    }

    /**
     * Round-trips encoder bytes through the value-state decoder for a structured ROW key. Expects
     * output columns ordered as: key field 0, key field 1, ..., value.
     */
    private void assertDecoderRoundTripRowKey(
            CobbleStateSourceRuntime.RuntimeSchema runtimeSchema,
            StateSourceConfig config,
            byte[] rowKey,
            Object[] expected)
            throws Exception {
        CobbleStateRowDecoder decoder = new CobbleStateRowDecoder(config, runtimeSchema);
        byte[] valueColumn =
                serialize(IntSerializer.INSTANCE, (Integer) expected[expected.length - 1]);
        List<RowData> rows = decoder.decode(rowKey, new byte[][] {valueColumn}, "lk", 0);
        assertEquals(1, rows.size());
        RowData row = rows.get(0);
        // The decoder splits the row key into key + namespace. The key is deserialized by
        // RowDataSerializer and then the decoder extracts each field via RowData.FieldGetter.
        // For BIGINT, the decoded value is a long; for VARCHAR, a StringData.
        if (expected[0] instanceof Long) {
            assertEquals(expected[0], row.getLong(0), "column 0");
        } else if (expected[0] instanceof Integer) {
            assertEquals(expected[0], row.getInt(0), "column 0");
        }
        if (expected[1] instanceof String) {
            assertEquals(expected[1], row.getString(1).toString(), "column 1");
        }
    }

    /**
     * Round-trips encoder bytes through the map-state decoder. Expects output columns ordered as
     * key, [namespace], map_key, map_value.
     */
    private void assertDecoderRoundTripMap(
            CobbleStateSourceRuntime.RuntimeSchema runtimeSchema,
            StateSourceConfig config,
            byte[] rowKey,
            Object[] expectedKeyNamespaceMapKey)
            throws Exception {
        CobbleStateRowDecoder decoder = new CobbleStateRowDecoder(config, runtimeSchema);
        // Map value column: first byte 0x01 => present-null marker (null value).
        List<RowData> rows = decoder.decode(rowKey, new byte[][] {new byte[] {0x01}}, "lk", 0);
        assertEquals(1, rows.size());
        RowData row = rows.get(0);
        int pos = 0;
        for (Object want : expectedKeyNamespaceMapKey) {
            if (want instanceof Integer) {
                assertEquals(want, row.getInt(pos), "column " + pos);
            } else if (want instanceof String) {
                assertEquals(want, row.getString(pos).toString(), "column " + pos);
            }
            pos++;
        }
        // The trailing map_value column should be null (present-null marker 0x01).
        assertTrue(row.isNullAt(pos), "map_value column " + pos + " should be null");
    }

    /**
     * A {@link TypeSerializer} whose {@link #snapshotConfiguration()} throws and that carries a
     * non-serializable field so {@link java.io.ObjectOutputStream} fails, so {@link
     * SerializerInspectSchema#fromSerializer} captures null snapshot and null serialized bytes.
     * {@link SerializerInspectSchema#restoreSerializer} then returns null, exercising the encoder's
     * fail-fast path.
     */
    static final class UnrestorableIntSerializer extends TypeSerializer<Integer> {

        // A non-serializable field: makes InstantiationUtil.serializeObject(this) fail so
        // serializedSerializerBytes is null.
        @SuppressWarnings("serial")
        private final Object nonSerializable = new Object();

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<Integer> duplicate() {
            return this;
        }

        @Override
        public Integer createInstance() {
            return 0;
        }

        @Override
        public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
            throw new RuntimeException("intentionally unrestorable for test");
        }

        @Override
        public void serialize(Integer record, DataOutputView target) throws IOException {
            target.writeInt(record);
        }

        @Override
        public Integer deserialize(DataInputView source) throws IOException {
            return source.readInt();
        }

        @Override
        public Integer copy(Integer from) {
            return from;
        }

        @Override
        public Integer copy(Integer from, Integer reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 4;
        }

        @Override
        public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
            return source.readInt();
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeInt(source.readInt());
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof UnrestorableIntSerializer;
        }

        @Override
        public int hashCode() {
            return UnrestorableIntSerializer.class.hashCode();
        }
    }
}
