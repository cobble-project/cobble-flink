package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Coverage for source-side semantic projection without writer POJO or flink-avro runtime classes.
 */
class CobbleStateClasslessValueDecoderTest {

    private static final Schema PERSON_SCHEMA = Schema.createRecord("Person", null, "test", false);

    static {
        PERSON_SCHEMA.setFields(
                Arrays.asList(
                        new Schema.Field("id", Schema.create(Schema.Type.INT), null, null),
                        new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));
    }

    @Test
    void valuePojoAndGenericAvroDecodeWithoutWriterClasses() throws Exception {
        TypeSerializer<PersonPojo> pojoSerializer =
                TypeInformation.of(PersonPojo.class).createSerializer(new ExecutionConfig());
        StateInspectType personType = personType();
        CobbleStateRowDecoder pojoDecoder =
                decoder(
                        StateInspectSchema.forValue(
                                "pojo",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                pojoSerializer),
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                personType),
                        "value");

        List<RowData> pojoRows =
                decodeWithBlockedClasses(
                        pojoDecoder,
                        valueKey(1),
                        serialize(pojoSerializer, new PersonPojo(7, "pojo")),
                        PersonPojo.class.getName());
        assertEquals(7, pojoRows.get(0).getInt(1));
        assertEquals("pojo", pojoRows.get(0).getString(2).toString());

        AvroSerializer<GenericRecord> avroSerializer =
                new AvroSerializer<>(GenericRecord.class, PERSON_SCHEMA);
        GenericRecord generic = new GenericData.Record(PERSON_SCHEMA);
        generic.put("id", 8);
        generic.put("name", "generic");
        CobbleStateRowDecoder avroDecoder =
                decoder(
                        StateInspectSchema.forValue(
                                "generic",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                avroSerializer),
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                personType),
                        "value");

        List<RowData> genericRows =
                decodeWithBlockedClasses(
                        avroDecoder,
                        valueKey(2),
                        serialize(avroSerializer, generic),
                        "org.apache.flink.formats.avro.typeutils.AvroSerializer");
        assertEquals(8, genericRows.get(0).getInt(1));
        assertEquals("generic", genericRows.get(0).getString(2).toString());
    }

    @Test
    void valueSpecificAvroAndPortableTupleRemainReadable() throws Exception {
        AvroSerializer<SpecificPerson> specificSerializer =
                new AvroSerializer<>(SpecificPerson.class);
        CobbleStateRowDecoder specificDecoder =
                decoder(
                        StateInspectSchema.forValue(
                                "specific",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                specificSerializer),
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                personType()),
                        "value");
        List<RowData> specificRows =
                decodeWithBlockedClasses(
                        specificDecoder,
                        valueKey(3),
                        serialize(specificSerializer, new SpecificPerson(9, "specific")),
                        SpecificPerson.class.getName(),
                        "org.apache.flink.formats.avro.typeutils.AvroSerializer");
        assertEquals(9, specificRows.get(0).getInt(1));
        assertEquals("specific", specificRows.get(0).getString(2).toString());

        TypeSerializer<Tuple3<Integer, String, Integer>> tupleSerializer =
                TypeInformation.of(new TypeHint<Tuple3<Integer, String, Integer>>() {})
                        .createSerializer(new ExecutionConfig());
        StateInspectType tupleType =
                StateInspectType.tuple(
                        Arrays.asList(
                                new StateInspectField("f0", StateInspectType.scalar("INT")),
                                new StateInspectField(
                                        "f1", StateInspectType.scalar("VARCHAR(2147483647)")),
                                new StateInspectField("f2", StateInspectType.scalar("INT"))));
        CobbleStateRowDecoder tupleDecoder =
                decoder(
                        StateInspectSchema.forValue(
                                "tuple",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                tupleSerializer),
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                tupleType),
                        "value",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField("f0", "INT", StateSourceField.Group.VALUE, 0),
                                new StateSourceField(
                                        "f1",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.VALUE,
                                        1),
                                new StateSourceField(
                                        "f2", "INT", StateSourceField.Group.VALUE, 2)));
        List<RowData> tupleRows =
                tupleDecoder.decode(
                        valueKey(4),
                        new byte[][] {serialize(tupleSerializer, Tuple3.of(10, "tuple", 11))},
                        "scan",
                        0);
        assertEquals(10, tupleRows.get(0).getInt(1));
        assertEquals("tuple", tupleRows.get(0).getString(2).toString());
        assertEquals(11, tupleRows.get(0).getInt(3));
    }

    @Test
    void valueGenericAvroEnumProjectsAsVarchar() throws Exception {
        Schema schema =
                SchemaBuilder.record("StatusValue")
                        .fields()
                        .name("status")
                        .type()
                        .enumeration("Status")
                        .symbols("OPEN", "CLOSED")
                        .noDefault()
                        .endRecord();
        AvroSerializer<GenericRecord> serializer =
                new AvroSerializer<>(GenericRecord.class, schema);
        GenericRecord record = new GenericData.Record(schema);
        record.put(
                "status", new GenericData.EnumSymbol(schema.getField("status").schema(), "OPEN"));
        StateInspectType type =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField(
                                        "status", StateInspectType.scalar("VARCHAR(2147483647)"))));
        CobbleStateRowDecoder decoder =
                decoder(
                        StateInspectSchema.forValue(
                                "enum",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                serializer),
                        StateInspectSemanticSchema.forValue(
                                StateInspectType.scalar("INT"), StateInspectType.unknown(), type),
                        "value",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "status",
                                        "VARCHAR(2147483647)",
                                        StateSourceField.Group.VALUE,
                                        0)));

        List<RowData> rows =
                decodeWithBlockedClasses(
                        decoder,
                        valueKey(10),
                        serialize(serializer, record),
                        "org.apache.flink.formats.avro.typeutils.AvroSerializer");
        assertEquals("OPEN", rows.get(0).getString(1).toString());
    }

    @Test
    void listPojoAndAvroUseCursorAcrossElements() throws Exception {
        TypeSerializer<PersonPojo> pojoSerializer =
                TypeInformation.of(PersonPojo.class).createSerializer(new ExecutionConfig());
        CobbleStateRowDecoder pojoDecoder =
                decoder(
                        StateInspectSchema.forList(
                                "pojo-list",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                pojoSerializer),
                        StateInspectSemanticSchema.forList(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                personType()),
                        "list");
        List<RowData> pojoRows =
                decodeWithBlockedClasses(
                        pojoDecoder,
                        valueKey(5),
                        join(
                                serialize(pojoSerializer, new PersonPojo(1, "first")),
                                serialize(pojoSerializer, new PersonPojo(2, "second"))),
                        PersonPojo.class.getName());
        assertEquals(2, pojoRows.size());
        assertEquals("first", pojoRows.get(0).getString(2).toString());
        assertEquals("second", pojoRows.get(1).getString(2).toString());

        AvroSerializer<GenericRecord> avroSerializer =
                new AvroSerializer<>(GenericRecord.class, PERSON_SCHEMA);
        GenericRecord first = new GenericData.Record(PERSON_SCHEMA);
        first.put("id", 3);
        first.put("name", "avro-first");
        GenericRecord second = new GenericData.Record(PERSON_SCHEMA);
        second.put("id", 4);
        second.put("name", "avro-second");
        CobbleStateRowDecoder avroDecoder =
                decoder(
                        StateInspectSchema.forList(
                                "avro-list",
                                "cf",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                avroSerializer),
                        StateInspectSemanticSchema.forList(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                personType()),
                        "list");
        List<RowData> avroRows =
                decodeWithBlockedClasses(
                        avroDecoder,
                        valueKey(6),
                        join(serialize(avroSerializer, first), serialize(avroSerializer, second)),
                        "org.apache.flink.formats.avro.typeutils.AvroSerializer");
        assertEquals(3, avroRows.get(0).getInt(1));
        assertEquals("avro-second", avroRows.get(1).getString(2).toString());
    }

    @Test
    void mapAvroValueKeepsPresentNullSemantics() throws Exception {
        AvroSerializer<GenericRecord> avroSerializer =
                new AvroSerializer<>(GenericRecord.class, PERSON_SCHEMA);
        GenericRecord record = new GenericData.Record(PERSON_SCHEMA);
        record.put("id", 12);
        record.put("name", "map-value");
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "avro-map",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        avroSerializer);
        CobbleStateRowDecoder decoder =
                decoder(
                        schema,
                        StateInspectSemanticSchema.forMap(
                                StateInspectType.scalar("INT"), StateInspectType.unknown(),
                                StateInspectType.scalar("INT"), personType()),
                        "map");
        byte[] rowKey =
                concat(
                        serialize(IntSerializer.INSTANCE, 7),
                        namespaceBytes(),
                        new byte[] {0},
                        serialize(IntSerializer.INSTANCE, 8));
        List<RowData> rows =
                decodeWithBlockedClasses(
                        decoder,
                        rowKey,
                        concat(new byte[] {0}, serialize(avroSerializer, record)),
                        "org.apache.flink.formats.avro.typeutils.AvroSerializer");
        assertEquals(8, rows.get(0).getInt(1));
        assertEquals(12, rows.get(0).getInt(2));
        assertEquals("map-value", rows.get(0).getString(3).toString());

        List<RowData> nullRows = decoder.decode(rowKey, new byte[][] {new byte[] {1}}, "scan", 0);
        assertNull(nullRows.get(0).isNullAt(2) ? null : "not-null");
        assertNull(nullRows.get(0).isNullAt(3) ? null : "not-null");
    }

    private static CobbleStateRowDecoder decoder(
            StateInspectSchema schema, StateInspectSemanticSchema semantic, String kind)
            throws Exception {
        return decoder(schema, semantic, kind, fields(kind));
    }

    private static CobbleStateRowDecoder decoder(
            StateInspectSchema schema,
            StateInspectSemanticSchema semantic,
            String kind,
            List<StateSourceField> outputFields)
            throws Exception {
        return new CobbleStateRowDecoder(
                new StateSourceConfig(
                        "file:///tmp/checkpoints",
                        StateSourceConfig.Layout.CHECKPOINT_ROOT,
                        "operator",
                        "state",
                        kind,
                        "7",
                        "batch",
                        7L,
                        -1,
                        0L,
                        outputFields),
                new CobbleStateSourceRuntime.RuntimeSchema(schema, semantic));
    }

    private static List<RowData> decodeWithBlockedClasses(
            CobbleStateRowDecoder decoder, byte[] rowKey, byte[] value, String... blocked)
            throws Exception {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(new RejectingClassLoader(original, blocked));
        try {
            return decoder.decode(rowKey, new byte[][] {value}, "lookup", 0);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private static List<StateSourceField> fields(String kind) {
        if ("map".equals(kind)) {
            return Arrays.asList(
                    new StateSourceField("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                    new StateSourceField("map_key", "INT", StateSourceField.Group.MAP_KEY, 0),
                    new StateSourceField("id", "INT", StateSourceField.Group.MAP_VALUE, 0),
                    new StateSourceField(
                            "name", "VARCHAR(2147483647)", StateSourceField.Group.MAP_VALUE, 1));
        }
        StateSourceField.Group valueGroup =
                "list".equals(kind)
                        ? StateSourceField.Group.LIST_ELEMENT
                        : StateSourceField.Group.VALUE;
        return Arrays.asList(
                new StateSourceField("key", "INT", StateSourceField.Group.STATE_KEY, 0),
                new StateSourceField("id", "INT", valueGroup, 0),
                new StateSourceField("name", "VARCHAR(2147483647)", valueGroup, 1));
    }

    private static StateInspectType personType() {
        return StateInspectType.row(
                Arrays.asList(
                        new StateInspectField("id", StateInspectType.scalar("INT")),
                        new StateInspectField(
                                "name", StateInspectType.scalar("VARCHAR(2147483647)"))));
    }

    private static byte[] valueKey(int key) throws Exception {
        return concat(serialize(IntSerializer.INSTANCE, key), namespaceBytes());
    }

    private static byte[] namespaceBytes() throws Exception {
        return serialize(VoidNamespaceSerializer.INSTANCE, VoidNamespace.INSTANCE);
    }

    private static byte[] join(byte[] first, byte[] second) {
        return concat(first, new byte[] {','}, second);
    }

    private static <T> byte[] serialize(TypeSerializer<T> serializer, T value) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(64);
        serializer.serialize(value, output);
        return output.getCopyOfBuffer();
    }

    private static byte[] concat(byte[]... parts) {
        int length = 0;
        for (byte[] part : parts) {
            length += part.length;
        }
        byte[] result = new byte[length];
        int offset = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, result, offset, part.length);
            offset += part.length;
        }
        return result;
    }

    public static final class PersonPojo {
        public int id;
        public String name;

        public PersonPojo() {}

        PersonPojo(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public static final class SpecificPerson extends SpecificRecordBase {
        private int id;
        private CharSequence name;

        public SpecificPerson() {}

        SpecificPerson(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public Schema getSchema() {
            return PERSON_SCHEMA;
        }

        @Override
        public Object get(int field) {
            return field == 0 ? id : name;
        }

        @Override
        public void put(int field, Object value) {
            if (field == 0) {
                id = (Integer) value;
            } else {
                name = (CharSequence) value;
            }
        }
    }

    private static final class RejectingClassLoader extends ClassLoader {
        private final List<String> rejected;

        private RejectingClassLoader(ClassLoader parent, String... rejected) {
            super(parent);
            this.rejected = Arrays.asList(rejected);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (rejected.contains(name)) {
                throw new ClassNotFoundException("Blocked for classless decode test: " + name);
            }
            return super.loadClass(name, resolve);
        }
    }
}
