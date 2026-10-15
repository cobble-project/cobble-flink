package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.decode.ClasslessPojoValue;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class StateSemanticValueConverterTest {
    private static final StateInspectType INTEGER = StateInspectType.scalar("INT");
    private static final StateInspectType STRING = StateInspectType.scalar("VARCHAR");
    private static final StateInspectType DETAIL =
            StateInspectType.row(
                    Arrays.asList(
                            new StateInspectField("code", INTEGER),
                            new StateInspectField(
                                    "payload", StateInspectType.scalar("VARBINARY"))));
    private static final StateInspectType ROOT =
            StateInspectType.row(
                    Arrays.asList(
                            new StateInspectField("details", DETAIL),
                            new StateInspectField("labels", StateInspectType.list(STRING)),
                            new StateInspectField(
                                    "attributes",
                                    StateInspectType.map(STRING, StateInspectType.list(INTEGER)))));

    @Test
    void convertsLivePojoRecursivelyWithNullAndEmptyCollections() throws Exception {
        Root value = new Root();
        value.details = new Detail();
        value.details.code = 42;
        value.details.payload = new byte[] {1, 2, 3};
        value.labels = Arrays.asList("long-label", null);
        value.attributes = Collections.singletonMap("scores", Arrays.asList(9, null));
        StateSemanticValueConverter converter = new StateSemanticValueConverter(ROOT);
        assertValues(converter.flatten(value, new boolean[] {true, true, true}));
        value.details = null;
        value.labels = Collections.emptyList();
        value.attributes = Collections.emptyMap();
        Object[] empty = converter.flatten(value, new boolean[] {true, true, true});
        assertNull(empty[0]);
        assertEquals(0, ((ArrayData) empty[1]).size());
        assertEquals(0, ((MapData) empty[2]).size());
        assertNull(converter.flatten(null, new boolean[] {true, true, true})[0]);
    }

    @Test
    void convertsClasslessNestedFieldsAndDoesNotMutateByteBuffer() throws Exception {
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("code", 42);
        ByteBuffer payload = ByteBuffer.wrap(new byte[] {1, 2, 3});
        detail.put("payload", payload);
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("details", ClasslessPojoValue.of(detail));
        root.put("labels", Arrays.asList("long-label", null));
        root.put("attributes", Collections.singletonMap("scores", Arrays.asList(9, null)));
        assertValues(
                new StateSemanticValueConverter(ROOT)
                        .flatten(ClasslessPojoValue.of(root), new boolean[] {true, true, true}));
        assertEquals(0, payload.position());
    }

    @Test
    void projectionDoesNotReflectMissingUnselectedPojoFields() throws Exception {
        StateInspectType type =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("present", INTEGER),
                                new StateInspectField("missing", INTEGER)));
        StateSemanticValueConverter converter = new StateSemanticValueConverter(type);
        assertEquals(7, converter.flatten(new Partial(), new boolean[] {true, false})[0]);
        assertThrows(
                IOException.class,
                () -> converter.flatten(new Partial(), new boolean[] {false, true}));
    }

    private static void assertValues(Object[] values) {
        RowData detail = (RowData) values[0];
        assertEquals(42, detail.getInt(0));
        assertArrayEquals(new byte[] {1, 2, 3}, detail.getBinary(1));
        ArrayData labels = (ArrayData) values[1];
        assertEquals("long-label", labels.getString(0).toString());
        assertEquals(true, labels.isNullAt(1));
        MapData map = (MapData) values[2];
        assertEquals("scores", map.keyArray().getString(0).toString());
        ArrayData scores = map.valueArray().getArray(0);
        assertEquals(9, scores.getInt(0));
        assertEquals(true, scores.isNullAt(1));
    }

    public static class Root {
        public Detail details;
        public List<String> labels;
        public Map<String, List<Integer>> attributes;
    }

    public static class Detail {
        public int code;
        public byte[] payload;
    }

    public static class Partial {
        public int present = 7;
    }
}
