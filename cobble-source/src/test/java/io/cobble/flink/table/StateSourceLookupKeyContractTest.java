package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.StateKind;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Tests for {@link StateSourceLookupKeyContract} exact full-key derivation and PK validation. */
class StateSourceLookupKeyContractTest {

    // ------------------------------------------------------------------------------------------
    //  Absent contract
    // ------------------------------------------------------------------------------------------

    @Test
    void noPrimaryKeyYieldsAbsentContract() {
        StateSourceLookupKeyContract contract =
                StateSourceLookupKeyContract.derive(
                        "orders",
                        StateKind.VALUE,
                        outputFields(
                                field("key", StateSourceField.Group.STATE_KEY, 0),
                                field("value", StateSourceField.Group.VALUE, 0)),
                        ResolvedSchema.of(
                                Column.physical("key", DataTypes.INT()),
                                Column.physical("value", DataTypes.INT())));

        assertEquals(StateSourceLookupKeyContract.Status.ABSENT, contract.status());
        assertFalse(contract.isPresent());
    }

    // ------------------------------------------------------------------------------------------
    //  Value-like state
    // ------------------------------------------------------------------------------------------

    @Test
    void valueStateWithScalarKeyAcceptsPrimaryKeyOnKey() {
        StateSourceLookupKeyContract contract =
                StateSourceLookupKeyContract.derive(
                        "orders",
                        StateKind.VALUE,
                        outputFields(
                                field("key", StateSourceField.Group.STATE_KEY, 0),
                                field("value", StateSourceField.Group.VALUE, 0)),
                        schema(columns("key", "value"), Collections.singletonList("key")));

        assertTrue(contract.isPresent());
        assertEquals(Collections.singletonList("key"), names(contract.requiredFields()));
        assertArrayEquals(new int[] {0}, contract.requiredPhysicalPositions());
    }

    @Test
    void valueStateWithNonVoidNamespaceAcceptsKeyAndNamespace() {
        StateSourceLookupKeyContract contract =
                StateSourceLookupKeyContract.derive(
                        "orders",
                        StateKind.VALUE,
                        outputFields(
                                field("key", StateSourceField.Group.STATE_KEY, 0),
                                field("namespace", StateSourceField.Group.NAMESPACE, 0),
                                field("value", StateSourceField.Group.VALUE, 0)),
                        schema(
                                columns("key", "namespace", "value"),
                                Arrays.asList("key", "namespace")));

        assertTrue(contract.isPresent());
        assertEquals(Arrays.asList("key", "namespace"), names(contract.requiredFields()));
        assertArrayEquals(new int[] {0, 1}, contract.requiredPhysicalPositions());
    }

    @Test
    void structuredStateKeyRequiresEveryFlattenedFieldInOrder() {
        StateSourceLookupKeyContract contract =
                StateSourceLookupKeyContract.derive(
                        "orders",
                        StateKind.VALUE,
                        outputFields(
                                field("customer_id", StateSourceField.Group.STATE_KEY, 0),
                                field("tenant", StateSourceField.Group.STATE_KEY, 1),
                                field("amount", StateSourceField.Group.VALUE, 0)),
                        schema(
                                columns("customer_id", "tenant", "amount"),
                                Arrays.asList("customer_id", "tenant")));

        assertTrue(contract.isPresent());
        assertEquals(Arrays.asList("customer_id", "tenant"), names(contract.requiredFields()));
        assertArrayEquals(new int[] {0, 1}, contract.requiredPhysicalPositions());
    }

    @Test
    void reducingAndAggregatingUseSameKeyShapeAsValue() {
        for (StateKind kind : Arrays.asList(StateKind.REDUCING, StateKind.AGGREGATING)) {
            StateSourceLookupKeyContract contract =
                    StateSourceLookupKeyContract.derive(
                            "orders",
                            kind,
                            outputFields(
                                    field("key", StateSourceField.Group.STATE_KEY, 0),
                                    field("value", StateSourceField.Group.VALUE, 0)),
                            schema(columns("key", "value"), Collections.singletonList("key")));
            assertTrue(contract.isPresent(), "expected present contract for " + kind);
        }
    }

    // ------------------------------------------------------------------------------------------
    //  Map state
    // ------------------------------------------------------------------------------------------

    @Test
    void mapStateRequiresStateKeyNamespaceAndMapKey() {
        StateSourceLookupKeyContract contract =
                StateSourceLookupKeyContract.derive(
                        "orders",
                        StateKind.MAP,
                        outputFields(
                                field("key", StateSourceField.Group.STATE_KEY, 0),
                                field("namespace", StateSourceField.Group.NAMESPACE, 0),
                                field("map_key", StateSourceField.Group.MAP_KEY, 0),
                                field("map_value", StateSourceField.Group.MAP_VALUE, 0)),
                        schema(
                                columns("key", "namespace", "map_key", "map_value"),
                                Arrays.asList("key", "namespace", "map_key")));

        assertTrue(contract.isPresent());
        assertEquals(
                Arrays.asList("key", "namespace", "map_key"), names(contract.requiredFields()));
        assertArrayEquals(new int[] {0, 1, 2}, contract.requiredPhysicalPositions());
    }

    @Test
    void mapStateWithVoidNamespaceRequiresStateKeyAndMapKey() {
        StateSourceLookupKeyContract contract =
                StateSourceLookupKeyContract.derive(
                        "orders",
                        StateKind.MAP,
                        outputFields(
                                field("key", StateSourceField.Group.STATE_KEY, 0),
                                field("map_key", StateSourceField.Group.MAP_KEY, 0),
                                field("map_value", StateSourceField.Group.MAP_VALUE, 0)),
                        schema(
                                columns("key", "map_key", "map_value"),
                                Arrays.asList("key", "map_key")));

        assertTrue(contract.isPresent());
        assertEquals(Arrays.asList("key", "map_key"), names(contract.requiredFields()));
    }

    // ------------------------------------------------------------------------------------------
    //  List state
    // ------------------------------------------------------------------------------------------

    @Test
    void listStateContractIsPresentForStateKeyButRuntimeStillUnsupported() {
        // The contract may be derived for list state (state key + namespace), but the lookup
        // runtime
        // still rejects it. Step 1F asserts that rejection separately.
        StateSourceLookupKeyContract contract =
                StateSourceLookupKeyContract.derive(
                        "orders",
                        StateKind.LIST,
                        outputFields(
                                field("key", StateSourceField.Group.STATE_KEY, 0),
                                field("value", StateSourceField.Group.LIST_ELEMENT, 0)),
                        schema(columns("key", "value"), Collections.singletonList("key")));

        assertTrue(contract.isPresent());
        assertEquals(Collections.singletonList("key"), names(contract.requiredFields()));
    }

    // ------------------------------------------------------------------------------------------
    //  Timer state
    // ------------------------------------------------------------------------------------------

    @Test
    void timerPrimaryKeyFails() {
        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceLookupKeyContract.derive(
                                        "orders",
                                        StateKind.TIMER,
                                        outputFields(
                                                field("key", StateSourceField.Group.STATE_KEY, 0),
                                                field(
                                                        "timestamp",
                                                        StateSourceField.Group.TIMER_TIMESTAMP,
                                                        0)),
                                        schema(
                                                columns("key", "timestamp"),
                                                Collections.singletonList("key"))));
        assertTrue(
                error.getMessage().contains("timer state source does not support lookup"),
                error.getMessage());
    }

    // ------------------------------------------------------------------------------------------
    //  PK validation failures
    // ------------------------------------------------------------------------------------------

    @Test
    void primaryKeyReorderFails() {
        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceLookupKeyContract.derive(
                                        "orders",
                                        StateKind.VALUE,
                                        outputFields(
                                                field(
                                                        "customer_id",
                                                        StateSourceField.Group.STATE_KEY,
                                                        0),
                                                field(
                                                        "tenant",
                                                        StateSourceField.Group.STATE_KEY,
                                                        1),
                                                field("amount", StateSourceField.Group.VALUE, 0)),
                                        schema(
                                                columns("customer_id", "tenant", "amount"),
                                                Arrays.asList("tenant", "customer_id"))));
        assertTrue(error.getMessage().contains("requires PRIMARY KEY"), error.getMessage());
        assertTrue(error.getMessage().contains("`customer_id`, `tenant`"), error.getMessage());
        assertTrue(error.getMessage().contains("`tenant`, `customer_id`"), error.getMessage());
    }

    @Test
    void missingNamespaceFails() {
        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceLookupKeyContract.derive(
                                        "orders",
                                        StateKind.VALUE,
                                        outputFields(
                                                field("key", StateSourceField.Group.STATE_KEY, 0),
                                                field(
                                                        "namespace",
                                                        StateSourceField.Group.NAMESPACE,
                                                        0),
                                                field("value", StateSourceField.Group.VALUE, 0)),
                                        schema(
                                                columns("key", "namespace", "value"),
                                                Collections.singletonList("key"))));
        assertTrue(error.getMessage().contains("requires PRIMARY KEY"), error.getMessage());
        assertTrue(error.getMessage().contains("`key`, `namespace`"), error.getMessage());
    }

    @Test
    void mapPrimaryKeyMissingMapKeyFails() {
        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceLookupKeyContract.derive(
                                        "orders",
                                        StateKind.MAP,
                                        outputFields(
                                                field("key", StateSourceField.Group.STATE_KEY, 0),
                                                field("map_key", StateSourceField.Group.MAP_KEY, 0),
                                                field(
                                                        "map_value",
                                                        StateSourceField.Group.MAP_VALUE,
                                                        0)),
                                        schema(
                                                columns("key", "map_key", "map_value"),
                                                Collections.singletonList("key"))));
        assertTrue(error.getMessage().contains("full map-entry key"), error.getMessage());
        assertTrue(error.getMessage().contains("`key`, `map_key`"), error.getMessage());
    }

    @Test
    void primaryKeyIncludingMapValueFails() {
        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceLookupKeyContract.derive(
                                        "orders",
                                        StateKind.MAP,
                                        outputFields(
                                                field("key", StateSourceField.Group.STATE_KEY, 0),
                                                field("map_key", StateSourceField.Group.MAP_KEY, 0),
                                                field(
                                                        "map_value",
                                                        StateSourceField.Group.MAP_VALUE,
                                                        0)),
                                        schema(
                                                columns("key", "map_key", "map_value"),
                                                Arrays.asList("key", "map_value"))));
        assertTrue(
                error.getMessage().contains("full map-entry key")
                        || error.getMessage().contains("requires PRIMARY KEY"),
                error.getMessage());
    }

    // ------------------------------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------------------------------

    private static StateSourceField field(String name, StateSourceField.Group group, int index) {
        return new StateSourceField(name, "INT", group, index);
    }

    private static List<StateSourceField> outputFields(StateSourceField... fields) {
        return Arrays.asList(fields);
    }

    private static List<Column> columns(String... names) {
        List<Column> columns = new ArrayList<>();
        for (String name : names) {
            columns.add(Column.physical(name, DataTypes.INT()));
        }
        return columns;
    }

    private static ResolvedSchema schema(List<Column> columns, List<String> pkColumns) {
        return new ResolvedSchema(
                columns, Collections.emptyList(), UniqueConstraint.primaryKey("pk", pkColumns));
    }

    private static List<String> names(List<StateSourceField> fields) {
        List<String> names = new ArrayList<>();
        for (StateSourceField field : fields) {
            names.add(field.name());
        }
        return names;
    }

    private static void assertArrayEquals(int[] expected, int[] actual) {
        org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual);
    }
}
