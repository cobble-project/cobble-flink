package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.StateKind;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link StateSourceOptions} parsing and sink rejection rules. */
class StateSourceOptionsTest {

    @Test
    void stateNameIsRequiredForState() {
        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () -> StateSourceOptions.parseForState(new Configuration()));
        assertTrue(
                error.getMessage().contains("'state.name' is required"),
                "expected required message but got: " + error.getMessage());
    }

    @Test
    void parsesAllStateOptions() {
        Configuration config = new Configuration();
        config.set(CobbleSourceTableOptions.STATE_NAME, "orders");
        config.set(CobbleSourceTableOptions.STATE_OPERATOR_ID, "op-a");
        config.set(CobbleSourceTableOptions.STATE_KIND, "map");

        StateSourceOptions options = StateSourceOptions.parseForState(config);

        assertEquals("orders", options.stateName());
        assertEquals("op-a", options.operatorId());
        assertEquals(StateKind.MAP, options.stateKindHint());
    }

    @Test
    void operatorIdAndKindDefaultToNull() {
        Configuration config = new Configuration();
        config.set(CobbleSourceTableOptions.STATE_NAME, "orders");

        StateSourceOptions options = StateSourceOptions.parseForState(config);

        assertNull(options.operatorId());
        assertNull(options.stateKindHint());
    }

    @Test
    void stateKindIsCaseInsensitive() {
        Configuration config = new Configuration();
        config.set(CobbleSourceTableOptions.STATE_NAME, "orders");
        config.set(CobbleSourceTableOptions.STATE_KIND, "AGGREGATING");

        assertEquals(
                StateKind.AGGREGATING, StateSourceOptions.parseForState(config).stateKindHint());
    }

    @Test
    void invalidStateKindIsRejectedWithValidValues() {
        Configuration config = new Configuration();
        config.set(CobbleSourceTableOptions.STATE_NAME, "orders");
        config.set(CobbleSourceTableOptions.STATE_KIND, "bogus");

        ValidationException error =
                assertThrows(
                        ValidationException.class, () -> StateSourceOptions.parseForState(config));
        assertTrue(
                error.getMessage().contains("value, list, map, timer, reducing, aggregating"),
                "expected valid kinds list but got: " + error.getMessage());
    }

    @Test
    void rejectsStateNameOnSink() {
        Configuration config = new Configuration();
        config.set(CobbleSourceTableOptions.STATE_NAME, "orders");

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () -> StateSourceOptions.rejectStateOptionsForSink(config));
        assertTrue(
                error.getMessage().contains("'state.name' is only valid when source.kind='state'"),
                "expected sink-rejection message but got: " + error.getMessage());
    }

    @Test
    void rejectsStateOperatorIdOnSink() {
        Configuration config = new Configuration();
        config.set(CobbleSourceTableOptions.STATE_OPERATOR_ID, "op-a");

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () -> StateSourceOptions.rejectStateOptionsForSink(config));
        assertTrue(
                error.getMessage().contains("'state.operator-id' is only valid"),
                "expected sink-rejection message but got: " + error.getMessage());
    }

    @Test
    void acceptsSinkWithoutStateOptions() {
        StateSourceOptions.rejectStateOptionsForSink(new Configuration());
    }
}
