package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link CobbleSourceKind#fromUserOption(String)} parsing rules. */
class CobbleSourceKindTest {

    @Test
    void nullResolvesToAuto() {
        assertEquals(CobbleSourceKind.AUTO, CobbleSourceKind.fromUserOption(null));
    }

    @Test
    void blankResolvesToAuto() {
        assertEquals(CobbleSourceKind.AUTO, CobbleSourceKind.fromUserOption("   "));
    }

    @Test
    void acceptsKnownValuesCaseInsensitively() {
        assertEquals(CobbleSourceKind.AUTO, CobbleSourceKind.fromUserOption("auto"));
        assertEquals(CobbleSourceKind.AUTO, CobbleSourceKind.fromUserOption("AUTO"));
        assertEquals(CobbleSourceKind.SINK, CobbleSourceKind.fromUserOption("Sink"));
        assertEquals(CobbleSourceKind.SINK, CobbleSourceKind.fromUserOption(" sink "));
        assertEquals(CobbleSourceKind.STATE, CobbleSourceKind.fromUserOption("STATE"));
    }

    @Test
    void rawIsRejectedAsReserved() {
        ValidationException error =
                assertThrows(
                        ValidationException.class, () -> CobbleSourceKind.fromUserOption("raw"));
        assertTrue(
                error.getMessage().contains("reserved"),
                "expected reserved message but got: " + error.getMessage());
    }

    @Test
    void unknownValueIsRejectedWithValidValues() {
        ValidationException error =
                assertThrows(
                        ValidationException.class, () -> CobbleSourceKind.fromUserOption("bogus"));
        assertTrue(
                error.getMessage().contains("auto, sink, state"),
                "expected valid values list but got: " + error.getMessage());
    }
}
