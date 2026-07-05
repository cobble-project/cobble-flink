package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/**
 * Pure byte-level tests for {@link CobbleCanonicalValueEncoder}, focusing on the LIST trailing
 * delimiter strip contract.
 */
class CobbleCanonicalValueEncoderTest {

    @Test
    void stripsTrailingDelimiter() {
        // "a,b,c," → "a,b,c"
        byte[] input = "a,b,c,".getBytes();
        byte[] expected = "a,b,c".getBytes();
        assertArrayEquals(expected, CobbleCanonicalValueEncoder.toCanonicalListValue(input));
    }

    @Test
    void emptyListProducesEmptyBytes() {
        // A Cobble list with one empty element would be just "," — stripping gives "".
        byte[] input = ",".getBytes();
        byte[] expected = "".getBytes();
        assertArrayEquals(expected, CobbleCanonicalValueEncoder.toCanonicalListValue(input));
    }

    @Test
    void singleElement() {
        // "x," → "x"
        byte[] input = "x,".getBytes();
        byte[] expected = "x".getBytes();
        assertArrayEquals(expected, CobbleCanonicalValueEncoder.toCanonicalListValue(input));
    }

    @Test
    void noTrailingDelimiterIsPassthrough() {
        // Defensive: if the value has no trailing delimiter, return as-is (no crash).
        byte[] input = "a,b".getBytes();
        assertArrayEquals(input, CobbleCanonicalValueEncoder.toCanonicalListValue(input));
    }

    @Test
    void nullInputReturnsNull() {
        assertNull(CobbleCanonicalValueEncoder.toCanonicalListValue(null));
    }

    @Test
    void emptyInputReturnsEmpty() {
        byte[] input = new byte[0];
        assertEquals(0, CobbleCanonicalValueEncoder.toCanonicalListValue(input).length);
    }
}
