package io.cobble.flink.state;

/**
 * Converts Cobble state values into Flink canonical savepoint value bytes.
 *
 * <p>Most state kinds (VALUE, REDUCING, AGGREGATING, MAP) store values in a format that is already
 * byte-identical to the canonical savepoint value format. The one exception is LIST state: Cobble
 * stores list elements with a trailing {@code ','} delimiter after every element (including the
 * last), while the canonical format omits the trailing delimiter. This class centralizes that
 * transformation so it can be tested as an explicit contract rather than an implementation detail
 * of {@link DirectDelimitedListSerializer}.
 */
final class CobbleCanonicalValueEncoder {

    /** The trailing delimiter byte that Cobble appends after every list element. */
    static final byte LIST_DELIMITER = DirectDelimitedListSerializer.DELIMITER;

    private CobbleCanonicalValueEncoder() {}

    /**
     * Strips the trailing list delimiter from a Cobble list value for canonical export.
     *
     * <p>Cobble stores {@code [elem0, elem1, ..., elemN,]} — each element followed by a delimiter,
     * including the last. The canonical format is {@code [elem0, elem1, ..., elemN]} (no trailing
     * delimiter). This method removes exactly one trailing delimiter if present.
     *
     * @param cobbleListValue the raw Cobble list value bytes
     * @return the canonical list value bytes (without the trailing delimiter)
     */
    static byte[] toCanonicalListValue(byte[] cobbleListValue) {
        if (cobbleListValue == null || cobbleListValue.length == 0) {
            return cobbleListValue;
        }
        if (cobbleListValue[cobbleListValue.length - 1] == LIST_DELIMITER) {
            byte[] result = new byte[cobbleListValue.length - 1];
            System.arraycopy(cobbleListValue, 0, result, 0, result.length);
            return result;
        }
        // Defensive: if there is no trailing delimiter, return as-is (should not happen with
        // well-formed Cobble list values, but should not crash the savepoint).
        return cobbleListValue;
    }
}
