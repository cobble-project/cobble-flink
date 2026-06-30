package io.cobble.flink.table;

import org.apache.flink.table.api.ValidationException;

import java.util.Locale;

/**
 * Kind of Cobble path a SQL source can consume.
 *
 * <p>{@link #RAW_RESERVED} is an internal placeholder for a future "arbitrary raw Cobble source"
 * feature. It is never produced from user configuration and the {@code raw} option value is
 * rejected at planning time.
 */
enum CobbleSourceKind {

    /** Detect the concrete kind from the on-disk layout of the configured path. */
    AUTO,

    /** A Cobble SQL sink table root, read through the existing sink source runtime. */
    SINK,

    /** A Flink state checkpoint root or state operator root. */
    STATE,

    /** Reserved for future raw Cobble source support; not selectable by users. */
    RAW_RESERVED;

    /**
     * Parses a user-supplied {@code source.kind} option value.
     *
     * <ul>
     *   <li>{@code null} or blank resolves to {@link #AUTO}.
     *   <li>{@code auto}, {@code sink}, {@code state} are accepted case-insensitively.
     *   <li>{@code raw} is rejected as reserved.
     *   <li>any other value is rejected with the list of valid values.
     * </ul>
     */
    static CobbleSourceKind fromUserOption(String value) {
        if (value == null) {
            return AUTO;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return AUTO;
        }
        switch (trimmed.toLowerCase(Locale.ROOT)) {
            case "auto":
                return AUTO;
            case "sink":
                return SINK;
            case "state":
                return STATE;
            case "raw":
                throw new ValidationException(
                        "source.kind='raw' is reserved for future raw Cobble source support and is"
                                + " not available yet.");
            default:
                throw new ValidationException(
                        "Invalid "
                                + CobbleSourceTableOptions.SOURCE_KIND.key()
                                + " '"
                                + trimmed
                                + "'. Valid values are: auto, sink, state.");
        }
    }
}
