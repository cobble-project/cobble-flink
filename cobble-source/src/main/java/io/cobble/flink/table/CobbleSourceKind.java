package io.cobble.flink.table;

import org.apache.flink.table.api.ValidationException;

import java.util.Locale;

/**
 * Kind of Cobble path a SQL source can consume.
 *
 * <p>{@link #RAW} selects the schema-less raw source: it reads a standard Cobble table root and
 * emits raw key bytes plus selected value columns as {@code ARRAY<BYTES>}, without depending on
 * sink sidecar schema, state inspect schema, or Flink serializers.
 */
enum CobbleSourceKind {

    /** Detect the concrete kind from the on-disk layout of the configured path. */
    AUTO,

    /** A Cobble SQL sink table root, read through the existing sink source runtime. */
    SINK,

    /** A Flink state checkpoint root or state operator root. */
    STATE,

    /**
     * A standard Cobble table root read as raw bytes, without typed schema resolution. Never chosen
     * by {@code auto}; users must set {@code source.kind='raw'} explicitly.
     */
    RAW;

    /**
     * Parses a user-supplied {@code source.kind} option value.
     *
     * <ul>
     *   <li>{@code null} or blank resolves to {@link #AUTO}.
     *   <li>{@code auto}, {@code sink}, {@code state}, {@code raw} are accepted case-insensitively.
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
                return RAW;
            default:
                throw new ValidationException(
                        "Invalid "
                                + CobbleSourceTableOptions.SOURCE_KIND.key()
                                + " '"
                                + trimmed
                                + "'. Valid values are: auto, sink, state, raw.");
        }
    }
}
