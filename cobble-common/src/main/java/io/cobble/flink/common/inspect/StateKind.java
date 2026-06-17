package io.cobble.flink.common.inspect;

import java.util.Locale;

/**
 * Kind of Flink state recorded in an {@link StateInspectSchema}.
 *
 * <p>The integer ordinal is persisted in the inspect schema store, so existing ordinals must remain
 * stable and new kinds must be appended.
 */
public enum StateKind {
    VALUE,
    LIST,
    MAP;

    private static final StateKind[] VALUES = values();

    static StateKind fromOrdinal(int ordinal) {
        if (ordinal < 0 || ordinal >= VALUES.length) {
            throw new IllegalArgumentException("Unknown state kind ordinal: " + ordinal);
        }
        return VALUES[ordinal];
    }

    public String wireName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
