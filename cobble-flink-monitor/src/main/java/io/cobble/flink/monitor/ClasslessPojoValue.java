package io.cobble.flink.monitor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Neutral marker type for a classlessly-decoded POJO value. Prevents a POJO from being mistaken for
 * a user {@code MapState} value during semantic rendering.
 *
 * <p>Contains an immutable ordered map of decoded field name to value, and a null flag (set when
 * the POJO wire byte was {@code IS_NULL}).
 */
final class ClasslessPojoValue {

    private final Map<String, Object> fields;
    private final boolean isNull;

    private ClasslessPojoValue(Map<String, Object> fields, boolean isNull) {
        this.fields = fields;
        this.isNull = isNull;
    }

    static ClasslessPojoValue of(Map<String, Object> fields) {
        return new ClasslessPojoValue(
                Collections.unmodifiableMap(new LinkedHashMap<>(fields)), false);
    }

    static ClasslessPojoValue nullValue() {
        return new ClasslessPojoValue(Collections.emptyMap(), true);
    }

    /** Immutable ordered map of field name to decoded value. Empty when {@link #isNull()}. */
    Map<String, Object> fields() {
        return fields;
    }

    boolean isNull() {
        return isNull;
    }
}
