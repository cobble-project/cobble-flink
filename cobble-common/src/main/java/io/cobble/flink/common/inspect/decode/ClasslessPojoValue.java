package io.cobble.flink.common.inspect.decode;

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
public final class ClasslessPojoValue {

    private final Map<String, Object> fields;
    private final boolean isNull;

    private ClasslessPojoValue(Map<String, Object> fields, boolean isNull) {
        this.fields = fields;
        this.isNull = isNull;
    }

    public static ClasslessPojoValue of(Map<String, Object> fields) {
        return new ClasslessPojoValue(
                Collections.unmodifiableMap(new LinkedHashMap<>(fields)), false);
    }

    public static ClasslessPojoValue nullValue() {
        return new ClasslessPojoValue(Collections.emptyMap(), true);
    }

    /** Immutable ordered map of field name to decoded value. Empty when {@link #isNull()}. */
    public Map<String, Object> fields() {
        return fields;
    }

    public boolean isNull() {
        return isNull;
    }
}
