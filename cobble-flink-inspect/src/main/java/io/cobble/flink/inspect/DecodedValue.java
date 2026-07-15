package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Recursive decoded value preserving both logical rendering and raw fallback bytes. */
public final class DecodedValue {
    public enum Kind {
        SCALAR,
        ROW,
        LIST,
        MAP,
        RAW
    }

    private final Kind kind;
    private final String logicalType;
    private final Object scalar;
    private final RawBytes raw;
    private final List<DecodedField> fields;
    private final List<DecodedValue> elements;
    private final List<MapEntry> entries;

    public DecodedValue(
            Kind kind,
            String logicalType,
            Object scalar,
            RawBytes raw,
            List<DecodedField> fields,
            List<DecodedValue> elements,
            List<MapEntry> entries) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.logicalType = logicalType;
        this.scalar = scalar;
        this.raw = raw;
        this.fields = immutable(fields);
        this.elements = immutable(elements);
        this.entries = immutable(entries);
    }

    public Kind kind() {
        return kind;
    }

    public String logicalType() {
        return logicalType;
    }

    public Object scalar() {
        return scalar;
    }

    public RawBytes raw() {
        return raw;
    }

    public List<DecodedField> fields() {
        return fields;
    }

    public List<DecodedValue> elements() {
        return elements;
    }

    public List<MapEntry> entries() {
        return entries;
    }

    private static <T> List<T> immutable(List<T> values) {
        return Collections.unmodifiableList(
                new ArrayList<>(values == null ? Collections.<T>emptyList() : values));
    }

    public static final class DecodedField {
        private final String name;
        private final DecodedValue value;

        public DecodedField(String name, DecodedValue value) {
            this.name = Objects.requireNonNull(name, "name");
            this.value = value;
        }

        public String name() {
            return name;
        }

        public DecodedValue value() {
            return value;
        }
    }

    public static final class MapEntry {
        private final DecodedValue key;
        private final DecodedValue value;

        public MapEntry(DecodedValue key, DecodedValue value) {
            this.key = key;
            this.value = value;
        }

        public DecodedValue key() {
            return key;
        }

        public DecodedValue value() {
            return value;
        }
    }
}
