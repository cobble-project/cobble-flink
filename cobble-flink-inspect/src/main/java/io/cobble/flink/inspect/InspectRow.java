package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A single raw inspection row with optional decoded values and decode issues. */
public final class InspectRow {
    private final int bucket;
    private final RawBytes key;
    private final List<RawBytes> columns;
    private final boolean found;
    private final RawBytes value;
    private final DecodedValue decodedKey;
    private final DecodedValue decodedValue;
    private final List<DecodedValue> decodedColumns;
    private final Map<String, DecodedValue> decodedParts;
    private final List<DecodeIssue> decodeIssues;

    public InspectRow(
            int bucket, RawBytes key, List<RawBytes> columns, List<DecodeIssue> decodeIssues) {
        this(
                bucket,
                key,
                columns,
                true,
                first(columns),
                null,
                null,
                Collections.<DecodedValue>emptyList(),
                Collections.<String, DecodedValue>emptyMap(),
                decodeIssues);
    }

    public InspectRow(
            int bucket,
            RawBytes key,
            List<RawBytes> columns,
            boolean found,
            RawBytes value,
            DecodedValue decodedKey,
            DecodedValue decodedValue,
            List<DecodedValue> decodedColumns,
            Map<String, DecodedValue> decodedParts,
            List<DecodeIssue> decodeIssues) {
        this.bucket = bucket;
        this.key = key;
        this.columns = immutableList(columns);
        this.found = found;
        this.value = value;
        this.decodedKey = decodedKey;
        this.decodedValue = decodedValue;
        this.decodedColumns = immutableList(decodedColumns);
        this.decodedParts =
                Collections.unmodifiableMap(
                        new LinkedHashMap<String, DecodedValue>(
                                decodedParts == null
                                        ? Collections.<String, DecodedValue>emptyMap()
                                        : decodedParts));
        this.decodeIssues = immutableList(decodeIssues);
    }

    public int bucket() {
        return bucket;
    }

    public RawBytes key() {
        return key;
    }

    public List<RawBytes> columns() {
        return columns;
    }

    public boolean found() {
        return found;
    }

    public RawBytes value() {
        return value;
    }

    public DecodedValue decodedKey() {
        return decodedKey;
    }

    public DecodedValue decodedValue() {
        return decodedValue;
    }

    public List<DecodedValue> decodedColumns() {
        return decodedColumns;
    }

    public Map<String, DecodedValue> decodedParts() {
        return decodedParts;
    }

    public List<DecodeIssue> decodeIssues() {
        return decodeIssues;
    }

    private static RawBytes first(List<RawBytes> values) {
        return values == null || values.isEmpty() ? null : values.get(0);
    }

    private static <T> List<T> immutableList(List<T> values) {
        return Collections.unmodifiableList(
                new ArrayList<T>(values == null ? Collections.<T>emptyList() : values));
    }
}
