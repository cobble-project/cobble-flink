package io.cobble.flink.inspect;

import java.util.Objects;

/** An opaque continuation token returned by a scan. */
public final class PageToken {
    private final String value;

    public PageToken(String value) {
        this.value = Objects.requireNonNull(value, "value");
        if (value.isEmpty()) {
            throw new IllegalArgumentException("value must not be empty");
        }
    }

    public String value() {
        return value;
    }
}
