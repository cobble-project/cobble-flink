package io.cobble.flink.inspect;

import java.util.Arrays;

/** Immutable raw bytes, kept distinct from UTF-8 display values. */
public final class RawBytes {
    private final byte[] value;

    public RawBytes(byte[] value) {
        this.value = value == null ? null : Arrays.copyOf(value, value.length);
    }

    public byte[] value() {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }

    @Override
    public boolean equals(Object other) {
        return other == this
                || (other instanceof RawBytes && Arrays.equals(value, ((RawBytes) other).value));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }
}
