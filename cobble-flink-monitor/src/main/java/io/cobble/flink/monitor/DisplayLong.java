package io.cobble.flink.monitor;

/** A 64-bit integer that must cross the JSON boundary as text to preserve its exact value. */
final class DisplayLong {

    private static final long MAX_SAFE_JAVASCRIPT_INTEGER = 9_007_199_254_740_991L;

    private final long value;

    private DisplayLong(long value) {
        this.value = value;
    }

    static Object forJson(long value) {
        return value < -MAX_SAFE_JAVASCRIPT_INTEGER || value > MAX_SAFE_JAVASCRIPT_INTEGER
                ? new DisplayLong(value)
                : Long.valueOf(value);
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }
}
