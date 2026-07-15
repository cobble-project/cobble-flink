package io.cobble.flink.inspect.internal;

public final class LookupItem {
    public final int bucket;
    public final byte[] key;

    public LookupItem(int bucket, byte[] key) {
        this.bucket = bucket;
        this.key = key;
    }
}
