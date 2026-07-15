package io.cobble.flink.inspect;

/** A raw lookup key and its target bucket. */
public final class LookupKey {
    private final int bucket;
    private final RawBytes key;

    public LookupKey(int bucket, RawBytes key) {
        this.bucket = bucket;
        this.key = key;
    }

    public int bucket() {
        return bucket;
    }

    public RawBytes key() {
        return key;
    }
}
