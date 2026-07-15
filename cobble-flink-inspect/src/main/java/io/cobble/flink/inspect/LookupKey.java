package io.cobble.flink.inspect;

/** One raw or schema-aware exact lookup key. */
public final class LookupKey {
    private final int bucket;
    private final RawBytes key;
    private final TypedLookupKey typedKey;

    public LookupKey(int bucket, RawBytes key) {
        this.bucket = bucket;
        this.key = key;
        this.typedKey = null;
    }

    private LookupKey(TypedLookupKey typedKey) {
        this.bucket = -1;
        this.key = null;
        this.typedKey = typedKey;
    }

    public static LookupKey typed(TypedLookupKey typedKey) {
        if (typedKey == null) {
            throw new IllegalArgumentException("typedKey must not be null");
        }
        return new LookupKey(typedKey);
    }

    public int bucket() {
        return bucket;
    }

    public RawBytes key() {
        return key;
    }

    public boolean typed() {
        return typedKey != null;
    }

    public TypedLookupKey typedKey() {
        return typedKey;
    }
}
