package io.cobble.flink.monitor;

final class LookupItem {
    final int bucket;
    final byte[] key;

    LookupItem(int bucket, byte[] key) {
        this.bucket = bucket;
        this.key = key;
    }
}
