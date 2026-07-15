package io.cobble.flink.inspect;

import java.util.Arrays;

/** Immutable raw or schema-aware scan request. */
public final class ScanRequest {
    private final String targetId;
    private final int limit;
    private final PageToken pageToken;
    private final Integer bucket;
    private final RawBytes prefix;
    private final int[] columns;
    private final ScanFilter filter;

    public ScanRequest(String targetId, int limit, PageToken pageToken) {
        this(targetId, limit, pageToken, null, null, null);
    }

    public ScanRequest(
            String targetId,
            int limit,
            PageToken pageToken,
            Integer bucket,
            RawBytes prefix,
            int[] columns) {
        this(targetId, limit, pageToken, bucket, prefix, columns, null);
    }

    public ScanRequest(
            String targetId,
            int limit,
            PageToken pageToken,
            Integer bucket,
            RawBytes prefix,
            int[] columns,
            ScanFilter filter) {
        this.targetId = targetId;
        this.limit = limit;
        this.pageToken = pageToken;
        this.bucket = bucket;
        this.prefix = prefix;
        this.columns = columns == null ? null : Arrays.copyOf(columns, columns.length);
        this.filter = filter;
    }

    public String targetId() {
        return targetId;
    }

    public int limit() {
        return limit;
    }

    public PageToken pageToken() {
        return pageToken;
    }

    public Integer bucket() {
        return bucket;
    }

    public RawBytes prefix() {
        return prefix;
    }

    public int[] columns() {
        return columns == null ? null : Arrays.copyOf(columns, columns.length);
    }

    public ScanFilter filter() {
        return filter;
    }
}
