package io.cobble.flink.common.inspect.decode;

/** Classification of a failure while consuming a persisted classless decoder descriptor. */
public enum ClasslessDecodeIssueKind {
    CLASSLESS_UNSUPPORTED,
    SERIALIZER_RESTORE_FAILED,
    MALFORMED_BYTES,
    UNKNOWN
}
