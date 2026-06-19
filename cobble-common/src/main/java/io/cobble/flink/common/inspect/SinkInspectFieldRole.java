package io.cobble.flink.common.inspect;

import java.io.IOException;

/** Role of a sink inspect field in the physical Cobble row layout. */
public enum SinkInspectFieldRole {
    KEY,
    VALUE;

    static SinkInspectFieldRole fromOrdinal(int ordinal) throws IOException {
        SinkInspectFieldRole[] values = values();
        if (ordinal < 0 || ordinal >= values.length) {
            throw new IOException("Unknown sink inspect field role ordinal: " + ordinal);
        }
        return values[ordinal];
    }
}
