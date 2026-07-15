package io.cobble.flink.inspect;

/** Physical Cobble data represented by an inspection target. */
public enum InspectTargetKind {
    STATE,
    TIMER,
    SINK,
    RAW
}
