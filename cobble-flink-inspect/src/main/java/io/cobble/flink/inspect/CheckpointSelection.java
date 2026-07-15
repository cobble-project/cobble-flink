package io.cobble.flink.inspect;

/** Selects either the latest available checkpoint or one concrete checkpoint id. */
public final class CheckpointSelection {
    private final boolean latest;
    private final long checkpointId;

    private CheckpointSelection(boolean latest, long checkpointId) {
        this.latest = latest;
        this.checkpointId = checkpointId;
    }

    public static CheckpointSelection latest() {
        return new CheckpointSelection(true, -1L);
    }

    public static CheckpointSelection checkpoint(long checkpointId) {
        if (checkpointId < 0) {
            throw new IllegalArgumentException("checkpointId must not be negative");
        }
        return new CheckpointSelection(false, checkpointId);
    }

    public boolean isLatest() {
        return latest;
    }

    public long checkpointId() {
        return checkpointId;
    }
}
