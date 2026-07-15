package io.cobble.flink.inspect;

/** A checkpoint/operator pair selected inside one inspection session. */
public final class InspectSelection {
    private final String sourcePath;
    private final long checkpointId;
    private final String operatorId;
    private final boolean latest;

    public InspectSelection(long checkpointId, String operatorId, boolean latest) {
        this(null, checkpointId, operatorId, latest);
    }

    public InspectSelection(
            String sourcePath, long checkpointId, String operatorId, boolean latest) {
        this.sourcePath = sourcePath;
        this.checkpointId = checkpointId;
        this.operatorId = operatorId;
        this.latest = latest;
    }

    public String sourcePath() {
        return sourcePath;
    }

    public long checkpointId() {
        return checkpointId;
    }

    public String operatorId() {
        return operatorId;
    }

    public boolean latest() {
        return latest;
    }

    public static InspectSelection latest(String sourcePath, String operatorId) {
        return new InspectSelection(sourcePath, -1L, operatorId, true);
    }

    public static InspectSelection checkpoint(
            String sourcePath, long checkpointId, String operatorId) {
        return new InspectSelection(sourcePath, checkpointId, operatorId, false);
    }

    public static Builder builder(String sourcePath) {
        return new Builder(sourcePath);
    }

    public static final class Builder {
        private final String sourcePath;
        private CheckpointSelection checkpoint = CheckpointSelection.latest();
        private String operatorId;

        private Builder(String sourcePath) {
            this.sourcePath = sourcePath;
        }

        public Builder checkpoint(CheckpointSelection checkpoint) {
            this.checkpoint = checkpoint == null ? CheckpointSelection.latest() : checkpoint;
            return this;
        }

        public Builder operatorId(String operatorId) {
            this.operatorId = operatorId;
            return this;
        }

        public InspectSelection build() {
            return checkpoint.isLatest()
                    ? InspectSelection.latest(sourcePath, operatorId)
                    : InspectSelection.checkpoint(
                            sourcePath, checkpoint.checkpointId(), operatorId);
        }
    }
}
