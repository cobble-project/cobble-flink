package io.cobble.flink.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable checkpoint metadata discovered by an inspection session. */
public final class CheckpointInfo {
    private final long checkpointId;
    private final String directory;
    private final List<OperatorInfo> operators;

    public CheckpointInfo(long checkpointId, String directory, List<OperatorInfo> operators) {
        this.checkpointId = checkpointId;
        this.directory = Objects.requireNonNull(directory, "directory");
        this.operators =
                Collections.unmodifiableList(
                        new ArrayList<>(Objects.requireNonNull(operators, "operators")));
    }

    public long checkpointId() {
        return checkpointId;
    }

    public String directory() {
        return directory;
    }

    public List<OperatorInfo> operators() {
        return operators;
    }
}
