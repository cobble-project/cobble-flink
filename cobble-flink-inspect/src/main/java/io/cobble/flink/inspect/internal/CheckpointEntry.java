package io.cobble.flink.inspect.internal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CheckpointEntry {
    public final long id;
    public final String directory;
    public final List<OperatorEntry> operators;

    public CheckpointEntry(long id, String directory, List<OperatorEntry> operators) {
        this.id = id;
        this.directory = directory;
        this.operators = operators;
    }

    public OperatorEntry defaultOperator() {
        return operators.get(0);
    }

    public OperatorEntry findOperator(String operatorId) {
        for (OperatorEntry operator : operators) {
            if (operator.operatorId.equals(operatorId)) {
                return operator;
            }
        }
        throw new InspectInputException(
                "unknown operator_id " + operatorId + " for checkpoint " + id);
    }

    public OperatorEntry findOperatorOrDefault(String operatorId) {
        if (operatorId != null) {
            for (OperatorEntry operator : operators) {
                if (operator.operatorId.equals(operatorId)) {
                    return operator;
                }
            }
        }
        return defaultOperator();
    }

    public Map<String, Object> toJson() {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("id", id);
        output.put("directory", directory);
        List<Map<String, Object>> operatorItems = new ArrayList<>(operators.size());
        for (OperatorEntry operator : operators) {
            operatorItems.add(operator.toJson());
        }
        output.put("operators", operatorItems);
        return output;
    }
}
