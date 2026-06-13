package io.cobble.flink.monitor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class CheckpointEntry {
    final long id;
    final String directory;
    final List<OperatorEntry> operators;

    CheckpointEntry(long id, String directory, List<OperatorEntry> operators) {
        this.id = id;
        this.directory = directory;
        this.operators = operators;
    }

    OperatorEntry defaultOperator() {
        return operators.get(0);
    }

    OperatorEntry findOperator(String operatorId) {
        for (OperatorEntry operator : operators) {
            if (operator.operatorId.equals(operatorId)) {
                return operator;
            }
        }
        throw new InputException("unknown operator_id " + operatorId + " for checkpoint " + id);
    }

    OperatorEntry findOperatorOrDefault(String operatorId) {
        if (operatorId != null) {
            for (OperatorEntry operator : operators) {
                if (operator.operatorId.equals(operatorId)) {
                    return operator;
                }
            }
        }
        return defaultOperator();
    }

    Map<String, Object> toJson() {
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
