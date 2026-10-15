package io.cobble.flink.table;

import io.cobble.table.TableKeyBuilder;
import io.cobble.table.TableReader;
import io.cobble.table.Value;

import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Safe full-key routing only: Flink must still evaluate every predicate, including collisions. */
final class SourceKeyFilter implements Serializable {
    private static final long serialVersionUID = 1L;
    private final List<Value> key;

    private SourceKeyFilter(List<Value> key) {
        this.key = Collections.unmodifiableList(new ArrayList<>(key));
    }

    static Selection select(
            List<ResolvedExpression> filters,
            RowType fullType,
            int[] projection,
            int[] primaryPositions) {
        Map<Integer, Value> values = new LinkedHashMap<>();
        List<ResolvedExpression> candidates = new ArrayList<>();
        for (ResolvedExpression filter : filters) {
            List<ResolvedExpression> matched = new ArrayList<>();
            collect(filter, fullType, projection, primaryPositions, values, matched);
            if (!matched.isEmpty()) candidates.add(filter);
        }
        List<Value> key = new ArrayList<>();
        for (int position : primaryPositions) {
            Value value = values.get(position);
            if (value == null) {
                return new Selection(null, Collections.emptyList(), filters);
            }
            key.add(value);
        }
        return new Selection(key.isEmpty() ? null : new SourceKeyFilter(key), candidates, filters);
    }

    private static void collect(
            ResolvedExpression expression,
            RowType fullType,
            int[] projection,
            int[] primaryPositions,
            Map<Integer, Value> values,
            List<ResolvedExpression> candidates) {
        if (!(expression instanceof CallExpression)) return;
        CallExpression call = (CallExpression) expression;
        List<ResolvedExpression> args = call.getResolvedChildren();
        if (call.getFunctionDefinition().equals(BuiltInFunctionDefinitions.AND)) {
            for (ResolvedExpression arg : args)
                collect(arg, fullType, projection, primaryPositions, values, candidates);
            return;
        }
        if (!call.getFunctionDefinition().equals(BuiltInFunctionDefinitions.EQUALS)
                || args.size() != 2) return;
        ResolvedExpression left = args.get(0);
        ResolvedExpression right = args.get(1);
        if (left instanceof ValueLiteralExpression) {
            left = args.get(1);
            right = args.get(0);
        }
        if (!(left instanceof FieldReferenceExpression)
                || !(right instanceof ValueLiteralExpression)) return;
        FieldReferenceExpression field = (FieldReferenceExpression) left;
        if (field.getInputIndex() != 0
                || field.getFieldIndex() < 0
                || field.getFieldIndex() >= projection.length) return;
        int original = projection[field.getFieldIndex()];
        boolean primary = false;
        for (int position : primaryPositions) primary |= position == original;
        if (!primary) return;
        LogicalType type = fullType.getTypeAt(original);
        if (!type.copy(true).equals(field.getOutputDataType().getLogicalType().copy(true))) return;
        Value value = literal((ValueLiteralExpression) right, type);
        if (value != null) {
            // Either equality is a necessary condition, even if other equalities contradict it.
            values.putIfAbsent(original, value);
            candidates.add(expression);
        }
    }

    private static Value literal(ValueLiteralExpression literal, LogicalType type) {
        if (literal.isNull()) return null;
        // Restrict routing to exact integral/boolean representations. Floating point, CHAR,
        // decimal and temporal comparison coercions must not produce a wrong physical bucket.
        if (literal.getOutputDataType().getLogicalType().getTypeRoot() != type.getTypeRoot())
            return null;
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return literal.getValueAs(Boolean.class).map(Value::bool).orElse(null);
            case TINYINT:
                return literal.getValueAs(Byte.class).map(Value::int8).orElse(null);
            case SMALLINT:
                return literal.getValueAs(Short.class).map(Value::int16).orElse(null);
            case INTEGER:
                return literal.getValueAs(Integer.class).map(Value::int32).orElse(null);
            case BIGINT:
                return literal.getValueAs(Long.class).map(Value::int64).orElse(null);
            default:
                return null;
        }
    }

    int bucket(TableReader reader) {
        TableKeyBuilder builder = reader.keyBuilder();
        for (Value value : key) builder.push(value);
        return builder.build().bucket();
    }

    static final class Selection {
        final SourceKeyFilter filter;
        final SupportsFilterPushDown.Result result;

        private Selection(
                SourceKeyFilter filter,
                List<ResolvedExpression> accepted,
                List<ResolvedExpression> remaining) {
            this.filter = filter;
            this.result = SupportsFilterPushDown.Result.of(accepted, remaining);
        }
    }
}
