package io.cobble.flink.inspect.internal;

import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.StateInspectExactLookupSupport;
import io.cobble.flink.common.inspect.StateKind;
import io.cobble.flink.common.inspect.StateSourceSchemaLayout;
import io.cobble.flink.inspect.InspectOverview;
import io.cobble.flink.inspect.InspectOverviewItem;
import io.cobble.flink.inspect.SourceSqlExample;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Generates source-compatible Overview and SQL from one pinned inspect session. */
public final class InspectOverviewGenerator {
    private InspectOverviewGenerator() {}

    public static InspectOverview generate(
            String sourcePath, long checkpointId, String operatorId, List<InspectTarget> targets) {
        List<InspectOverviewItem> items = new ArrayList<>();
        for (InspectTarget target : targets) {
            try {
                if (target.sinkSchema != null) {
                    items.add(sink(sourcePath, checkpointId, target));
                } else if (target.schema != null) {
                    items.add(state(sourcePath, checkpointId, operatorId, target));
                } else {
                    items.add(raw(target));
                }
            } catch (RuntimeException error) {
                items.add(unavailableItem(target, error.getMessage()));
            }
        }
        return new InspectOverview(checkpointId, operatorId, items);
    }

    private static InspectOverviewItem sink(
            String sourcePath, long checkpointId, InspectTarget target) {
        List<SinkInspectField> physical = new ArrayList<>();
        physical.addAll(target.sinkSchema.keyFields());
        physical.addAll(target.sinkSchema.valueFields());
        physical.sort(Comparator.comparingInt(SinkInspectField::rowIndex));
        validateSinkLayout(
                target.sinkSchema.keyFields(), target.sinkSchema.valueFields(), physical);

        List<InspectOverviewItem.Field> fields = new ArrayList<>();
        List<String> required = new ArrayList<>();
        for (SinkInspectField key : target.sinkSchema.keyFields()) {
            required.add(key.name());
        }
        for (SinkInspectField field : physical) {
            fields.add(
                    new InspectOverviewItem.Field(
                            target.sinkSchema.keyFields().contains(field)
                                    ? "Primary key"
                                    : "Column",
                            field.name(),
                            field.logicalType()));
        }
        String ddl =
                ddl(
                        tableName(sourcePath, "cobble_source"),
                        physicalFields(physical),
                        required,
                        sinkOptions(sourcePath, checkpointId));
        SourceSqlExample sql =
                new SourceSqlExample(
                        ddl,
                        true,
                        true,
                        required,
                        null,
                        "Use this source for batch scans and exact temporal lookup joins.");
        return new InspectOverviewItem(
                target.id, target.name, "Sink", "Cobble SQL sink snapshot", fields, sql);
    }

    private static InspectOverviewItem state(
            String sourcePath, long checkpointId, String operatorId, InspectTarget target) {
        StateKind kind = target.schema.stateKind();
        if (kind == StateKind.TIMER) {
            SourceSqlExample sql =
                    unavailable(
                            "Timer state is visible in inspect, but the Cobble SQL state source does not read timer queues.");
            return new InspectOverviewItem(
                    target.id,
                    target.name,
                    "Timer",
                    "Cobble-backed Flink timer queue",
                    Collections.<InspectOverviewItem.Field>emptyList(),
                    sql);
        }

        final List<StateSourceSchemaLayout.Field> layout;
        try {
            layout = StateSourceSchemaLayout.derive(target.schema, target.semanticSchema);
        } catch (IllegalArgumentException error) {
            return new InspectOverviewItem(
                    target.id,
                    target.name,
                    kind.name(),
                    kind.name() + " keyed state",
                    Collections.<InspectOverviewItem.Field>emptyList(),
                    unavailable(error.getMessage()));
        }
        List<InspectOverviewItem.Field> fields = new ArrayList<>();
        List<String> required = new ArrayList<>();
        for (StateSourceSchemaLayout.Field field : layout) {
            fields.add(
                    new InspectOverviewItem.Field(
                            role(field.group()), field.name(), field.logicalType()));
            if (requiredGroup(field.group(), kind)) {
                required.add(field.name());
            }
        }
        boolean lookupKind =
                kind == StateKind.VALUE
                        || kind == StateKind.REDUCING
                        || kind == StateKind.AGGREGATING
                        || kind == StateKind.MAP;
        StateInspectExactLookupSupport.Result lookupSupport =
                StateInspectExactLookupSupport.evaluate(target.schema, target.semanticSchema);
        boolean exact = lookupKind && lookupSupport.supported();
        String ddl =
                ddl(
                        tableName(null, target.name),
                        statePhysicalFields(layout),
                        exact ? required : Collections.<String>emptyList(),
                        stateOptions(sourcePath, checkpointId, operatorId, target));
        String note;
        if (kind == StateKind.LIST) {
            note =
                    "Use this state source for batch scans. ListState exact lookup is not supported.";
        } else if (!exact) {
            note =
                    "Use this state source for batch scans. Exact lookup is unavailable: "
                            + (lookupSupport.reason() == null
                                    ? "the state key cannot be reconstructed exactly"
                                    : lookupSupport.reason())
                            + ".";
        } else {
            note = "Use this state source for batch scans and exact temporal lookup joins.";
        }
        return new InspectOverviewItem(
                target.id,
                target.name,
                kind.name(),
                kind.name() + " keyed state",
                fields,
                new SourceSqlExample(
                        ddl,
                        true,
                        exact,
                        exact ? required : Collections.<String>emptyList(),
                        null,
                        note));
    }

    private static InspectOverviewItem raw(InspectTarget target) {
        if ("timer".equals(target.kind)) {
            String reason =
                    "Timer state is visible in inspect, but the Cobble SQL state source does not read timer queues.";
            return new InspectOverviewItem(
                    target.id,
                    target.name,
                    "Timer",
                    "Cobble-backed Flink timer queue",
                    Collections.<InspectOverviewItem.Field>emptyList(),
                    unavailable(reason));
        }
        String reason =
                "Raw inspect metadata does not expose a safe physical column layout; no source DDL was generated.";
        return new InspectOverviewItem(
                target.id,
                target.name,
                "Raw",
                "Raw Cobble snapshot",
                Collections.<InspectOverviewItem.Field>emptyList(),
                unavailable(reason));
    }

    private static InspectOverviewItem unavailableItem(InspectTarget target, String reason) {
        return new InspectOverviewItem(
                target.id,
                target.name,
                displayKind(target),
                null,
                Collections.<InspectOverviewItem.Field>emptyList(),
                unavailable(reason == null ? "Source schema is invalid." : reason));
    }

    private static SourceSqlExample unavailable(String reason) {
        return new SourceSqlExample(
                null, false, false, Collections.<String>emptyList(), reason, reason);
    }

    private static void validateSinkLayout(
            List<SinkInspectField> keyFields,
            List<SinkInspectField> valueFields,
            List<SinkInspectField> fields) {
        if (keyFields.isEmpty()) {
            throw new InspectInputException("Sink inspect schema has no primary-key fields");
        }
        Set<String> names = new HashSet<>();
        for (int index = 0; index < fields.size(); index++) {
            if (fields.get(index).rowIndex() != index) {
                throw new InspectInputException(
                        "Sink inspect schema has no unique field for rowIndex " + index);
            }
            if (!names.add(fields.get(index).name())) {
                throw new InspectInputException(
                        "Sink inspect schema contains duplicate field '"
                                + fields.get(index).name()
                                + "'");
            }
        }
        Set<Integer> structuredColumns = new HashSet<>();
        for (SinkInspectField field : valueFields) {
            int column = field.structuredColumnIndex();
            if (column < 0 || column >= valueFields.size() || !structuredColumns.add(column)) {
                throw new InspectInputException(
                        "Sink inspect schema has an invalid structured column index for field '"
                                + field.name()
                                + "'");
            }
        }
    }

    private static String displayKind(InspectTarget target) {
        if ("timer".equals(target.kind)) {
            return "Timer";
        }
        if ("sink".equals(target.kind)) {
            return "Sink";
        }
        if ("state".equals(target.kind)) {
            return target.stateKind == null ? "State" : target.stateKind;
        }
        return "Raw";
    }

    private static List<FieldSql> physicalFields(List<SinkInspectField> fields) {
        List<FieldSql> output = new ArrayList<>();
        for (SinkInspectField field : fields) {
            output.add(new FieldSql(field.name(), field.logicalType()));
        }
        return output;
    }

    private static List<FieldSql> statePhysicalFields(List<StateSourceSchemaLayout.Field> fields) {
        List<FieldSql> output = new ArrayList<>();
        for (StateSourceSchemaLayout.Field field : fields) {
            output.add(new FieldSql(field.name(), field.logicalType()));
        }
        return output;
    }

    private static List<String> sinkOptions(String path, long checkpointId) {
        List<String> options = new ArrayList<>();
        options.add("'connector' = 'cobble'");
        options.add("'path' = '" + escapeString(path) + "'");
        options.add("'scan.checkpoint-id' = '" + checkpointId + "'");
        options.add("'scan.mode' = 'batch'");
        return options;
    }

    private static List<String> stateOptions(
            String path, long checkpointId, String operatorId, InspectTarget target) {
        List<String> options = new ArrayList<>();
        options.add("'connector' = 'cobble'");
        options.add("'source.kind' = 'state'");
        options.add("'path' = '" + escapeString(path) + "'");
        if (operatorId != null && !operatorId.isEmpty()) {
            options.add("'state.operator-id' = '" + escapeString(operatorId) + "'");
        }
        options.add("'state.name' = '" + escapeString(target.name) + "'");
        options.add("'state.kind' = '" + target.schema.stateKind().wireName() + "'");
        options.add("'scan.checkpoint-id' = '" + checkpointId + "'");
        options.add("'scan.mode' = 'batch'");
        return options;
    }

    private static String ddl(
            String tableName,
            List<FieldSql> fields,
            List<String> primaryKey,
            List<String> options) {
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("Source DDL requires at least one physical field");
        }
        StringBuilder sql =
                new StringBuilder("CREATE TABLE ").append(quote(tableName)).append(" (\n");
        for (int index = 0; index < fields.size(); index++) {
            FieldSql field = fields.get(index);
            sql.append("  ").append(quote(field.name)).append(' ').append(field.logicalType);
            if (index < fields.size() - 1 || !primaryKey.isEmpty()) {
                sql.append(',');
            }
            sql.append('\n');
        }
        if (!primaryKey.isEmpty()) {
            sql.append("  PRIMARY KEY (");
            for (int index = 0; index < primaryKey.size(); index++) {
                if (index > 0) {
                    sql.append(", ");
                }
                sql.append(quote(primaryKey.get(index)));
            }
            sql.append(") NOT ENFORCED\n");
        }
        sql.append(") WITH (\n");
        for (int index = 0; index < options.size(); index++) {
            sql.append("  ").append(options.get(index));
            if (index < options.size() - 1) {
                sql.append(',');
            }
            sql.append('\n');
        }
        return sql.append(");").toString();
    }

    private static boolean requiredGroup(StateSourceSchemaLayout.Group group, StateKind kind) {
        return group == StateSourceSchemaLayout.Group.STATE_KEY
                || group == StateSourceSchemaLayout.Group.NAMESPACE
                || (kind == StateKind.MAP && group == StateSourceSchemaLayout.Group.MAP_KEY);
    }

    private static String role(StateSourceSchemaLayout.Group group) {
        if (group == StateSourceSchemaLayout.Group.STATE_KEY) {
            return "State key";
        }
        if (group == StateSourceSchemaLayout.Group.NAMESPACE) {
            return "Namespace";
        }
        if (group == StateSourceSchemaLayout.Group.MAP_KEY) {
            return "Map key";
        }
        if (group == StateSourceSchemaLayout.Group.MAP_VALUE) {
            return "Map value";
        }
        if (group == StateSourceSchemaLayout.Group.LIST_ELEMENT) {
            return "List element";
        }
        return "Value";
    }

    static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    static String escapeString(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

    private static String tableName(String path, String fallback) {
        String source = path == null ? "" : path;
        int slash = Math.max(source.lastIndexOf('/'), source.lastIndexOf('\\'));
        String candidate = slash >= 0 ? source.substring(slash + 1) : source;
        int marker = candidate.indexOf('?');
        if (marker >= 0) {
            candidate = candidate.substring(0, marker);
        }
        if (candidate.isEmpty()) {
            candidate = fallback;
        }
        String normalized = candidate.replaceAll("[^A-Za-z0-9_]", "_");
        normalized = normalized.replaceAll("^_+|_+$", "");
        if (normalized.isEmpty()) {
            normalized = "field";
        }
        if (Character.isDigit(normalized.charAt(0))) {
            normalized = "_" + normalized;
        }
        return normalized;
    }

    private static final class FieldSql {
        private final String name;
        private final String logicalType;

        private FieldSql(String name, String logicalType) {
            this.name = name;
            this.logicalType = logicalType;
        }
    }
}
