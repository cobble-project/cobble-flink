package io.cobble.flink.monitor;

import io.cobble.GlobalSnapshot;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds {@link InspectTarget} objects from a resolved schema store.
 *
 * <p>When a schema store is available, one target is produced per schema entry using the schema's
 * state name as the target id and name. Timer targets are preserved from the existing
 * global-snapshot column-family list. When no schema is available, falls back to the raw
 * column-family-based targets.
 */
final class StateInspectTargetBuilder {

    private static final String DEFAULT_COLUMN_FAMILY = "default";
    static final String TIMER_QUEUE_COLUMN_FAMILY_PREFIX = "__cobble_timer__";

    private StateInspectTargetBuilder() {}

    static List<InspectTarget> build(GlobalSnapshot snapshot, SchemaResolveResult schemaResult) {
        return build(snapshot, schemaResult, null);
    }

    static List<InspectTarget> build(
            GlobalSnapshot snapshot,
            SchemaResolveResult schemaResult,
            SinkSchemaResolveResult sinkSchemaResult) {
        if (sinkSchemaResult != null && sinkSchemaResult.hasSchema()) {
            return Collections.singletonList(
                    InspectTarget.sink("sink", sinkSchemaResult.store.schema()));
        }
        if (schemaResult != null
                && schemaResult.hasSchema()
                && !schemaResult.store.isEmpty()
                && snapshot != null
                && snapshot.columnFamilyIds != null
                && !snapshot.columnFamilyIds.isEmpty()) {

            Map<String, Integer> columnFamilyIds = snapshot.columnFamilyIds;
            List<InspectTarget> targets = new ArrayList<>();

            // State targets from schema entries that match a column family.
            for (StateInspectSchema schema : schemaResult.store.schemas()) {
                if (columnFamilyIds.containsKey(schema.columnFamily())) {
                    targets.add(
                            schemaTarget(
                                    schema, schemaResult.store.semanticSchema(schema.stateName())));
                }
            }

            // Timer targets from remaining column families.
            for (Map.Entry<String, Integer> entry : sortedColumnFamilyEntries(columnFamilyIds)) {
                String columnFamily = entry.getKey();
                if (DEFAULT_COLUMN_FAMILY.equals(columnFamily)) {
                    continue;
                }
                if (columnFamily.startsWith(TIMER_QUEUE_COLUMN_FAMILY_PREFIX)) {
                    if (!alreadyAddedAsState(targets, columnFamily)) {
                        String stateName =
                                columnFamily.startsWith(TIMER_QUEUE_COLUMN_FAMILY_PREFIX)
                                        ? columnFamily.substring(
                                                TIMER_QUEUE_COLUMN_FAMILY_PREFIX.length())
                                        : columnFamily;
                        if (stateName.isEmpty()) {
                            stateName = columnFamily;
                        }
                        targets.add(InspectTarget.timer(stateName, columnFamily));
                    }
                } else if (!alreadyAddedAsState(targets, columnFamily)) {
                    // Non-timer column family not covered by schema → raw state target.
                    targets.add(InspectTarget.state(columnFamily, columnFamily));
                }
            }

            if (!targets.isEmpty()) {
                return targets;
            }
        }

        return rawTargets(snapshot);
    }

    private static InspectTarget schemaTarget(
            StateInspectSchema schema, StateInspectSemanticSchema semanticSchema) {
        Map<String, String> serializerClasses = new LinkedHashMap<>();
        serializerClasses.put("key", schema.keySerializer().serializerClassName());
        serializerClasses.put("namespace", schema.namespaceSerializer().serializerClassName());
        if (schema.valueSerializer() != null) {
            serializerClasses.put("value", schema.valueSerializer().serializerClassName());
        }
        if (schema.listElementSerializer() != null) {
            serializerClasses.put("element", schema.listElementSerializer().serializerClassName());
        }
        if (schema.mapUserKeySerializer() != null) {
            serializerClasses.put("map_key", schema.mapUserKeySerializer().serializerClassName());
        }
        if (schema.mapUserValueSerializer() != null) {
            serializerClasses.put(
                    "map_value", schema.mapUserValueSerializer().serializerClassName());
        }
        if ("TIMER".equals(schema.stateKind().name())) {
            return new InspectTarget(
                    "timer:" + schema.stateName(),
                    schema.stateName(),
                    "timer",
                    schema.columnFamily(),
                    false,
                    schema.stateKind().name(),
                    serializerClasses,
                    schema,
                    semanticSchema,
                    null);
        }
        return new InspectTarget(
                schema.stateName(),
                schema.stateName(),
                "state",
                schema.columnFamily(),
                false,
                schema.stateKind().name(),
                serializerClasses,
                schema,
                semanticSchema,
                null);
    }

    private static boolean alreadyAddedAsState(List<InspectTarget> targets, String columnFamily) {
        for (InspectTarget target : targets) {
            if (columnFamily.equals(target.columnFamily)) {
                return true;
            }
        }
        return false;
    }

    private static List<Map.Entry<String, Integer>> sortedColumnFamilyEntries(
            Map<String, Integer> columnFamilyIds) {
        List<Map.Entry<String, Integer>> entries = new ArrayList<>(columnFamilyIds.entrySet());
        entries.sort(Comparator.comparing(Map.Entry::getValue));
        return entries;
    }

    private static List<InspectTarget> rawTargets(GlobalSnapshot snapshot) {
        if (snapshot == null
                || snapshot.columnFamilyIds == null
                || snapshot.columnFamilyIds.isEmpty()) {
            return Collections.emptyList();
        }
        List<Map.Entry<String, Integer>> entries =
                new ArrayList<>(snapshot.columnFamilyIds.entrySet());
        entries.sort(Comparator.comparing(Map.Entry::getValue));

        List<InspectTarget> states = new ArrayList<>();
        boolean hasUserState = false;
        for (Map.Entry<String, Integer> entry : entries) {
            String columnFamily = entry.getKey();
            if (DEFAULT_COLUMN_FAMILY.equals(columnFamily)) {
                continue;
            }
            if (columnFamily.startsWith(TIMER_QUEUE_COLUMN_FAMILY_PREFIX)) {
                String stateName =
                        columnFamily.substring(TIMER_QUEUE_COLUMN_FAMILY_PREFIX.length());
                if (stateName.isEmpty()) {
                    stateName = columnFamily;
                }
                states.add(InspectTarget.timer(stateName, columnFamily));
            } else {
                hasUserState = true;
                states.add(InspectTarget.state(columnFamily, columnFamily));
            }
        }
        if (!states.isEmpty() || hasUserState) {
            return states;
        }
        return Collections.singletonList(InspectTarget.sink("sink"));
    }
}
