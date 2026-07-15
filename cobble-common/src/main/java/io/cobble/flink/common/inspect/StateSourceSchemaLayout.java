package io.cobble.flink.common.inspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Pure source-column layout derived from persisted state inspect metadata. */
public final class StateSourceSchemaLayout {
    private static final String VOID_NAMESPACE_SERIALIZER =
            "org.apache.flink.runtime.state.VoidNamespaceSerializer";

    private StateSourceSchemaLayout() {}

    public static List<Field> derive(
            StateInspectSchema stateSchema, StateInspectSemanticSchema semanticSchema) {
        Objects.requireNonNull(stateSchema, "stateSchema");
        if (semanticSchema == null || semanticSchema.isEmpty()) {
            throw new IllegalArgumentException(
                    "State '"
                            + stateSchema.stateName()
                            + "' has no semantic schema; cannot derive source columns.");
        }
        OutputFields output = new OutputFields(stateSchema.stateName());
        boolean namespaced = !isVoidNamespace(stateSchema.namespaceSerializer());
        switch (stateSchema.stateKind()) {
            case VALUE:
            case REDUCING:
            case AGGREGATING:
                output.addGroup(Group.STATE_KEY, semanticSchema.stateKey(), "key");
                output.addNamespace(namespaced, semanticSchema.namespace());
                output.addGroup(Group.VALUE, semanticSchema.value(), "value");
                break;
            case LIST:
                output.addGroup(Group.STATE_KEY, semanticSchema.stateKey(), "key");
                output.addNamespace(namespaced, semanticSchema.namespace());
                output.addGroup(Group.LIST_ELEMENT, semanticSchema.listElement(), "value");
                break;
            case MAP:
                output.addGroup(Group.STATE_KEY, semanticSchema.stateKey(), "key");
                output.addNamespace(namespaced, semanticSchema.namespace());
                output.addGroup(Group.MAP_KEY, semanticSchema.mapUserKey(), "map_key");
                output.addGroup(Group.MAP_VALUE, semanticSchema.mapUserValue(), "map_value");
                break;
            case TIMER:
                output.addGroup(Group.STATE_KEY, semanticSchema.stateKey(), "key");
                output.addNamespace(namespaced, semanticSchema.namespace());
                output.addField(Group.TIMER_TIMESTAMP, "timestamp", "BIGINT", 0);
                break;
            default:
                throw new IllegalArgumentException(
                        "Cobble state source does not support state kind "
                                + stateSchema.stateKind()
                                + " for state '"
                                + stateSchema.stateName()
                                + "'.");
        }
        return output.fields();
    }

    public enum Group {
        STATE_KEY,
        NAMESPACE,
        VALUE,
        LIST_ELEMENT,
        MAP_KEY,
        MAP_VALUE,
        TIMER_TIMESTAMP
    }

    public static final class Field {
        private final String name;
        private final String logicalType;
        private final Group group;
        private final int groupFieldIndex;

        private Field(String name, String logicalType, Group group, int groupFieldIndex) {
            this.name = name;
            this.logicalType = logicalType;
            this.group = group;
            this.groupFieldIndex = groupFieldIndex;
        }

        public String name() {
            return name;
        }

        public String logicalType() {
            return logicalType;
        }

        public Group group() {
            return group;
        }

        public int groupFieldIndex() {
            return groupFieldIndex;
        }
    }

    private static final class OutputFields {
        private final String stateName;
        private final List<Field> fields = new ArrayList<>();
        private final Map<String, Group> seen = new LinkedHashMap<>();

        private OutputFields(String stateName) {
            this.stateName = stateName;
        }

        private void addNamespace(boolean namespaced, StateInspectType type) {
            if (namespaced) {
                addGroup(Group.NAMESPACE, type, "namespace");
            }
        }

        private void addGroup(Group group, StateInspectType type, String scalarName) {
            if (type == null || type.kind() == StateInspectTypeKind.UNKNOWN) {
                throw new IllegalArgumentException(
                        "Cobble state '"
                                + stateName
                                + "' "
                                + groupLabel(group)
                                + " semantic type is not available; cannot derive output columns.");
            }
            switch (type.kind()) {
                case SCALAR:
                    addField(group, scalarName, type.logicalType(), 0);
                    return;
                case ROW:
                case TUPLE:
                    addStructuredFields(group, type);
                    return;
                case LIST:
                    throw new IllegalArgumentException(
                            "Cobble state '"
                                    + stateName
                                    + "' "
                                    + groupLabel(group)
                                    + " is a nested list, which is not supported as a SQL source column yet.");
                default:
                    throw new IllegalArgumentException(
                            "Cobble state '"
                                    + stateName
                                    + "' "
                                    + groupLabel(group)
                                    + " has an unsupported semantic type "
                                    + type.kind()
                                    + ".");
            }
        }

        private void addStructuredFields(Group group, StateInspectType type) {
            List<StateInspectField> structured = type.fields();
            for (int index = 0; index < structured.size(); index++) {
                StateInspectField field = structured.get(index);
                if (field.type().kind() != StateInspectTypeKind.SCALAR) {
                    throw new IllegalArgumentException(
                            "Cobble state '"
                                    + stateName
                                    + "' "
                                    + groupLabel(group)
                                    + " field '"
                                    + field.name()
                                    + "' has a nested non-scalar type, which is not supported as a SQL source column yet.");
                }
                addField(group, field.name(), field.type().logicalType(), index);
            }
        }

        private void addField(Group group, String name, String logicalType, int groupFieldIndex) {
            Group existing = seen.get(name);
            if (existing != null) {
                throw new IllegalArgumentException(
                        "Cobble state '"
                                + stateName
                                + "' produces duplicate output column name '"
                                + name
                                + "' across "
                                + groupLabel(existing)
                                + " and "
                                + groupLabel(group)
                                + "; this state shape is not supported as a SQL source yet.");
            }
            seen.put(name, group);
            fields.add(new Field(name, logicalType, group, groupFieldIndex));
        }

        private List<Field> fields() {
            return Collections.unmodifiableList(new ArrayList<>(fields));
        }
    }

    private static boolean isVoidNamespace(SerializerInspectSchema serializer) {
        return serializer != null
                && VOID_NAMESPACE_SERIALIZER.equals(serializer.serializerClassName());
    }

    private static String groupLabel(Group group) {
        switch (group) {
            case STATE_KEY:
                return "state key";
            case NAMESPACE:
                return "namespace";
            case VALUE:
                return "value";
            case LIST_ELEMENT:
                return "list element";
            case MAP_KEY:
                return "map key";
            case MAP_VALUE:
                return "map value";
            case TIMER_TIMESTAMP:
                return "timer timestamp";
            default:
                return group.name();
        }
    }
}
