package io.cobble.flink.table;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SerializerInspectSchema;
import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.StateInspectTypeKind;
import io.cobble.flink.common.inspect.StateKind;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Resolves a Cobble state source against the content-addressed inspect-schema registry under a
 * checkpoint root, then validates the user DDL against the resolved semantic schema.
 *
 * <p>This is a planning-time, schema-only resolver: it reads the schema registry (events + blobs)
 * but never opens state data. Registry layout mirrors {@code MonitorInspectSchemaResolver}:
 *
 * <pre>
 *   &lt;checkpoint-root&gt;/cobble/&lt;operatorId&gt;/inspect-schema/events/SCHEMA-&lt;checkpoint&gt;-&lt;hash&gt;.ref
 *   &lt;checkpoint-root&gt;/cobble/&lt;operatorId&gt;/inspect-schema/blobs/&lt;hash&gt;.csch
 * </pre>
 */
final class StateSourceSchemaResolver {

    private static final String COBBLE_DIR = "cobble";
    private static final String INSPECT_SCHEMA = "inspect-schema";
    private static final String EVENTS = "events";
    private static final String BLOBS = "blobs";
    private static final String VOID_NAMESPACE_SERIALIZER_CLASS =
            "org.apache.flink.runtime.state.VoidNamespaceSerializer";

    private StateSourceSchemaResolver() {}

    /**
     * Resolves and validates a state source.
     *
     * @param checkpointRootUri normalized checkpoint-root path URI
     * @param stateOptions parsed {@code state.*} options
     * @param scanCheckpointId normalized {@code scan.checkpoint-id} ({@code "latest"} or a positive
     *     numeric string)
     * @param ddlSchema the resolved table schema to validate against
     * @throws ValidationException on any resolution or validation failure, with operator/state
     *     context.
     */
    static StateSourceResolvedSchema resolve(
            String checkpointRootUri,
            StateSourceOptions stateOptions,
            String scanCheckpointId,
            ResolvedSchema ddlSchema) {
        Path root = new Path(checkpointRootUri);
        Path cobbleDir = new Path(root, COBBLE_DIR);
        FileSystem fs = fileSystem(cobbleDir, checkpointRootUri);

        List<String> operators = discoverOperators(fs, cobbleDir, checkpointRootUri);
        if (operators.isEmpty()) {
            throw new ValidationException(
                    "No Cobble state inspect schema registry was found under "
                            + checkpointRootUri
                            + "/"
                            + COBBLE_DIR
                            + ".");
        }
        String operatorId = selectOperator(operators, stateOptions.operatorId(), checkpointRootUri);

        Path inspectSchemaDir = new Path(new Path(cobbleDir, operatorId), INSPECT_SCHEMA);
        Path eventsDir = new Path(inspectSchemaDir, EVENTS);
        Path blobsDir = new Path(inspectSchemaDir, BLOBS);

        InspectSchemaRegistryLayout.SchemaEvent event =
                selectEvent(fs, eventsDir, scanCheckpointId, operatorId, checkpointRootUri);
        StateInspectSchemaStore store = readStore(fs, blobsDir, event, operatorId);

        StateInspectSchema stateSchema = store.byStateName().get(stateOptions.stateName());
        if (stateSchema == null) {
            throw new ValidationException(
                    "State '"
                            + stateOptions.stateName()
                            + "' was not found. Available states: "
                            + availableStateNames(store)
                            + ".");
        }

        StateKind stateKind = stateSchema.stateKind();
        if (stateOptions.stateKindHint() != null && stateOptions.stateKindHint() != stateKind) {
            throw new ValidationException(
                    "state.kind='"
                            + stateOptions.stateKindHint().wireName()
                            + "' was requested, but state '"
                            + stateOptions.stateName()
                            + "' is "
                            + stateKind.name()
                            + ".");
        }

        StateInspectSemanticSchema semanticSchema = store.semanticSchema(stateOptions.stateName());
        if (semanticSchema == null || semanticSchema.isEmpty()) {
            throw new ValidationException(
                    "State '"
                            + stateOptions.stateName()
                            + "' has inspect metadata but no semantic schema. Cobble state source"
                            + " requires semantic schema metadata.");
        }

        List<StateSourceField> outputFields =
                deriveOutputFields(stateOptions.stateName(), stateSchema, semanticSchema);
        validateDdl(stateOptions.stateName(), outputFields, ddlSchema);

        return new StateSourceResolvedSchema(
                operatorId,
                stateOptions.stateName(),
                stateKind,
                event.checkpointId(),
                outputFields);
    }

    // ------------------------------------------------------------------------------------------
    //  Operator discovery and selection
    // ------------------------------------------------------------------------------------------

    private static List<String> discoverOperators(
            FileSystem fs, Path cobbleDir, String checkpointRootUri) {
        FileStatus[] children = listStatus(fs, cobbleDir, checkpointRootUri);
        if (children == null) {
            return new ArrayList<>();
        }
        // TreeSet for a stable, deterministic order in error messages and single-operator
        // selection.
        TreeSet<String> operators = new TreeSet<>();
        for (FileStatus child : children) {
            if (!child.isDir()) {
                continue;
            }
            Path inspectSchema = new Path(child.getPath(), INSPECT_SCHEMA);
            if (exists(fs, new Path(inspectSchema, EVENTS), checkpointRootUri)
                    || exists(fs, new Path(inspectSchema, BLOBS), checkpointRootUri)) {
                operators.add(child.getPath().getName());
            }
        }
        return new ArrayList<>(operators);
    }

    private static String selectOperator(
            List<String> operators, String requestedOperatorId, String checkpointRootUri) {
        if (requestedOperatorId != null) {
            if (!operators.contains(requestedOperatorId)) {
                throw new ValidationException(
                        "Cobble operator '"
                                + requestedOperatorId
                                + "' was not found under "
                                + checkpointRootUri
                                + "/"
                                + COBBLE_DIR
                                + ". Available operators: "
                                + String.join(", ", operators)
                                + ".");
            }
            return requestedOperatorId;
        }
        if (operators.size() > 1) {
            throw new ValidationException(
                    "Multiple Cobble operators were found under "
                            + checkpointRootUri
                            + "/"
                            + COBBLE_DIR
                            + ". Set '"
                            + CobbleSourceTableOptions.STATE_OPERATOR_ID.key()
                            + "'. Available operators: "
                            + String.join(", ", operators)
                            + ".");
        }
        return operators.get(0);
    }

    // ------------------------------------------------------------------------------------------
    //  Event selection and blob reading
    // ------------------------------------------------------------------------------------------

    private static InspectSchemaRegistryLayout.SchemaEvent selectEvent(
            FileSystem fs,
            Path eventsDir,
            String scanCheckpointId,
            String operatorId,
            String checkpointRootUri) {
        List<InspectSchemaRegistryLayout.SchemaEvent> events =
                listEvents(fs, eventsDir, checkpointRootUri);
        if (events.isEmpty()) {
            throw new ValidationException(
                    "No Cobble state inspect schema events were found for operator '"
                            + operatorId
                            + "' under "
                            + checkpointRootUri
                            + "/"
                            + COBBLE_DIR
                            + "/"
                            + operatorId
                            + "/"
                            + INSPECT_SCHEMA
                            + "/"
                            + EVENTS
                            + ".");
        }

        InspectSchemaRegistryLayout.SchemaEvent best = null;
        if ("latest".equals(scanCheckpointId)) {
            for (InspectSchemaRegistryLayout.SchemaEvent event : events) {
                if (best == null || event.checkpointId() > best.checkpointId()) {
                    best = event;
                }
            }
            return best;
        }

        long selectedId = Long.parseLong(scanCheckpointId);
        for (InspectSchemaRegistryLayout.SchemaEvent event : events) {
            if (event.checkpointId() <= selectedId
                    && (best == null || event.checkpointId() > best.checkpointId())) {
                best = event;
            }
        }
        if (best == null) {
            throw new ValidationException(
                    "No Cobble state inspect schema event with checkpoint id <= "
                            + selectedId
                            + " was found for operator '"
                            + operatorId
                            + "'. Adjust "
                            + CobbleSourceTableOptions.SCAN_CHECKPOINT_ID.key()
                            + " or use 'latest'.");
        }
        return best;
    }

    private static List<InspectSchemaRegistryLayout.SchemaEvent> listEvents(
            FileSystem fs, Path eventsDir, String checkpointRootUri) {
        FileStatus[] statuses = listStatus(fs, eventsDir, checkpointRootUri);
        List<InspectSchemaRegistryLayout.SchemaEvent> events = new ArrayList<>();
        if (statuses == null) {
            return events;
        }
        for (FileStatus status : statuses) {
            if (status.isDir()) {
                continue;
            }
            // parseEventFileName returns null for malformed names, which are skipped silently.
            InspectSchemaRegistryLayout.SchemaEvent event =
                    InspectSchemaRegistryLayout.parseEventFileName(status.getPath().getName());
            if (event != null) {
                events.add(event);
            }
        }
        return events;
    }

    private static StateInspectSchemaStore readStore(
            FileSystem fs,
            Path blobsDir,
            InspectSchemaRegistryLayout.SchemaEvent event,
            String operatorId) {
        Path blobPath = new Path(blobsDir, InspectSchemaRegistryLayout.blobFileName(event.hash()));
        StateInspectSchemaStore store;
        try {
            if (!fs.exists(blobPath)) {
                throw new ValidationException(
                        "Cobble state inspect schema blob is missing for hash "
                                + event.hash()
                                + " (operator '"
                                + operatorId
                                + "', expected at "
                                + blobPath
                                + ").");
            }
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            try (FSDataInputStream input = fs.open(blobPath)) {
                byte[] chunk = new byte[8 * 1024];
                int read;
                while ((read = input.read(chunk)) >= 0) {
                    buffer.write(chunk, 0, read);
                }
            }
            store = StateInspectSchemaStore.fromBytes(buffer.toByteArray());
        } catch (ValidationException e) {
            throw e;
        } catch (IOException e) {
            throw new ValidationException(
                    "Failed to read Cobble state inspect schema blob "
                            + blobPath
                            + ": "
                            + e.getMessage(),
                    e);
        }
        if (store.isEmpty()) {
            throw new ValidationException(
                    "Cobble state inspect schema blob "
                            + blobPath
                            + " parsed as an empty store; it does not contain any state schema.");
        }
        return store;
    }

    // ------------------------------------------------------------------------------------------
    //  Output-field derivation
    // ------------------------------------------------------------------------------------------

    private static List<StateSourceField> deriveOutputFields(
            String stateName,
            StateInspectSchema stateSchema,
            StateInspectSemanticSchema semanticSchema) {
        OutputFields output = new OutputFields(stateName);
        boolean hasNamespace = !isVoidNamespace(stateSchema.namespaceSerializer());

        switch (stateSchema.stateKind()) {
            case VALUE:
            case REDUCING:
            case AGGREGATING:
                output.addGroup(StateSourceField.Group.STATE_KEY, semanticSchema.stateKey(), "key");
                if (hasNamespace) {
                    output.addGroup(
                            StateSourceField.Group.NAMESPACE,
                            semanticSchema.namespace(),
                            "namespace");
                }
                output.addGroup(StateSourceField.Group.VALUE, semanticSchema.value(), "value");
                break;
            case LIST:
                output.addGroup(StateSourceField.Group.STATE_KEY, semanticSchema.stateKey(), "key");
                if (hasNamespace) {
                    output.addGroup(
                            StateSourceField.Group.NAMESPACE,
                            semanticSchema.namespace(),
                            "namespace");
                }
                output.addGroup(
                        StateSourceField.Group.LIST_ELEMENT, semanticSchema.listElement(), "value");
                break;
            case MAP:
                output.addGroup(StateSourceField.Group.STATE_KEY, semanticSchema.stateKey(), "key");
                if (hasNamespace) {
                    output.addGroup(
                            StateSourceField.Group.NAMESPACE,
                            semanticSchema.namespace(),
                            "namespace");
                }
                output.addGroup(
                        StateSourceField.Group.MAP_KEY, semanticSchema.mapUserKey(), "map_key");
                output.addGroup(
                        StateSourceField.Group.MAP_VALUE,
                        semanticSchema.mapUserValue(),
                        "map_value");
                break;
            case TIMER:
                output.addGroup(StateSourceField.Group.STATE_KEY, semanticSchema.stateKey(), "key");
                if (hasNamespace) {
                    output.addGroup(
                            StateSourceField.Group.NAMESPACE,
                            semanticSchema.namespace(),
                            "namespace");
                }
                output.addField(StateSourceField.Group.TIMER_TIMESTAMP, "timestamp", "BIGINT", 0);
                break;
            default:
                throw new ValidationException(
                        "Cobble state source does not support state kind "
                                + stateSchema.stateKind()
                                + " for state '"
                                + stateName
                                + "'.");
        }
        return output.fields();
    }

    /** Accumulates output fields and fails fast on duplicate column names across parts. */
    private static final class OutputFields {
        private final String stateName;
        private final List<StateSourceField> fields = new ArrayList<>();
        private final Map<String, StateSourceField.Group> seen = new LinkedHashMap<>();

        OutputFields(String stateName) {
            this.stateName = stateName;
        }

        void addGroup(StateSourceField.Group group, StateInspectType type, String scalarName) {
            if (type == null || type.kind() == StateInspectTypeKind.UNKNOWN) {
                throw new ValidationException(
                        "Cobble state '"
                                + stateName
                                + "' "
                                + groupLabel(group)
                                + " semantic type is not available; cannot derive output columns.");
            }
            switch (type.kind()) {
                case SCALAR:
                    addField(group, scalarName, type.logicalType(), 0);
                    break;
                case ROW:
                case TUPLE:
                    addStructuredFields(group, type);
                    break;
                case LIST:
                    throw new ValidationException(
                            "Cobble state '"
                                    + stateName
                                    + "' "
                                    + groupLabel(group)
                                    + " is a nested list, which is not supported as a SQL source"
                                    + " column yet.");
                default:
                    throw new ValidationException(
                            "Cobble state '"
                                    + stateName
                                    + "' "
                                    + groupLabel(group)
                                    + " has an unsupported semantic type "
                                    + type.kind()
                                    + ".");
            }
        }

        private void addStructuredFields(StateSourceField.Group group, StateInspectType type) {
            List<StateInspectField> structuredFields = type.fields();
            for (int index = 0; index < structuredFields.size(); index++) {
                StateInspectField field = structuredFields.get(index);
                if (field.type().kind() != StateInspectTypeKind.SCALAR) {
                    throw new ValidationException(
                            "Cobble state '"
                                    + stateName
                                    + "' "
                                    + groupLabel(group)
                                    + " field '"
                                    + field.name()
                                    + "' has a nested non-scalar type, which is not supported as a"
                                    + " SQL source column yet.");
                }
                addField(group, field.name(), field.type().logicalType(), index);
            }
        }

        void addField(
                StateSourceField.Group group,
                String name,
                String logicalType,
                int groupFieldIndex) {
            StateSourceField.Group existing = seen.get(name);
            if (existing != null) {
                throw new ValidationException(
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
            fields.add(new StateSourceField(name, logicalType, group, groupFieldIndex));
        }

        List<StateSourceField> fields() {
            return fields;
        }
    }

    private static String groupLabel(StateSourceField.Group group) {
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

    // ------------------------------------------------------------------------------------------
    //  DDL validation
    // ------------------------------------------------------------------------------------------

    /**
     * Validates the DDL against the derived output fields. Columns are matched position by
     * position: the DDL column order must equal the state output order (key, [namespace],
     * value/...). This matters because the Step 3 runtime will emit rows in the semantic output
     * order, and Flink interprets them in the DDL physical order — a reordered DDL would silently
     * swap columns.
     */
    private static void validateDdl(
            String stateName, List<StateSourceField> expected, ResolvedSchema ddlSchema) {
        if (ddlSchema.getPrimaryKey().isPresent()) {
            throw new ValidationException(
                    "Cobble state source does not support a PRIMARY KEY. Remove the PRIMARY KEY"
                            + " from the table definition for state '"
                            + stateName
                            + "'.");
        }

        RowType physicalRowType = (RowType) ddlSchema.toPhysicalRowDataType().getLogicalType();
        List<RowType.RowField> actualFields = physicalRowType.getFields();

        int max = Math.max(actualFields.size(), expected.size());
        for (int i = 0; i < max; i++) {
            RowType.RowField actual = i < actualFields.size() ? actualFields.get(i) : null;
            StateSourceField expectedField = i < expected.size() ? expected.get(i) : null;

            if (actual == null) {
                // Expected has a column at this position but the DDL does not.
                throw new ValidationException(
                        "Cobble state source is missing column '"
                                + expectedField.name()
                                + "' ("
                                + expectedField.logicalType()
                                + ") for state '"
                                + stateName
                                + "'. Expected columns: "
                                + describeExpected(expected)
                                + ".");
            }
            if (expectedField == null) {
                // The DDL has a column at this position that is not part of the state.
                throw new ValidationException(
                        "Cobble state source column '"
                                + actual.getName()
                                + "' is not part of state '"
                                + stateName
                                + "'. Expected columns: "
                                + describeExpected(expected)
                                + ".");
            }
            if (!actual.getName().equals(expectedField.name())) {
                throw new ValidationException(
                        "Cobble state source column at position "
                                + i
                                + " is '"
                                + actual.getName()
                                + "' but state '"
                                + stateName
                                + "' expects '"
                                + expectedField.name()
                                + "' at that position. The DDL column order must match the state"
                                + " output order. Expected columns: "
                                + describeExpected(expected)
                                + ".");
            }
            if (!typesMatch(expectedField.logicalType(), actual.getType())) {
                throw new ValidationException(
                        "Cobble state source column '"
                                + actual.getName()
                                + "' has type "
                                + actual.getType().asSerializableString()
                                + " but state '"
                                + stateName
                                + "' expects "
                                + expectedField.logicalType()
                                + ".");
            }
        }
    }

    private static String describeExpected(List<StateSourceField> expected) {
        StringBuilder builder = new StringBuilder("[");
        for (int index = 0; index < expected.size(); index++) {
            if (index > 0) {
                builder.append(", ");
            }
            StateSourceField field = expected.get(index);
            builder.append(field.name()).append(' ').append(field.logicalType());
        }
        return builder.append(']').toString();
    }

    /**
     * Compares an expected type (an inspect logical-type string) with a DDL column type, ignoring
     * only nullability. Both are parsed and re-serialized as nullable so equivalent declarations
     * (for example {@code INT} vs {@code INT NOT NULL}) compare equal.
     */
    private static boolean typesMatch(String expectedLogicalType, LogicalType actualType) {
        try {
            LogicalType expected =
                    LogicalTypeParser.parse(
                            expectedLogicalType, StateSourceSchemaResolver.class.getClassLoader());
            return expected.copy(true)
                    .asSerializableString()
                    .equals(actualType.copy(true).asSerializableString());
        } catch (RuntimeException e) {
            // Fall back to a strict string compare when the inspect type is not parseable.
            return expectedLogicalType.equals(actualType.asSerializableString());
        }
    }

    // ------------------------------------------------------------------------------------------
    //  Filesystem helpers
    // ------------------------------------------------------------------------------------------

    private static String availableStateNames(StateInspectSchemaStore store) {
        TreeSet<String> names = new TreeSet<>(store.byStateName().keySet());
        return names.isEmpty() ? "(none)" : String.join(", ", names);
    }

    private static boolean isVoidNamespace(SerializerInspectSchema serializer) {
        return serializer != null
                && VOID_NAMESPACE_SERIALIZER_CLASS.equals(serializer.serializerClassName());
    }

    private static FileSystem fileSystem(Path path, String checkpointRootUri) {
        try {
            return path.getFileSystem();
        } catch (IOException e) {
            throw new ValidationException(
                    "Failed to open filesystem for Cobble state source path "
                            + checkpointRootUri
                            + ": "
                            + e.getMessage(),
                    e);
        }
    }

    private static boolean exists(FileSystem fs, Path path, String checkpointRootUri) {
        try {
            return fs.exists(path);
        } catch (IOException e) {
            throw new ValidationException(
                    "Failed to access "
                            + path
                            + " under Cobble state source path "
                            + checkpointRootUri
                            + ": "
                            + e.getMessage(),
                    e);
        }
    }

    private static FileStatus[] listStatus(FileSystem fs, Path dir, String checkpointRootUri) {
        try {
            if (!fs.exists(dir)) {
                return null;
            }
            return fs.listStatus(dir);
        } catch (IOException e) {
            throw new ValidationException(
                    "Failed to list "
                            + dir
                            + " under Cobble state source path "
                            + checkpointRootUri
                            + ": "
                            + e.getMessage(),
                    e);
        }
    }
}
