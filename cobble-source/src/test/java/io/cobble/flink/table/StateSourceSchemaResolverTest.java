package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.StateKind;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Tests for {@link StateSourceSchemaResolver} registry resolution and DDL validation. */
class StateSourceSchemaResolverTest {

    @TempDir private Path tempDir;

    // ------------------------------------------------------------------------------------------
    //  Output shapes per state kind
    // ------------------------------------------------------------------------------------------

    @Test
    void resolvesScalarValueStateWithVoidNamespace() throws Exception {
        Path root = tempDir.resolve("scalar-value");
        writeRegistry(
                root,
                "op-a",
                100L,
                store(
                        valueSchema("orders", VoidNamespaceSerializer.INSTANCE),
                        "orders",
                        StateInspectSemanticSchema.forValue(
                                scalar("INT"), unknown(), scalar("INT"))));

        StateSourceResolvedSchema resolved =
                StateSourceSchemaResolver.resolve(
                        uri(root),
                        opts("orders", null, null),
                        "latest",
                        schema(
                                physical("key", DataTypes.INT()),
                                physical("value", DataTypes.INT())));

        assertEquals("op-a", resolved.operatorId());
        assertEquals(StateKind.VALUE, resolved.stateKind());
        assertEquals(100L, resolved.schemaCheckpointId());
        assertEquals(
                Arrays.asList("key:INT:STATE_KEY", "value:INT:VALUE"),
                describe(resolved.outputFields()));
    }

    @Test
    void resolvesRowDataValueState() throws Exception {
        Path root = tempDir.resolve("row-value");
        StateInspectType keyRow =
                row(field("id", scalar("BIGINT")), field("region", scalar("VARCHAR(2147483647)")));
        StateInspectType valueRow = row(field("amount", scalar("INT")));
        writeRegistry(
                root,
                "op-a",
                100L,
                store(
                        valueSchema("orders", VoidNamespaceSerializer.INSTANCE),
                        "orders",
                        StateInspectSemanticSchema.forValue(keyRow, unknown(), valueRow)));

        StateSourceResolvedSchema resolved =
                StateSourceSchemaResolver.resolve(
                        uri(root),
                        opts("orders", null, null),
                        "latest",
                        schema(
                                physical("id", DataTypes.BIGINT()),
                                physical("region", DataTypes.STRING()),
                                physical("amount", DataTypes.INT())));

        assertEquals(
                Arrays.asList(
                        "id:BIGINT:STATE_KEY",
                        "region:VARCHAR(2147483647):STATE_KEY",
                        "amount:INT:VALUE"),
                describe(resolved.outputFields()));
    }

    @Test
    void nonVoidNamespaceAddsNamespaceColumn() throws Exception {
        Path root = tempDir.resolve("ns-value");
        // A non-void namespace serializer (StringSerializer) means the namespace column is
        // required.
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "orders",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        writeRegistry(
                root,
                "op-a",
                100L,
                store(
                        schema,
                        "orders",
                        StateInspectSemanticSchema.forValue(
                                scalar("INT"), scalar("VARCHAR(2147483647)"), scalar("INT"))));

        StateSourceResolvedSchema resolved =
                StateSourceSchemaResolver.resolve(
                        uri(root),
                        opts("orders", null, null),
                        "latest",
                        schema(
                                physical("key", DataTypes.INT()),
                                physical("namespace", DataTypes.STRING()),
                                physical("value", DataTypes.INT())));

        assertEquals(
                Arrays.asList(
                        "key:INT:STATE_KEY",
                        "namespace:VARCHAR(2147483647):NAMESPACE",
                        "value:INT:VALUE"),
                describe(resolved.outputFields()));
    }

    @Test
    void resolvesMapState() throws Exception {
        Path root = tempDir.resolve("map");
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "counters",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        writeRegistry(
                root,
                "op-a",
                100L,
                store(
                        schema,
                        "counters",
                        StateInspectSemanticSchema.forMap(
                                scalar("INT"),
                                unknown(),
                                scalar("INT"),
                                scalar("VARCHAR(2147483647)"))));

        StateSourceResolvedSchema resolved =
                StateSourceSchemaResolver.resolve(
                        uri(root),
                        opts("counters", null, null),
                        "latest",
                        schema(
                                physical("key", DataTypes.INT()),
                                physical("map_key", DataTypes.INT()),
                                physical("map_value", DataTypes.STRING())));

        assertEquals(StateKind.MAP, resolved.stateKind());
        assertEquals(
                Arrays.asList(
                        "key:INT:STATE_KEY",
                        "map_key:INT:MAP_KEY",
                        "map_value:VARCHAR(2147483647):MAP_VALUE"),
                describe(resolved.outputFields()));
    }

    @Test
    void resolvesListState() throws Exception {
        Path root = tempDir.resolve("list");
        StateInspectSchema schema =
                StateInspectSchema.forList(
                        "events",
                        "cf",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        writeRegistry(
                root,
                "op-a",
                100L,
                store(
                        schema,
                        "events",
                        StateInspectSemanticSchema.forList(
                                scalar("INT"), unknown(), scalar("INT"))));

        StateSourceResolvedSchema resolved =
                StateSourceSchemaResolver.resolve(
                        uri(root),
                        opts("events", null, null),
                        "latest",
                        schema(
                                physical("key", DataTypes.INT()),
                                physical("value", DataTypes.INT())));

        assertEquals(StateKind.LIST, resolved.stateKind());
        assertEquals(
                Arrays.asList("key:INT:STATE_KEY", "value:INT:LIST_ELEMENT"),
                describe(resolved.outputFields()));
    }

    @Test
    void resolvesTimerState() throws Exception {
        Path root = tempDir.resolve("timer");
        StateInspectSchema schema =
                StateInspectSchema.forTimer(
                        "timers", "cf", IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE);
        writeRegistry(
                root,
                "op-a",
                100L,
                store(
                        schema,
                        "timers",
                        StateInspectSemanticSchema.forValue(scalar("INT"), unknown(), unknown())));

        StateSourceResolvedSchema resolved =
                StateSourceSchemaResolver.resolve(
                        uri(root),
                        opts("timers", null, null),
                        "latest",
                        schema(
                                physical("key", DataTypes.INT()),
                                physical("timestamp", DataTypes.BIGINT())));

        assertEquals(StateKind.TIMER, resolved.stateKind());
        assertEquals(
                Arrays.asList("key:INT:STATE_KEY", "timestamp:BIGINT:TIMER_TIMESTAMP"),
                describe(resolved.outputFields()));
    }

    // ------------------------------------------------------------------------------------------
    //  Operator selection
    // ------------------------------------------------------------------------------------------

    @Test
    void multipleOperatorsRequireOperatorId() throws Exception {
        Path root = tempDir.resolve("multi-op");
        writeRegistry(root, "op-a", 100L, valueStore("orders"));
        writeRegistry(root, "op-b", 100L, valueStore("orders"));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(root),
                                        opts("orders", null, null),
                                        "latest",
                                        valueSchemaDdl()));
        assertTrue(
                error.getMessage().contains("Multiple Cobble operators")
                        && error.getMessage().contains("op-a, op-b"),
                "expected multi-operator message but got: " + error.getMessage());
    }

    @Test
    void explicitOperatorIdResolves() throws Exception {
        Path root = tempDir.resolve("explicit-op");
        writeRegistry(root, "op-a", 100L, valueStore("orders"));
        writeRegistry(root, "op-b", 100L, valueStore("orders"));

        StateSourceResolvedSchema resolved =
                StateSourceSchemaResolver.resolve(
                        uri(root), opts("orders", "op-b", null), "latest", valueSchemaDdl());

        assertEquals("op-b", resolved.operatorId());
    }

    @Test
    void unknownOperatorIdFailsWithAvailableIds() throws Exception {
        Path root = tempDir.resolve("unknown-op");
        writeRegistry(root, "op-a", 100L, valueStore("orders"));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(root),
                                        opts("orders", "op-x", null),
                                        "latest",
                                        valueSchemaDdl()));
        assertTrue(
                error.getMessage().contains("op-x")
                        && error.getMessage().contains("Available operators: op-a"),
                "expected unknown-operator message but got: " + error.getMessage());
    }

    @Test
    void noRegistryFails() throws Exception {
        Path root = tempDir.resolve("no-registry");
        Files.createDirectories(root);

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(root),
                                        opts("orders", null, null),
                                        "latest",
                                        valueSchemaDdl()));
        assertTrue(
                error.getMessage().contains("No Cobble state inspect schema registry"),
                "expected no-registry message but got: " + error.getMessage());
    }

    // ------------------------------------------------------------------------------------------
    //  Checkpoint / event selection
    // ------------------------------------------------------------------------------------------

    @Test
    void numericCheckpointPicksLatestAtOrBefore() throws Exception {
        Path root = tempDir.resolve("numeric");
        writeRegistry(root, "op-a", 100L, valueStore("orders"));
        writeRegistry(root, "op-a", 200L, valueStore("orders"));

        StateSourceResolvedSchema resolved =
                StateSourceSchemaResolver.resolve(
                        uri(root), opts("orders", null, null), "150", valueSchemaDdl());

        assertEquals(100L, resolved.schemaCheckpointId());
    }

    @Test
    void numericCheckpointBeforeFirstEventFails() throws Exception {
        Path root = tempDir.resolve("numeric-before");
        writeRegistry(root, "op-a", 100L, valueStore("orders"));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(root),
                                        opts("orders", null, null),
                                        "50",
                                        valueSchemaDdl()));
        assertTrue(
                error.getMessage().contains("checkpoint id <= 50"),
                "expected no-event message but got: " + error.getMessage());
    }

    @Test
    void malformedEventFilesAreIgnored() throws Exception {
        Path root = tempDir.resolve("malformed");
        writeRegistry(root, "op-a", 100L, valueStore("orders"));
        Path events = schemaRoot(root, "op-a").resolve("events");
        Files.write(events.resolve("SCHEMA-00000000000000000150-bad.ref"), new byte[0]);

        StateSourceResolvedSchema resolved =
                StateSourceSchemaResolver.resolve(
                        uri(root), opts("orders", null, null), "latest", valueSchemaDdl());

        assertEquals(100L, resolved.schemaCheckpointId());
    }

    @Test
    void corruptBlobFails() throws Exception {
        Path root = tempDir.resolve("corrupt");
        Path base = schemaRoot(root, "op-a");
        Path events = base.resolve("events");
        Path blobs = base.resolve("blobs");
        Files.createDirectories(events);
        Files.createDirectories(blobs);
        byte[] corrupt = "not-a-schema".getBytes(StandardCharsets.UTF_8);
        String hash = InspectSchemaRegistryLayout.sha256(corrupt);
        Files.write(
                events.resolve(InspectSchemaRegistryLayout.eventFileName(100L, hash)), new byte[0]);
        Files.write(blobs.resolve(InspectSchemaRegistryLayout.blobFileName(hash)), corrupt);

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(root),
                                        opts("orders", null, null),
                                        "latest",
                                        valueSchemaDdl()));
        assertTrue(
                error.getMessage().contains("empty store"),
                "expected empty-store message but got: " + error.getMessage());
    }

    // ------------------------------------------------------------------------------------------
    //  State name / kind / semantic schema
    // ------------------------------------------------------------------------------------------

    @Test
    void missingStateListsAvailableNames() throws Exception {
        Path root = tempDir.resolve("missing-state");
        writeRegistry(root, "op-a", 100L, valueStore("orders"));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(root),
                                        opts("missing", null, null),
                                        "latest",
                                        valueSchemaDdl()));
        assertTrue(
                error.getMessage().contains("State 'missing' was not found")
                        && error.getMessage().contains("orders"),
                "expected missing-state message but got: " + error.getMessage());
    }

    @Test
    void stateKindMismatchFails() throws Exception {
        Path root = tempDir.resolve("kind-mismatch");
        writeRegistry(root, "op-a", 100L, valueStore("orders"));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(root),
                                        opts("orders", null, "map"),
                                        "latest",
                                        valueSchemaDdl()));
        assertTrue(
                error.getMessage()
                        .contains("state.kind='map' was requested, but state 'orders' is VALUE"),
                "expected kind-mismatch message but got: " + error.getMessage());
    }

    @Test
    void missingSemanticSchemaFails() throws Exception {
        Path root = tempDir.resolve("no-semantic");
        // Store with a state schema but no semantic schema entry.
        StateInspectSchemaStore store =
                new StateInspectSchemaStore(
                        Collections.singletonList(
                                valueSchema("orders", VoidNamespaceSerializer.INSTANCE)));
        writeRegistry(root, "op-a", 100L, store);

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(root),
                                        opts("orders", null, null),
                                        "latest",
                                        valueSchemaDdl()));
        assertTrue(
                error.getMessage().contains("no semantic schema"),
                "expected no-semantic message but got: " + error.getMessage());
    }

    // ------------------------------------------------------------------------------------------
    //  DDL validation
    // ------------------------------------------------------------------------------------------

    @Test
    void primaryKeyIsRejected() throws Exception {
        Path root = tempDir.resolve("pk");
        writeRegistry(root, "op-a", 100L, valueStore("orders"));

        List<Column> columns =
                Arrays.asList(
                        Column.physical("key", DataTypes.INT()),
                        Column.physical("value", DataTypes.INT()));
        ResolvedSchema withPk =
                new ResolvedSchema(
                        columns,
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("key")));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(root), opts("orders", null, null), "latest", withPk));
        assertTrue(
                error.getMessage().contains("does not support a PRIMARY KEY"),
                "expected PK-rejection message but got: " + error.getMessage());
    }

    @Test
    void missingColumnFails() throws Exception {
        Path root = tempDir.resolve("missing-col");
        writeRegistry(root, "op-a", 100L, valueStore("orders"));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(root),
                                        opts("orders", null, null),
                                        "latest",
                                        schema(physical("key", DataTypes.INT()))));
        assertTrue(
                error.getMessage().contains("missing column 'value'"),
                "expected missing-column message but got: " + error.getMessage());
    }

    @Test
    void extraColumnFails() throws Exception {
        Path root = tempDir.resolve("extra-col");
        writeRegistry(root, "op-a", 100L, valueStore("orders"));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(root),
                                        opts("orders", null, null),
                                        "latest",
                                        schema(
                                                physical("key", DataTypes.INT()),
                                                physical("value", DataTypes.INT()),
                                                physical("extra", DataTypes.INT()))));
        assertTrue(
                error.getMessage().contains("column 'extra' is not part of state"),
                "expected extra-column message but got: " + error.getMessage());
    }

    @Test
    void typeMismatchFails() throws Exception {
        Path root = tempDir.resolve("type-mismatch");
        writeRegistry(root, "op-a", 100L, valueStore("orders"));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(root),
                                        opts("orders", null, null),
                                        "latest",
                                        schema(
                                                physical("key", DataTypes.INT()),
                                                physical("value", DataTypes.STRING()))));
        assertTrue(
                error.getMessage().contains("column 'value' has type")
                        && error.getMessage().contains("expects INT"),
                "expected type-mismatch message but got: " + error.getMessage());
    }

    @Test
    void nullabilityDifferencesAreTolerated() throws Exception {
        Path root = tempDir.resolve("nullability");
        writeRegistry(root, "op-a", 100L, valueStore("orders"));

        // NOT NULL columns must still match the nullable inspect scalar types.
        StateSourceResolvedSchema resolved =
                StateSourceSchemaResolver.resolve(
                        uri(root),
                        opts("orders", null, null),
                        "latest",
                        schema(
                                physical("key", DataTypes.INT().notNull()),
                                physical("value", DataTypes.INT().notNull())));

        assertEquals(2, resolved.outputFields().size());
    }

    @Test
    void reorderedDdlColumnsFail() throws Exception {
        Path root = tempDir.resolve("reorder");
        writeRegistry(root, "op-a", 100L, valueStore("orders"));

        // Semantic order is key, value; swapping them must fail because the Step 3 runtime emits
        // in semantic order and Flink reads in DDL order.
        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(root),
                                        opts("orders", null, null),
                                        "latest",
                                        schema(
                                                physical("value", DataTypes.INT()),
                                                physical("key", DataTypes.INT()))));
        assertTrue(
                error.getMessage().contains("column at position 0")
                        && error.getMessage().contains("expects 'key'"),
                "expected position-order message but got: " + error.getMessage());
    }

    @Test
    void duplicateSemanticNamesFail() throws Exception {
        Path root = tempDir.resolve("dup-names");
        StateInspectType keyRow = row(field("id", scalar("INT")));
        StateInspectType valueRow = row(field("id", scalar("INT")));
        writeRegistry(
                root,
                "op-a",
                100L,
                store(
                        valueSchema("orders", VoidNamespaceSerializer.INSTANCE),
                        "orders",
                        StateInspectSemanticSchema.forValue(keyRow, unknown(), valueRow)));

        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () ->
                                StateSourceSchemaResolver.resolve(
                                        uri(root),
                                        opts("orders", null, null),
                                        "latest",
                                        schema(physical("id", DataTypes.INT()))));
        assertTrue(
                error.getMessage().contains("duplicate output column name 'id'"),
                "expected duplicate-name message but got: " + error.getMessage());
    }

    // ------------------------------------------------------------------------------------------
    //  Fixtures and helpers
    // ------------------------------------------------------------------------------------------

    private static StateInspectType scalar(String logicalType) {
        return StateInspectType.scalar(logicalType);
    }

    private static StateInspectType unknown() {
        return StateInspectType.unknown();
    }

    private static StateInspectField field(String name, StateInspectType type) {
        return new StateInspectField(name, type);
    }

    private static StateInspectType row(StateInspectField... fields) {
        return StateInspectType.row(Arrays.asList(fields));
    }

    private static StateInspectSchema valueSchema(
            String stateName, org.apache.flink.api.common.typeutils.TypeSerializer<?> namespace) {
        return StateInspectSchema.forValue(
                stateName, "cf", false, IntSerializer.INSTANCE, namespace, IntSerializer.INSTANCE);
    }

    private static StateInspectSchemaStore valueStore(String stateName) {
        return store(
                valueSchema(stateName, VoidNamespaceSerializer.INSTANCE),
                stateName,
                StateInspectSemanticSchema.forValue(scalar("INT"), unknown(), scalar("INT")));
    }

    private static StateInspectSchemaStore store(
            StateInspectSchema schema, String stateName, StateInspectSemanticSchema semantic) {
        Map<String, StateInspectSemanticSchema> semanticSchemas = new LinkedHashMap<>();
        semanticSchemas.put(stateName, semantic);
        return new StateInspectSchemaStore(Collections.singletonList(schema), semanticSchemas);
    }

    private static StateSourceOptions opts(String name, String operatorId, String kind) {
        Configuration config = new Configuration();
        config.set(CobbleSourceTableOptions.STATE_NAME, name);
        if (operatorId != null) {
            config.set(CobbleSourceTableOptions.STATE_OPERATOR_ID, operatorId);
        }
        if (kind != null) {
            config.set(CobbleSourceTableOptions.STATE_KIND, kind);
        }
        return StateSourceOptions.parseForState(config);
    }

    private static Column physical(String name, DataType type) {
        return Column.physical(name, type);
    }

    private static ResolvedSchema schema(Column... columns) {
        return ResolvedSchema.of(columns);
    }

    private static ResolvedSchema valueSchemaDdl() {
        return schema(physical("key", DataTypes.INT()), physical("value", DataTypes.INT()));
    }

    private static List<String> describe(List<StateSourceField> fields) {
        List<String> out = new ArrayList<>();
        for (StateSourceField field : fields) {
            out.add(field.name() + ":" + field.logicalType() + ":" + field.group());
        }
        return out;
    }

    private static String uri(Path path) {
        return path.toUri().toString();
    }

    private static Path schemaRoot(Path root, String operatorId) {
        return root.resolve("cobble").resolve(operatorId).resolve("inspect-schema");
    }

    private static void writeRegistry(
            Path root, String operatorId, long checkpointId, StateInspectSchemaStore store)
            throws Exception {
        byte[] bytes = store.toBytes();
        String hash = InspectSchemaRegistryLayout.sha256(bytes);
        Path base = schemaRoot(root, operatorId);
        Path events = base.resolve("events");
        Path blobs = base.resolve("blobs");
        Files.createDirectories(events);
        Files.createDirectories(blobs);
        Files.write(blobs.resolve(InspectSchemaRegistryLayout.blobFileName(hash)), bytes);
        Files.write(
                events.resolve(InspectSchemaRegistryLayout.eventFileName(checkpointId, hash)),
                new byte[0]);
    }
}
