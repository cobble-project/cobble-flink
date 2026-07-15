package io.cobble.flink.inspect.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;
import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.inspect.InspectOverview;
import io.cobble.flink.inspect.InspectOverviewItem;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

class InspectOverviewGeneratorTest {
    @Test
    void sinkDdlUsesRowIndexPkSidecarOrderAndPinnedSnapshot() {
        SinkInspectSchema schema =
                new SinkInspectSchema(
                        Arrays.asList(
                                SinkInspectField.key("region", "VARCHAR", 2, -1),
                                SinkInspectField.key("id", "BIGINT", 0, -1)),
                        Collections.singletonList(
                                SinkInspectField.value("payload", "VARCHAR", 1, 0)));
        InspectOverviewItem item =
                only(
                        InspectOverviewGenerator.generate(
                                "s3://bucket/a'b/table-name",
                                19L,
                                "sink",
                                Collections.singletonList(InspectTarget.sink("sink", schema))));

        String ddl = item.sourceSql().ddl();
        assertTrue(ddl.indexOf("`id` BIGINT") < ddl.indexOf("`payload` VARCHAR"));
        assertTrue(ddl.indexOf("`payload` VARCHAR") < ddl.indexOf("`region` VARCHAR"));
        assertTrue(ddl.contains("PRIMARY KEY (`region`, `id`) NOT ENFORCED"));
        assertTrue(ddl.contains("'scan.checkpoint-id' = '19'"));
        assertTrue(ddl.contains("s3://bucket/a''b/table-name"));
        assertEquals(Arrays.asList("region", "id"), item.sourceSql().requiredLookupFields());
    }

    @Test
    void stateUsesSharedLayoutAndListIsScanOnly() {
        InspectTarget value = valueTarget("orders");
        InspectTarget list = listTarget("items");
        InspectOverview overview =
                InspectOverviewGenerator.generate(
                        "file:///checkpoints/job/chk-7/_metadata",
                        7L,
                        "operator-1",
                        Arrays.asList(value, list));

        String valueDdl = overview.items().get(0).sourceSql().ddl();
        assertTrue(valueDdl.contains("`key` INT"));
        assertTrue(valueDdl.contains("`value` VARCHAR"));
        assertTrue(valueDdl.contains("PRIMARY KEY (`key`) NOT ENFORCED"));
        assertTrue(valueDdl.contains("'path' = 'file:///checkpoints/job/chk-7/_metadata'"));
        assertTrue(valueDdl.contains("'state.operator-id' = 'operator-1'"));
        assertTrue(valueDdl.contains("'scan.checkpoint-id' = '7'"));

        assertTrue(overview.items().get(1).sourceSql().available());
        assertTrue(overview.items().get(1).sourceSql().batchScanSupported());
        assertFalse(overview.items().get(1).sourceSql().exactLookupSupported());
        assertTrue(overview.items().get(1).sourceSql().requiredLookupFields().isEmpty());
        assertFalse(overview.items().get(1).sourceSql().ddl().contains("PRIMARY KEY"));
    }

    @Test
    void timerRawAndMalformedSinkReturnItemLocalUnavailable() {
        InspectTarget timer = InspectTarget.timer("event-time", "timer-cf");
        InspectTarget raw = InspectTarget.sink("raw");
        SinkInspectSchema malformed =
                new SinkInspectSchema(
                        Collections.singletonList(SinkInspectField.key("id", "INT", 2, -1)),
                        Collections.singletonList(SinkInspectField.value("value", "INT", 2, 0)));
        InspectOverview overview =
                InspectOverviewGenerator.generate(
                        "file:///data",
                        3L,
                        "op",
                        Arrays.asList(timer, raw, InspectTarget.sink("broken", malformed)));

        assertEquals("Timer", overview.items().get(0).kind());
        for (InspectOverviewItem item : overview.items()) {
            assertNull(item.sourceSql().ddl());
            assertFalse(item.sourceSql().batchScanSupported());
            assertFalse(item.sourceSql().exactLookupSupported());
        }
    }

    @Test
    void stateCapabilitiesAndStructuredColumnsMatchSourceContracts() {
        StateInspectType structuredValue =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("order_id", StateInspectType.scalar("INT")),
                                new StateInspectField(
                                        "region", StateInspectType.scalar("VARCHAR"))));
        InspectTarget reducing =
                stateTarget(
                        StateInspectSchema.forReducing(
                                "reduced",
                                "reduced",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                StringSerializer.INSTANCE),
                        StateInspectSemanticSchema.forReducing(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                structuredValue));
        InspectTarget aggregating =
                stateTarget(
                        StateInspectSchema.forAggregating(
                                "aggregated",
                                "aggregated",
                                false,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                StringSerializer.INSTANCE),
                        StateInspectSemanticSchema.forAggregating(
                                StateInspectType.scalar("INT"),
                                StateInspectType.unknown(),
                                structuredValue));
        InspectTarget map =
                stateTarget(
                        StateInspectSchema.forMap(
                                "attributes",
                                "attributes",
                                false,
                                IntSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                IntSerializer.INSTANCE),
                        StateInspectSemanticSchema.forMap(
                                StateInspectType.scalar("INT"),
                                StateInspectType.scalar("VARCHAR"),
                                StateInspectType.scalar("VARCHAR"),
                                StateInspectType.scalar("INT")));

        InspectOverview overview =
                InspectOverviewGenerator.generate(
                        "file:///checkpoints",
                        12L,
                        "op",
                        Arrays.asList(reducing, aggregating, map));
        for (InspectOverviewItem item : overview.items()) {
            assertTrue(item.sourceSql().batchScanSupported());
            assertTrue(item.sourceSql().exactLookupSupported());
        }
        assertTrue(overview.items().get(0).sourceSql().ddl().contains("`order_id` INT"));
        assertTrue(overview.items().get(1).sourceSql().ddl().contains("`region` VARCHAR"));
        assertEquals(
                Arrays.asList("key", "namespace", "map_key"),
                overview.items().get(2).sourceSql().requiredLookupFields());
    }

    private static InspectTarget valueTarget(String name) {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        name,
                        name,
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        StateInspectSemanticSchema semantic =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("VARCHAR"));
        return new InspectTarget(
                name,
                name,
                "state",
                name,
                false,
                "VALUE",
                Collections.<String, String>emptyMap(),
                schema,
                semantic,
                null);
    }

    private static InspectTarget listTarget(String name) {
        StateInspectSchema schema =
                StateInspectSchema.forList(
                        name,
                        name,
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        StateInspectSemanticSchema semantic =
                StateInspectSemanticSchema.forList(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("VARCHAR"));
        return new InspectTarget(
                name,
                name,
                "state",
                name,
                false,
                "LIST",
                Collections.<String, String>emptyMap(),
                schema,
                semantic,
                null);
    }

    private static InspectTarget stateTarget(
            StateInspectSchema schema, StateInspectSemanticSchema semantic) {
        return new InspectTarget(
                schema.stateName(),
                schema.stateName(),
                "state",
                schema.columnFamily(),
                false,
                schema.stateKind().name(),
                Collections.<String, String>emptyMap(),
                schema,
                semantic,
                null);
    }

    private static InspectOverviewItem only(InspectOverview overview) {
        assertEquals(1, overview.items().size());
        return overview.items().get(0);
    }
}
