package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

class CobbleStateDescriptorTest {

    @Test
    void describesEveryRuntimeStateRowFormat() {
        assertDescriptor(
                StateDescriptor.Type.VALUE,
                CobbleStateDescriptor.StateKind.VALUE,
                CobbleStateDescriptor.RowKeyEncoding.KEY_NAMESPACE,
                CobbleStateDescriptor.RowValueEncoding.SERIALIZED_VALUE);
        assertDescriptor(
                StateDescriptor.Type.LIST,
                CobbleStateDescriptor.StateKind.LIST,
                CobbleStateDescriptor.RowKeyEncoding.KEY_NAMESPACE,
                CobbleStateDescriptor.RowValueEncoding.DELIMITED_LIST);
        assertDescriptor(
                StateDescriptor.Type.MAP,
                CobbleStateDescriptor.StateKind.MAP,
                CobbleStateDescriptor.RowKeyEncoding.KEY_NAMESPACE_MAP_KEY,
                CobbleStateDescriptor.RowValueEncoding.NULLABLE_MAP_VALUE);
        assertDescriptor(
                StateDescriptor.Type.REDUCING,
                CobbleStateDescriptor.StateKind.REDUCING,
                CobbleStateDescriptor.RowKeyEncoding.KEY_NAMESPACE,
                CobbleStateDescriptor.RowValueEncoding.SERIALIZED_VALUE);
        assertDescriptor(
                StateDescriptor.Type.AGGREGATING,
                CobbleStateDescriptor.StateKind.AGGREGATING,
                CobbleStateDescriptor.RowKeyEncoding.KEY_NAMESPACE,
                CobbleStateDescriptor.RowValueEncoding.SERIALIZED_VALUE);

        CobbleStateDescriptor timer =
                CobbleStateDescriptor.forTimer("timer", "__cobble_timer__timer");
        assertEquals(CobbleStateDescriptor.StateKind.TIMER, timer.stateKind());
        assertEquals(CobbleStateDescriptor.RowKeyEncoding.TIMER_ELEMENT, timer.rowKeyEncoding());
        assertEquals(CobbleStateDescriptor.RowValueEncoding.NONE, timer.rowValueEncoding());
        assertCurrentFormatVersions(timer);
    }

    @Test
    void snapshotMetadataRoundTripsStateDescriptors() throws Exception {
        List<CobbleStateDescriptor> descriptors =
                Arrays.asList(
                        CobbleStateDescriptor.forKeyValue(
                                "value", "value", StateDescriptor.Type.VALUE),
                        CobbleStateDescriptor.forKeyValue("map", "map", StateDescriptor.Type.MAP),
                        CobbleStateDescriptor.forTimer("timer", "__cobble_timer__timer"));
        CobbleSnapshotMetadataPayload payload =
                new CobbleSnapshotMetadataPayload(
                        shard(), false, descriptors, StateInspectSchemaStore.empty());
        DataOutputSerializer output = new DataOutputSerializer(256);

        CobbleSnapshotMetadataCodec.write(payload, output);
        CobbleSnapshotMetadataPayload restored =
                CobbleSnapshotMetadataCodec.read(
                        new DataInputDeserializer(output.getCopyOfBuffer()));

        assertEquals(descriptors, restored.stateDescriptors());
        assertEquals(Collections.emptyMap(), restored.shardSnapshot().columnFamilyIds);
    }

    @Test
    void snapshotMetadataRoundTripsDirectShardSchema() throws Exception {
        ShardSnapshot shard = directShard();
        CobbleSnapshotMetadataPayload payload =
                new CobbleSnapshotMetadataPayload(
                        shard, false, Collections.emptyList(), StateInspectSchemaStore.empty());
        DataOutputSerializer output = new DataOutputSerializer(256);

        CobbleSnapshotMetadataCodec.write(payload, output);
        ShardSnapshot restored =
                CobbleSnapshotMetadataCodec.read(
                                new DataInputDeserializer(output.getCopyOfBuffer()))
                        .shardSnapshot();

        assertEquals(shard.dbId, restored.dbId);
        assertEquals(shard.schemaId, restored.schemaId);
        assertNull(restored.columnFamilyIds);
        ShardSnapshot.SnapshotColumnFamily family = restored.columnFamilies.get("state");
        assertNotNull(family);
        assertEquals(7, family.id);
        assertEquals(2, family.numColumns);
        assertFalse(family.options.valueHasTtl);
        assertEquals("{\"logical_type\":\"BIGINT\"}", family.options.metadata);
    }

    @Test
    void stateIdentityRejectsKeyedStateKindChangesButKeepsTimersSeparate() {
        CobbleStateDescriptor value =
                CobbleStateDescriptor.forKeyValue("shared", "shared", StateDescriptor.Type.VALUE);
        CobbleStateDescriptor map =
                CobbleStateDescriptor.forKeyValue("shared", "shared", StateDescriptor.Type.MAP);
        CobbleStateDescriptor timer =
                CobbleStateDescriptor.forTimer("shared", "__cobble_timer__shared");

        assertEquals(value.stateIdentity(), map.stateIdentity());
        assertNotEquals(value.stateIdentity(), timer.stateIdentity());
    }

    private static void assertDescriptor(
            StateDescriptor.Type stateType,
            CobbleStateDescriptor.StateKind stateKind,
            CobbleStateDescriptor.RowKeyEncoding rowKeyEncoding,
            CobbleStateDescriptor.RowValueEncoding rowValueEncoding) {
        CobbleStateDescriptor descriptor =
                CobbleStateDescriptor.forKeyValue("state", "state", stateType);
        assertEquals(stateKind, descriptor.stateKind());
        assertEquals(rowKeyEncoding, descriptor.rowKeyEncoding());
        assertEquals(rowValueEncoding, descriptor.rowValueEncoding());
        assertCurrentFormatVersions(descriptor);
    }

    private static void assertCurrentFormatVersions(CobbleStateDescriptor descriptor) {
        assertEquals(
                descriptor.rowKeyEncoding().currentVersion(), descriptor.rowKeyFormatVersion());
        assertEquals(
                descriptor.rowValueEncoding().currentVersion(), descriptor.rowValueFormatVersion());
    }

    private static ShardSnapshot shard() {
        ShardSnapshot shard = new ShardSnapshot();
        shard.dbId = "db";
        shard.snapshotId = 1L;
        shard.manifestPath = "file:///snapshot/SNAPSHOT-1";
        shard.columnFamilyIds = Collections.emptyMap();
        return shard;
    }

    private static ShardSnapshot directShard() {
        ShardSnapshot shard = shard();
        shard.columnFamilyIds = null;
        shard.schemaId = 9L;
        ShardSnapshot.SnapshotColumnFamily family = new ShardSnapshot.SnapshotColumnFamily();
        family.id = 7;
        family.numColumns = 2;
        family.options.valueHasTtl = false;
        family.options.metadata = "{\"logical_type\":\"BIGINT\"}";
        shard.columnFamilies = new LinkedHashMap<>();
        shard.columnFamilies.put("state", family);
        return shard;
    }
}
