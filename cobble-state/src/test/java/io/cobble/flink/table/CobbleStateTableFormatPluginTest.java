package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.ColumnFamilyOptions;
import io.cobble.Config;
import io.cobble.Db;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.SchemaBuilder;
import io.cobble.ShardSnapshot;
import io.cobble.SnapshotTools;
import io.cobble.flink.common.CobbleStateDescriptor;
import io.cobble.flink.common.CobbleStateReadFormatMetadata;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.table.TableReadCursor;
import io.cobble.table.TableReadEntry;
import io.cobble.table.TableReadPosition;
import io.cobble.table.TableReadProvider;
import io.cobble.table.TableReadRange;
import io.cobble.table.TableReadSession;
import io.cobble.table.TableReadSnapshot;
import io.cobble.table.TableReader;
import io.cobble.table.TableScanPlan;
import io.cobble.table.Value;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class CobbleStateTableFormatPluginTest {
    @TempDir Path directory;

    @Test
    void serviceReadsValueMapAndListFromSnapshotEmbeddedMetadata() throws Exception {
        Fixture fixture = writeSnapshot();
        assertEquals(Arrays.asList(Value.int32(7), Value.int32(70)), read(fixture, "value").get(0));
        List<List<Value>> maps = read(fixture, "map");
        assertEquals(2, maps.size());
        assertEquals(Arrays.asList(Value.int32(7), Value.int32(1), Value.int32(71)), maps.get(0));
        assertEquals(Value.Kind.NULL, maps.get(1).get(2).kind());
        assertEquals(
                Arrays.asList(
                        Arrays.asList(Value.int32(7), Value.int32(71)),
                        Arrays.asList(Value.int32(7), Value.int32(72)),
                        Arrays.asList(Value.int32(7), Value.int32(73))),
                read(fixture, "list"));
    }

    @Test
    void publicReaderDiscoversStateFormatFromTheFixedSnapshot() throws Exception {
        Fixture fixture = writeSnapshot();

        try (TableReader fixed = TableReader.open(fixture.config, "value", fixture.snapshot.id);
                TableReader current = TableReader.openCurrent(fixture.config, "value")) {
            TableScanPlan fixedPlan = fixed.scanPlan();
            TableScanPlan currentPlan = current.scanPlan();
            assertEquals(CobbleStateReadFormatMetadata.FORMAT_ID, fixedPlan.formatId());
            assertEquals(fixture.snapshot.id, fixedPlan.snapshotId());
            assertEquals(1, fixedPlan.totalBuckets());
            assertEquals(Arrays.asList("key", "value"), names(fixedPlan.readSchema()));
            assertEquals(fixedPlan.snapshotId(), currentPlan.snapshotId());
            assertEquals(names(fixedPlan.readSchema()), names(currentPlan.readSchema()));
            assertFalse(fixed.refresh());
            assertFalse(current.refresh());
        }
    }

    @Test
    void unifiedReaderUsesFixedStateSnapshotForFullKeyLookup() throws Exception {
        Fixture fixture = writeSnapshot();
        try (TableReader value = TableReader.open(fixture.config, "value", fixture.snapshot.id);
                TableReader map = TableReader.open(fixture.config, "map", fixture.snapshot.id);
                TableReader list = TableReader.open(fixture.config, "list", fixture.snapshot.id)) {
            assertTrue(value.capabilities().exactLookup());
            assertEquals(
                    Arrays.asList(Value.int32(7), Value.int32(70)),
                    value.lookup(Collections.singletonList(Value.int32(7)))
                            .iterator()
                            .next()
                            .value());
            assertTrue(value.lookup(Collections.singletonList(Value.int32(8))).isEmpty());
            assertEquals(
                    Arrays.asList(Value.int32(7), Value.int32(1), Value.int32(71)),
                    map.lookup(Arrays.asList(Value.int32(7), Value.int32(1)))
                            .iterator()
                            .next()
                            .value());
            assertFalse(list.capabilities().exactLookup());
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> list.lookup(Collections.singletonList(Value.int32(7))));
        }
    }

    @Test
    void serializedListPlanResumesInsideEntryAndPreservesCountProjection() throws Exception {
        Fixture fixture = writeSnapshot();
        TableScanPlan plan = roundTrip(plan(fixture, "list"));
        TableReadPosition resume;
        try (TableReadProvider<List<Value>, ?> provider =
                        plan.open(fixture.config, plan.splits().get(0));
                TableReadSession<List<Value>, ?> session = provider.open();
                TableReadCursor<List<Value>> cursor =
                        session.scan(new TableReadRange(0, 0), null)) {
            TableReadEntry<List<Value>> first = cursor.next();
            assertTrue(first.countsPhysicalEntry());
            assertTrue(first.physicalBytes() > 0L);
            resume = first.position();
            byte[] callerCopy = resume.physicalKey();
            callerCopy[0] ^= 1;
            assertTrue(
                    !Arrays.equals(callerCopy, resume.physicalKey()),
                    "public positions must retain a defensive physical-key copy");
            TableReadEntry<List<Value>> second = cursor.next();
            assertFalse(second.countsPhysicalEntry());
            assertEquals(0L, second.physicalBytes());
        }
        TableScanPlan projected = plan.project(Collections.singletonList("value"));
        try (TableReadProvider<List<Value>, ?> provider =
                        projected.open(fixture.config, projected.splits().get(0));
                TableReadSession<List<Value>, ?> session = provider.open();
                TableReadCursor<List<Value>> cursor =
                        session.scan(new TableReadRange(0, 0), resume)) {
            assertEquals(Collections.singletonList(Value.int32(72)), cursor.next().value());
            assertEquals(Collections.singletonList(Value.int32(73)), cursor.next().value());
            assertNull(cursor.next());
        }
        TableScanPlan count = plan.project(Collections.emptyList());
        try (TableReadProvider<List<Value>, ?> provider =
                        count.open(fixture.config, count.splits().get(0));
                TableReadSession<List<Value>, ?> session = provider.open();
                TableReadCursor<List<Value>> cursor =
                        session.scan(new TableReadRange(0, 0), null)) {
            for (int i = 0; i < 3; i++)
                assertEquals(Collections.emptyList(), cursor.next().value());
            assertNull(cursor.next());
        }
    }

    @Test
    void rejectsRecognizedStateWithMissingOrUnsupportedFormatMetadata() throws Exception {
        Fixture fixture = writeSnapshot();
        GlobalSnapshot invalid = fixture.snapshot.copy();
        hydrate(fixture.config, invalid);
        invalid.shardSnapshots.get(0).columnFamilies.get("value").options.metadata = null;
        assertThrows(
                IllegalArgumentException.class,
                () -> TableReadSnapshot.forGlobal(fixture.config, invalid, "value"));
        GlobalSnapshot newer = fixture.snapshot.copy();
        hydrate(fixture.config, newer);
        String metadata = newer.shardSnapshots.get(0).columnFamilies.get("value").options.metadata;
        com.google.gson.JsonObject json =
                com.google.gson.JsonParser.parseString(metadata).getAsJsonObject();
        json.addProperty("row_key_format_version", 999);
        newer.shardSnapshots.get(0).columnFamilies.get("value").options.metadata = json.toString();
        assertThrows(
                IllegalStateException.class,
                () ->
                        TableReader.open(
                                fixture.config,
                                TableReadSnapshot.forGlobal(fixture.config, newer, "value")));
    }

    @Test
    void lookupEncodingCompatibilityRejectsDifferentShardSerializerLayouts() {
        StateInspectType integer = StateInspectType.scalar("INT");
        StateInspectSemanticSchema semantic =
                StateInspectSemanticSchema.forValue(integer, null, integer);
        StateInspectSchema first =
                StateInspectSchema.forValue(
                        "value",
                        "value",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        StateInspectSchema differentValueSerializer =
                StateInspectSchema.forValue(
                        "value",
                        "value",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        StringSerializer.INSTANCE);

        assertFalse(
                CobbleStateTableFormatPlugin.lookupEncodingCompatible(
                        first, semantic, differentValueSerializer, semantic));
        assertTrue(CobbleStateTableFormatPlugin.lookupEncodingCompatible(first, null, first, null));
    }

    private Fixture writeSnapshot() throws Exception {
        Config config = new Config().addVolume(directory.toString()).totalBuckets(1).numColumns(1);
        IntSerializer serializer = IntSerializer.INSTANCE;
        StateInspectType integer = StateInspectType.scalar("INT");
        try (Db db = Db.open(config);
                SchemaBuilder builder = db.updateSchema()) {
            for (String name : Arrays.asList("value", "list", "map")) {
                CobbleStateDescriptor.StateKind kind =
                        CobbleStateDescriptor.StateKind.valueOf(
                                name.toUpperCase(java.util.Locale.ROOT));
                StateInspectSchema schema;
                StateInspectSemanticSchema semantic;
                if (kind == CobbleStateDescriptor.StateKind.MAP) {
                    schema =
                            StateInspectSchema.forMap(
                                    name,
                                    name,
                                    false,
                                    serializer,
                                    VoidNamespaceSerializer.INSTANCE,
                                    serializer,
                                    serializer);
                    semantic = StateInspectSemanticSchema.forMap(integer, null, integer, integer);
                } else if (kind == CobbleStateDescriptor.StateKind.LIST) {
                    schema =
                            StateInspectSchema.forList(
                                    name,
                                    name,
                                    false,
                                    serializer,
                                    VoidNamespaceSerializer.INSTANCE,
                                    serializer);
                    semantic = StateInspectSemanticSchema.forList(integer, null, integer);
                } else {
                    schema =
                            StateInspectSchema.forValue(
                                    name,
                                    name,
                                    false,
                                    serializer,
                                    VoidNamespaceSerializer.INSTANCE,
                                    serializer);
                    semantic = StateInspectSemanticSchema.forValue(integer, null, integer);
                }
                builder.setColumnFamilyOptions(
                        name,
                        ColumnFamilyOptions.defaults()
                                .valueHasTtl(false)
                                .metadata(
                                        CobbleStateReadFormatMetadata.encode(
                                                CobbleStateDescriptor.forKeyValue(name, name, kind),
                                                schema,
                                                semantic)));
                builder.addColumn(name, 0, null, null);
            }
            builder.commit();
            db.put(0, key(7, null), "value", 0, integer(70));
            DataOutputSerializer list = new DataOutputSerializer(32);
            for (int i = 71; i <= 73; i++) {
                list.writeInt(i);
                list.writeByte(',');
            }
            db.put(0, key(7, null), "list", 0, list.getCopyOfBuffer());
            DataOutputSerializer mapValue = new DataOutputSerializer(8);
            mapValue.writeBoolean(false);
            mapValue.writeInt(71);
            db.put(0, key(7, 1), "map", 0, mapValue.getCopyOfBuffer());
            db.put(0, key(7, 2), "map", 0, new byte[] {1});
            try (DbCoordinator coordinator = DbCoordinator.open(config)) {
                return new Fixture(
                        config,
                        coordinator.materializeGlobalSnapshot(
                                1, 1, Collections.singletonList(db.snapshot())));
            }
        }
    }

    private static byte[] key(int value, Integer mapKey) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeInt(value);
        out.writeByte(0); // VoidNamespaceSerializer persists one byte despite getLength() == 0.
        if (mapKey != null) {
            out.writeByte(0);
            out.writeInt(mapKey);
        }
        return out.getCopyOfBuffer();
    }

    private static byte[] integer(int value) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(4);
        out.writeInt(value);
        return out.getCopyOfBuffer();
    }

    private static TableScanPlan plan(Fixture fixture, String name) throws Exception {
        try (TableReader reader =
                TableReader.open(
                        fixture.config,
                        TableReadSnapshot.forGlobal(fixture.config, fixture.snapshot, name))) {
            return reader.scanPlan();
        }
    }

    private static void hydrate(Config config, GlobalSnapshot snapshot) {
        ShardSnapshot reference = snapshot.shardSnapshots.get(0);
        snapshot.shardSnapshots.set(
                0, SnapshotTools.loadShardSnapshot(config, reference.dbId, reference.manifestPath));
    }

    private static List<List<Value>> read(Fixture fixture, String name) throws Exception {
        TableScanPlan plan = plan(fixture, name);
        List<List<Value>> result = new ArrayList<>();
        try (TableReadProvider<List<Value>, ?> provider =
                        plan.open(fixture.config, plan.splits().get(0));
                TableReadSession<List<Value>, ?> session = provider.open();
                TableReadCursor<List<Value>> cursor =
                        session.scan(new TableReadRange(0, 0), null)) {
            TableReadEntry<List<Value>> entry;
            while ((entry = cursor.next()) != null) result.add(entry.value());
        }
        return result;
    }

    private static List<String> names(io.cobble.table.TableReadSchema schema) {
        List<String> names = new ArrayList<>();
        for (io.cobble.table.DataField field : schema.fields()) names.add(field.name());
        return names;
    }

    private static TableScanPlan roundTrip(TableScanPlan plan) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(plan);
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (TableScanPlan) in.readObject();
        }
    }

    private static final class Fixture {
        private final Config config;
        private final GlobalSnapshot snapshot;

        private Fixture(Config config, GlobalSnapshot snapshot) {
            this.config = config;
            this.snapshot = snapshot;
        }
    }
}
