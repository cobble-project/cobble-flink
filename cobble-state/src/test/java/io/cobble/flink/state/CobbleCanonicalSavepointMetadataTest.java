package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;

/**
 * Tests for the canonical savepoint metadata registry, covering registration order, kvStateId
 * assignment, backend state types, serializer snapshots, and heap-timer rejection.
 */
class CobbleCanonicalSavepointMetadataTest {

    @TempDir private Path tempDir;

    @Test
    void kvMetadataOrderAndKvStateId() throws Exception {
        try (CobbleStateBackendTest.TestBackendContext context = createBackend()) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);

            // Register states in a specific order.
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new ValueStateDescriptor<>("value-state", StringSerializer.INSTANCE));
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new ListStateDescriptor<>("list-state", StringSerializer.INSTANCE));
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new MapStateDescriptor<>(
                            "map-state", StringSerializer.INSTANCE, StringSerializer.INSTANCE));
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new ReducingStateDescriptor<>(
                            "reducing-state", (a, b) -> a + b, IntSerializer.INSTANCE));
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new AggregatingStateDescriptor<>(
                            "aggregating-state", new SumLongAggregator(), LongSerializer.INSTANCE));

            CobbleCanonicalSavepointMetadataSnapshot snapshot = backend.canonicalMetadataSnapshot();

            assertEquals(5, snapshot.stateCount(), "expected 5 KV states");

            // Verify kvStateId, name, type, backend state type.
            String[] names = {
                "value-state", "list-state", "map-state", "reducing-state", "aggregating-state"
            };
            StateDescriptor.Type[] types = {
                StateDescriptor.Type.VALUE,
                StateDescriptor.Type.LIST,
                StateDescriptor.Type.MAP,
                StateDescriptor.Type.REDUCING,
                StateDescriptor.Type.AGGREGATING
            };

            for (int i = 0; i < 5; i++) {
                CobbleCanonicalStateMeta entry = snapshot.entry(i);
                assertNotNull(entry, "entry " + i + " should exist");
                assertEquals(i, entry.kvStateId(), "kvStateId for " + names[i]);
                assertEquals(names[i], entry.stateName(), "stateName for entry " + i);
                assertEquals(types[i], entry.stateType(), "stateType for " + names[i]);
                assertFalse(entry.priorityQueue(), names[i] + " should not be priority queue");
                assertEquals(names[i], entry.columnFamily(), "columnFamily for " + names[i]);
                assertEquals(
                        StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                        entry.metaInfoSnapshot().getBackendStateType(),
                        "backend state type for " + names[i]);
            }
        }
    }

    @Test
    void timerMetadataAsPriorityQueue() throws Exception {
        try (CobbleStateBackendTest.TestBackendContext context = createBackend()) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);

            // Register a KV state first.
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new ValueStateDescriptor<>("value-state", StringSerializer.INSTANCE));

            // Register a timer state.
            backend.create(
                    "timer-state",
                    new TimerSerializer<>(
                            IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE));

            CobbleCanonicalSavepointMetadataSnapshot snapshot = backend.canonicalMetadataSnapshot();

            assertEquals(2, snapshot.stateCount(), "expected 1 KV + 1 PQ state");

            CobbleCanonicalStateMeta kvEntry = snapshot.entry(0);
            assertEquals("value-state", kvEntry.stateName());
            assertFalse(kvEntry.priorityQueue());

            CobbleCanonicalStateMeta pqEntry = snapshot.entry(1);
            assertEquals("timer-state", pqEntry.stateName());
            assertEquals(1, pqEntry.kvStateId());
            assertTrue(pqEntry.priorityQueue(), "timer state should be priority queue");
            assertEquals(
                    CobblePriorityQueueSetFactory.timerQueueColumnFamilyName("timer-state"),
                    pqEntry.columnFamily(),
                    "timer column family");
            assertEquals(
                    StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE,
                    pqEntry.metaInfoSnapshot().getBackendStateType(),
                    "timer backend state type");
        }
    }

    @Test
    void emptyBackendProducesEmptySnapshot() throws Exception {
        try (CobbleStateBackendTest.TestBackendContext context = createBackend()) {
            CobbleCanonicalSavepointMetadataSnapshot snapshot =
                    context.cobbleBackend.canonicalMetadataSnapshot();
            assertEquals(0, snapshot.stateCount(), "expected empty snapshot");
            assertTrue(snapshot.entries().isEmpty());
            assertTrue(snapshot.metaInfoSnapshots().isEmpty());
        }
    }

    @Test
    void serializerSnapshotsResolveCompatibleAsIs() throws Exception {
        try (CobbleStateBackendTest.TestBackendContext context = createBackend()) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);

            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new ValueStateDescriptor<>("value-state", StringSerializer.INSTANCE));
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new ListStateDescriptor<>("list-state", StringSerializer.INSTANCE));
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new MapStateDescriptor<>(
                            "map-state", StringSerializer.INSTANCE, StringSerializer.INSTANCE));

            CobbleCanonicalSavepointMetadataSnapshot snapshot = backend.canonicalMetadataSnapshot();

            // VALUE: namespace + value serializer snapshots should resolve compatible-as-is.
            StateMetaInfoSnapshot valueMeta = snapshot.entry(0).metaInfoSnapshot();
            assertCompatibleAsIs(
                    valueMeta,
                    StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER,
                    StringSerializer.INSTANCE,
                    "value namespace serializer");
            assertCompatibleAsIs(
                    valueMeta,
                    StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER,
                    StringSerializer.INSTANCE,
                    "value serializer");

            // LIST: value serializer is ListSerializer<String>.
            StateMetaInfoSnapshot listMeta = snapshot.entry(1).metaInfoSnapshot();
            assertCompatibleAsIs(
                    listMeta,
                    StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER,
                    new ListSerializer<>(StringSerializer.INSTANCE),
                    "list serializer");

            // MAP: value serializer is MapSerializer<String,String>.
            StateMetaInfoSnapshot mapMeta = snapshot.entry(2).metaInfoSnapshot();
            assertCompatibleAsIs(
                    mapMeta,
                    StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER,
                    new MapSerializer<>(StringSerializer.INSTANCE, StringSerializer.INSTANCE),
                    "map serializer");
        }
    }

    @Test
    void aggregatingStateRecordsAccumulatorSerializer() throws Exception {
        try (CobbleStateBackendTest.TestBackendContext context = createBackend()) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);

            // AggregatingState: ACC=Long, OUT=String. The canonical value serializer must be the
            // accumulator serializer (LongSerializer), not the output serializer.
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new AggregatingStateDescriptor<>(
                            "agg-state", new SumLongAggregator(), LongSerializer.INSTANCE));

            CobbleCanonicalSavepointMetadataSnapshot snapshot = backend.canonicalMetadataSnapshot();

            assertEquals(1, snapshot.stateCount());
            assertEquals(StateDescriptor.Type.AGGREGATING, snapshot.entry(0).stateType());

            assertCompatibleAsIs(
                    snapshot.entry(0).metaInfoSnapshot(),
                    StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER,
                    LongSerializer.INSTANCE,
                    "aggregating state value serializer should be the accumulator (Long) serializer");
        }
    }

    /**
     * Re-registering a KV state with the same name and type but an incompatible value serializer
     * must fail fast with {@link IllegalStateException}. This catches programming errors where the
     * same state name is accidentally used with different serializers.
     *
     * <p>This test calls {@code registerKeyValueState} directly because the backend's {@code
     * getPartitionedState} caches state by name and will not re-enter {@code
     * createOrUpdateInternalState} for the same name.
     */
    @Test
    void duplicateKvRegistrationWithIncompatibleSerializerFails() {
        CobbleCanonicalSavepointMetadata metadata = new CobbleCanonicalSavepointMetadata();
        metadata.registerKeyValueState(
                "dup-state",
                StateDescriptor.Type.VALUE,
                StringSerializer.INSTANCE,
                IntSerializer.INSTANCE,
                "dup-state");

        // Re-register with an incompatible value serializer (String vs Int).
        IllegalStateException error =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                metadata.registerKeyValueState(
                                        "dup-state",
                                        StateDescriptor.Type.VALUE,
                                        StringSerializer.INSTANCE,
                                        StringSerializer.INSTANCE,
                                        "dup-state"));
        assertTrue(
                error.getMessage().contains("incompatible value serializer"),
                "expected incompatible serializer error but got: " + error.getMessage());
        assertTrue(
                error.getMessage().contains("dup-state"),
                "error should reference the state name: " + error.getMessage());
    }

    /**
     * Re-registering a KV state with the same name, type, and compatible serializer is allowed (no
     * exception). This is the normal case when the same state descriptor is used multiple times.
     */
    @Test
    void duplicateKvRegistrationWithCompatibleSerializerSucceeds() {
        CobbleCanonicalSavepointMetadata metadata = new CobbleCanonicalSavepointMetadata();
        metadata.registerKeyValueState(
                "dup-state",
                StateDescriptor.Type.VALUE,
                StringSerializer.INSTANCE,
                IntSerializer.INSTANCE,
                "dup-state");

        // Re-register with the same serializer — should not throw.
        metadata.registerKeyValueState(
                "dup-state",
                StateDescriptor.Type.VALUE,
                StringSerializer.INSTANCE,
                IntSerializer.INSTANCE,
                "dup-state");

        CobbleCanonicalSavepointMetadataSnapshot snapshot = metadata.snapshot();
        assertEquals(1, snapshot.stateCount(), "duplicate registration should not add a new entry");
    }

    /** Re-registering a KV state with the same name but a different state type must fail fast. */
    @Test
    void duplicateKvRegistrationWithDifferentTypeFails() {
        CobbleCanonicalSavepointMetadata metadata = new CobbleCanonicalSavepointMetadata();
        metadata.registerKeyValueState(
                "dup-state",
                StateDescriptor.Type.VALUE,
                StringSerializer.INSTANCE,
                IntSerializer.INSTANCE,
                "dup-state");

        IllegalStateException error =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                metadata.registerKeyValueState(
                                        "dup-state",
                                        StateDescriptor.Type.LIST,
                                        StringSerializer.INSTANCE,
                                        new ListSerializer<>(IntSerializer.INSTANCE),
                                        "dup-state"));
        assertTrue(
                error.getMessage().contains("was already registered as"),
                "expected type mismatch error but got: " + error.getMessage());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void assertCompatibleAsIs(
            StateMetaInfoSnapshot meta,
            StateMetaInfoSnapshot.CommonSerializerKeys key,
            TypeSerializer<?> serializer,
            String message) {
        TypeSerializerSnapshot<?> snapshot = meta.getTypeSerializerSnapshot(key);
        TypeSerializerSchemaCompatibility<?> compat =
                ((TypeSerializerSnapshot) snapshot).resolveSchemaCompatibility(serializer);
        assertTrue(compat.isCompatibleAsIs(), message);
    }

    /**
     * Phase-2 readiness test: reconstruct {@link RegisteredKeyValueStateBackendMetaInfo} and {@link
     * RegisteredPriorityQueueStateBackendMetaInfo} from the stored {@link StateMetaInfoSnapshot}s
     * and verify that the namespace, value, and element serializers can be obtained and are usable
     * for serialization. This proves the snapshot is self-contained — Phase 2 can obtain all
     * serializers it needs for key/value encoding without any live backend reference.
     *
     * <p>Covers all KV state kinds: VALUE, LIST, MAP, REDUCING, AGGREGATING, plus a timer
     * (priority-queue) state.
     */
    @Test
    void phase2SerializerReconstructionFromSnapshot() throws Exception {
        try (CobbleStateBackendTest.TestBackendContext context = createBackend()) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);

            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new ValueStateDescriptor<>("value-state", IntSerializer.INSTANCE));
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new ListStateDescriptor<>("list-state", StringSerializer.INSTANCE));
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new MapStateDescriptor<>(
                            "map-state", StringSerializer.INSTANCE, StringSerializer.INSTANCE));
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new ReducingStateDescriptor<>(
                            "reducing-state", (a, b) -> a + b, IntSerializer.INSTANCE));
            backend.getPartitionedState(
                    "ns",
                    StringSerializer.INSTANCE,
                    new AggregatingStateDescriptor<>(
                            "agg-state", new SumLongAggregator(), LongSerializer.INSTANCE));
            backend.create(
                    "timer-state",
                    new TimerSerializer<>(
                            IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE));

            CobbleCanonicalSavepointMetadataSnapshot snapshot = backend.canonicalMetadataSnapshot();
            assertEquals(6, snapshot.stateCount());

            // VALUE state — reconstruct KV meta info and obtain namespace + value serializers.
            StateMetaInfoSnapshot valueMeta = snapshot.entry(0).metaInfoSnapshot();
            RegisteredKeyValueStateBackendMetaInfo<?, ?> valueReconstructed =
                    new RegisteredKeyValueStateBackendMetaInfo<>(valueMeta);
            assertEquals(
                    StateDescriptor.Type.VALUE, valueReconstructed.getStateType(), "value type");
            assertEquals("value-state", valueReconstructed.getName(), "value name");
            TypeSerializer<?> valueNsSer = valueReconstructed.getNamespaceSerializer();
            TypeSerializer<?> valueSer = valueReconstructed.getStateSerializer();
            assertInstanceOf(StringSerializer.class, valueNsSer, "value namespace serializer");
            assertInstanceOf(IntSerializer.class, valueSer, "value serializer");

            // LIST state — value serializer must be a ListSerializer.
            StateMetaInfoSnapshot listMeta = snapshot.entry(1).metaInfoSnapshot();
            RegisteredKeyValueStateBackendMetaInfo<?, ?> listReconstructed =
                    new RegisteredKeyValueStateBackendMetaInfo<>(listMeta);
            assertEquals(StateDescriptor.Type.LIST, listReconstructed.getStateType(), "list type");
            assertInstanceOf(
                    ListSerializer.class,
                    listReconstructed.getStateSerializer(),
                    "list value serializer");

            // MAP state — value serializer must be a MapSerializer.
            StateMetaInfoSnapshot mapMeta = snapshot.entry(2).metaInfoSnapshot();
            RegisteredKeyValueStateBackendMetaInfo<?, ?> mapReconstructed =
                    new RegisteredKeyValueStateBackendMetaInfo<>(mapMeta);
            assertEquals(StateDescriptor.Type.MAP, mapReconstructed.getStateType(), "map type");
            assertInstanceOf(
                    MapSerializer.class,
                    mapReconstructed.getStateSerializer(),
                    "map value serializer");

            // REDUCING state — value serializer must be the reduce value serializer
            // (IntSerializer).
            StateMetaInfoSnapshot reducingMeta = snapshot.entry(3).metaInfoSnapshot();
            RegisteredKeyValueStateBackendMetaInfo<?, ?> reducingReconstructed =
                    new RegisteredKeyValueStateBackendMetaInfo<>(reducingMeta);
            assertEquals(
                    StateDescriptor.Type.REDUCING,
                    reducingReconstructed.getStateType(),
                    "reducing type");
            assertInstanceOf(
                    IntSerializer.class,
                    reducingReconstructed.getStateSerializer(),
                    "reducing value serializer");

            // AGGREGATING state — value serializer must be the accumulator (LongSerializer).
            StateMetaInfoSnapshot aggMeta = snapshot.entry(4).metaInfoSnapshot();
            RegisteredKeyValueStateBackendMetaInfo<?, ?> aggReconstructed =
                    new RegisteredKeyValueStateBackendMetaInfo<>(aggMeta);
            assertEquals(
                    StateDescriptor.Type.AGGREGATING,
                    aggReconstructed.getStateType(),
                    "aggregating type");
            assertInstanceOf(
                    LongSerializer.class,
                    aggReconstructed.getStateSerializer(),
                    "aggregating value serializer should be accumulator (Long)");

            // Timer state — reconstruct PQ meta info and obtain the element serializer.
            StateMetaInfoSnapshot timerMeta = snapshot.entry(5).metaInfoSnapshot();
            RegisteredPriorityQueueStateBackendMetaInfo<?> timerReconstructed =
                    new RegisteredPriorityQueueStateBackendMetaInfo<>(timerMeta);
            assertEquals("timer-state", timerReconstructed.getName(), "timer name");
            TypeSerializer<?> elementSer = timerReconstructed.getElementSerializer();
            assertNotNull(elementSer, "timer element serializer should not be null");
            assertInstanceOf(TimerSerializer.class, elementSer, "timer element serializer");
        }
    }

    @Test
    void heapTimersAreRejected() throws Exception {
        Configuration overrides = new Configuration();
        overrides.set(
                CobbleOptions.TIMER_SERVICE_FACTORY,
                CobbleStateBackend.PriorityQueueStateType.HEAP);

        try (CobbleStateBackendTest.TestBackendContext context =
                createBackendWithOverrides(overrides)) {
            CobbleKeyedStateBackend<Integer> backend = context.cobbleBackend;
            backend.setCurrentKey(1);

            // Register a heap timer.
            backend.create(
                    "heap-timer",
                    new TimerSerializer<>(
                            IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE));

            UnsupportedOperationException error =
                    assertThrows(
                            UnsupportedOperationException.class,
                            backend::canonicalMetadataSnapshot);
            assertTrue(
                    error.getMessage().contains("heap-backed timers"),
                    "expected heap-timer rejection but got: " + error.getMessage());
        }
    }

    // ------------------------------------------------------------------------------------------
    //  Test helpers
    // ------------------------------------------------------------------------------------------

    private CobbleStateBackendTest.TestBackendContext createBackend() throws Exception {
        return createBackendWithOverrides(new Configuration());
    }

    private CobbleStateBackendTest.TestBackendContext createBackendWithOverrides(
            Configuration overrides) throws Exception {
        // Reuse the existing test infrastructure from CobbleStateBackendTest. That class's
        // createBackendContext is a package-private instance method, so we instantiate the test
        // class to call it.
        CobbleStateBackendTest helper = new CobbleStateBackendTest();
        return helper.createBackendContext(
                tempDir,
                false,
                null,
                null,
                TtlTimeProvider.DEFAULT,
                false,
                Collections.emptyList(),
                KeyGroupRange.of(0, 15),
                overrides);
    }

    /** Simple aggregate function: ACC=Long, OUT=String. */
    private static final class SumLongAggregator
            implements org.apache.flink.api.common.functions.AggregateFunction<
                    Integer, Long, String> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Integer value, Long accumulator) {
            return accumulator + value;
        }

        @Override
        public String getResult(Long accumulator) {
            return "sum=" + accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}
