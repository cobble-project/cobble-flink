package io.cobble.flink.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Live, mutable registry of canonical savepoint metadata, populated at state-registration time.
 *
 * <p>Every call to {@code CobbleKeyedStateBackend.createOrUpdateInternalState(...)} or {@code
 * CobbleKeyedStateBackend.create(...)} (timer path) registers a {@link
 * CobbleCanonicalStateMeta} here. The registry uses a {@link LinkedHashMap} keyed by a composite
 * of {@code (BackendStateType, stateName)} so that KV and priority-queue states with the same name
 * do not collide. Insertion order — the order in which the Flink job registers states — defines the
 * {@code kvStateId} sequence. This mirrors how RocksDB assigns state IDs and ensures the metadata
 * list and the Phase 2 iterator share the same stable ordering.
 *
 * <p>The registry is not thread-safe; it is accessed only on the task thread during state
 * registration and savepoint creation.
 */
final class CobbleCanonicalSavepointMetadata {

    /**
     * Keyed by a composite of backend state type + state name. Flink metadata distinguishes
     * KEY_VALUE and PRIORITY_QUEUE states even if they share the same name, so we must too.
     */
    private final LinkedHashMap<MetaKey, CobbleCanonicalStateMeta> entries = new LinkedHashMap<>();

    /**
     * Registers a KV state (VALUE / LIST / MAP / REDUCING / AGGREGATING).
     *
     * @param stateName the state name from the descriptor
     * @param stateType the {@link StateDescriptor.Type} from the descriptor
     * @param namespaceSerializer the live namespace serializer
     * @param valueSerializer the live value serializer (for LIST this is a {@code
     *     ListSerializer}; for MAP a {@code MapSerializer}; for AGGREGATING the accumulator
     *     serializer — all from {@code descriptor.getSerializer()})
     * @param columnFamily the Cobble column family that stores this state's rows
     * @throws IllegalStateException if a KV state with the same name is already registered with a
     *     different type or incompatible serializers
     */
    @SuppressWarnings("unchecked")
    void registerKeyValueState(
            String stateName,
            StateDescriptor.Type stateType,
            TypeSerializer<?> namespaceSerializer,
            TypeSerializer<?> valueSerializer,
            String columnFamily) {
        MetaKey key = MetaKey.keyValue(stateName);
        CobbleCanonicalStateMeta existing = entries.get(key);
        if (existing != null) {
            // Same name + same backend type already registered. This is expected when the same
            // state descriptor is used multiple times — silently ignore. But validate that the
            // type hasn't changed and the serializers are compatible.
            if (existing.stateType() != stateType) {
                throw new IllegalStateException(
                        "State '"
                                + stateName
                                + "' was already registered as "
                                + existing.stateType()
                                + " but is now requested as "
                                + stateType
                                + " for canonical savepoint metadata.");
            }
            // Validate serializer compatibility: the existing snapshot must resolve
            // compatible-as-is against the new serializers. This catches accidental
            // re-registration with incompatible serializers (a programming error) while
            // allowing schema-compatible re-registration (e.g. serializer instance change).
            checkSerializerCompatibility(
                    existing.metaInfoSnapshot(),
                    namespaceSerializer,
                    valueSerializer,
                    stateName);
            return;
        }
        int kvStateId = entries.size();
        RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        stateType, stateName, namespaceSerializer, valueSerializer);
        StateMetaInfoSnapshot snapshot = metaInfo.snapshot();
        entries.put(
                key,
                new CobbleCanonicalStateMeta(
                        kvStateId, stateName, stateType, false, columnFamily, snapshot));
    }

    /**
     * Registers a priority-queue (timer) state.
     *
     * @param stateName the timer state name
     * @param elementSerializer the live timer element serializer (typically a {@code
     *     TimerSerializer})
     * @param columnFamily the Cobble timer queue column family ({@code
     *     __cobble_timer__<stateName>})
     */
    @SuppressWarnings("unchecked")
    void registerPriorityQueueState(
            String stateName,
            TypeSerializer<?> elementSerializer,
            String columnFamily) {
        MetaKey key = MetaKey.priorityQueue(stateName);
        if (entries.containsKey(key)) {
            return;
        }
        int kvStateId = entries.size();
        RegisteredPriorityQueueStateBackendMetaInfo<?> metaInfo =
                new RegisteredPriorityQueueStateBackendMetaInfo<>(stateName, elementSerializer);
        StateMetaInfoSnapshot snapshot = metaInfo.snapshot();
        entries.put(
                key,
                new CobbleCanonicalStateMeta(
                        kvStateId, stateName, null, true, columnFamily, snapshot));
    }

    /**
     * Validates that the namespace and value serializers in the given {@link
     * StateMetaInfoSnapshot} resolve {@code compatible-as-is} against the new live serializers.
     *
     * <p>This is called on duplicate KV registration to catch accidental re-registration with
     * incompatible serializers — a programming error that would otherwise silently keep the
     * original snapshot and produce a savepoint that doesn't match the live state.
     *
     * <p>The {@code resolveSchemaCompatibility} method signature changed across Flink versions:
     * 1.17 accepts {@code TypeSerializer}, 2.0 accepts {@code TypeSerializerSnapshot}. Since this
     * class lives in the shared {@code cobble-state-common} module, we use reflection to invoke
     * the correct overload.
     */
    private static void checkSerializerCompatibility(
            StateMetaInfoSnapshot existingSnapshot,
            TypeSerializer<?> namespaceSerializer,
            TypeSerializer<?> valueSerializer,
            String stateName) {
        TypeSerializerSnapshot<?> nsSnapshot =
                existingSnapshot.getTypeSerializerSnapshot(
                        StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER);
        TypeSerializerSnapshot<?> valSnapshot =
                existingSnapshot.getTypeSerializerSnapshot(
                        StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
        if (nsSnapshot != null) {
            TypeSerializerSchemaCompatibility<?> nsCompat =
                    resolveCompatibility(nsSnapshot, namespaceSerializer);
            if (!nsCompat.isCompatibleAsIs()) {
                throw new IllegalStateException(
                        "State '"
                                + stateName
                                + "' was re-registered with an incompatible namespace serializer"
                                + " for canonical savepoint metadata (compatibility: "
                                + nsCompat
                                + ").");
            }
        }
        if (valSnapshot != null) {
            TypeSerializerSchemaCompatibility<?> valCompat =
                    resolveCompatibility(valSnapshot, valueSerializer);
            if (!valCompat.isCompatibleAsIs()) {
                throw new IllegalStateException(
                        "State '"
                                + stateName
                                + "' was re-registered with an incompatible value serializer"
                                + " for canonical savepoint metadata (compatibility: "
                                + valCompat
                                + ").");
            }
        }
    }

    /**
     * Resolves schema compatibility between an existing serializer snapshot and a new serializer,
     * handling the API difference between Flink 1.17 (accepts {@code TypeSerializer}) and 2.0
     * (accepts {@code TypeSerializerSnapshot}).
     *
     * <p>Because this class lives in the shared {@code cobble-state-common} module compiled against
     * all three Flink versions, we use reflection to invoke the correct overload. In Flink 2.0,
     * {@code resolveSchemaCompatibility(TypeSerializerSnapshot)} is called on the new serializer's
     * snapshot with the existing snapshot as the argument. In Flink 1.17/1.19,
     * {@code resolveSchemaCompatibility(TypeSerializer)} is called on the existing snapshot with
     * the new serializer as the argument.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static TypeSerializerSchemaCompatibility<?> resolveCompatibility(
            TypeSerializerSnapshot<?> existingSnapshot, TypeSerializer<?> newSerializer) {
        // Try Flink 2.0 API first: newSnapshot.resolveSchemaCompatibility(existingSnapshot)
        TypeSerializerSnapshot<?> newSnapshot = newSerializer.snapshotConfiguration();
        try {
            java.lang.reflect.Method m =
                    TypeSerializerSnapshot.class.getMethod(
                            "resolveSchemaCompatibility", TypeSerializerSnapshot.class);
            return (TypeSerializerSchemaCompatibility<?>)
                    m.invoke(newSnapshot, existingSnapshot);
        } catch (NoSuchMethodException e) {
            // Flink 1.17: existingSnapshot.resolveSchemaCompatibility(newSerializer)
            try {
                java.lang.reflect.Method m =
                        TypeSerializerSnapshot.class.getMethod(
                                "resolveSchemaCompatibility", TypeSerializer.class);
                return (TypeSerializerSchemaCompatibility<?>)
                        m.invoke(existingSnapshot, newSerializer);
            } catch (ReflectiveOperationException e2) {
                throw new IllegalStateException(
                        "Failed to resolve serializer compatibility via reflection", e2);
            }
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Failed to resolve serializer compatibility via reflection", e);
        }
    }

    /**
     * Produces an immutable {@link CobbleCanonicalSavepointMetadataSnapshot} of the current
     * registry. The returned snapshot is a defensive copy; subsequent registrations do not affect
     * it.
     */
    CobbleCanonicalSavepointMetadataSnapshot snapshot() {
        List<CobbleCanonicalStateMeta> entryList =
                Collections.unmodifiableList(new java.util.ArrayList<>(entries.values()));
        return new CobbleCanonicalSavepointMetadataSnapshot(entryList);
    }

    /** Returns the number of registered states. */
    int size() {
        return entries.size();
    }

    /** Returns whether any state has been registered. */
    boolean isEmpty() {
        return entries.isEmpty();
    }

    /** Returns an unmodifiable view of the registered state metadata in kvStateId order. */
    Map<MetaKey, CobbleCanonicalStateMeta> entries() {
        return Collections.unmodifiableMap(entries);
    }

    /** Composite key distinguishing KV states from priority-queue states with the same name. */
    static final class MetaKey {
        private final boolean priorityQueue;
        private final String stateName;

        private MetaKey(boolean priorityQueue, String stateName) {
            this.priorityQueue = priorityQueue;
            this.stateName = stateName;
        }

        static MetaKey keyValue(String stateName) {
            return new MetaKey(false, stateName);
        }

        static MetaKey priorityQueue(String stateName) {
            return new MetaKey(true, stateName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MetaKey metaKey = (MetaKey) o;
            return priorityQueue == metaKey.priorityQueue
                    && stateName.equals(metaKey.stateName);
        }

        @Override
        public int hashCode() {
            return 31 * (priorityQueue ? 1 : 0) + stateName.hashCode();
        }
    }
}
