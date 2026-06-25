package io.cobble.flink.state;

import io.cobble.ColumnFamilyOptions;
import io.cobble.structured.Db;
import io.cobble.structured.PriorityQueue;
import io.cobble.structured.StructuredSchemaBuilder;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.restore.FullSnapshotRestoreOperation;
import org.apache.flink.runtime.state.restore.KeyGroup;
import org.apache.flink.runtime.state.restore.KeyGroupEntry;
import org.apache.flink.runtime.state.restore.SavepointRestoreResult;
import org.apache.flink.runtime.state.restore.ThrowingIterator;
import org.apache.flink.util.StateMigrationException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Imports Flink's unified canonical savepoint format into a newly opened Cobble DB.
 *
 * <p>The restore runs in two strict phases:
 *
 * <ol>
 *   <li><b>Preflight</b> — open every handle's metadata only, validate that same-named state is
 *       defined consistently across handles (kind / state type / serializer snapshots via Flink's
 *       {@link TypeSerializerSnapshot#resolveSchemaCompatibility}), and accumulate the resulting
 *       {@link RestoredKeyedStateMetadata} registry. The row iterators are closed without being
 *       consumed; no column family is created and no row is written. Any failure here keeps the
 *       backing Cobble DB schema empty.
 *   <li><b>Import</b> — re-open the same handles via a fresh {@link FullSnapshotRestoreOperation},
 *       create the required column families, and write rows.
 * </ol>
 *
 * <p>The two-pass design is required because (a) the row iterator and the metadata share a single
 * stream, and (b) the reviewer's contract states that a metadata-inconsistent input must fail
 * before any schema or data mutation. Re-opening the handles is safe — every {@link
 * org.apache.flink.runtime.state.KeyGroupsStateHandle} owns a {@link
 * org.apache.flink.runtime.state.StreamStateHandle} whose {@code openInputStream()} returns a fresh
 * stream per call.
 */
final class CanonicalSavepointRestoreOperation<K> {

    private static final int STATE_COLUMN_INDEX = 0;
    private static final byte LIST_DELIMITER = ',';
    private static final byte[] EMPTY_TIMER_VALUE = new byte[0];

    private final Db db;
    private final KeyGroupRange keyGroupRange;
    private final int numberOfKeyGroups;
    private final ClassLoader userCodeClassLoader;
    private final Collection<KeyedStateHandle> restoreStateHandles;
    private final Supplier<StateSerializerProvider<K>> preflightKeySerializerProviderFactory;
    private final StateSerializerProvider<K> importKeySerializerProvider;

    /**
     * @param preflightKeySerializerProviderFactory factory invoked exactly once to obtain a
     *     throw-away {@link StateSerializerProvider} for the metadata-only preflight pass. {@link
     *     FullSnapshotRestoreOperation#restore()} mutates the supplied provider via {@code
     *     setPreviousSerializerSnapshotForRestoredState(...)} which is a one-shot operation per
     *     provider instance, so the preflight must NOT share a provider with the import phase.
     * @param importKeySerializerProvider the provider the builder will later use to construct the
     *     backend. The import phase invokes {@link FullSnapshotRestoreOperation#restore()} on this
     *     instance so that the canonical key-serializer snapshot is registered into the builder's
     *     own provider; this lets {@code importKeySerializerProvider.currentSchemaSerializer()}
     *     return the (possibly reconfigured) key serializer Flink chose for the running job, and
     *     guarantees the backend ends up holding that exact serializer.
     */
    CanonicalSavepointRestoreOperation(
            Db db,
            KeyGroupRange keyGroupRange,
            int numberOfKeyGroups,
            ClassLoader userCodeClassLoader,
            Collection<KeyedStateHandle> restoreStateHandles,
            Supplier<StateSerializerProvider<K>> preflightKeySerializerProviderFactory,
            StateSerializerProvider<K> importKeySerializerProvider) {
        this.db = db;
        this.keyGroupRange = keyGroupRange;
        this.numberOfKeyGroups = numberOfKeyGroups;
        this.userCodeClassLoader = userCodeClassLoader;
        this.restoreStateHandles = restoreStateHandles;
        this.preflightKeySerializerProviderFactory = preflightKeySerializerProviderFactory;
        this.importKeySerializerProvider = importKeySerializerProvider;
    }

    /**
     * Imports every canonical handle into the backing DB and returns the canonical metadata indexed
     * by state name. Callers (the backend builder) hand the map to {@link CobbleKeyedStateBackend}
     * so that subsequent descriptor / priority-queue registrations are checked for compatibility
     * against the canonical definition.
     */
    Map<String, RestoredKeyedStateMetadata> restore() throws IOException, StateMigrationException {
        LinkedHashMap<String, RestoredKeyedStateMetadata> metadata = preflightMetadata();
        importRows(metadata);
        return Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
    }

    /**
     * Phase 1 — opens every handle's metadata and validates cross-handle consistency without
     * creating a column family or writing a row. The row iterators returned by Flink are closed
     * immediately without being consumed; their underlying streams are released and the next phase
     * will re-open the handles from scratch.
     */
    private LinkedHashMap<String, RestoredKeyedStateMetadata> preflightMetadata()
            throws IOException, StateMigrationException {
        LinkedHashMap<String, RestoredKeyedStateMetadata> metadata = new LinkedHashMap<>();
        FullSnapshotRestoreOperation<K> restoreOperation =
                new FullSnapshotRestoreOperation<>(
                        keyGroupRange,
                        userCodeClassLoader,
                        restoreStateHandles,
                        preflightKeySerializerProviderFactory.get());
        try (ThrowingIterator<SavepointRestoreResult> restore = restoreOperation.restore()) {
            while (restore.hasNext()) {
                SavepointRestoreResult result = restore.next();
                // The row iterator must be closed even if we never consume it, so that its
                // underlying stream is released before the import phase opens a fresh one.
                try (ThrowingIterator<KeyGroup> ignored = result.getRestoredKeyGroups()) {
                    recordMetadataFromHandle(result.getStateMetaInfoSnapshots(), metadata);
                }
            }
        }
        return metadata;
    }

    private void recordMetadataFromHandle(
            List<StateMetaInfoSnapshot> snapshots,
            LinkedHashMap<String, RestoredKeyedStateMetadata> metadata)
            throws IOException, StateMigrationException {
        for (StateMetaInfoSnapshot snapshot : snapshots) {
            switch (snapshot.getBackendStateType()) {
                case KEY_VALUE:
                    recordKeyValueMetadata(snapshot, metadata);
                    break;
                case PRIORITY_QUEUE:
                    recordPriorityQueueMetadata(snapshot, metadata);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Cobble canonical savepoint restore supports only keyed state and "
                                    + "priority queues, but found "
                                    + snapshot.getBackendStateType()
                                    + " for state '"
                                    + snapshot.getName()
                                    + "'.");
            }
        }
    }

    /**
     * Validates / records a KV metadata snapshot. Same state name across multiple handles must
     * agree on kind, state type, and serializer snapshots; the serializer check uses Flink's {@link
     * TypeSerializerSnapshot#resolveSchemaCompatibility} and accepts only {@code
     * isCompatibleAsIs()}.
     */
    private void recordKeyValueMetadata(
            StateMetaInfoSnapshot snapshot,
            LinkedHashMap<String, RestoredKeyedStateMetadata> metadata)
            throws IOException, StateMigrationException {
        RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(snapshot);
        StateDescriptor.Type stateType = metaInfo.getStateType();
        if (stateType != StateDescriptor.Type.VALUE
                && stateType != StateDescriptor.Type.LIST
                && stateType != StateDescriptor.Type.MAP
                && stateType != StateDescriptor.Type.REDUCING
                && stateType != StateDescriptor.Type.AGGREGATING) {
            throw new UnsupportedOperationException(
                    "Cobble canonical savepoint restore does not support "
                            + stateType
                            + " state '"
                            + metaInfo.getName()
                            + "'. Cobble currently supports VALUE, LIST, MAP, REDUCING, and"
                            + " AGGREGATING state.");
        }
        TypeSerializerSnapshot<?> namespaceSnapshot =
                snapshot.getTypeSerializerSnapshot(
                        StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER);
        TypeSerializerSnapshot<?> valueSnapshot =
                snapshot.getTypeSerializerSnapshot(
                        StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
        if (namespaceSnapshot == null || valueSnapshot == null) {
            throw new IOException(
                    "Canonical savepoint metadata for state '"
                            + metaInfo.getName()
                            + "' is missing a required serializer snapshot.");
        }

        String stateName = metaInfo.getName();
        RestoredKeyedStateMetadata existing = metadata.get(stateName);
        if (existing == null) {
            metadata.put(
                    stateName,
                    RestoredKeyedStateMetadata.keyValue(
                            stateName, stateType, namespaceSnapshot, valueSnapshot, snapshot));
            return;
        }
        if (existing.kind() != RestoredKeyedStateMetadata.Kind.KEY_VALUE) {
            throw new IOException(
                    "Canonical savepoint state '"
                            + stateName
                            + "' was previously seen as "
                            + existing.kind()
                            + " but now appears as KEY_VALUE.");
        }
        if (existing.stateType() != stateType) {
            throw new IOException(
                    "Canonical savepoint state '"
                            + stateName
                            + "' kind mismatch across handles: previously "
                            + existing.stateType()
                            + ", now "
                            + stateType
                            + ".");
        }
        rejectIfNotCompatibleAsIs(
                stateName,
                "namespace serializer",
                existing.namespaceSerializerSnapshot(),
                namespaceSnapshot.restoreSerializer(),
                "between canonical savepoint handles");
        rejectIfNotCompatibleAsIs(
                stateName,
                "value serializer",
                existing.stateSerializerSnapshot(),
                valueSnapshot.restoreSerializer(),
                "between canonical savepoint handles");
    }

    private void recordPriorityQueueMetadata(
            StateMetaInfoSnapshot snapshot,
            LinkedHashMap<String, RestoredKeyedStateMetadata> metadata)
            throws IOException, StateMigrationException {
        RegisteredPriorityQueueStateBackendMetaInfo<?> metaInfo =
                new RegisteredPriorityQueueStateBackendMetaInfo<>(snapshot);
        TypeSerializerSnapshot<?> elementSnapshot =
                snapshot.getTypeSerializerSnapshot(
                        StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
        if (elementSnapshot == null) {
            throw new IOException(
                    "Canonical savepoint metadata for priority-queue state '"
                            + metaInfo.getName()
                            + "' is missing its element serializer snapshot.");
        }

        String stateName = metaInfo.getName();
        RestoredKeyedStateMetadata existing = metadata.get(stateName);
        if (existing == null) {
            metadata.put(
                    stateName,
                    RestoredKeyedStateMetadata.priorityQueue(stateName, elementSnapshot, snapshot));
            return;
        }
        if (existing.kind() != RestoredKeyedStateMetadata.Kind.PRIORITY_QUEUE) {
            throw new IOException(
                    "Canonical savepoint state '"
                            + stateName
                            + "' was previously seen as "
                            + existing.kind()
                            + " but now appears as PRIORITY_QUEUE.");
        }
        rejectIfNotCompatibleAsIs(
                stateName,
                "timer element serializer",
                existing.elementSerializerSnapshot(),
                elementSnapshot.restoreSerializer(),
                "between canonical savepoint handles");
    }

    /**
     * Phase 2 — opens fresh streams to every handle, creates a column family per KV state, and
     * writes each row using the metadata accumulated in phase 1.
     */
    private void importRows(LinkedHashMap<String, RestoredKeyedStateMetadata> metadata)
            throws IOException, StateMigrationException {
        // Use the builder's own key-serializer provider here. FullSnapshotRestoreOperation will
        // call setPreviousSerializerSnapshotForRestoredState(...) on it, which is what lets
        // importKeySerializerProvider.currentSchemaSerializer() reflect any reconfigured key
        // serializer Flink picked (e.g. when resolveSchemaCompatibility returns
        // compatibleWithReconfiguredSerializer). The backend is constructed from that very
        // provider, so it ends up holding the reconfigured serializer instance.
        FullSnapshotRestoreOperation<K> restoreOperation =
                new FullSnapshotRestoreOperation<>(
                        keyGroupRange,
                        userCodeClassLoader,
                        restoreStateHandles,
                        importKeySerializerProvider);
        try (ThrowingIterator<SavepointRestoreResult> restore = restoreOperation.restore()) {
            while (restore.hasNext()) {
                restoreResult(restore.next(), importKeySerializerProvider);
            }
        }
    }

    private void restoreResult(
            SavepointRestoreResult restoreResult, StateSerializerProvider<K> keySerializerProvider)
            throws IOException, StateMigrationException {
        Map<Integer, RestoredState> statesById =
                createStates(restoreResult.getStateMetaInfoSnapshots());
        TypeSerializer<K> keySerializer = keySerializerProvider.previousSchemaSerializer();
        int keyGroupPrefixBytes =
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                        numberOfKeyGroups);

        try (ThrowingIterator<KeyGroup> keyGroups = restoreResult.getRestoredKeyGroups()) {
            while (keyGroups.hasNext()) {
                KeyGroup keyGroup = keyGroups.next();
                try (ThrowingIterator<KeyGroupEntry> entries = keyGroup.getKeyGroupEntries()) {
                    while (entries.hasNext()) {
                        restoreEntry(
                                keyGroup.getKeyGroupId(),
                                keyGroupPrefixBytes,
                                keySerializer,
                                statesById,
                                entries.next());
                    }
                }
            }
        }
    }

    private Map<Integer, RestoredState> createStates(List<StateMetaInfoSnapshot> snapshots)
            throws IOException {
        Map<Integer, RestoredState> statesById = new HashMap<>(snapshots.size());
        for (int stateId = 0; stateId < snapshots.size(); stateId++) {
            StateMetaInfoSnapshot snapshot = snapshots.get(stateId);
            RestoredState state;
            switch (snapshot.getBackendStateType()) {
                case KEY_VALUE:
                    RestoredKeyValueState kv = RestoredKeyValueState.from(snapshot);
                    ensureStateColumnFamily(kv.name());
                    state = kv;
                    break;
                case PRIORITY_QUEUE:
                    state = RestoredPriorityQueueState.from(snapshot);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Cobble canonical savepoint restore supports only keyed state and "
                                    + "priority queues, but found "
                                    + snapshot.getBackendStateType()
                                    + " for state '"
                                    + snapshot.getName()
                                    + "'.");
            }
            statesById.put(stateId, state);
        }
        return statesById;
    }

    /**
     * Runs {@link TypeSerializerSnapshot#resolveSchemaCompatibility} on {@code previous} against
     * {@code current} and rejects anything that is not {@code isCompatibleAsIs()}. Cobble Flink is
     * unreleased, so {@code compatibleAfterMigration} / {@code
     * compatibleWithReconfiguredSerializer} / {@code incompatible} are all treated as outright
     * incompatibilities and reported with the canonical / runtime class names.
     *
     * @param compareContext short prepositional phrase added to the failure message, e.g. {@code
     *     "between canonical savepoint handles"} or {@code "for the running job"}.
     */
    static void rejectIfNotCompatibleAsIs(
            String stateName,
            String role,
            TypeSerializerSnapshot<?> previous,
            TypeSerializer<?> current,
            String compareContext)
            throws StateMigrationException {
        @SuppressWarnings({"rawtypes", "unchecked"})
        TypeSerializerSchemaCompatibility<?> compatibility =
                ((TypeSerializerSnapshot) previous).resolveSchemaCompatibility(current);
        if (compatibility.isCompatibleAsIs()) {
            return;
        }
        String outcome;
        if (compatibility.isCompatibleAfterMigration()) {
            outcome = "compatible only after migration";
        } else if (compatibility.isCompatibleWithReconfiguredSerializer()) {
            outcome = "compatible only with a reconfigured serializer";
        } else {
            outcome = "incompatible";
        }
        throw new StateMigrationException(
                "Incompatible "
                        + role
                        + " for state '"
                        + stateName
                        + "' "
                        + compareContext
                        + ": canonical savepoint snapshot "
                        + previous.getClass().getName()
                        + " vs runtime serializer "
                        + current.getClass().getName()
                        + " — resolveSchemaCompatibility reported "
                        + outcome
                        + ". Cobble does not perform serializer migration on canonical restore.");
    }

    private void restoreEntry(
            int keyGroup,
            int keyGroupPrefixBytes,
            TypeSerializer<K> keySerializer,
            Map<Integer, RestoredState> statesById,
            KeyGroupEntry entry)
            throws IOException {
        RestoredState state = statesById.get(entry.getKvStateId());
        if (state == null) {
            throw new IOException(
                    "Canonical savepoint entry references unknown state id "
                            + entry.getKvStateId()
                            + ".");
        }
        state.restore(db, keyGroup, keyGroupPrefixBytes, keySerializer, entry);
    }

    private void ensureStateColumnFamily(String stateName) throws IOException {
        Map<Integer, io.cobble.structured.Schema.ColumnType> family =
                db.currentSchema().columnFamilies().get(stateName);
        if (family != null) {
            return;
        }
        try (StructuredSchemaBuilder builder = db.updateSchema()) {
            builder.setColumnFamilyOptions(stateName, ColumnFamilyOptions.defaults());
            builder.addBytesColumn(stateName, STATE_COLUMN_INDEX);
            builder.commit();
        }
    }

    private interface RestoredState {
        String name();

        void restore(
                Db db,
                int keyGroup,
                int keyGroupPrefixBytes,
                TypeSerializer<?> keySerializer,
                KeyGroupEntry entry)
                throws IOException;
    }

    private static final class RestoredKeyValueState implements RestoredState {
        private final String name;
        private final StateDescriptor.Type stateType;
        private final TypeSerializer<?> namespaceSerializer;
        private final TypeSerializer<?> valueSerializer;

        private RestoredKeyValueState(
                String name,
                StateDescriptor.Type stateType,
                TypeSerializer<?> namespaceSerializer,
                TypeSerializer<?> valueSerializer) {
            this.name = name;
            this.stateType = stateType;
            this.namespaceSerializer = namespaceSerializer;
            this.valueSerializer = valueSerializer;
        }

        static RestoredKeyValueState from(StateMetaInfoSnapshot snapshot) {
            RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(snapshot);
            StateDescriptor.Type stateType = metaInfo.getStateType();
            if (stateType != StateDescriptor.Type.VALUE
                    && stateType != StateDescriptor.Type.LIST
                    && stateType != StateDescriptor.Type.MAP
                    && stateType != StateDescriptor.Type.REDUCING
                    && stateType != StateDescriptor.Type.AGGREGATING) {
                throw new UnsupportedOperationException(
                        "Cobble canonical savepoint restore does not support "
                                + stateType
                                + " state '"
                                + metaInfo.getName()
                                + "'. Cobble currently supports VALUE, LIST, MAP, REDUCING, and"
                                + " AGGREGATING state.");
            }
            return new RestoredKeyValueState(
                    metaInfo.getName(),
                    stateType,
                    metaInfo.getPreviousNamespaceSerializer(),
                    metaInfo.getPreviousStateSerializer());
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public void restore(
                Db db,
                int keyGroup,
                int keyGroupPrefixBytes,
                TypeSerializer<?> keySerializer,
                KeyGroupEntry entry)
                throws IOException {
            DataInputDeserializer keyInput = new DataInputDeserializer(entry.getKey());
            int serializedKeyGroup =
                    CompositeKeySerializationUtils.readKeyGroup(keyGroupPrefixBytes, keyInput);
            if (serializedKeyGroup != keyGroup) {
                throw new IOException(
                        "Canonical savepoint key-group mismatch for state '"
                                + name
                                + "': iterator="
                                + keyGroup
                                + ", key="
                                + serializedKeyGroup
                                + '.');
            }
            boolean ambiguousKey =
                    CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                            keySerializer, namespaceSerializer);
            Object key =
                    CompositeKeySerializationUtils.readKey(
                            (TypeSerializer) keySerializer, keyInput, ambiguousKey);
            Object namespace =
                    CompositeKeySerializationUtils.readNamespace(
                            (TypeSerializer) namespaceSerializer, keyInput, ambiguousKey);

            switch (stateType) {
                case VALUE:
                case REDUCING:
                case AGGREGATING:
                    // ReducingState and AggregatingState share the VALUE storage shape (single
                    // serialized payload in column 0); canonical bytes are wire-identical to what
                    // Cobble{Reducing,Aggregating}State would write (the persisted type is the
                    // accumulator for AGGREGATING), so import them verbatim with no transformation.
                    db.put(
                            keyGroup,
                            buildKeyAndNamespace(
                                    key, namespace, keySerializer, namespaceSerializer),
                            name,
                            STATE_COLUMN_INDEX,
                            entry.getValue());
                    return;
                case LIST:
                    db.put(
                            keyGroup,
                            buildKeyAndNamespace(
                                    key, namespace, keySerializer, namespaceSerializer),
                            name,
                            STATE_COLUMN_INDEX,
                            appendListDelimiter(entry.getValue()));
                    return;
                case MAP:
                    restoreMapEntry(
                            db,
                            keyGroup,
                            keyInput,
                            key,
                            namespace,
                            entry.getValue(),
                            keySerializer);
                    return;
                default:
                    throw new IllegalStateException(
                            "Unsupported restored state type: " + stateType);
            }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private void restoreMapEntry(
                Db db,
                int keyGroup,
                DataInputDeserializer keyInput,
                Object key,
                Object namespace,
                byte[] rawValue,
                TypeSerializer<?> keySerializer)
                throws IOException {
            MapSerializer mapSerializer = (MapSerializer) valueSerializer;
            Object userKey = mapSerializer.getKeySerializer().deserialize(keyInput);
            // Canonical MapState present-non-null values match Cobble's row-value layout:
            // leading isNull=false byte + user-value bytes. Present-null rows are looser in real
            // RocksDB canonical savepoints: after the isNull=true marker, RocksDB may keep
            // serializer-produced bytes, and its read path ignores those bytes. Cobble validates
            // that shape, imports the canonical bytes verbatim, and its decode path also ignores
            // the trailing bytes for present-null rows.
            MapValueCodec.validate(rawValue, mapSerializer.getValueSerializer());
            db.put(
                    keyGroup,
                    buildMapKey(
                            key,
                            namespace,
                            userKey,
                            keySerializer,
                            namespaceSerializer,
                            mapSerializer.getKeySerializer()),
                    name,
                    STATE_COLUMN_INDEX,
                    rawValue);
        }
    }

    private static final class RestoredPriorityQueueState implements RestoredState {
        private final String name;
        private final TypeSerializer<?> elementSerializer;

        private RestoredPriorityQueueState(String name, TypeSerializer<?> elementSerializer) {
            this.name = name;
            this.elementSerializer = elementSerializer;
        }

        static RestoredPriorityQueueState from(StateMetaInfoSnapshot snapshot) {
            RegisteredPriorityQueueStateBackendMetaInfo<?> metaInfo =
                    new RegisteredPriorityQueueStateBackendMetaInfo<>(snapshot);
            return new RestoredPriorityQueueState(
                    metaInfo.getName(), metaInfo.getPreviousElementSerializer());
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public void restore(
                Db db,
                int keyGroup,
                int keyGroupPrefixBytes,
                TypeSerializer<?> keySerializer,
                KeyGroupEntry entry)
                throws IOException {
            DataInputDeserializer keyInput = new DataInputDeserializer(entry.getKey());
            int serializedKeyGroup =
                    CompositeKeySerializationUtils.readKeyGroup(keyGroupPrefixBytes, keyInput);
            if (serializedKeyGroup != keyGroup) {
                throw new IOException(
                        "Canonical savepoint timer key-group mismatch for state '" + name + "'.");
            }
            Object timer = ((TypeSerializer) elementSerializer).deserialize(keyInput);
            byte[] timerBytes =
                    CobbleStateKeySerializer.serialize((TypeSerializer) elementSerializer, timer);
            try (PriorityQueue queue =
                    db.getOrNewPriorityQueue(
                            CobblePriorityQueueSetFactory.timerQueueColumnFamilyName(name))) {
                queue.offer(keyGroup, timerBytes, EMPTY_TIMER_VALUE);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static byte[] buildKeyAndNamespace(
            Object key,
            Object namespace,
            TypeSerializer<?> keySerializer,
            TypeSerializer<?> namespaceSerializer)
            throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(128);
        ((TypeSerializer) keySerializer).serialize(key, output);
        int keyLength = output.length();
        if (namespace != null) {
            ((TypeSerializer) namespaceSerializer).serialize(namespace, output);
        }
        if (CobbleStateKeySerializer.shouldStoreKeyLengthForKeyNamespace(
                CobbleStateKeySerializer.maybeFixedLength(keySerializer),
                CobbleStateKeySerializer.maybeFixedLength(namespaceSerializer))) {
            output.writeInt(keyLength);
        }
        return output.getCopyOfBuffer();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static byte[] buildMapKey(
            Object key,
            Object namespace,
            Object userKey,
            TypeSerializer<?> keySerializer,
            TypeSerializer<?> namespaceSerializer,
            TypeSerializer<?> userKeySerializer)
            throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(128);
        ((TypeSerializer) keySerializer).serialize(key, output);
        int keyLength = output.length();
        if (namespace != null) {
            ((TypeSerializer) namespaceSerializer).serialize(namespace, output);
        }
        int namespaceLength = output.length() - keyLength;
        output.writeByte(0);
        ((TypeSerializer) userKeySerializer).serialize(userKey, output);
        int keyLengthTag = CobbleStateKeySerializer.maybeFixedLength(keySerializer);
        int namespaceLengthTag = CobbleStateKeySerializer.maybeFixedLength(namespaceSerializer);
        int userKeyLengthTag = CobbleStateKeySerializer.maybeFixedLength(userKeySerializer);
        if (CobbleStateKeySerializer.shouldStoreMapKeyLength(
                keyLengthTag, namespaceLengthTag, userKeyLengthTag)) {
            output.writeInt(keyLength);
        }
        if (CobbleStateKeySerializer.shouldStoreMapNamespaceLength(
                keyLengthTag, namespaceLengthTag, userKeyLengthTag)) {
            output.writeInt(namespaceLength);
        }
        return output.getCopyOfBuffer();
    }

    private static byte[] appendListDelimiter(byte[] bytes) {
        if (bytes.length == 0) {
            return bytes;
        }
        byte[] result = Arrays.copyOf(bytes, bytes.length + 1);
        result[bytes.length] = LIST_DELIMITER;
        return result;
    }
}
