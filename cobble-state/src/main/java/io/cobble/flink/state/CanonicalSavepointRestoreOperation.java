package io.cobble.flink.state;

import io.cobble.ColumnFamilyOptions;
import io.cobble.structured.Db;
import io.cobble.structured.PriorityQueue;
import io.cobble.structured.StructuredSchemaBuilder;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Imports Flink's unified canonical savepoint format into a newly opened Cobble DB. */
final class CanonicalSavepointRestoreOperation<K> {

    private static final int STATE_COLUMN_INDEX = 0;
    private static final byte LIST_DELIMITER = ',';
    private static final byte[] EMPTY_TIMER_VALUE = new byte[0];

    private final Db db;
    private final KeyGroupRange keyGroupRange;
    private final int numberOfKeyGroups;
    private final ClassLoader userCodeClassLoader;
    private final Collection<KeyedStateHandle> restoreStateHandles;
    private final StateSerializerProvider<K> keySerializerProvider;

    CanonicalSavepointRestoreOperation(
            Db db,
            KeyGroupRange keyGroupRange,
            int numberOfKeyGroups,
            ClassLoader userCodeClassLoader,
            Collection<KeyedStateHandle> restoreStateHandles,
            StateSerializerProvider<K> keySerializerProvider) {
        this.db = db;
        this.keyGroupRange = keyGroupRange;
        this.numberOfKeyGroups = numberOfKeyGroups;
        this.userCodeClassLoader = userCodeClassLoader;
        this.restoreStateHandles = restoreStateHandles;
        this.keySerializerProvider = keySerializerProvider;
    }

    void restore() throws IOException, StateMigrationException {
        FullSnapshotRestoreOperation<K> restoreOperation =
                new FullSnapshotRestoreOperation<>(
                        keyGroupRange,
                        userCodeClassLoader,
                        restoreStateHandles,
                        keySerializerProvider);
        try (ThrowingIterator<SavepointRestoreResult> restore = restoreOperation.restore()) {
            while (restore.hasNext()) {
                restoreResult(restore.next());
            }
        }
    }

    private void restoreResult(SavepointRestoreResult restoreResult)
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
                    state = RestoredKeyValueState.from(snapshot);
                    ensureStateColumnFamily(state.name());
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
                    && stateType != StateDescriptor.Type.MAP) {
                throw new UnsupportedOperationException(
                        "Cobble canonical savepoint restore does not support "
                                + stateType
                                + " state '"
                                + metaInfo.getName()
                                + "'. Cobble currently supports VALUE, LIST, and MAP state.");
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
            DataInputDeserializer valueInput = new DataInputDeserializer(rawValue);
            if (valueInput.readBoolean()) {
                throw new UnsupportedOperationException(
                        "Cobble canonical savepoint restore does not support null MapState values "
                                + "for state '"
                                + name
                                + "'.");
            }
            Object userValue = mapSerializer.getValueSerializer().deserialize(valueInput);
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
                    CobbleStateKeySerializer.serialize(
                            mapSerializer.getValueSerializer(), userValue));
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
