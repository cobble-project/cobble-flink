package io.cobble.flink.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.util.Objects;

/**
 * Canonical-savepoint state metadata kept inside the backend until the user's state descriptor
 * registers and is validated against it. Two kinds of metadata are represented:
 *
 * <ul>
 *   <li>{@link Kind#KEY_VALUE} for {@code ValueState}, {@code ListState}, and {@code MapState}.
 *   <li>{@link Kind#PRIORITY_QUEUE} for timer / priority-queue state.
 * </ul>
 *
 * <p>The serializer information is preserved as {@link TypeSerializerSnapshot} (not just the Java
 * class) so that the backend can run Flink's standard {@link
 * TypeSerializerSnapshot#resolveSchemaCompatibility} against the runtime serializer's snapshot.
 * Cobble Flink is unreleased so we accept <em>only</em> {@code isCompatibleAsIs()}; everything else
 * (migration / reconfigured / incompatible) is rejected, even when the outer Java class matches but
 * a nested type serializer differs.
 *
 * <p>The backend looks up an entry by state name when the running job registers a descriptor for
 * the same state. A registry hit triggers the compatibility check; a registry miss is fine and
 * means the state did not exist in the canonical savepoint (Flink permits adding new states after
 * restore).
 */
final class RestoredKeyedStateMetadata {

    enum Kind {
        KEY_VALUE,
        PRIORITY_QUEUE
    }

    private final String stateName;
    private final Kind kind;
    private final StateMetaInfoSnapshot snapshot;
    // KV-only:
    private final StateDescriptor.Type stateType;
    private final TypeSerializerSnapshot<?> namespaceSerializerSnapshot;
    private final TypeSerializerSnapshot<?> stateSerializerSnapshot;
    // Priority-queue-only:
    private final TypeSerializerSnapshot<?> elementSerializerSnapshot;

    private RestoredKeyedStateMetadata(
            String stateName,
            Kind kind,
            StateMetaInfoSnapshot snapshot,
            StateDescriptor.Type stateType,
            TypeSerializerSnapshot<?> namespaceSerializerSnapshot,
            TypeSerializerSnapshot<?> stateSerializerSnapshot,
            TypeSerializerSnapshot<?> elementSerializerSnapshot) {
        this.stateName = Objects.requireNonNull(stateName);
        this.kind = Objects.requireNonNull(kind);
        this.snapshot = Objects.requireNonNull(snapshot);
        this.stateType = stateType;
        this.namespaceSerializerSnapshot = namespaceSerializerSnapshot;
        this.stateSerializerSnapshot = stateSerializerSnapshot;
        this.elementSerializerSnapshot = elementSerializerSnapshot;
    }

    static RestoredKeyedStateMetadata keyValue(
            String stateName,
            StateDescriptor.Type stateType,
            TypeSerializerSnapshot<?> namespaceSerializerSnapshot,
            TypeSerializerSnapshot<?> stateSerializerSnapshot,
            StateMetaInfoSnapshot snapshot) {
        return new RestoredKeyedStateMetadata(
                stateName,
                Kind.KEY_VALUE,
                snapshot,
                Objects.requireNonNull(stateType),
                Objects.requireNonNull(namespaceSerializerSnapshot),
                Objects.requireNonNull(stateSerializerSnapshot),
                null);
    }

    static RestoredKeyedStateMetadata priorityQueue(
            String stateName,
            TypeSerializerSnapshot<?> elementSerializerSnapshot,
            StateMetaInfoSnapshot snapshot) {
        return new RestoredKeyedStateMetadata(
                stateName,
                Kind.PRIORITY_QUEUE,
                snapshot,
                null,
                null,
                null,
                Objects.requireNonNull(elementSerializerSnapshot));
    }

    String stateName() {
        return stateName;
    }

    Kind kind() {
        return kind;
    }

    StateMetaInfoSnapshot snapshot() {
        return snapshot;
    }

    StateDescriptor.Type stateType() {
        return stateType;
    }

    TypeSerializerSnapshot<?> namespaceSerializerSnapshot() {
        return namespaceSerializerSnapshot;
    }

    TypeSerializerSnapshot<?> stateSerializerSnapshot() {
        return stateSerializerSnapshot;
    }

    TypeSerializerSnapshot<?> elementSerializerSnapshot() {
        return elementSerializerSnapshot;
    }
}
