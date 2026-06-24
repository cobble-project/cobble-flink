package io.cobble.flink.common.inspect;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/** Optional semantic shapes for the logical parts of one keyed state. */
public final class StateInspectSemanticSchema {

    private final StateInspectType stateKey;
    private final StateInspectType namespace;
    private final StateInspectType value;
    private final StateInspectType listElement;
    private final StateInspectType mapUserKey;
    private final StateInspectType mapUserValue;

    private StateInspectSemanticSchema(
            StateInspectType stateKey,
            StateInspectType namespace,
            StateInspectType value,
            StateInspectType listElement,
            StateInspectType mapUserKey,
            StateInspectType mapUserValue) {
        this.stateKey = stateKey;
        this.namespace = namespace;
        this.value = value;
        this.listElement = listElement;
        this.mapUserKey = mapUserKey;
        this.mapUserValue = mapUserValue;
    }

    public static StateInspectSemanticSchema forValue(
            StateInspectType stateKey, StateInspectType namespace, StateInspectType value) {
        return new StateInspectSemanticSchema(stateKey, namespace, value, null, null, null);
    }

    /** Reducing state has the same logical part shape as value state. */
    public static StateInspectSemanticSchema forReducing(
            StateInspectType stateKey, StateInspectType namespace, StateInspectType value) {
        return new StateInspectSemanticSchema(stateKey, namespace, value, null, null, null);
    }

    public static StateInspectSemanticSchema forList(
            StateInspectType stateKey, StateInspectType namespace, StateInspectType listElement) {
        return new StateInspectSemanticSchema(stateKey, namespace, null, listElement, null, null);
    }

    public static StateInspectSemanticSchema forMap(
            StateInspectType stateKey,
            StateInspectType namespace,
            StateInspectType mapUserKey,
            StateInspectType mapUserValue) {
        return new StateInspectSemanticSchema(
                stateKey, namespace, null, null, mapUserKey, mapUserValue);
    }

    public static StateInspectSemanticSchema empty() {
        return new StateInspectSemanticSchema(null, null, null, null, null, null);
    }

    public StateInspectType stateKey() {
        return stateKey;
    }

    public StateInspectType namespace() {
        return namespace;
    }

    public StateInspectType value() {
        return value;
    }

    public StateInspectType listElement() {
        return listElement;
    }

    public StateInspectType mapUserKey() {
        return mapUserKey;
    }

    public StateInspectType mapUserValue() {
        return mapUserValue;
    }

    public boolean isEmpty() {
        return stateKey == null
                && namespace == null
                && value == null
                && listElement == null
                && mapUserKey == null
                && mapUserValue == null;
    }

    void write(DataOutputView output) throws IOException {
        writeNullableType(output, stateKey);
        writeNullableType(output, namespace);
        writeNullableType(output, value);
        writeNullableType(output, listElement);
        writeNullableType(output, mapUserKey);
        writeNullableType(output, mapUserValue);
    }

    static StateInspectSemanticSchema read(DataInputView input) throws IOException {
        return new StateInspectSemanticSchema(
                readNullableType(input),
                readNullableType(input),
                readNullableType(input),
                readNullableType(input),
                readNullableType(input),
                readNullableType(input));
    }

    private static void writeNullableType(DataOutputView output, StateInspectType type)
            throws IOException {
        output.writeBoolean(type != null);
        if (type != null) {
            type.write(output);
        }
    }

    private static StateInspectType readNullableType(DataInputView input) throws IOException {
        return input.readBoolean() ? StateInspectType.read(input) : null;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StateInspectSemanticSchema)) {
            return false;
        }
        StateInspectSemanticSchema that = (StateInspectSemanticSchema) other;
        return Objects.equals(stateKey, that.stateKey)
                && Objects.equals(namespace, that.namespace)
                && Objects.equals(value, that.value)
                && Objects.equals(listElement, that.listElement)
                && Objects.equals(mapUserKey, that.mapUserKey)
                && Objects.equals(mapUserValue, that.mapUserValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateKey, namespace, value, listElement, mapUserKey, mapUserValue);
    }
}
