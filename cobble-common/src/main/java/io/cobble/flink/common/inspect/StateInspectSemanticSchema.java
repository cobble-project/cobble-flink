package io.cobble.flink.common.inspect;

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
