package io.cobble.flink.state;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable snapshot of all in-memory timer overlay elements, captured in the synchronous phase of
 * {@code savepoint()} so the asynchronous writer sees a consistent timer set.
 *
 * <p>Each entry maps a timer state name to a list of pre-serialized overlay key bytes for a
 * specific key group. The keys are already serialized via {@link
 * CobbleTimerSerializationContext#serializeElementKey(Object)}, so the async iterator does not need
 * to access the live priority queue or its element serializer.
 *
 * <p>This class is immutable and holds no references to live queue state.
 */
final class CobbleTimerOverlaySnapshot {

    /**
     * Keyed by state name. Each value maps a key-group to the list of serialized overlay timer keys
     * for that key group.
     */
    private final Map<String, Map<Integer, List<byte[]>>> overlayKeysByStateAndKeyGroup;

    private CobbleTimerOverlaySnapshot(
            Map<String, Map<Integer, List<byte[]>>> overlayKeysByStateAndKeyGroup) {
        this.overlayKeysByStateAndKeyGroup = overlayKeysByStateAndKeyGroup;
    }

    static CobbleTimerOverlaySnapshot empty() {
        return new CobbleTimerOverlaySnapshot(Collections.emptyMap());
    }

    /**
     * Creates a builder for collecting overlay timer keys.
     *
     * @return a new builder
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the serialized overlay timer keys for the given state name and key group, or an empty
     * list if none.
     */
    List<byte[]> overlayKeysFor(String stateName, int keyGroup) {
        Map<Integer, List<byte[]>> byKeyGroup = overlayKeysByStateAndKeyGroup.get(stateName);
        if (byKeyGroup == null) {
            return Collections.emptyList();
        }
        List<byte[]> keys = byKeyGroup.get(keyGroup);
        return keys == null ? Collections.emptyList() : keys;
    }

    /** Builder for collecting overlay timer keys during the sync phase. */
    static final class Builder {
        private final Map<String, Map<Integer, List<byte[]>>> data = new LinkedHashMap<>();

        /**
         * Adds serialized overlay timer keys for a state name and key group.
         *
         * @param stateName the timer state name
         * @param keyGroup the key group
         * @param keys the pre-serialized overlay timer key bytes (already in Cobble timer key
         *     format)
         */
        void addOverlayKeys(String stateName, int keyGroup, List<byte[]> keys) {
            if (keys == null || keys.isEmpty()) {
                return;
            }
            data.computeIfAbsent(stateName, k -> new LinkedHashMap<>())
                    .put(keyGroup, Collections.unmodifiableList(keys));
        }

        CobbleTimerOverlaySnapshot build() {
            Map<String, Map<Integer, List<byte[]>>> copy = new LinkedHashMap<>();
            for (Map.Entry<String, Map<Integer, List<byte[]>>> entry : data.entrySet()) {
                copy.put(
                        entry.getKey(),
                        Collections.unmodifiableMap(new LinkedHashMap<>(entry.getValue())));
            }
            return new CobbleTimerOverlaySnapshot(Collections.unmodifiableMap(copy));
        }
    }
}
