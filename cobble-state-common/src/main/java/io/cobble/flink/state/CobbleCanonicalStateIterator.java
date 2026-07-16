package io.cobble.flink.state;

import io.cobble.Reader;
import io.cobble.ScanCursor;
import io.cobble.ScanOptions;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * {@link KeyValueStateIterator} that scans a Cobble snapshot reader and produces entries in
 * canonical savepoint order: {@code (keyGroup, kvStateId, key)}.
 *
 * <p>The iterator is created with a {@link Reader} pinned to a global snapshot, so all scans read a
 * stable point-in-time view — not the live DB. Overlay timer keys are provided as a pre-serialized
 * immutable snapshot ({@link CobbleTimerOverlaySnapshot}) captured in the sync phase.
 *
 * <p>For each key group in {@link KeyGroupRange} (ascending), and for each registered state in
 * {@code kvStateId} order, the iterator opens a per-bucket scan in the state's column family and
 * emits entries one at a time. KV state keys are transformed from Cobble row-key layout to Flink
 * canonical composite-key layout via {@link CobbleCanonicalKeyEncoder}. Timer states merge native
 * queue rows (from the reader scan) with the immutable overlay timer snapshot.
 *
 * <p>The iterator is pre-positioned on the first valid entry by the constructor. {@link
 * #isValid()} returns false when all key groups and states are exhausted.
 */
final class CobbleCanonicalStateIterator<K> implements KeyValueStateIterator {

    private static final int CANONICAL_SCAN_READ_AHEAD_BYTES = 256 * 1024;

    private final Reader reader;
    private final List<CobbleCanonicalStateMeta> entries;
    private final KeyGroupRange keyGroupRange;
    private final CobbleCanonicalKeyEncoder keyEncoder;
    private final CobbleTimerOverlaySnapshot timerOverlaySnapshot;
    private final TypeSerializer<K> keySerializer;

    /** Reconstructed per-state serializer info, indexed by kvStateId. */
    private final StateSerializerInfo[] serializerInfos;

    // -- Iteration state --

    private final List<Integer> keyGroupList;

    private int keyGroupIndex;
    private int stateIndex;

    /** Active scan cursor for the current (keyGroup, state), or null for timer iteration. */
    private ScanCursor currentScanCursor;
    private Iterator<ScanCursor.Entry> currentEntryIterator;

    /** Buffered timer entries (sorted, deduplicated) for the current (keyGroup, timer state). */
    private Iterator<byte[]> timerKeyIterator;

    /** Current entry values. */
    private int currentKeyGroup;
    private int currentKvStateId;
    private byte[] currentKey;
    private byte[] currentValue;

    /** Previous entry values for boundary detection. */
    private int previousKeyGroup = -1;
    private int previousKvStateId = -1;

    private boolean valid;

    /**
     * @param reader a reader pinned to a global snapshot
     * @param metadataSnapshot the immutable metadata snapshot from the backend
     * @param keyGroupRange the local key-group range
     * @param totalKeyGroups the total number of key groups (for key-group prefix width)
     * @param keySerializer the key serializer
     * @param timerOverlaySnapshot immutable overlay timer keys captured in the sync phase
     */
    CobbleCanonicalStateIterator(
            Reader reader,
            CobbleCanonicalSavepointMetadataSnapshot metadataSnapshot,
            KeyGroupRange keyGroupRange,
            int totalKeyGroups,
            TypeSerializer<K> keySerializer,
            CobbleTimerOverlaySnapshot timerOverlaySnapshot)
            throws IOException {
        this.reader = reader;
        this.entries = metadataSnapshot.entries();
        this.keyGroupRange = keyGroupRange;
        this.keyEncoder = new CobbleCanonicalKeyEncoder(totalKeyGroups);
        this.keySerializer = keySerializer;
        this.timerOverlaySnapshot = timerOverlaySnapshot;

        if (entries.size() > Short.MAX_VALUE) {
            throw new IOException(
                    "Cannot create canonical savepoint: "
                            + entries.size()
                            + " registered states exceed the maximum of "
                            + Short.MAX_VALUE
                            + " (kvStateId is written as a short).");
        }

        this.serializerInfos = reconstructSerializerInfos(entries);

        // Build the ascending key-group list from the range.
        this.keyGroupList = new ArrayList<>(keyGroupRange.getNumberOfKeyGroups());
        keyGroupRange.forEach(keyGroupList::add);

        this.keyGroupIndex = 0;
        this.stateIndex = 0;

        // Pre-position on the first valid entry.
        advanceToNextValid();
    }

    // ------------------------------------------------------------------------------------------
    //  KeyValueStateIterator contract
    // ------------------------------------------------------------------------------------------

    @Override
    public boolean isValid() {
        return valid;
    }

    @Override
    public int keyGroup() {
        return currentKeyGroup;
    }

    @Override
    public byte[] key() {
        return currentKey;
    }

    @Override
    public byte[] value() {
        return currentValue;
    }

    @Override
    public int kvStateId() {
        return currentKvStateId;
    }

    @Override
    public boolean isNewKeyGroup() {
        return currentKeyGroup != previousKeyGroup;
    }

    @Override
    public boolean isNewKeyValueState() {
        return currentKvStateId != previousKvStateId;
    }

    @Override
    public void next() throws IOException {
        if (!valid) {
            return;
        }
        // Record current as previous before advancing.
        previousKeyGroup = currentKeyGroup;
        previousKvStateId = currentKvStateId;
        advanceToNextValid();
    }

    @Override
    public void close() {
        closeCurrentScan();
    }

    // ------------------------------------------------------------------------------------------
    //  Core advance logic
    // ------------------------------------------------------------------------------------------

    /**
     * Advances to the next valid entry, skipping empty (keyGroup, state) pairs. When exhausted,
     * sets {@link #valid} to false.
     */
    private void advanceToNextValid() throws IOException {
        while (keyGroupIndex < keyGroupList.size()) {
            int keyGroup = keyGroupList.get(keyGroupIndex);

            // If no source is open for the current (keyGroup, state), open one.
            if (currentEntryIterator == null && timerKeyIterator == null) {
                if (!openStateForCurrentKeyGroup(keyGroup)) {
                    // State couldn't be opened (e.g. unknown column family) — skip to next state.
                    advanceToNextState();
                    continue;
                }
            }

            // Try to read the next entry from the currently active source.
            if (tryAdvanceCurrent(keyGroup)) {
                valid = true;
                return;
            }

            // Current source exhausted — advance to the next state.
            closeCurrentScan();
            advanceToNextState();
        }

        // All exhausted.
        valid = false;
        currentKey = null;
        currentValue = null;
    }

    /**
     * Advances the (keyGroup, stateIndex) cursor to the next state, wrapping to the next key group
     * when all states for the current key group are exhausted.
     */
    private void advanceToNextState() {
        stateIndex++;
        if (stateIndex >= entries.size()) {
            stateIndex = 0;
            keyGroupIndex++;
        }
    }

    /**
     * Tries to read the next entry from the currently active cursor / timer iterator. Returns true
     * if an entry was read, false if the current source is exhausted.
     */
    private boolean tryAdvanceCurrent(int keyGroup) throws IOException {
        if (currentEntryIterator != null && currentEntryIterator.hasNext()) {
            ScanCursor.Entry entry = currentEntryIterator.next();
            emitKvStateRow(keyGroup, entries.get(stateIndex), entry);
            return true;
        }
        if (timerKeyIterator != null && timerKeyIterator.hasNext()) {
            byte[] timerKey = timerKeyIterator.next();
            emitTimerEntry(keyGroup, entries.get(stateIndex), timerKey);
            return true;
        }
        return false;
    }

    /**
     * Opens the scan or timer collection for the current (keyGroup, stateIndex). Returns true if the
     * source was successfully opened (even if empty), false if the state should be skipped.
     */
    private boolean openStateForCurrentKeyGroup(int keyGroup) throws IOException {
        closeCurrentScan();
        CobbleCanonicalStateMeta meta = entries.get(stateIndex);

        if (meta.priorityQueue()) {
            return openTimerState(keyGroup, meta);
        } else {
            return openKvState(keyGroup, meta);
        }
    }

    // ------------------------------------------------------------------------------------------
    //  KV state scanning
    // ------------------------------------------------------------------------------------------

    /**
     * Opens a scan for a KV state in the current key group. Returns true if the scan was opened
     * (even if empty), false if the column family doesn't exist in this snapshot (treated as an
     * empty state). Any other I/O error is propagated.
     */
    private boolean openKvState(int keyGroup, CobbleCanonicalStateMeta meta) throws IOException {
        // The plain Reader API rejects null start/end keys, so use an empty start (smallest
        // key) and a 0xFF-filled end (practical upper bound) to scan the entire bucket.
        try (ScanOptions options =
                new ScanOptions()
                        .readAheadBytes(CANONICAL_SCAN_READ_AHEAD_BYTES)
                        .columnFamily(meta.columnFamily())
                        .columns(0)) {
            try {
                currentScanCursor =
                        reader.scanWithOptions(keyGroup, EMPTY_SCAN_KEY, MAX_SCAN_KEY, options);
            } catch (RuntimeException e) {
                if (isUnknownColumnFamily(e)) {
                    // Column family doesn't exist in this snapshot — no data for this state.
                    return false;
                }
                throw e;
            }
        }
        currentEntryIterator = currentScanCursor.iterator();
        return true;
    }

    /**
     * Reads an entry from the KV scan and transforms it to canonical format, setting the current
     * entry fields.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void emitKvStateRow(int keyGroup, CobbleCanonicalStateMeta meta, ScanCursor.Entry entry)
            throws IOException {
        StateSerializerInfo info = serializerInfos[meta.kvStateId()];
        byte[] cobbleRowKey = entry.key;
        byte[] cobbleValue;
        if (entry.columns != null && entry.columns.length > 0) {
            cobbleValue = entry.columns[0];
        } else {
            throw new IOException(
                    "Missing value column (index 0) for state '"
                            + meta.stateName()
                            + "' in key group "
                            + keyGroup
                            + " — snapshot may be corrupt.");
        }

        byte[] canonicalKey;
        if (meta.isMapState()) {
            canonicalKey =
                    keyEncoder.encodeMapKey(
                            keyGroup,
                            cobbleRowKey,
                            keySerializer,
                            info.namespaceSerializer,
                            info.mapUserKeySerializer,
                            meta.stateName());
        } else {
            canonicalKey =
                    keyEncoder.encodeSimpleKey(
                            keyGroup,
                            cobbleRowKey,
                            keySerializer,
                            info.namespaceSerializer,
                            meta.stateName());
        }

        byte[] canonicalValue = transformKvValue(meta, cobbleValue);

        currentKeyGroup = keyGroup;
        currentKvStateId = meta.kvStateId();
        currentKey = canonicalKey;
        currentValue = canonicalValue;
    }

    /**
     * Transforms a Cobble value to canonical format. LIST state strips the trailing delimiter; all
     * other kinds pass through.
     */
    private static byte[] transformKvValue(CobbleCanonicalStateMeta meta, byte[] cobbleValue) {
        if (meta.stateType() == StateDescriptor.Type.LIST) {
            return CobbleCanonicalValueEncoder.toCanonicalListValue(cobbleValue);
        }
        return cobbleValue;
    }

    // ------------------------------------------------------------------------------------------
    //  Timer state scanning
    // ------------------------------------------------------------------------------------------

    /**
     * Collects all timer keys (native + overlay) for the current key group, sorted and deduplicated.
     * Returns true if the collection was opened (even if empty). If the timer column family doesn't
     * exist in this snapshot, the native scan is skipped (only overlay keys are collected) — this is
     * not an error, just an empty native timer set.
     */
    private boolean openTimerState(int keyGroup, CobbleCanonicalStateMeta meta) {
        // Collect native timer keys from the reader scan.
        TreeSet<byte[]> sortedKeys = new TreeSet<>(CobbleTimerSerializationContext::compareSerializedKeys);

        ScanCursor scan = null;
        try (ScanOptions options =
                new ScanOptions()
                        .readAheadBytes(CANONICAL_SCAN_READ_AHEAD_BYTES)
                        .columnFamily(meta.columnFamily())
                        .columns(0)) {
            scan = reader.scanWithOptions(keyGroup, EMPTY_SCAN_KEY, MAX_SCAN_KEY, options);
            for (ScanCursor.Entry entry : scan) {
                sortedKeys.add(entry.key);
            }
        } catch (RuntimeException e) {
            if (isUnknownColumnFamily(e)) {
                // Column family doesn't exist in this snapshot — no native timer data.
            } else {
                throw e;
            }
        } finally {
            if (scan != null) {
                try {
                    scan.close();
                } catch (RuntimeException ignored) {
                    // best effort
                }
            }
        }

        // Collect overlay timer keys from the immutable sync-phase snapshot.
        for (byte[] overlayKey : timerOverlaySnapshot.overlayKeysFor(meta.stateName(), keyGroup)) {
            sortedKeys.add(overlayKey);
        }

        timerKeyIterator = sortedKeys.iterator();
        return true;
    }

    /**
     * Emits a timer entry: the canonical key is the key-group prefix + raw timer bytes, and the
     * value is empty bytes.
     */
    private void emitTimerEntry(int keyGroup, CobbleCanonicalStateMeta meta, byte[] timerKey)
            throws IOException {
        currentKeyGroup = keyGroup;
        currentKvStateId = meta.kvStateId();
        currentKey = keyEncoder.encodeTimerKey(keyGroup, timerKey);
        currentValue = EMPTY_BYTES;
    }

    // ------------------------------------------------------------------------------------------
    //  Serializer reconstruction
    // ------------------------------------------------------------------------------------------

    /**
     * Reconstructs per-state serializer information from the {@link StateMetaInfoSnapshot}s. This
     * avoids holding live serializer references from the backend and instead rebuilds them from the
     * frozen snapshots — exactly as the restore path does.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static StateSerializerInfo[] reconstructSerializerInfos(
            List<CobbleCanonicalStateMeta> entries) {
        StateSerializerInfo[] infos = new StateSerializerInfo[entries.size()];
        for (CobbleCanonicalStateMeta meta : entries) {
            StateMetaInfoSnapshot snapshot = meta.metaInfoSnapshot();
            StateSerializerInfo info = new StateSerializerInfo();

            if (meta.priorityQueue()) {
                RegisteredPriorityQueueStateBackendMetaInfo<?> pqMeta =
                        new RegisteredPriorityQueueStateBackendMetaInfo<>(snapshot);
                info.elementSerializer = pqMeta.getElementSerializer();
            } else {
                RegisteredKeyValueStateBackendMetaInfo<?, ?> kvMeta =
                        new RegisteredKeyValueStateBackendMetaInfo<>(snapshot);
                info.namespaceSerializer = kvMeta.getNamespaceSerializer();
                info.stateSerializer = kvMeta.getStateSerializer();

                // For MAP states, extract the user-key serializer from the MapSerializer.
                if (meta.isMapState()
                        && info.stateSerializer instanceof MapSerializer) {
                    info.mapUserKeySerializer =
                            ((MapSerializer<?, ?>) info.stateSerializer).getKeySerializer();
                }
            }
            infos[meta.kvStateId()] = info;
        }
        return infos;
    }

    /**
     * Detects whether a {@link RuntimeException} from a Reader scan indicates that the requested
     * column family doesn't exist in the snapshot. Cobble throws {@code IllegalStateException} with
     * the message {@code "IO error: Unknown column family '<name>'"}. Only this specific error is
     * treated as an empty state; all other exceptions propagate.
     */
    private static boolean isUnknownColumnFamily(RuntimeException e) {
        if (!(e instanceof IllegalStateException)) {
            return false;
        }
        String message = e.getMessage();
        if (message == null) {
            return false;
        }
        if (message.startsWith("IO error: ")) {
            message = message.substring("IO error: ".length());
        }
        return message.startsWith("Unknown column family");
    }

    // ------------------------------------------------------------------------------------------
    //  Cleanup helpers
    // ------------------------------------------------------------------------------------------

    private void closeCurrentScan() {
        if (currentScanCursor != null) {
            try {
                currentScanCursor.close();
            } catch (RuntimeException ignored) {
                // best effort
            }
            currentScanCursor = null;
        }
        currentEntryIterator = null;
        timerKeyIterator = null;
    }

    // ------------------------------------------------------------------------------------------
    //  Inner classes
    // ------------------------------------------------------------------------------------------

    /** Per-state serializer info reconstructed from the metadata snapshot. */
    private static final class StateSerializerInfo {
        /** Namespace serializer for KV states. */
        TypeSerializer<?> namespaceSerializer;

        /** Value/state serializer for KV states (ListSerializer, MapSerializer, etc.). */
        TypeSerializer<?> stateSerializer;

        /** User-key serializer for MAP states (extracted from MapSerializer). */
        TypeSerializer<?> mapUserKeySerializer;

        /** Element serializer for priority-queue (timer) states. */
        TypeSerializer<?> elementSerializer;
    }

    private static final byte[] EMPTY_BYTES = new byte[0];

    /** Empty start key — smallest possible key in a bucket (inclusive). */
    private static final byte[] EMPTY_SCAN_KEY = new byte[0];

    /** Practical upper-bound end key — 64 bytes of 0xFF (exclusive). */
    private static final byte[] MAX_SCAN_KEY;

    static {
        byte[] max = new byte[64];
        java.util.Arrays.fill(max, (byte) 0xFF);
        MAX_SCAN_KEY = max;
    }
}
