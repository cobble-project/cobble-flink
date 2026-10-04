package io.cobble.flink.inspect.internal;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.io.PostVersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.streaming.api.operators.InternalTimersSnapshot;
import org.apache.flink.streaming.api.operators.InternalTimersSnapshotReaderWriters;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/** The checkpoint's prefetched timers, decoded once independently of the native tail. */
final class LegacyTimerSnapshot {
    private static final NavigableSet<byte[]> EMPTY_KEYS =
            Collections.unmodifiableNavigableSet(
                    new TreeSet<>(InspectReaderOperations::compareKeys));
    private final Map<String, Map<Integer, NavigableSet<byte[]>>> timers = new HashMap<>();

    static LegacyTimerSnapshot read(List<KeyGroupsStateHandle> handles, ClassLoader loader)
            throws IOException {
        LegacyTimerSnapshot snapshot = new LegacyTimerSnapshot();
        for (KeyGroupsStateHandle handle : handles) {
            try (FSDataInputStream input = handle.openInputStream()) {
                for (Tuple2<Integer, Long> group : handle.getGroupRangeOffsets()) {
                    if (group.f1 < 0) {
                        continue;
                    }
                    input.seek(group.f1);
                    snapshot.new GroupReader(group.f0, loader).read(input);
                }
            } catch (Exception error) {
                throw new IOException("Failed to decode checkpoint legacy timer stream", error);
            }
        }
        return snapshot;
    }

    Set<String> stateNames() {
        return timers.keySet();
    }

    NavigableSet<byte[]> keys(String stateName, int bucket) {
        Map<Integer, NavigableSet<byte[]>> groups = timers.get(stateName);
        return groups == null ? EMPTY_KEYS : groups.getOrDefault(bucket, EMPTY_KEYS);
    }

    private final class GroupReader extends PostVersionedIOReadableWritable {
        private final int group;
        private final ClassLoader loader;

        private GroupReader(int group, ClassLoader loader) {
            this.group = group;
            this.loader = loader;
        }

        @Override
        public int getVersion() {
            return 2;
        }

        @Override
        public int[] getCompatibleVersions() {
            return new int[] {2, 1};
        }

        @Override
        protected void read(DataInputView input, boolean wasVersioned) throws IOException {
            int count = input.readInt();
            if (count < 0) {
                throw new IOException("Negative timer service count");
            }
            for (int index = 0; index < count; index++) {
                String service = input.readUTF();
                InternalTimersSnapshot<?, ?> restored =
                        InternalTimersSnapshotReaderWriters.getReaderForVersion(
                                        wasVersioned
                                                ? getReadVersion()
                                                : InternalTimersSnapshotReaderWriters.NO_VERSION,
                                        loader)
                                .readTimersSnapshot(input);
                add("_timer_state/event_" + service, restored, restored.getEventTimeTimers());
                add(
                        "_timer_state/processing_" + service,
                        restored,
                        restored.getProcessingTimeTimers());
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private void add(
                String stateName,
                InternalTimersSnapshot<?, ?> restored,
                Set<? extends TimerHeapInternalTimer<?, ?>> elements)
                throws IOException {
            if (elements == null || elements.isEmpty()) {
                return;
            }
            TimerSerializer serializer =
                    new TimerSerializer(
                            (TypeSerializer)
                                    restored.getKeySerializerSnapshot().restoreSerializer(),
                            (TypeSerializer)
                                    restored.getNamespaceSerializerSnapshot().restoreSerializer());
            NavigableSet<byte[]> keys =
                    timers.computeIfAbsent(stateName, ignored -> new HashMap<>())
                            .computeIfAbsent(
                                    group,
                                    ignored -> new TreeSet<>(InspectReaderOperations::compareKeys));
            DataOutputSerializer output = new DataOutputSerializer(128);
            for (TimerHeapInternalTimer<?, ?> timer : elements) {
                output.clear();
                serializer.serialize(timer, output);
                keys.add(output.getCopyOfBuffer());
            }
        }
    }
}
