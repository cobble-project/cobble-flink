package io.cobble.flink.state;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.Reader;
import io.cobble.ShardSnapshot;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link FullSnapshotResources} for Cobble canonical savepoint creation.
 *
 * <p>This resources object is created in the synchronous phase of {@code
 * CobbleKeyedStateBackend.savepoint()}. It holds a retained Cobble {@link ShardSnapshot} and a
 * pre-serialized immutable copy of all overlay timers (captured in the sync phase so the async
 * writer sees a consistent timer set). In the asynchronous phase, {@link
 * #createKVStateIterator()} materializes a global snapshot via {@link DbCoordinator}, then opens a
 * lightweight {@link Reader} pinned to that global snapshot. All state scans read through the
 * reader, which is a read-only proxy over the retained snapshot files — not a second DB instance
 * on the same volume.
 *
 * <p>The retained shard snapshot on the live {@code Db} is released via {@code shardSnapshotCleanup}
 * — a callback provided by the backend that calls {@code cobbleDb.expireSnapshot(snapshotId)}. This
 * is invoked in {@link #release()} after the async writer completes (or fails), and also if
 * {@link #createKVStateIterator()} fails after the shard snapshot was retained.
 *
 * <p>After the async writer completes (or fails), {@link #release()} closes the reader, expires the
 * global snapshot on the coordinator, closes the coordinator, and expires the retained shard
 * snapshot — each exactly once.
 *
 * <p>The empty-backend optimization: when no state has been registered ({@code stateCount == 0}),
 * {@link #empty(...)} returns a resources object with an empty metadata list. {@code
 * SavepointSnapshotStrategy} short-circuits to {@code SnapshotResult.empty()} in that case, so no
 * native snapshot is started.
 */
final class CobbleSavepointResources<K> implements FullSnapshotResources<K> {

    private final Config cobbleConfig;
    private final long shardSnapshotId;
    private final ShardSnapshot shardSnapshot;
    private final int totalBuckets;
    private final CobbleCanonicalSavepointMetadataSnapshot metadataSnapshot;
    private final TypeSerializer<K> keySerializer;
    private final KeyGroupRange keyGroupRange;
    private final CobbleTimerOverlaySnapshot timerOverlaySnapshot;

    /**
     * Callback to expire the retained shard snapshot on the live {@code Db}. Called exactly once in
     * {@link #release()}. May be {@code null} for the empty-backend case (no shard snapshot was
     * retained).
     */
    private final Runnable shardSnapshotCleanup;

    /** Guards single-shot cleanup of the reader, global snapshot, and retained shard snapshot. */
    private final AtomicBoolean released = new AtomicBoolean(false);

    /** The reader opened lazily in {@link #createKVStateIterator()}. */
    private volatile Reader reader;
    private volatile DbCoordinator coordinator;
    private volatile long globalSnapshotId;

    CobbleSavepointResources(
            Config cobbleConfig,
            long shardSnapshotId,
            ShardSnapshot shardSnapshot,
            int totalBuckets,
            CobbleCanonicalSavepointMetadataSnapshot metadataSnapshot,
            TypeSerializer<K> keySerializer,
            KeyGroupRange keyGroupRange,
            CobbleTimerOverlaySnapshot timerOverlaySnapshot,
            Runnable shardSnapshotCleanup) {
        this.cobbleConfig = cobbleConfig;
        this.shardSnapshotId = shardSnapshotId;
        this.shardSnapshot = shardSnapshot;
        this.totalBuckets = totalBuckets;
        this.metadataSnapshot = metadataSnapshot;
        this.keySerializer = keySerializer;
        this.keyGroupRange = keyGroupRange;
        this.timerOverlaySnapshot = timerOverlaySnapshot;
        this.shardSnapshotCleanup = shardSnapshotCleanup;
    }

    /**
     * Creates an empty resources object for backends with no registered state. No native snapshot is
     * needed; {@code SavepointSnapshotStrategy} returns {@code SnapshotResult.empty()} for empty
     * metadata.
     */
    static <K> CobbleSavepointResources<K> empty(
            CobbleCanonicalSavepointMetadataSnapshot metadataSnapshot,
            TypeSerializer<K> keySerializer,
            KeyGroupRange keyGroupRange) {
        return new CobbleSavepointResources<>(
                null,
                -1L,
                null,
                0,
                metadataSnapshot,
                keySerializer,
                keyGroupRange,
                CobbleTimerOverlaySnapshot.empty(),
                null);
    }

    @Override
    public List<StateMetaInfoSnapshot> getMetaInfoSnapshots() {
        return metadataSnapshot.metaInfoSnapshots();
    }

    @Override
    public KeyValueStateIterator createKVStateIterator() throws IOException {
        // Materialize a global snapshot from the retained shard snapshot, then open a lightweight
        // reader pinned to it. The reader is a read-only proxy — it does not create a second DB
        // instance on the same volume.
        coordinator = DbCoordinator.open(cobbleConfig);
        globalSnapshotId = shardSnapshotId;
        coordinator.materializeGlobalSnapshot(
                totalBuckets, globalSnapshotId, Collections.singletonList(shardSnapshot));
        if (!coordinator.retainSnapshot(globalSnapshotId)) {
            // The coordinator could not retain the global snapshot — clean up and fail fast so the
            // shard snapshot is not leaked.
            release();
            throw new IOException(
                    "Failed to retain global snapshot " + globalSnapshotId + " on coordinator.");
        }
        reader = Reader.open(cobbleConfig, globalSnapshotId);
        return new CobbleCanonicalStateIterator<>(
                reader,
                metadataSnapshot,
                keyGroupRange,
                totalBuckets,
                keySerializer,
                timerOverlaySnapshot);
    }

    @Nonnull
    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    public org.apache.flink.runtime.state.StreamCompressionDecorator getStreamCompressionDecorator() {
        return UncompressedStreamCompressionDecorator.INSTANCE;
    }

    @Override
    public void release() {
        if (!released.compareAndSet(false, true)) {
            return;
        }
        // Close the reader (lightweight — no DB instance to shut down).
        if (reader != null) {
            try {
                reader.close();
            } catch (RuntimeException ignored) {
                // best effort cleanup
            }
        }
        // Expire the global snapshot on the coordinator.
        if (coordinator != null && globalSnapshotId >= 0) {
            try {
                coordinator.expireSnapshot(globalSnapshotId);
            } catch (RuntimeException ignored) {
                // best effort cleanup
            }
            try {
                coordinator.close();
            } catch (RuntimeException ignored) {
                // best effort cleanup
            }
        }
        // Expire the retained shard snapshot on the live Db.
        if (shardSnapshotCleanup != null) {
            try {
                shardSnapshotCleanup.run();
            } catch (RuntimeException ignored) {
                // best effort cleanup
            }
        }
    }
}
