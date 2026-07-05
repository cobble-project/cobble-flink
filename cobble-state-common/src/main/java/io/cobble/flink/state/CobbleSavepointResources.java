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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
 * global snapshot on the coordinator (only if this resource successfully materialized it), closes
 * the coordinator, and expires the retained shard snapshot — each exactly once.
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

    /**
     * Tracks whether {@link #createKVStateIterator()} successfully materialized the global snapshot.
     * Only if this is true will {@link #release()} expire the global snapshot — preventing
     * accidental deletion of a pre-existing global snapshot that happens to have a colliding ID.
     */
    private volatile boolean globalSnapshotMaterialized;

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
        // Use a unique global snapshot ID to avoid collisions when multiple subtasks share the
        // same coordinator volume (e.g. in a cluster). The shard snapshot ID alone is not unique
        // across subtasks — each subtask's DB starts its snapshot chain from 1. We derive a
        // unique ID by hashing dbId + snapshotId + manifestPath into a positive 63-bit value.
        globalSnapshotId = deriveUniqueGlobalSnapshotId(shardSnapshotId, shardSnapshot);
        coordinator.materializeGlobalSnapshot(
                totalBuckets, globalSnapshotId, Collections.singletonList(shardSnapshot));
        // Mark as materialized so release() knows it is safe to expire this global snapshot.
        globalSnapshotMaterialized = true;
        if (!coordinator.retainSnapshot(globalSnapshotId)) {
            // The coordinator could not retain the global snapshot — clean up what we created
            // (the materialized global snapshot and the coordinator), then fail fast. We do NOT
            // call release() here because release() also expires the shard snapshot, which is
            // the responsibility of Flink's outer cleanup.
            try {
                coordinator.expireSnapshot(globalSnapshotId);
            } catch (RuntimeException ignored) {
                // best effort
            }
            try {
                coordinator.close();
            } catch (RuntimeException ignored) {
                // best effort
            }
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
        // Expire the global snapshot on the coordinator — only if we successfully materialized it.
        // This prevents accidental deletion of a pre-existing global snapshot with a colliding ID
        // when materializeGlobalSnapshot() failed before creating our snapshot.
        if (coordinator != null && globalSnapshotMaterialized) {
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
        } else if (coordinator != null) {
            // materializeGlobalSnapshot failed or was never called — still close the coordinator.
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

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    /**
     * Derives a coordinator-unique global snapshot ID by hashing the shard snapshot's identity
     * (dbId, snapshotId, manifestPath) into a positive 63-bit value. This is necessary because
     * multiple subtasks in the same JVM may share the same coordinator volume, and each subtask's
     * DB starts its snapshot chain from the same IDs (1, 2, ...).
     *
     * <p>The hash is SHA-256 of {@code "dbId:snapshotId:manifestPath"}, truncated to 8 bytes and
     * forced into a positive 63-bit value by masking the sign bit. This provides stronger uniqueness
     * guarantees than a 28-bit {@code hashCode()} truncation, and is stable across JVM restarts
     * (unlike {@code hashCode()} which can vary between JVM instances for strings longer than 32
     * characters in Java 9+).
     */
    private static long deriveUniqueGlobalSnapshotId(
            long shardSnapshotId, ShardSnapshot shardSnapshot) {
        String dbId = shardSnapshot != null && shardSnapshot.dbId != null
                ? shardSnapshot.dbId
                : "unknown";
        String manifestPath = shardSnapshot != null && shardSnapshot.manifestPath != null
                ? shardSnapshot.manifestPath
                : "";
        String identity = dbId + ":" + shardSnapshotId + ":" + manifestPath;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(identity.getBytes(StandardCharsets.UTF_8));
            // Take the first 8 bytes and mask the sign bit to produce a positive 63-bit value.
            long value = 0;
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (hash[i] & 0xFFL);
            }
            // Ensure positive (Cobble requires non-negative snapshot IDs) and non-zero.
            value &= Long.MAX_VALUE;
            return value == 0 ? 1 : value;
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed to be available on all Java platforms. If somehow missing,
            // fall back to a simple hash that is still better than raw shardSnapshotId.
            long fallback = (long) (identity.hashCode() & 0x7FFFFFFFL);
            return fallback == 0 ? 1 : fallback;
        }
    }
}
