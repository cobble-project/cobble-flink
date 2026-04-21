package io.cobble.flink.state;

import io.cobble.Config;
import io.cobble.Db;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FileUtils;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * Minimal keyed backend shell that owns the Cobble resources while keyed-state operations remain
 * unsupported for now.
 */
final class CobbleKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private final File instanceBasePath;
    private final File volumePath;
    private final Path configPath;
    private final Config cobbleConfig;
    private final Db cobbleDb;
    private final AtomicBoolean resourcesClosed;

    CobbleKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            InternalKeyContext<K> keyContext,
            File instanceBasePath,
            File volumePath,
            Path configPath,
            Config cobbleConfig,
            Db cobbleDb) {
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                keyContext);
        this.instanceBasePath = instanceBasePath;
        this.volumePath = volumePath;
        this.configPath = configPath;
        this.cobbleConfig = cobbleConfig;
        this.cobbleDb = cobbleDb;
        this.resourcesClosed = new AtomicBoolean(false);
    }

    /** No-op until the backend starts integrating with real checkpoint notifications. */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /** No-op until the backend starts integrating with real checkpoint notifications. */
    @Override
    public void notifyCheckpointAborted(long checkpointId) {}

    /** No-op until the backend starts integrating with real checkpoint notifications. */
    @Override
    public void notifyCheckpointSubsumed(long checkpointId) {}

    /** Savepoints stay unsupported until actual keyed-state serialization is implemented. */
    @Nonnull
    @Override
    public SavepointResources<K> savepoint() {
        throw unsupported("savepoint");
    }

    /** There is no real keyed state yet, so entry counting is intentionally unsupported. */
    @Override
    public int numKeyValueStateEntries() {
        throw unsupported("numKeyValueStateEntries");
    }

    /** Key iteration depends on real state storage and is therefore unsupported for now. */
    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        throw unsupported("getKeys");
    }

    /** Key/namespace iteration depends on real state storage and is unsupported for now. */
    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        throw unsupported("getKeysAndNamespaces");
    }

    /** State creation is blocked until Cobble-backed keyed state is implemented. */
    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull
                    StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                            snapshotTransformFactory) {
        throw unsupported("createOrUpdateInternalState");
    }

    /** Priority queues are not available until the real keyed-state backend exists. */
    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        throw unsupported("create");
    }

    /** Priority queues are not available until the real keyed-state backend exists. */
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
                    boolean allowFutureMetadataUpdates) {
        throw unsupported("create");
    }

    /** Snapshotting is intentionally disabled while this backend only validates config flow. */
    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {
        throw unsupported("snapshot");
    }

    /** The current shell backend cannot safely reuse keyed state across restore paths. */
    @Override
    public boolean isSafeToReuseKVState() {
        return false;
    }

    /** Legacy synchronous timer snapshots are irrelevant until keyed state is implemented. */
    @Override
    public boolean requiresLegacySynchronousTimerSnapshots(SnapshotType checkpointType) {
        return false;
    }

    /** Returns this shell backend because there is no deeper delegated backend anymore. */
    @Override
    public KeyedStateBackend<K> getDelegatedKeyedStateBackend(boolean recursive) {
        return this;
    }

    /** Disposes native/local Cobble resources even though keyed-state APIs are unsupported. */
    @Override
    public void dispose() {
        try {
            closeResources();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to dispose Cobble backend resources.", e);
        } finally {
            super.dispose();
        }
    }

    /** Closes the backend shell and then releases the temporary Cobble resources. */
    @Override
    public void close() throws IOException {
        IOException error = null;
        try {
            super.close();
        } catch (IOException e) {
            error = e;
        }

        try {
            closeResources();
        } catch (IOException e) {
            if (error == null) {
                error = e;
            } else {
                error.addSuppressed(e);
            }
        }

        if (error != null) {
            throw error;
        }
    }

    /** Exposes the local working directory for tests that validate path layout. */
    File getInstanceBasePath() {
        return instanceBasePath;
    }

    /** Exposes the local Cobble volume directory for tests that validate path layout. */
    File getVolumePath() {
        return volumePath;
    }

    /** Exposes the generated Cobble config file path for tests. */
    Path getConfigPath() {
        return configPath;
    }

    /** Exposes the in-memory Cobble config object for tests. */
    Config getCobbleConfig() {
        return cobbleConfig;
    }

    /** Exposes the live native DB handle for tests. */
    Db getCobbleDb() {
        return cobbleDb;
    }

    /** Closes the native DB exactly once and removes the temporary local working directory. */
    private void closeResources() throws IOException {
        if (!resourcesClosed.compareAndSet(false, true)) {
            return;
        }

        IOException error = null;

        try {
            cobbleDb.close();
        } catch (RuntimeException e) {
            error = new IOException("Failed to close Cobble DB.", e);
        }

        try {
            FileUtils.deleteDirectory(instanceBasePath);
        } catch (Exception e) {
            IOException deleteError =
                    new IOException(
                            "Failed to delete Cobble working directory " + instanceBasePath + ".",
                            e);
            if (error == null) {
                error = deleteError;
            } else {
                error.addSuppressed(deleteError);
            }
        }

        if (error != null) {
            throw error;
        }
    }

    /** Creates a consistent error message for not-yet-implemented keyed-state operations. */
    private static UnsupportedOperationException unsupported(String operation) {
        return new UnsupportedOperationException(
                "Cobble keyed state backend operation '" + operation + "' is not implemented yet.");
    }
}
