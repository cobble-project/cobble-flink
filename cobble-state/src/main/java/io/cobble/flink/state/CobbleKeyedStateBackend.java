package io.cobble.flink.state;

import io.cobble.ColumnFamilyOptions;
import io.cobble.Config;
import io.cobble.structured.Db;
import io.cobble.structured.Schema;
import io.cobble.structured.StructuredSchemaBuilder;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.HeapPriorityQueuesManager;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotExecutionType;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FileUtils;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * Keyed backend that keeps the Cobble DB and config live and serves Flink Value/List/Map state
 * through Cobble column families.
 */
final class CobbleKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private final File instanceBasePath;
    private final File volumePath;
    private final Path configPath;
    private final Config cobbleConfig;
    private final Db cobbleDb;
    private final Map<String, StateDescriptor.Type> stateTypes;
    private final PriorityQueueSetFactory priorityQueueFactory;
    private final HeapPriorityQueuesManager heapPriorityQueuesManager;
    private final List<AbstractCobbleState<?, ?, ?>> stateResources;
    private final CobbleSnapshotStrategy snapshotStrategy;
    private final AtomicBoolean resourcesClosed;
    private final boolean manualTtlTimeProviderForTests;

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
            Db cobbleDb,
            boolean manualTtlTimeProviderForTests,
            boolean restoredNativeQueuesMayContainEntries,
            CobbleStateBackend.PriorityQueueStateType priorityQueueStateType) {
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
        this.stateTypes = new HashMap<>();
        this.priorityQueueFactory =
                createPriorityQueueFactory(
                        cobbleDb,
                        keyContext,
                        restoredNativeQueuesMayContainEntries,
                        priorityQueueStateType);
        this.heapPriorityQueuesManager =
                priorityQueueFactory instanceof HeapPriorityQueueSetFactory
                        ? new HeapPriorityQueuesManager(
                                new HashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>>(),
                                (HeapPriorityQueueSetFactory) priorityQueueFactory,
                                keyContext.getKeyGroupRange(),
                                keyContext.getNumberOfKeyGroups())
                        : null;
        this.stateResources = new ArrayList<>();
        this.snapshotStrategy =
                new CobbleSnapshotStrategy(
                        cobbleDb,
                        keyGroupRange,
                        () -> !stateTypes.isEmpty() || hasCobblePriorityQueues(),
                        this::hasCobblePriorityQueues);
        this.resourcesClosed = new AtomicBoolean(false);
        this.manualTtlTimeProviderForTests = manualTtlTimeProviderForTests;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        snapshotStrategy.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        snapshotStrategy.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void notifyCheckpointSubsumed(long checkpointId) {
        snapshotStrategy.notifyCheckpointSubsumed(checkpointId);
    }

    /** Savepoints remain unsupported until real snapshot support is implemented. */
    @Nonnull
    @Override
    public SavepointResources<K> savepoint() {
        throw unsupported("savepoint");
    }

    /** Counting all entries efficiently is out of scope for the current implementation. */
    @Override
    public int numKeyValueStateEntries() {
        throw unsupported("numKeyValueStateEntries");
    }

    /** Key iteration stays unsupported until scan semantics are wired into Flink iterators. */
    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        throw unsupported("getKeys");
    }

    /** Key/namespace iteration stays unsupported until scan semantics are wired in. */
    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        throw unsupported("getKeysAndNamespaces");
    }

    /** Creates the supported Flink state wrappers and ensures their Cobble schema exists. */
    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull
                    StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                            snapshotTransformFactory)
            throws Exception {
        ensureStateColumnFamily(stateDesc);

        switch (stateDesc.getType()) {
            case VALUE:
                ValueStateDescriptor<SV> valueStateDescriptor =
                        (ValueStateDescriptor<SV>) stateDesc;
                CobbleValueState<K, N, SV> valueState =
                        new CobbleValueState<>(
                                this,
                                cobbleDb,
                                stateDesc.getName(),
                                keySerializer,
                                namespaceSerializer,
                                valueStateDescriptor.getSerializer(),
                                valueStateDescriptor.getDefaultValue(),
                                stateDesc.getTtlConfig());
                trackStateResource(valueState);
                return (IS) valueState;
            case LIST:
                IS listState =
                        createListState(namespaceSerializer, (ListStateDescriptor<?>) stateDesc);
                trackStateResource((AbstractCobbleState<?, ?, ?>) listState);
                return listState;
            case MAP:
                IS mapState =
                        createMapState(namespaceSerializer, (MapStateDescriptor<?, ?>) stateDesc);
                trackStateResource((AbstractCobbleState<?, ?, ?>) mapState);
                return mapState;
            default:
                throw unsupportedState(stateDesc);
        }
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        return create(stateName, byteOrderedElementSerializer, false);
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
                    boolean allowFutureMetadataUpdates) {
        if (heapPriorityQueuesManager != null) {
            return heapPriorityQueuesManager.createOrUpdate(
                    stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
        }
        return priorityQueueFactory.create(
                stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {
        try {
            return new SnapshotStrategyRunner<>(
                            "Cobble shard snapshot",
                            snapshotStrategy,
                            cancelStreamRegistry,
                            SnapshotExecutionType.ASYNCHRONOUS)
                    .snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to start Cobble snapshot for checkpoint " + checkpointId + '.', e);
        }
    }

    @Override
    public boolean isSafeToReuseKVState() {
        return true;
    }

    @Override
    public boolean requiresLegacySynchronousTimerSnapshots(SnapshotType checkpointType) {
        // Heap queues already rely on Flink's timer snapshot path. Cobble queues use that same
        // path for the prefetched overlay because pollBatchDirect() has already advanced the
        // native cursor before snapshotting the shard.
        return true;
    }

    /** Disposes the backend and always releases the live Cobble resources. */
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

    /** Closes the backend and then releases the Cobble DB/resources exactly once. */
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

    /** Exposes the live Cobble DB handle for tests and internal helpers. */
    Db getCobbleDb() {
        return cobbleDb;
    }

    @VisibleForTesting
    boolean hasTrackedSnapshot(long checkpointId) {
        return snapshotStrategy.hasTrackedSnapshot(checkpointId);
    }

    @VisibleForTesting
    Long snapshotIdForCheckpoint(long checkpointId) {
        return snapshotStrategy.snapshotIdForCheckpoint(checkpointId);
    }

    /** Exposes Flink's total key-group count so state wrappers can map to Cobble buckets. */
    int getTotalNumberOfKeyGroupsForState() {
        return numberOfKeyGroups;
    }

    /** Creates the typed Cobble-backed Flink ListState wrapper. */
    @SuppressWarnings("unchecked")
    private <N, T, S extends State, IS extends S> IS createListState(
            TypeSerializer<N> namespaceSerializer, ListStateDescriptor<?> stateDesc) {
        ListStateDescriptor<T> listStateDescriptor = (ListStateDescriptor<T>) stateDesc;
        return (IS)
                new CobbleListState<>(
                        this,
                        cobbleDb,
                        stateDesc.getName(),
                        keySerializer,
                        namespaceSerializer,
                        (ListSerializer<T>) listStateDescriptor.getSerializer(),
                        listStateDescriptor.getElementSerializer(),
                        stateDesc.getTtlConfig());
    }

    /** Creates the typed Cobble-backed Flink MapState wrapper. */
    @SuppressWarnings("unchecked")
    private <N, UK, UV, S extends State, IS extends S> IS createMapState(
            TypeSerializer<N> namespaceSerializer, MapStateDescriptor<?, ?> stateDesc) {
        MapStateDescriptor<UK, UV> mapStateDescriptor = (MapStateDescriptor<UK, UV>) stateDesc;
        return (IS)
                new CobbleMapState<>(
                        this,
                        cobbleDb,
                        stateDesc.getName(),
                        keySerializer,
                        namespaceSerializer,
                        (MapSerializer<UK, UV>) mapStateDescriptor.getSerializer(),
                        stateDesc.getTtlConfig());
    }

    /**
     * Creates or validates the bytes-only column-family contract for keyed state.
     *
     * <p>Structured schema families default unspecified columns to bytes, so an existing family
     * with no explicit typed columns is still valid for our state layout as long as column 0 stays
     * bytes-typed.
     */
    private <S extends State, SV> void ensureStateColumnFamily(StateDescriptor<S, SV> stateDesc)
            throws Exception {
        StateDescriptor.Type previousType =
                stateTypes.putIfAbsent(stateDesc.getName(), stateDesc.getType());
        if (previousType != null && previousType != stateDesc.getType()) {
            throw new IllegalStateException(
                    "State '"
                            + stateDesc.getName()
                            + "' was already registered as "
                            + previousType
                            + " but is now requested as "
                            + stateDesc.getType()
                            + '.');
        }

        java.util.Map<Integer, Schema.ColumnType> family =
                cobbleDb.currentSchema().columnFamilies().get(stateDesc.getName());
        if (family == null) {
            boolean valueTtlEnabledForState =
                    stateDesc.getTtlConfig() != null && stateDesc.getTtlConfig().isEnabled();
            try (StructuredSchemaBuilder builder = cobbleDb.updateSchema()) {
                builder.setColumnFamilyOptions(
                        stateDesc.getName(),
                        ColumnFamilyOptions.defaults().valueHasTtl(valueTtlEnabledForState));
                builder.addBytesColumn(stateDesc.getName(), 0);
                builder.commit();
            }
            return;
        }

        if (!family.isEmpty() && (family.size() != 1 || !family.containsKey(0))) {
            throw new IllegalStateException(
                    "Cobble state column family '"
                            + stateDesc.getName()
                            + "' must be bytes-only with at most explicit column 0, but found "
                            + family.keySet()
                            + " columns.");
        }

        if (!family.isEmpty() && !(family.get(0) instanceof Schema.ColumnType.Bytes)) {
            throw new IllegalStateException(
                    "Cobble state column family '"
                            + stateDesc.getName()
                            + "' must store bytes values in column 0."
                            + '.');
        }
    }

    /** Closes the native DB exactly once and removes the temporary local working directory. */
    private void closeResources() throws IOException {
        if (!resourcesClosed.compareAndSet(false, true)) {
            return;
        }

        IOException error = null;
        snapshotStrategy.close();

        try {
            closeTimerQueues();
        } catch (IOException e) {
            error = e;
        }

        try {
            cobbleDb.close();
        } catch (RuntimeException e) {
            IOException dbError = new IOException("Failed to close Cobble DB.", e);
            if (error == null) {
                error = dbError;
            } else {
                error.addSuppressed(dbError);
            }
        }

        try {
            // We delay the state close after the db close, since the cobble close will ensure no
            // further access. It's only safe to close state here.
            closeStateResources();
        } catch (IOException e) {
            if (error == null) {
                error = e;
            } else {
                error.addSuppressed(e);
            }
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

    /** Tracks state-owned native option objects so the backend can close them with the DB. */
    private void trackStateResource(AbstractCobbleState<?, ?, ?> state) {
        stateResources.add(state);
    }

    /** Releases Cobble timer queues before the native DB closes. */
    private void closeTimerQueues() throws IOException {
        if (priorityQueueFactory instanceof CobblePriorityQueueSetFactory) {
            ((CobblePriorityQueueSetFactory) priorityQueueFactory).close();
        }
    }

    private boolean hasCobblePriorityQueues() {
        return priorityQueueFactory instanceof CobblePriorityQueueSetFactory
                && ((CobblePriorityQueueSetFactory) priorityQueueFactory).hasQueues();
    }

    private static PriorityQueueSetFactory createPriorityQueueFactory(
            Db cobbleDb,
            InternalKeyContext<?> keyContext,
            boolean restoredNativeQueuesMayContainEntries,
            CobbleStateBackend.PriorityQueueStateType priorityQueueStateType) {
        switch (priorityQueueStateType) {
            case HEAP:
                return new HeapPriorityQueueSetFactory(
                        keyContext.getKeyGroupRange(), keyContext.getNumberOfKeyGroups(), 128);
            case COBBLE:
                return new CobblePriorityQueueSetFactory(
                        cobbleDb,
                        keyContext.getKeyGroupRange(),
                        keyContext.getNumberOfKeyGroups(),
                        restoredNativeQueuesMayContainEntries);
            default:
                throw new IllegalArgumentException(
                        "Unknown Cobble priority queue state type: " + priorityQueueStateType);
        }
    }

    /** Closes cached state-local native option handles after the shared Cobble DB is closed. */
    private void closeStateResources() throws IOException {
        IOException error = null;
        for (AbstractCobbleState<?, ?, ?> stateResource : stateResources) {
            try {
                stateResource.close();
            } catch (RuntimeException e) {
                IOException closeError =
                        new IOException(
                                "Failed to close Cobble state resources for '"
                                        + stateResource.columnFamily
                                        + "'.",
                                e);
                if (error == null) {
                    error = closeError;
                } else {
                    error.addSuppressed(closeError);
                }
            }
        }
        if (error != null) {
            throw error;
        }
    }

    /** Advances Cobble's manual clock for TTL tests when the backend was opened in manual mode. */
    void setTimeForTests(long timestampMillis) {
        if (!manualTtlTimeProviderForTests) {
            throw new IllegalStateException(
                    "Cobble backend is not using the manual TTL time provider.");
        }
        long ttlSeconds = Math.max(0L, timestampMillis / 1000L);
        cobbleDb.setTime((int) Math.min(Integer.MAX_VALUE, ttlSeconds));
    }

    /** Creates a consistent error message for not-yet-implemented backend operations. */
    private static UnsupportedOperationException unsupported(String operation) {
        return new UnsupportedOperationException(
                "Cobble keyed state backend operation '" + operation + "' is not implemented yet.");
    }

    /**
     * Creates a consistent error message for state descriptor types this backend does not support.
     */
    private static IllegalArgumentException unsupportedState(StateDescriptor<?, ?> stateDesc) {
        return new IllegalArgumentException(
                "State " + stateDesc.getClass().getSimpleName() + " is not supported yet.");
    }
}
