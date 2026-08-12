package io.cobble.flink.state;

import io.cobble.ColumnFamilyOptions;
import io.cobble.Config;
import io.cobble.flink.common.CobbleNativeMetrics;
import io.cobble.flink.common.CobbleStateDescriptor;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.structured.Db;
import io.cobble.structured.Schema;
import io.cobble.structured.StructuredSchemaBuilder;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.v2.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.ReducingStateDescriptor;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.HeapPriorityQueuesManager;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.SnapshotExecutionType;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.v2.internal.InternalKeyedState;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/** Cobble implementation of Flink 2.0's asynchronous keyed-state backend SPI. */
final class CobbleAsyncKeyedStateBackend<K> implements AsyncKeyedStateBackend<K> {

    private final ExecutionConfig executionConfig;
    private final TtlTimeProvider ttlTimeProvider;
    private final TypeSerializer<K> keySerializer;
    private final InternalKeyContext<K> keyContext;
    private final File instanceBasePath;
    private final File volumePath;
    private final Path configPath;
    private final Config cobbleConfig;
    private final Db cobbleDb;
    private final int asyncReadThreads;
    private final int asyncWriteThreads;
    private final CobbleNativeMetrics.Monitor nativeMetricsMonitor;
    private final CloseableRegistry cancelStreamRegistry;
    private final LinkedHashMap<String, InternalKeyedState<K, ?, ?>> states;
    private final LinkedHashMap<String, CobbleStateDescriptor> stateDescriptors;
    private final LinkedHashMap<String, StateInspectSchema> stateInspectSchemas;
    private final LinkedHashMap<String, StateInspectSemanticSchema> stateInspectSemanticSchemas;
    private final LinkedHashMap<String, RestoredKeyedStateMetadata> restoredCanonicalMetadata;
    private final List<CobbleAsyncRequestState> stateResources;
    private final Set<CobbleStateExecutor> managedStateExecutors;
    private final PriorityQueueSetFactory priorityQueueFactory;
    private final HeapPriorityQueuesManager heapPriorityQueuesManager;
    private final CobbleSnapshotStrategy snapshotStrategy;
    private final AtomicBoolean resourcesClosed;

    private volatile StateRequestHandler stateRequestHandler;

    CobbleAsyncKeyedStateBackend(
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            TypeSerializer<K> keySerializer,
            InternalKeyContext<K> keyContext,
            CloseableRegistry cancelStreamRegistry,
            File instanceBasePath,
            File volumePath,
            Path configPath,
            Config cobbleConfig,
            Db cobbleDb,
            CobbleNativeMetrics.Monitor nativeMetricsMonitor,
            int asyncReadThreads,
            int asyncWriteThreads,
            boolean restoredNativeQueuesMayContainEntries,
            CobbleStateBackend.PriorityQueueStateType priorityQueueStateType,
            Map<String, RestoredKeyedStateMetadata> restoredCanonicalMetadata,
            List<CobbleStateDescriptor> restoredStateDescriptors) {
        this.executionConfig = executionConfig;
        this.ttlTimeProvider = ttlTimeProvider;
        this.keySerializer = keySerializer;
        this.keyContext = keyContext;
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.instanceBasePath = instanceBasePath;
        this.volumePath = volumePath;
        this.configPath = configPath;
        this.cobbleConfig = cobbleConfig;
        this.cobbleDb = cobbleDb;
        this.nativeMetricsMonitor = nativeMetricsMonitor;
        this.asyncReadThreads = asyncReadThreads;
        this.asyncWriteThreads = asyncWriteThreads;
        this.states = new LinkedHashMap<>();
        this.stateDescriptors = new LinkedHashMap<>();
        for (CobbleStateDescriptor descriptor : restoredStateDescriptors) {
            registerStateDescriptor(descriptor);
        }
        this.stateInspectSchemas = new LinkedHashMap<>();
        this.stateInspectSemanticSchemas = new LinkedHashMap<>();
        this.restoredCanonicalMetadata =
                restoredCanonicalMetadata == null
                        ? new LinkedHashMap<>()
                        : new LinkedHashMap<>(restoredCanonicalMetadata);
        this.stateResources = new ArrayList<>();
        this.managedStateExecutors = new HashSet<>();
        this.priorityQueueFactory =
                createPriorityQueueFactory(
                        restoredNativeQueuesMayContainEntries, priorityQueueStateType);
        this.heapPriorityQueuesManager =
                priorityQueueFactory instanceof HeapPriorityQueueSetFactory
                        ? new HeapPriorityQueuesManager(
                                new HashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>>(),
                                (HeapPriorityQueueSetFactory) priorityQueueFactory,
                                keyContext.getKeyGroupRange(),
                                keyContext.getNumberOfKeyGroups())
                        : null;
        this.snapshotStrategy =
                new CobbleSnapshotStrategy(
                        cobbleDb,
                        keyContext.getKeyGroupRange(),
                        () -> !stateDescriptors.isEmpty() || hasCobblePriorityQueues(),
                        this::hasCobblePriorityQueues,
                        this::stateDescriptorSnapshot,
                        this::buildSchemaStore);
        this.resourcesClosed = new AtomicBoolean(false);
    }

    @Override
    public void setup(@Nonnull StateRequestHandler stateRequestHandler) {
        this.stateRequestHandler = Preconditions.checkNotNull(stateRequestHandler);
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized <N, S extends State, SV> S getOrCreateKeyedState(
            N defaultNamespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<SV> stateDesc)
            throws Exception {
        Preconditions.checkNotNull(namespaceSerializer, "Namespace serializer");
        InternalKeyedState<K, ?, ?> existing = states.get(stateDesc.getStateId());
        if (existing == null) {
            if (!stateDesc.isSerializerInitialized()) {
                stateDesc.initializeSerializerUnlessSet(executionConfig);
            }
            existing = createStateInternal(defaultNamespace, namespaceSerializer, stateDesc);
            states.put(stateDesc.getStateId(), existing);
        }
        return (S) existing;
    }

    @Nonnull
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public synchronized <N, S extends InternalKeyedState, SV> S createStateInternal(
            @Nonnull N defaultNamespace,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<SV> stateDesc)
            throws Exception {
        StateRequestHandler requestHandler =
                Preconditions.checkNotNull(
                        stateRequestHandler,
                        "A StateRequestHandler must be set before creating async state.");
        if (!stateDesc.isSerializerInitialized()) {
            stateDesc.initializeSerializerUnlessSet(executionConfig);
        }
        validateCanonicalKeyValueMetadata(stateDesc, namespaceSerializer);
        ensureStateColumnFamily(stateDesc);
        CobbleStateDescriptor runtimeDescriptor =
                registerStateDescriptor(
                        CobbleStateDescriptor.forKeyValue(
                                stateDesc.getStateId(),
                                stateDesc.getStateId(),
                                toCommonStateKind(stateDesc.getType())));

        CobbleAsyncRequestState state;
        switch (stateDesc.getType()) {
            case VALUE:
                state =
                        new CobbleAsyncValueState<>(
                                requestHandler,
                                cobbleDb,
                                runtimeDescriptor,
                                keySerializer,
                                defaultNamespace,
                                namespaceSerializer,
                                (ValueStateDescriptor<SV>) stateDesc);
                break;
            case LIST:
                state =
                        new CobbleAsyncListState<>(
                                requestHandler,
                                cobbleDb,
                                runtimeDescriptor,
                                keySerializer,
                                defaultNamespace,
                                namespaceSerializer,
                                (ListStateDescriptor<SV>) stateDesc);
                break;
            case MAP:
                state =
                        new CobbleAsyncMapState<>(
                                requestHandler,
                                cobbleDb,
                                runtimeDescriptor,
                                keySerializer,
                                defaultNamespace,
                                namespaceSerializer,
                                (MapStateDescriptor) stateDesc);
                break;
            case REDUCING:
                state =
                        new CobbleAsyncReducingState<>(
                                requestHandler,
                                cobbleDb,
                                runtimeDescriptor,
                                keySerializer,
                                defaultNamespace,
                                namespaceSerializer,
                                (ReducingStateDescriptor<SV>) stateDesc);
                break;
            case AGGREGATING:
                state =
                        new CobbleAsyncAggregatingState<>(
                                requestHandler,
                                cobbleDb,
                                runtimeDescriptor,
                                keySerializer,
                                defaultNamespace,
                                namespaceSerializer,
                                (AggregatingStateDescriptor) stateDesc);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Flink v2 state type: " + stateDesc.getType());
        }
        stateResources.add(state);
        registerInspectSchema(stateDesc, namespaceSerializer);
        restoredCanonicalMetadata.remove(stateDesc.getStateId());
        return (S) state;
    }

    @Nonnull
    @Override
    public synchronized StateExecutor createStateExecutor() {
        ensureOpen();
        CobbleStateExecutor executor = new CobbleStateExecutor(asyncReadThreads, asyncWriteThreads);
        managedStateExecutors.add(executor);
        return executor;
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyContext.getKeyGroupRange();
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {
        try {
            flushPendingTimerWrites();
            return new SnapshotStrategyRunner<>(
                            "Cobble async shard snapshot",
                            snapshotStrategy,
                            cancelStreamRegistry,
                            SnapshotExecutionType.ASYNCHRONOUS)
                    .snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to start Cobble async snapshot for checkpoint " + checkpointId + '.',
                    e);
        }
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

    @Override
    public boolean requiresLegacySynchronousTimerSnapshots(SnapshotType checkpointType) {
        return true;
    }

    @Override
    public boolean isSafeToReuseKVState() {
        return true;
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
    public synchronized <
                    T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
                    boolean allowFutureMetadataUpdates) {
        try {
            validateCanonicalPriorityQueueMetadata(stateName, byteOrderedElementSerializer);
        } catch (StateMigrationException e) {
            throw new UncheckedIOException(
                    "Canonical savepoint metadata rejected priority-queue '" + stateName + "'.",
                    new IOException(e.getMessage(), e));
        }
        if (heapPriorityQueuesManager != null) {
            KeyGroupedInternalPriorityQueue<T> queue =
                    heapPriorityQueuesManager.createOrUpdate(
                            stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
            restoredCanonicalMetadata.remove(stateName);
            return queue;
        }
        registerStateDescriptor(
                CobbleStateDescriptor.forTimer(
                        stateName,
                        CobblePriorityQueueSetFactory.timerQueueColumnFamilyName(stateName)));
        registerTimerSchema(stateName, byteOrderedElementSerializer);
        KeyGroupedInternalPriorityQueue<T> queue =
                priorityQueueFactory.create(
                        stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
        restoredCanonicalMetadata.remove(stateName);
        return queue;
    }

    private void validateCanonicalKeyValueMetadata(
            StateDescriptor<?> stateDesc, TypeSerializer<?> namespaceSerializer)
            throws StateMigrationException {
        RestoredKeyedStateMetadata canonical =
                restoredCanonicalMetadata.get(stateDesc.getStateId());
        if (canonical == null) {
            return;
        }
        if (canonical.kind() != RestoredKeyedStateMetadata.Kind.KEY_VALUE) {
            throw new StateMigrationException(
                    "State '"
                            + stateDesc.getStateId()
                            + "' was restored as a "
                            + canonical.kind()
                            + " but the running job registers it as a keyed state ("
                            + stateDesc.getType()
                            + ").");
        }
        if (!canonical.stateType().name().equals(stateDesc.getType().name())) {
            throw new StateMigrationException(
                    "State '"
                            + stateDesc.getStateId()
                            + "' kind mismatch: canonical savepoint had "
                            + canonical.stateType()
                            + ", but the running job registers "
                            + stateDesc.getType()
                            + '.');
        }
        CanonicalSavepointRestoreOperation.rejectIfNotCompatibleAsIs(
                stateDesc.getStateId(),
                "namespace serializer",
                canonical.namespaceSerializerSnapshot(),
                namespaceSerializer,
                "for the running job");
        CanonicalSavepointRestoreOperation.rejectIfNotCompatibleAsIs(
                stateDesc.getStateId(),
                "value serializer",
                canonical.stateSerializerSnapshot(),
                stateDesc.getSerializer(),
                "for the running job");
    }

    private void validateCanonicalPriorityQueueMetadata(
            String stateName, TypeSerializer<?> elementSerializer) throws StateMigrationException {
        RestoredKeyedStateMetadata canonical = restoredCanonicalMetadata.get(stateName);
        if (canonical == null) {
            return;
        }
        if (canonical.kind() != RestoredKeyedStateMetadata.Kind.PRIORITY_QUEUE) {
            throw new StateMigrationException(
                    "State '"
                            + stateName
                            + "' was restored as a "
                            + canonical.kind()
                            + " but the running job registers it as a priority-queue (timers).");
        }
        CanonicalSavepointRestoreOperation.rejectIfNotCompatibleAsIs(
                stateName,
                "timer element serializer",
                canonical.elementSerializerSnapshot(),
                elementSerializer,
                "for the running job");
    }

    @Override
    public void dispose() {
        try {
            closeResources();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to dispose Cobble async backend.", e);
        }
    }

    @Override
    public void close() throws IOException {
        closeResources();
    }

    File getInstanceBasePath() {
        return instanceBasePath;
    }

    File getVolumePath() {
        return volumePath;
    }

    Path getConfigPath() {
        return configPath;
    }

    Config getCobbleConfig() {
        return cobbleConfig;
    }

    Db getCobbleDb() {
        return cobbleDb;
    }

    private void registerInspectSchema(
            StateDescriptor<?> descriptor, TypeSerializer<?> namespaceSerializer) {
        String stateName = descriptor.getStateId();
        boolean ttlEnabled = descriptor.getTtlConfig().isEnabled();
        StateInspectSchema schema;
        StateInspectSemanticSchema semanticSchema;
        switch (descriptor.getType()) {
            case VALUE:
                schema =
                        StateInspectSchema.forValue(
                                stateName,
                                stateName,
                                ttlEnabled,
                                keySerializer,
                                namespaceSerializer,
                                descriptor.getSerializer());
                semanticSchema =
                        StateInspectSemanticSchemaExtractor.forValue(
                                schema.keySerializer(),
                                schema.namespaceSerializer(),
                                schema.valueSerializer());
                break;
            case LIST:
                schema =
                        StateInspectSchema.forList(
                                stateName,
                                stateName,
                                ttlEnabled,
                                keySerializer,
                                namespaceSerializer,
                                descriptor.getSerializer());
                semanticSchema =
                        StateInspectSemanticSchemaExtractor.forList(
                                schema.keySerializer(),
                                schema.namespaceSerializer(),
                                schema.listElementSerializer());
                break;
            case MAP:
                MapStateDescriptor<?, ?> mapDescriptor = (MapStateDescriptor<?, ?>) descriptor;
                schema =
                        StateInspectSchema.forMap(
                                stateName,
                                stateName,
                                ttlEnabled,
                                keySerializer,
                                namespaceSerializer,
                                mapDescriptor.getUserKeySerializer(),
                                mapDescriptor.getSerializer());
                semanticSchema =
                        StateInspectSemanticSchemaExtractor.forMap(
                                schema.keySerializer(),
                                schema.namespaceSerializer(),
                                schema.mapUserKeySerializer(),
                                schema.mapUserValueSerializer());
                break;
            case REDUCING:
                schema =
                        StateInspectSchema.forReducing(
                                stateName,
                                stateName,
                                ttlEnabled,
                                keySerializer,
                                namespaceSerializer,
                                descriptor.getSerializer());
                semanticSchema =
                        StateInspectSemanticSchemaExtractor.forReducing(
                                schema.keySerializer(),
                                schema.namespaceSerializer(),
                                schema.valueSerializer());
                break;
            case AGGREGATING:
                schema =
                        StateInspectSchema.forAggregating(
                                stateName,
                                stateName,
                                ttlEnabled,
                                keySerializer,
                                namespaceSerializer,
                                descriptor.getSerializer());
                semanticSchema =
                        StateInspectSemanticSchemaExtractor.forAggregating(
                                schema.keySerializer(),
                                schema.namespaceSerializer(),
                                schema.valueSerializer());
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Flink v2 state type: " + descriptor.getType());
        }
        stateInspectSchemas.putIfAbsent(stateName, schema);
        stateInspectSemanticSchemas.putIfAbsent(stateName, semanticSchema);
    }

    private void registerTimerSchema(String stateName, TypeSerializer<?> timerSerializer) {
        if (!(timerSerializer instanceof TimerSerializer)) {
            return;
        }
        TimerSerializer<?, ?> serializer = (TimerSerializer<?, ?>) timerSerializer;
        StateInspectSchema schema =
                StateInspectSchema.forTimer(
                        stateName,
                        CobblePriorityQueueSetFactory.timerQueueColumnFamilyName(stateName),
                        serializer.getKeySerializer(),
                        serializer.getNamespaceSerializer());
        stateInspectSchemas.putIfAbsent("timer:" + stateName, schema);
        stateInspectSemanticSchemas.putIfAbsent(
                stateName,
                StateInspectSemanticSchemaExtractor.forTimer(
                        schema.keySerializer(), schema.namespaceSerializer()));
    }

    private StateInspectSchemaStore buildSchemaStore() {
        return stateInspectSchemas.isEmpty()
                ? StateInspectSchemaStore.empty()
                : new StateInspectSchemaStore(
                        new ArrayList<>(stateInspectSchemas.values()), stateInspectSemanticSchemas);
    }

    private void ensureStateColumnFamily(StateDescriptor<?> descriptor) {
        String stateName = descriptor.getStateId();
        Map<Integer, Schema.ColumnType> family =
                cobbleDb.currentSchema().columnFamilies().get(stateName);
        if (family == null) {
            try (StructuredSchemaBuilder builder = cobbleDb.updateSchema()) {
                builder.setColumnFamilyOptions(
                        stateName,
                        ColumnFamilyOptions.defaults()
                                .valueHasTtl(descriptor.getTtlConfig().isEnabled()));
                builder.addBytesColumn(stateName, 0);
                builder.commit();
            }
            return;
        }
        if (!family.isEmpty() && (family.size() != 1 || !family.containsKey(0))) {
            throw new IllegalStateException(
                    "Cobble state column family '"
                            + stateName
                            + "' must contain only bytes column 0, but found "
                            + family.keySet()
                            + '.');
        }
        if (!family.isEmpty() && !(family.get(0) instanceof Schema.ColumnType.Bytes)) {
            throw new IllegalStateException(
                    "Cobble state column family '"
                            + stateName
                            + "' must store bytes values in column 0.");
        }
    }

    private PriorityQueueSetFactory createPriorityQueueFactory(
            boolean restoredNativeQueuesMayContainEntries,
            CobbleStateBackend.PriorityQueueStateType type) {
        switch (type) {
            case HEAP:
                return new HeapPriorityQueueSetFactory(
                        keyContext.getKeyGroupRange(), keyContext.getNumberOfKeyGroups(), 128);
            case COBBLE:
                return new CobblePriorityQueueSetFactory(
                        cobbleDb,
                        keyContext.getKeyGroupRange(),
                        keyContext.getNumberOfKeyGroups(),
                        restoredNativeQueuesMayContainEntries,
                        this::registerTimerSchema);
            default:
                throw new IllegalArgumentException("Unknown timer backend: " + type);
        }
    }

    private CobbleStateDescriptor registerStateDescriptor(CobbleStateDescriptor descriptor) {
        String identity = descriptor.stateIdentity();
        CobbleStateDescriptor existing = stateDescriptors.putIfAbsent(identity, descriptor);
        if (existing != null && !existing.equals(descriptor)) {
            throw new IllegalStateException(
                    "State '"
                            + descriptor.stateName()
                            + "' has conflicting Cobble row formats: "
                            + existing
                            + " and "
                            + descriptor
                            + '.');
        }
        return existing == null ? descriptor : existing;
    }

    private List<CobbleStateDescriptor> stateDescriptorSnapshot() {
        return new ArrayList<>(stateDescriptors.values());
    }

    private boolean hasCobblePriorityQueues() {
        return priorityQueueFactory instanceof CobblePriorityQueueSetFactory
                && ((CobblePriorityQueueSetFactory) priorityQueueFactory).hasQueues();
    }

    private void flushPendingTimerWrites() {
        if (priorityQueueFactory instanceof CobblePriorityQueueSetFactory) {
            ((CobblePriorityQueueSetFactory) priorityQueueFactory).flushPendingWrites();
        }
    }

    private void ensureOpen() {
        if (resourcesClosed.get()) {
            throw new IllegalStateException("Cobble async backend is closed.");
        }
    }

    private void closeResources() throws IOException {
        if (!resourcesClosed.compareAndSet(false, true)) {
            return;
        }
        IOException error = null;
        for (CobbleStateExecutor executor : managedStateExecutors) {
            executor.shutdown();
        }
        managedStateExecutors.clear();
        snapshotStrategy.close();
        nativeMetricsMonitor.close();
        if (priorityQueueFactory instanceof CobblePriorityQueueSetFactory) {
            try {
                ((CobblePriorityQueueSetFactory) priorityQueueFactory).close();
            } catch (RuntimeException e) {
                error = new IOException("Failed to close Cobble timer queues.", e);
            }
        }
        try {
            cobbleDb.close();
        } catch (RuntimeException e) {
            error = append(error, new IOException("Failed to close Cobble DB.", e));
        }
        for (CobbleAsyncRequestState state : stateResources) {
            try {
                state.close();
            } catch (RuntimeException e) {
                error = append(error, new IOException("Failed to close async state resources.", e));
            }
        }
        try {
            FileUtils.deleteDirectory(instanceBasePath);
        } catch (Exception e) {
            error = append(error, new IOException("Failed to remove Cobble working directory.", e));
        }
        if (error != null) {
            throw error;
        }
    }

    private static IOException append(IOException existing, IOException next) {
        if (existing == null) {
            return next;
        }
        existing.addSuppressed(next);
        return existing;
    }

    private static CobbleStateDescriptor.StateKind toCommonStateKind(StateDescriptor.Type type) {
        switch (type) {
            case VALUE:
                return CobbleStateDescriptor.StateKind.VALUE;
            case LIST:
                return CobbleStateDescriptor.StateKind.LIST;
            case MAP:
                return CobbleStateDescriptor.StateKind.MAP;
            case REDUCING:
                return CobbleStateDescriptor.StateKind.REDUCING;
            case AGGREGATING:
                return CobbleStateDescriptor.StateKind.AGGREGATING;
            default:
                throw new IllegalArgumentException("Unsupported Flink v2 state type: " + type);
        }
    }
}
