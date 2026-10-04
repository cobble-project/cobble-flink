package io.cobble.flink.common;

import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Locates and reads Cobble keyed-state payloads embedded in Flink checkpoint metadata. */
public final class CobbleEmbeddedCheckpoint {

    private static final Logger LOG = LoggerFactory.getLogger(CobbleEmbeddedCheckpoint.class);
    private static final String METADATA = "_metadata";
    private static final String CHECKPOINT_PREFIX = "chk-";
    private static final String SNAPSHOT_PREFIX = "SNAPSHOT-";
    private static final int MAX_SHARD_ANCESTORS = 8;
    private static final int MAX_CHECKPOINT_CHILD_DEPTH = 3;
    private static final Method META_STATE_HANDLE_ACCESSOR = resolveMetaStateHandleAccessor();

    private final long checkpointId;
    private final Map<String, OperatorSnapshot> operators;

    private CobbleEmbeddedCheckpoint(long checkpointId, Map<String, OperatorSnapshot> operators) {
        this.checkpointId = checkpointId;
        this.operators = Collections.unmodifiableMap(new LinkedHashMap<>(operators));
    }

    public long checkpointId() {
        return checkpointId;
    }

    public Map<String, OperatorSnapshot> operators() {
        return operators;
    }

    public OperatorSnapshot operator(String operatorId) {
        return operators.get(operatorId);
    }

    /**
     * Resolves a checkpoint root, a direct checkpoint/savepoint directory, an {@code _metadata}
     * file, or a shard {@code SNAPSHOT-N} manifest back to readable Cobble metadata.
     */
    public static List<Location> locate(Path entry) throws IOException {
        FileSystem fs = entry.getFileSystem();
        if (!fs.exists(entry)) {
            throw unsupported(entry, "path does not exist");
        }
        FileStatus status = fs.getFileStatus(entry);
        if (!status.isDir() && METADATA.equals(entry.getName())) {
            return Collections.singletonList(readLocation(entry));
        }
        if (status.isDir()) {
            Path metadata = new Path(entry, METADATA);
            if (fs.exists(metadata)) {
                return Collections.singletonList(readLocation(metadata));
            }
            List<Location> children = locateCheckpointChildren(fs, entry);
            if (!children.isEmpty()) {
                return children;
            }
        }
        if (parseSnapshotId(entry.getName()) != null) {
            Location location = locateFromShard(entry);
            if (location != null) {
                return Collections.singletonList(location);
            }
        }
        throw unsupported(entry, "no readable Cobble embedded checkpoint metadata was found");
    }

    /**
     * Discovers completed checkpoint metadata paths without decoding their payloads. Callers can
     * select one {@code chk-N} directory before strictly loading it, isolating a selected
     * checkpoint from unrelated retained checkpoint files.
     */
    public static List<MetadataLocation> locateMetadataPaths(Path entry) throws IOException {
        FileSystem fs = entry.getFileSystem();
        if (!fs.exists(entry)) {
            throw unsupported(entry, "path does not exist");
        }
        FileStatus status = fs.getFileStatus(entry);
        if (!status.isDir() && METADATA.equals(entry.getName())) {
            return Collections.singletonList(new MetadataLocation(entry.getParent(), entry));
        }
        if (!status.isDir()) {
            throw unsupported(entry, "expected a checkpoint directory or _metadata file");
        }
        Path metadata = new Path(entry, METADATA);
        if (fs.exists(metadata)) {
            return Collections.singletonList(new MetadataLocation(entry, metadata));
        }
        List<MetadataLocation> locations = new ArrayList<>();
        collectCheckpointMetadataPaths(fs, entry, 0, locations);
        if (locations.isEmpty()) {
            throw unsupported(entry, "no completed checkpoint metadata was found");
        }
        return locations;
    }

    /** Selects {@code latest} or an exact checkpoint id from {@link #locate(Path)}. */
    public static Location select(Path entry, String checkpointId) throws IOException {
        List<Location> locations = locate(entry);
        if ("latest".equals(checkpointId)) {
            return locations.get(0);
        }
        final long requested;
        try {
            requested = Long.parseLong(checkpointId);
        } catch (NumberFormatException e) {
            throw new IOException(
                    "checkpoint id must be 'latest' or a positive number: " + checkpointId, e);
        }
        for (Location location : locations) {
            if (location.checkpoint().checkpointId() == requested) {
                return location;
            }
        }
        throw new IOException(
                "Cobble embedded checkpoint "
                        + requested
                        + " was not found from "
                        + entry
                        + ". Available checkpoint ids: "
                        + checkpointIds(locations));
    }

    private static List<Location> locateCheckpointChildren(FileSystem fs, Path root)
            throws IOException {
        List<Location> locations = new ArrayList<>();
        collectCheckpointChildren(fs, root, 0, locations);
        locations.sort(
                Comparator.comparingLong((Location value) -> value.checkpoint().checkpointId())
                        .reversed());
        return locations;
    }

    private static void collectCheckpointMetadataPaths(
            FileSystem fs, Path root, int depth, List<MetadataLocation> locations)
            throws IOException {
        if (depth > MAX_CHECKPOINT_CHILD_DEPTH) {
            return;
        }
        FileStatus[] children = fs.listStatus(root);
        if (children == null) {
            return;
        }
        for (FileStatus child : children) {
            if (!child.isDir()) {
                continue;
            }
            Path childPath = child.getPath();
            Path metadata = new Path(childPath, METADATA);
            if (fs.exists(metadata)) {
                locations.add(new MetadataLocation(childPath, metadata));
                continue;
            }
            if (depth < MAX_CHECKPOINT_CHILD_DEPTH && !skipCheckpointChild(childPath.getName())) {
                collectCheckpointMetadataPaths(fs, childPath, depth + 1, locations);
            }
        }
    }

    private static void collectCheckpointChildren(
            FileSystem fs, Path root, int depth, List<Location> locations) throws IOException {
        if (depth > MAX_CHECKPOINT_CHILD_DEPTH) {
            return;
        }
        FileStatus[] children = fs.listStatus(root);
        if (children == null) {
            return;
        }
        for (FileStatus child : children) {
            if (!child.isDir()) {
                continue;
            }
            Path childPath = child.getPath();
            if (parseCheckpointDirectoryId(childPath.getName()) != null) {
                Path metadata = new Path(childPath, METADATA);
                if (!fs.exists(metadata)) {
                    continue;
                }
                try {
                    locations.add(readLocation(metadata));
                } catch (IOException ignored) {
                    // A checkpoint may contain another backend's state; keep looking at siblings.
                }
                continue;
            }
            if (depth < MAX_CHECKPOINT_CHILD_DEPTH && !skipCheckpointChild(childPath.getName())) {
                collectCheckpointChildren(fs, childPath, depth + 1, locations);
            }
        }
    }

    private static boolean skipCheckpointChild(String name) {
        return "shared".equals(name)
                || "taskowned".equals(name)
                || "cobble".equals(name)
                || "snapshot".equals(name);
    }

    private static Location locateFromShard(Path snapshotManifest) throws IOException {
        Path ancestor = snapshotManifest.getParent();
        for (int depth = 0; ancestor != null && depth < MAX_SHARD_ANCESTORS; depth++) {
            Map<String, Location> matching = new LinkedHashMap<>();
            FileSystem fs = ancestor.getFileSystem();
            for (Location location : locationsAtOrBelow(fs, ancestor)) {
                if (referencesShardManifest(location.checkpoint(), snapshotManifest)) {
                    matching.putIfAbsent(location.checkpointDirectory().toString(), location);
                }
            }
            if (matching.size() > 1) {
                throw new IOException(
                        "multiple Cobble checkpoints reference shard manifest " + snapshotManifest);
            }
            if (!matching.isEmpty()) return matching.values().iterator().next();
            ancestor = ancestor.getParent();
        }
        return null;
    }

    private static List<Location> locationsAtOrBelow(FileSystem fs, Path root) throws IOException {
        List<Location> locations = new ArrayList<>();
        Path metadata = new Path(root, METADATA);
        if (fs.exists(metadata)) {
            try {
                locations.add(readLocation(metadata));
            } catch (IOException ignored) {
                // This ancestor can be a non-Cobble checkpoint. Continue the bounded search.
            }
        }
        locations.addAll(locateCheckpointChildren(fs, root));
        return locations;
    }

    private static boolean referencesShardManifest(
            CobbleEmbeddedCheckpoint checkpoint, Path snapshotManifest) {
        String expected = normalizePath(snapshotManifest);
        for (OperatorSnapshot operator : checkpoint.operators().values()) {
            for (ShardSnapshot shard : operator.shards()) {
                if (shard != null
                        && shard.manifestPath != null
                        && expected.equals(normalizePath(new Path(shard.manifestPath)))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static String normalizePath(Path path) {
        if (path == null) return "";
        try {
            URI uri = path.toUri();
            if (uri.getScheme() == null || "file".equalsIgnoreCase(uri.getScheme())) {
                java.nio.file.Path local =
                        uri.getScheme() == null ? Paths.get(path.toString()) : Paths.get(uri);
                return local.toAbsolutePath().normalize().toUri().toString().replaceAll("/+$", "");
            }
            return uri.normalize().toString().replaceAll("/+$", "");
        } catch (RuntimeException ignored) {
            return path.toString().replaceAll("/+$", "");
        }
    }

    /** Strictly decodes one selected completed Flink checkpoint metadata file. */
    public static Location readLocation(Path metadataPath) throws IOException {
        CobbleEmbeddedCheckpoint checkpoint = read(metadataPath);
        return new Location(metadataPath.getParent(), metadataPath, checkpoint);
    }

    /** Reads a concrete Flink {@code _metadata} file containing Cobble keyed-state handles. */
    public static CobbleEmbeddedCheckpoint read(Path metadataPath) throws IOException {
        if (!METADATA.equals(metadataPath.getName())) {
            throw new IOException("Expected a Flink _metadata file, got " + metadataPath + ".");
        }
        CheckpointMetadata metadata;
        try (FSDataInputStream input = metadataPath.getFileSystem().open(metadataPath)) {
            metadata =
                    Checkpoints.loadCheckpointMetadata(
                            new DataInputStream(input),
                            Thread.currentThread().getContextClassLoader(),
                            metadataPath.getParent().toUri().toString());
        }
        Map<String, OperatorSnapshot> operators = new LinkedHashMap<>();
        for (OperatorState operatorState : metadata.getOperatorStates()) {
            Map<String, ShardSnapshot> shards = new LinkedHashMap<>();
            List<CobbleStateDescriptor> stateDescriptors = new ArrayList<>();
            List<StateInspectSchemaStore> stores = new ArrayList<>();
            List<String> volumeDirectories = new ArrayList<>();
            List<KeyGroupsStateHandle> rawKeyedState = new ArrayList<>();
            for (OperatorSubtaskState subtaskState : operatorState.getStates()) {
                for (KeyedStateHandle handle : subtaskState.getRawKeyedState()) {
                    if (handle instanceof KeyGroupsStateHandle) {
                        rawKeyedState.add((KeyGroupsStateHandle) handle);
                    }
                }
                collectCobbleHandles(
                        subtaskState.getManagedKeyedState(),
                        shards,
                        stateDescriptors,
                        stores,
                        volumeDirectories);
                collectCobbleHandles(
                        subtaskState.getRawKeyedState(),
                        shards,
                        stateDescriptors,
                        stores,
                        volumeDirectories);
            }
            if (!shards.isEmpty()) {
                String operatorId = operatorState.getOperatorID().toHexString();
                operators.put(
                        operatorId,
                        new OperatorSnapshot(
                                operatorId,
                                operatorState.getMaxParallelism(),
                                new ArrayList<>(shards.values()),
                                mergeStateDescriptors(stateDescriptors),
                                mergeSchemaStores(stores),
                                volumeDirectories,
                                rawKeyedState));
            }
        }
        if (operators.isEmpty()) {
            throw new IOException(
                    "No Cobble embedded keyed-state payloads were found in " + metadataPath + ".");
        }
        return new CobbleEmbeddedCheckpoint(metadata.getCheckpointId(), operators);
    }

    private static void collectCobbleHandles(
            Collection<KeyedStateHandle> handles,
            Map<String, ShardSnapshot> shards,
            List<CobbleStateDescriptor> stateDescriptors,
            List<StateInspectSchemaStore> stores,
            List<String> volumeDirectories)
            throws IOException {
        for (KeyedStateHandle handle : handles) {
            if (!(handle instanceof IncrementalRemoteKeyedStateHandle)) {
                continue;
            }
            IncrementalRemoteKeyedStateHandle incremental =
                    (IncrementalRemoteKeyedStateHandle) handle;
            StreamStateHandle metadataHandle = metaStateHandle(incremental);
            if (metadataHandle == null) {
                continue;
            }
            CobbleSnapshotMetadataPayload payload;
            try (FSDataInputStream input = metadataHandle.openInputStream()) {
                payload =
                        CobbleSnapshotMetadataCodec.readIfPresent(
                                new DataInputViewStreamWrapper(input));
            }
            if (payload == null || payload.shardSnapshot() == null) {
                continue;
            }
            ShardSnapshot shard = payload.shardSnapshot();
            shards.putIfAbsent(shard.dbId + ':' + shard.snapshotId, shard);
            stateDescriptors.addAll(payload.stateDescriptors());
            volumeDirectories.addAll(payload.volumeDirectories());
            if (!payload.schemaStore().isEmpty()) {
                stores.add(payload.schemaStore());
            }
        }
    }

    /**
     * Flink 1.19 renamed this accessor when introducing AbstractIncrementalStateHandle. Resolve
     * against the runtime Flink classes so this shared module also works on 1.17/1.18 and 2.x;
     * caching the method avoids repeating reflection lookup for every shard.
     */
    private static Method resolveMetaStateHandleAccessor() {
        for (String name : new String[] {"getMetaDataStateHandle", "getMetaStateHandle"}) {
            try {
                return IncrementalRemoteKeyedStateHandle.class.getMethod(name);
            } catch (NoSuchMethodException ignored) {
                // Try the legacy name when running on Flink before 1.19.
            }
        }
        throw new IllegalStateException(
                "Flink IncrementalRemoteKeyedStateHandle has no metadata handle accessor");
    }

    private static StreamStateHandle metaStateHandle(IncrementalRemoteKeyedStateHandle handle)
            throws IOException {
        try {
            return (StreamStateHandle) META_STATE_HANDLE_ACCESSOR.invoke(handle);
        } catch (ReflectiveOperationException error) {
            throw new IOException("Failed to read Flink incremental metadata handle", error);
        }
    }

    private static List<CobbleStateDescriptor> mergeStateDescriptors(
            List<CobbleStateDescriptor> descriptors) throws IOException {
        Map<String, CobbleStateDescriptor> merged = new LinkedHashMap<>();
        for (CobbleStateDescriptor descriptor : descriptors) {
            String key = descriptor.stateIdentity();
            CobbleStateDescriptor existing = merged.putIfAbsent(key, descriptor);
            if (existing != null && !existing.equals(descriptor)) {
                throw new IOException(
                        "Cobble checkpoint contains conflicting row format descriptors for state '"
                                + descriptor.stateName()
                                + "': "
                                + existing
                                + " and "
                                + descriptor
                                + '.');
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(merged.values()));
    }

    private static StateInspectSchemaStore mergeSchemaStores(List<StateInspectSchemaStore> stores) {
        Map<String, StateInspectSchema> schemas = new LinkedHashMap<>();
        Map<String, StateInspectSemanticSchema> semantic = new LinkedHashMap<>();
        for (StateInspectSchemaStore store : stores) {
            for (StateInspectSchema schema : store.schemas()) {
                String key = schema.stateKind().name() + ':' + schema.stateName();
                StateInspectSchema existing = schemas.putIfAbsent(key, schema);
                if (existing != null && !existing.equals(schema)) {
                    LOG.warn(
                            "Inspect schema mismatch for {} across subtasks; keeping first registration.",
                            key);
                }
            }
            for (Map.Entry<String, StateInspectSemanticSchema> entry :
                    store.semanticSchemas().entrySet()) {
                StateInspectSemanticSchema existing =
                        semantic.putIfAbsent(entry.getKey(), entry.getValue());
                if (existing != null && !existing.equals(entry.getValue())) {
                    LOG.warn(
                            "Inspect semantic schema mismatch for state '{}' across subtasks; keeping first registration.",
                            entry.getKey());
                }
            }
        }
        return schemas.isEmpty()
                ? StateInspectSchemaStore.empty()
                : new StateInspectSchemaStore(new ArrayList<>(schemas.values()), semantic);
    }

    private static Long parseCheckpointDirectoryId(String name) {
        if (name == null || !name.startsWith(CHECKPOINT_PREFIX)) {
            return null;
        }
        try {
            long id = Long.parseLong(name.substring(CHECKPOINT_PREFIX.length()));
            return id > 0L ? Long.valueOf(id) : null;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static Long parseSnapshotId(String name) {
        if (name == null || !name.startsWith(SNAPSHOT_PREFIX)) {
            return null;
        }
        try {
            long id = Long.parseLong(name.substring(SNAPSHOT_PREFIX.length()));
            return id >= 0L ? Long.valueOf(id) : null;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static IOException unsupported(Path entry, String detail) {
        return new IOException(
                "Could not locate Cobble embedded checkpoint metadata from "
                        + entry
                        + ": "
                        + detail
                        + ". Accepted entries are a checkpoint root containing chk-N directories, a chk-N/savepoint directory, an _metadata file, or a SNAPSHOT-N manifest under the same checkpoint tree.");
    }

    private static String checkpointIds(List<Location> locations) {
        List<String> ids = new ArrayList<>();
        for (Location location : locations) {
            ids.add(Long.toString(location.checkpoint().checkpointId()));
        }
        return String.join(", ", ids);
    }

    /** A concrete metadata location and its parsed Cobble operator payloads. */
    public static final class Location {
        private final Path checkpointDirectory;
        private final Path metadataPath;
        private final CobbleEmbeddedCheckpoint checkpoint;

        private Location(
                Path checkpointDirectory, Path metadataPath, CobbleEmbeddedCheckpoint checkpoint) {
            this.checkpointDirectory = checkpointDirectory;
            this.metadataPath = metadataPath;
            this.checkpoint = checkpoint;
        }

        public Path checkpointDirectory() {
            return checkpointDirectory;
        }

        public Path metadataPath() {
            return metadataPath;
        }

        public CobbleEmbeddedCheckpoint checkpoint() {
            return checkpoint;
        }
    }

    /** A completed Flink checkpoint metadata file, intentionally not decoded yet. */
    public static final class MetadataLocation {
        private final Path checkpointDirectory;
        private final Path metadataPath;
        private final Long directoryCheckpointId;

        private MetadataLocation(Path checkpointDirectory, Path metadataPath) {
            this.checkpointDirectory = checkpointDirectory;
            this.metadataPath = metadataPath;
            this.directoryCheckpointId = parseCheckpointDirectoryId(checkpointDirectory.getName());
        }

        public Path checkpointDirectory() {
            return checkpointDirectory;
        }

        public Path metadataPath() {
            return metadataPath;
        }

        /** Returns the {@code chk-N} directory ID when the location is named conventionally. */
        public Long directoryCheckpointId() {
            return directoryCheckpointId;
        }
    }

    /** Cobble shards and merged inspect schema for one Flink operator. */
    public static final class OperatorSnapshot {
        private final String operatorId;
        private final int maxParallelism;
        private final List<ShardSnapshot> shards;
        private final List<CobbleStateDescriptor> stateDescriptors;
        private final StateInspectSchemaStore schemaStore;
        private final List<String> volumeDirectories;
        private final List<KeyGroupsStateHandle> rawKeyedState;

        private OperatorSnapshot(
                String operatorId,
                int maxParallelism,
                List<ShardSnapshot> shards,
                List<CobbleStateDescriptor> stateDescriptors,
                StateInspectSchemaStore schemaStore,
                List<String> volumeDirectories,
                List<KeyGroupsStateHandle> rawKeyedState) {
            this.operatorId = operatorId;
            this.maxParallelism = maxParallelism;
            this.shards = Collections.unmodifiableList(new ArrayList<>(shards));
            this.stateDescriptors = stateDescriptors;
            this.schemaStore = schemaStore;
            this.volumeDirectories = CobbleSnapshotVolumeRoots.unique(volumeDirectories);
            this.rawKeyedState = Collections.unmodifiableList(new ArrayList<>(rawKeyedState));
        }

        public String operatorId() {
            return operatorId;
        }

        public int maxParallelism() {
            return maxParallelism;
        }

        public List<ShardSnapshot> shards() {
            return shards;
        }

        public List<CobbleStateDescriptor> stateDescriptors() {
            return stateDescriptors;
        }

        public StateInspectSchemaStore schemaStore() {
            return schemaStore;
        }

        public List<String> volumeDirectories() {
            return volumeDirectories;
        }

        /** Flink's raw keyed streams containing the timer queue's prefetched prefix. */
        public List<KeyGroupsStateHandle> rawKeyedState() {
            return rawKeyedState;
        }
    }
}
