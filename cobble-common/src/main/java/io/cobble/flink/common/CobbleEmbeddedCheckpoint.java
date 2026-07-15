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
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
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
        Long snapshotId = parseSnapshotId(entry.getName());
        if (snapshotId != null) {
            Location location = locateFromShard(entry, snapshotId.longValue());
            if (location != null) {
                return Collections.singletonList(location);
            }
        }
        throw unsupported(entry, "no readable Cobble embedded checkpoint metadata was found");
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

    private static Location locateFromShard(Path snapshotManifest, long snapshotId)
            throws IOException {
        Path ancestor = snapshotManifest.getParent();
        for (int depth = 0; ancestor != null && depth < MAX_SHARD_ANCESTORS; depth++) {
            Path metadata = new Path(new Path(ancestor, CHECKPOINT_PREFIX + snapshotId), METADATA);
            FileSystem fs = metadata.getFileSystem();
            if (fs.exists(metadata)) {
                try {
                    Location location = readLocation(metadata);
                    if (location.checkpoint().checkpointId() == snapshotId) {
                        return location;
                    }
                } catch (IOException ignored) {
                    // This sibling checkpoint is not Cobble embedded state; keep walking upward.
                }
            }
            ancestor = ancestor.getParent();
        }
        return null;
    }

    private static Location readLocation(Path metadataPath) throws IOException {
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
            List<StateInspectSchemaStore> stores = new ArrayList<>();
            for (OperatorSubtaskState subtaskState : operatorState.getStates()) {
                collectCobbleHandles(subtaskState.getManagedKeyedState(), shards, stores);
                collectCobbleHandles(subtaskState.getRawKeyedState(), shards, stores);
            }
            if (!shards.isEmpty()) {
                String operatorId = operatorState.getOperatorID().toHexString();
                operators.put(
                        operatorId,
                        new OperatorSnapshot(
                                operatorId,
                                operatorState.getMaxParallelism(),
                                new ArrayList<>(shards.values()),
                                mergeSchemaStores(stores)));
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
            List<StateInspectSchemaStore> stores)
            throws IOException {
        for (KeyedStateHandle handle : handles) {
            if (!(handle instanceof IncrementalRemoteKeyedStateHandle)) {
                continue;
            }
            IncrementalRemoteKeyedStateHandle incremental =
                    (IncrementalRemoteKeyedStateHandle) handle;
            if (incremental.getMetaStateHandle() == null) {
                continue;
            }
            CobbleSnapshotMetadataPayload payload;
            try (FSDataInputStream input = incremental.getMetaStateHandle().openInputStream()) {
                payload =
                        CobbleSnapshotMetadataCodec.readIfPresent(
                                new DataInputViewStreamWrapper(input));
            }
            if (payload == null || payload.shardSnapshot() == null) {
                continue;
            }
            ShardSnapshot shard = payload.shardSnapshot();
            shards.putIfAbsent(shard.dbId + ':' + shard.snapshotId, shard);
            if (!payload.schemaStore().isEmpty()) {
                stores.add(payload.schemaStore());
            }
        }
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
            return id > 0L ? Long.valueOf(id) : null;
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

    /** Cobble shards and merged inspect schema for one Flink operator. */
    public static final class OperatorSnapshot {
        private final String operatorId;
        private final int maxParallelism;
        private final List<ShardSnapshot> shards;
        private final StateInspectSchemaStore schemaStore;

        private OperatorSnapshot(
                String operatorId,
                int maxParallelism,
                List<ShardSnapshot> shards,
                StateInspectSchemaStore schemaStore) {
            this.operatorId = operatorId;
            this.maxParallelism = maxParallelism;
            this.shards = Collections.unmodifiableList(new ArrayList<>(shards));
            this.schemaStore = schemaStore;
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

        public StateInspectSchemaStore schemaStore() {
            return schemaStore;
        }
    }
}
