package io.cobble.flink.table;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Writes content-addressed inspect schema metadata for Cobble SQL sink snapshots. */
final class CobbleSinkInspectSchemaRegistry {

    static final String SCHEMA_DIR = "inspect-schema";
    static final String BLOBS_DIR = "blobs";
    static final String EVENTS_DIR = "events";

    private CobbleSinkInspectSchemaRegistry() {}

    static String writeForSnapshot(
            CobbleDynamicTableSink.SerializableConfig config, long snapshotId) throws IOException {
        byte[] bytes = SinkInspectSchemaStore.of(toSchema(config)).toBytes();
        String hash = InspectSchemaRegistryLayout.sha256(bytes);

        Path schemaRoot = new Path(new Path(config.pathUri), SCHEMA_DIR);
        Path blobsDir = new Path(schemaRoot, BLOBS_DIR);
        Path eventsDir = new Path(schemaRoot, EVENTS_DIR);
        Path blobPath = new Path(blobsDir, InspectSchemaRegistryLayout.blobFileName(hash));
        Path eventPath =
                new Path(eventsDir, InspectSchemaRegistryLayout.eventFileName(snapshotId, hash));

        FileSystem fileSystem = schemaRoot.getFileSystem();
        fileSystem.mkdirs(blobsDir);
        fileSystem.mkdirs(eventsDir);

        writeBlobIfMissing(fileSystem, blobPath, bytes);
        createEventIfMissing(fileSystem, eventPath);
        return hash;
    }

    static SinkInspectSchema toSchema(CobbleDynamicTableSink.SerializableConfig config) {
        List<SinkInspectField> keyFields = new ArrayList<>(config.keyFields.size());
        for (CobbleDynamicTableSink.SerializableField field : config.keyFields) {
            keyFields.add(
                    SinkInspectField.key(
                            field.name,
                            field.logicalType,
                            field.rowIndex,
                            field.structuredColumnIndex));
        }

        List<SinkInspectField> valueFields = new ArrayList<>(config.valueFields.size());
        for (CobbleDynamicTableSink.SerializableField field : config.valueFields) {
            valueFields.add(
                    SinkInspectField.value(
                            field.name,
                            field.logicalType,
                            field.rowIndex,
                            field.structuredColumnIndex));
        }
        return new SinkInspectSchema(keyFields, valueFields);
    }

    private static void writeBlobIfMissing(FileSystem fileSystem, Path path, byte[] bytes)
            throws IOException {
        if (fileSystem.exists(path)) {
            return;
        }
        try (FSDataOutputStream out = fileSystem.create(path, FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write(bytes);
        }
    }

    private static void createEventIfMissing(FileSystem fileSystem, Path path) throws IOException {
        if (fileSystem.exists(path)) {
            return;
        }
        try (FSDataOutputStream ignored =
                fileSystem.create(path, FileSystem.WriteMode.NO_OVERWRITE)) {
            // The event filename contains the snapshot id and target content hash.
        }
    }
}
