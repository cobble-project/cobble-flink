package io.cobble.flink.table;

import io.cobble.flink.common.CobbleMetadataFileIO;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;

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

        String blobsDir = SCHEMA_DIR + "/" + BLOBS_DIR;
        String eventsDir = SCHEMA_DIR + "/" + EVENTS_DIR;
        String blobPath = blobsDir + "/" + InspectSchemaRegistryLayout.blobFileName(hash);
        String eventPath =
                eventsDir + "/" + InspectSchemaRegistryLayout.eventFileName(snapshotId, hash);
        CobbleMetadataFileIO fileIO =
                CobbleMetadataFileIO.open(config.pathUri, config.storageOptions);
        fileIO.mkdirs(blobsDir);
        fileIO.mkdirs(eventsDir);
        fileIO.writeIfAbsent(blobPath, bytes);
        fileIO.writeIfAbsent(eventPath, new byte[0]);
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
}
