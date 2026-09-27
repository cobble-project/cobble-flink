package io.cobble.flink.inspect.internal;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.SnapshotTools;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleMetadataFileIO;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.TableReader;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.util.List;

/** Loads native table schema metadata for a pinned data-source snapshot. */
final class CobbleTableInspectSchemaResolver {

    private static final String LEGACY_SCHEMA_EVENTS = "inspect-schema/events";

    private CobbleTableInspectSchemaResolver() {}

    static TableInspectSchema resolve(
            String sourceRoot,
            GlobalSnapshot snapshot,
            CobbleConnectorStorageOptions storageOptions) {
        rejectLegacySinkFormat(sourceRoot, storageOptions);
        if (snapshot == null
                || snapshot.shardSnapshots == null
                || snapshot.shardSnapshots.isEmpty()) {
            throw new InspectInputException("Cobble Table snapshot has no shards");
        }
        // A raw database may also call its column family "data". Only persisted Table metadata
        // establishes that this family has the native Table layout.
        if (snapshot.columnFamilyIds == null
                || !snapshot.columnFamilyIds.containsKey(CobbleTableRowConverter.TABLE_NAME)) {
            return null;
        }
        int totalBuckets = snapshot.totalBuckets > 0 ? snapshot.totalBuckets : 1;
        Config config = CobbleReaderConfigs.dataSource(totalBuckets, sourceRoot, storageOptions);
        ShardSnapshot shard = snapshot.shardSnapshots.get(0);
        if (shard.columnFamilies == null || shard.columnFamilies.isEmpty()) {
            shard = SnapshotTools.loadShardSnapshot(config, shard.dbId, shard.manifestPath);
        }
        ShardSnapshot.SnapshotColumnFamily family =
                shard.columnFamilies == null
                        ? null
                        : shard.columnFamilies.get(CobbleTableRowConverter.TABLE_NAME);
        if (family == null || family.options == null || family.options.metadata == null) {
            return null;
        }
        JsonElement metadata;
        try {
            metadata = JsonParser.parseString(family.options.metadata);
        } catch (RuntimeException error) {
            return null;
        }
        if (!metadata.isJsonObject()) {
            return null;
        }
        JsonObject object = metadata.getAsJsonObject();
        JsonElement format = object.get("format");
        if (format == null
                || !format.isJsonPrimitive()
                || !format.getAsJsonPrimitive().isString()
                || !"cobble-table".equals(format.getAsString())) {
            return null;
        }
        try (TableReader table =
                TableReader.open(config, CobbleTableRowConverter.TABLE_NAME, snapshot.id)) {
            return new TableInspectSchema(table.tableSchema());
        } catch (RuntimeException error) {
            throw new InspectInputException(
                    "Failed to open native Cobble Table metadata: " + message(error));
        }
    }

    private static void rejectLegacySinkFormat(
            String sourceRoot, CobbleConnectorStorageOptions storageOptions) {
        try {
            CobbleMetadataFileIO fileIO = CobbleMetadataFileIO.open(sourceRoot, storageOptions);
            List<String> events = fileIO.list(LEGACY_SCHEMA_EVENTS);
            if (!events.isEmpty()) {
                throw new InspectInputException(
                        "This path contains the pre-Table Cobble sink format. Rewrite the table"
                                + " with the current Cobble sink before inspecting it.");
            }
        } catch (InspectInputException error) {
            throw error;
        } catch (IOException error) {
            throw new InspectInputException(
                    "Failed to inspect the Cobble Table path: " + message(error));
        }
    }

    private static String message(Throwable error) {
        return error.getMessage() == null ? error.getClass().getName() : error.getMessage();
    }
}
