package io.cobble.flink.inspect.internal;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.ReadOnlyDb;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleMetadataFileIO;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.ReadOnlyTable;

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
        ShardSnapshot shard = snapshot.shardSnapshots.get(0);
        int totalBuckets = snapshot.totalBuckets > 0 ? snapshot.totalBuckets : 1;
        Config config = CobbleReaderConfigs.dataSource(totalBuckets, sourceRoot, storageOptions);
        try (ReadOnlyDb db = ReadOnlyDb.open(config, shard.snapshotId, shard.dbId);
                ReadOnlyTable table = ReadOnlyTable.open(db, CobbleTableRowConverter.TABLE_NAME)) {
            return new TableInspectSchema(table.schema());
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
