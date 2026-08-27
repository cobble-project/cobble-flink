package io.cobble.flink.table;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.ReadOnlyDb;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.CobbleMetadataFileIO;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.ReadOnlyTable;
import io.cobble.table.TableSchema;

import org.apache.flink.table.api.ValidationException;

import java.io.IOException;
import java.util.List;

/** Validates a sink-source DDL against schema metadata persisted by Cobble Table. */
final class CobbleTableSourceSchemaResolver {

    private static final String LEGACY_SCHEMA_EVENTS = "inspect-schema/events";

    private CobbleTableSourceSchemaResolver() {}

    static void validate(CobbleDynamicTableSource.SerializableConfig config) {
        rejectLegacySinkFormat(config);
        try {
            GlobalSnapshot snapshot = CobbleSourceRuntime.loadConfiguredSnapshot(config);
            if (snapshot == null
                    || snapshot.shardSnapshots == null
                    || snapshot.shardSnapshots.isEmpty()) {
                throw new ValidationException(
                        "Cobble table source could not find a committed table snapshot.");
            }
            ShardSnapshot shard = snapshot.shardSnapshots.get(0);
            Config readConfig =
                    CobbleSourceRuntime.createSourceScanConfig(config, snapshot.totalBuckets);
            try (ReadOnlyDb db = ReadOnlyDb.open(readConfig, shard.snapshotId, shard.dbId);
                    ReadOnlyTable table =
                            ReadOnlyTable.open(db, CobbleTableRowConverter.TABLE_NAME)) {
                TableSchema expected = config.tableSchema();
                if (!expected.equals(table.schema())) {
                    throw new ValidationException(
                            "Cobble source DDL does not match the persisted Cobble Table schema."
                                    + " Column order, names, types, and PRIMARY KEY must match"
                                    + " exactly.");
                }
            }
        } catch (ValidationException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new ValidationException(
                    "Failed to open the persisted Cobble Table schema at " + config.pathUri + '.',
                    e);
        }
    }

    private static void rejectLegacySinkFormat(CobbleDynamicTableSource.SerializableConfig config) {
        try {
            CobbleMetadataFileIO fileIO =
                    CobbleMetadataFileIO.open(config.pathUri, config.storageOptions);
            List<String> events = fileIO.list(LEGACY_SCHEMA_EVENTS);
            if (!events.isEmpty()) {
                throw new ValidationException(
                        "This path contains the pre-Table Cobble sink format, which is not"
                                + " supported by this connector version. Rewrite the table with"
                                + " the current Cobble sink before reading it.");
            }
        } catch (ValidationException e) {
            throw e;
        } catch (IOException e) {
            throw new ValidationException("Failed to inspect the Cobble table path.", e);
        }
    }
}
