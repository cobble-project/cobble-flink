package io.cobble.flink.catalog;

import io.cobble.Config;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleFlinkStorageConfig;
import io.cobble.flink.common.CobbleLoader;
import io.cobble.table.CatalogTable;
import io.cobble.table.FileCatalog;
import io.cobble.table.TableIdentifier;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/** Serializable captured catalog identity used by connector workers and committers. */
public final class CobbleCatalogTableReference implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String warehouse;
    private final String storageId;
    private final String database;
    private final String table;
    private final long tableId;
    private final long schemaId;
    private final CobbleConnectorStorageOptions storageOptions;

    public CobbleCatalogTableReference(
            String warehouse,
            String storageId,
            String database,
            String table,
            long tableId,
            long schemaId) {
        this(
                warehouse,
                storageId,
                database,
                table,
                tableId,
                schemaId,
                CobbleConnectorStorageOptions.empty());
    }

    public CobbleCatalogTableReference(
            String warehouse,
            String storageId,
            String database,
            String table,
            long tableId,
            long schemaId,
            CobbleConnectorStorageOptions storageOptions) {
        this.warehouse = warehouse;
        this.storageId = storageId;
        this.database = database;
        this.table = table;
        this.tableId = tableId;
        this.schemaId = schemaId;
        this.storageOptions =
                storageOptions == null ? CobbleConnectorStorageOptions.empty() : storageOptions;
    }

    public static CobbleCatalogTableReference fromOptions(
            Map<String, String> options, String database, String table) {
        try {
            return new CobbleCatalogTableReference(
                    options.get(CobbleCatalog.OPTION_PATH),
                    options.get(CobbleCatalog.OPTION_STORAGE_ID),
                    database,
                    table,
                    Long.parseLong(options.get(CobbleCatalog.OPTION_TABLE_ID)),
                    Long.parseLong(options.get(CobbleCatalog.OPTION_SCHEMA_ID)),
                    CobbleConnectorStorageOptions.from(options));
        } catch (RuntimeException error) {
            throw new IllegalArgumentException("Invalid Cobble catalog table identity.", error);
        }
    }

    /** Opens the named entry and rejects a drop/recreate or changed captured schema identity. */
    public Opened openValidated() {
        CobbleLoader.ensureCobbleLoaded();
        FileCatalog catalog = FileCatalog.open(runtimeConfig(), storageId);
        try {
            CatalogTable loaded =
                    catalog.loadTable(
                            new TableIdentifier(Collections.singletonList(database), table));
            if (loaded.tableId() != tableId || loaded.catalogSchemaId() != schemaId) {
                loaded.close();
                throw new IllegalStateException(
                        "Cobble catalog table "
                                + database
                                + "."
                                + table
                                + " no longer matches the table identity captured by this job.");
            }
            return new Opened(catalog, loaded);
        } catch (RuntimeException error) {
            catalog.close();
            throw error;
        }
    }

    public CobbleCatalogTableReference withStorageConfig(CobbleFlinkStorageConfig config) {
        return new CobbleCatalogTableReference(
                warehouse,
                storageId,
                database,
                table,
                tableId,
                schemaId,
                config.resolve(warehouse, storageOptions));
    }

    /** Rebuilds the catalog runtime, including the connector-scoped storage options. */
    public Config runtimeConfig() {
        Config config = new Config();
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = warehouse;
        volume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT);
        storageOptions.applyTo(volume);
        CobbleFlinkStorageConfig.empty().register(config, volume);
        return config;
    }

    public static final class Opened implements AutoCloseable {
        private final FileCatalog catalog;
        private final CatalogTable table;

        private Opened(FileCatalog catalog, CatalogTable table) {
            this.catalog = catalog;
            this.table = table;
        }

        public CatalogTable table() {
            return table;
        }

        @Override
        public void close() {
            try {
                table.close();
            } finally {
                catalog.close();
            }
        }
    }
}
