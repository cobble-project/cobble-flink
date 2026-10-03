package io.cobble.flink.catalog;

import io.cobble.Config;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleLoader;
import io.cobble.table.FileCatalog;
import io.cobble.table.TableIdentifier;
import io.cobble.table.TableSchemaChange;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Flink catalog backed directly by Cobble's file catalog. */
public final class CobbleCatalog implements Catalog {
    public static final String OPTION_PATH = "cobble.catalog.path";
    public static final String OPTION_STORAGE_ID = "cobble.catalog.storage-id";
    public static final String OPTION_TABLE_ID = "cobble.catalog.table-id";
    public static final String OPTION_SCHEMA_ID = "cobble.catalog.schema-id";
    public static final String OPTION_BUCKETS = "bucket";

    private final String name;
    private final String path;
    private final String storageId;
    private final int buckets;
    private final CobbleConnectorStorageOptions storageOptions;
    private FileCatalog catalog;

    CobbleCatalog(String name, String path, String storageId, int buckets) {
        this(name, path, storageId, buckets, CobbleConnectorStorageOptions.empty());
    }

    CobbleCatalog(
            String name,
            String path,
            String storageId,
            int buckets,
            CobbleConnectorStorageOptions storageOptions) {
        this.name = name;
        this.path = path;
        this.storageId = storageId;
        this.buckets = buckets;
        this.storageOptions =
                storageOptions == null ? CobbleConnectorStorageOptions.empty() : storageOptions;
    }

    @Override
    public void open() {
        if (catalog != null) return;
        try {
            CobbleLoader.ensureCobbleLoaded();
            catalog = FileCatalog.open(runtimeConfig(), storageId);
            if (!databaseExists(getDefaultDatabase())) {
                catalog.createNamespace(Collections.singletonList(getDefaultDatabase()));
            }
        } catch (RuntimeException error) {
            if (catalog != null) {
                catalog.close();
                catalog = null;
            }
            throw new CatalogException("Unable to open Cobble catalog " + name, error);
        }
    }

    @Override
    public void close() {
        if (catalog != null) {
            catalog.close();
            catalog = null;
        }
    }

    @Override
    public String getDefaultDatabase() {
        return "default";
    }

    @Override
    public List<String> listDatabases() {
        ensureOpen();
        List<String> result = new ArrayList<String>();
        for (List<String> namespace : catalog.listNamespaces())
            if (namespace.size() == 1) result.add(namespace.get(0));
        return result;
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) throw new DatabaseNotExistException(name, databaseName);
        return new CatalogDatabaseImpl(Collections.<String, String>emptyMap(), "Cobble namespace");
    }

    @Override
    public boolean databaseExists(String databaseName) {
        ensureOpen();
        return catalog.listNamespaces().contains(Collections.singletonList(databaseName));
    }

    @Override
    public void createDatabase(
            String databaseName, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        rejectDatabaseMetadata(database);
        if (databaseExists(databaseName)) {
            if (ignoreIfExists) return;
            throw new DatabaseAlreadyExistException(name, databaseName);
        }
        try {
            catalog.createNamespace(Collections.singletonList(databaseName));
        } catch (RuntimeException error) {
            throw new CatalogException("Unable to create Cobble database " + databaseName, error);
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        if (!databaseExists(databaseName)) {
            if (ignoreIfNotExists) return;
            throw new DatabaseNotExistException(name, databaseName);
        }
        List<String> tables = tableNames(databaseName);
        if (!cascade && !tables.isEmpty()) throw new DatabaseNotEmptyException(name, databaseName);
        if (cascade)
            for (String table : tables)
                catalog.dropTable(
                        new TableIdentifier(Collections.singletonList(databaseName), table));
        try {
            catalog.dropNamespace(Collections.singletonList(databaseName));
        } catch (RuntimeException error) {
            throw new CatalogException("Unable to drop Cobble database " + databaseName, error);
        }
    }

    @Override
    public void alterDatabase(
            String databaseName, CatalogDatabase database, boolean ignoreIfNotExists)
            throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            if (ignoreIfNotExists) return;
            throw new DatabaseNotExistException(name, databaseName);
        }
        rejectDatabaseMetadata(database);
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        requireDatabase(databaseName);
        return tableNames(databaseName);
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException {
        requireDatabase(databaseName);
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath path) throws TableNotExistException {
        io.cobble.table.CatalogTable table = load(path);
        try {
            return toFlinkTable(table);
        } finally {
            table.close();
        }
    }

    @Override
    public boolean tableExists(ObjectPath path) {
        ensureOpen();
        return catalog.tableExists(identifier(path));
    }

    @Override
    public void dropTable(ObjectPath path, boolean ignoreIfNotExists)
            throws TableNotExistException {
        if (!tableExists(path)) {
            if (ignoreIfNotExists) return;
            throw new TableNotExistException(name, path);
        }
        try {
            catalog.dropTable(identifier(path));
        } catch (RuntimeException error) {
            throw new CatalogException("Unable to drop Cobble table " + path, error);
        }
    }

    @Override
    public void renameTable(ObjectPath path, String newName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        if (!tableExists(path)) {
            if (ignoreIfNotExists) return;
            throw new TableNotExistException(name, path);
        }
        ObjectPath target = new ObjectPath(path.getDatabaseName(), newName);
        if (tableExists(target)) throw new TableAlreadyExistException(name, target);
        try {
            io.cobble.table.CatalogTable renamed = catalog.renameTable(identifier(path), newName);
            renamed.close();
        } catch (RuntimeException error) {
            throw new CatalogException("Unable to rename Cobble table " + path, error);
        }
    }

    @Override
    public void createTable(ObjectPath path, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        requireDatabase(path.getDatabaseName());
        if (tableExists(path)) {
            if (ignoreIfExists) return;
            throw new TableAlreadyExistException(name, path);
        }
        if (!(table instanceof CatalogTable))
            throw new CatalogException("Cobble catalog supports tables, not views.");
        org.apache.flink.table.catalog.CatalogTable flinkTable =
                (org.apache.flink.table.catalog.CatalogTable) table;
        rejectTableMetadata(flinkTable);
        try {
            io.cobble.table.CatalogTable created =
                    catalog.createTable(
                            identifier(path),
                            CobbleCatalogTypes.toCobble(flinkTable.getUnresolvedSchema()));
            created.close();
        } catch (ValidationException error) {
            throw error;
        } catch (RuntimeException error) {
            throw new CatalogException("Unable to create Cobble table " + path, error);
        }
    }

    @Override
    public void alterTable(ObjectPath path, CatalogBaseTable table, boolean ignoreIfNotExists)
            throws TableNotExistException {
        if (!tableExists(path)) {
            if (ignoreIfNotExists) return;
            throw new TableNotExistException(name, path);
        }
        throw new CatalogException(
                "Cobble catalog requires explicit ALTER TABLE changes; replacing a table schema is "
                        + "rejected to preserve field identity.");
    }

    @Override
    public void alterTable(
            ObjectPath path,
            CatalogBaseTable table,
            List<TableChange> changes,
            boolean ignoreIfNotExists)
            throws TableNotExistException {
        if (!tableExists(path)) {
            if (ignoreIfNotExists) return;
            throw new TableNotExistException(name, path);
        }
        io.cobble.table.CatalogTable current = load(path);
        try {
            List<TableSchemaChange> nativeChanges = CobbleCatalogAlter.toCobble(current, changes);
            io.cobble.table.CatalogTable evolved =
                    catalog.evolveSchema(identifier(path), nativeChanges);
            evolved.close();
        } finally {
            current.close();
        }
    }

    private org.apache.flink.table.catalog.CatalogTable toFlinkTable(
            io.cobble.table.CatalogTable table) {
        Map<String, String> options = new HashMap<String, String>();
        options.put("connector", "cobble");
        options.put(OPTION_PATH, path);
        options.put(OPTION_STORAGE_ID, storageId);
        options.put(OPTION_TABLE_ID, Long.toString(table.tableId()));
        options.put(OPTION_SCHEMA_ID, Long.toString(table.catalogSchemaId()));
        options.put(OPTION_BUCKETS, Integer.toString(buckets));
        options.putAll(storageOptions.asTableOptions());
        return new CobbleCatalogTable(
                CobbleCatalogTypes.toFlink(table.schema()),
                "Cobble catalog table",
                Collections.<String>emptyList(),
                options);
    }

    private io.cobble.table.CatalogTable load(ObjectPath path) throws TableNotExistException {
        ensureOpen();
        if (!tableExists(path)) throw new TableNotExistException(name, path);
        try {
            return catalog.loadTable(identifier(path));
        } catch (RuntimeException error) {
            throw new CatalogException("Unable to load Cobble table " + path, error);
        }
    }

    private TableIdentifier identifier(ObjectPath path) {
        return new TableIdentifier(
                Collections.singletonList(path.getDatabaseName()), path.getObjectName());
    }

    private void ensureOpen() {
        if (catalog == null) open();
    }

    private void requireDatabase(String database) throws DatabaseNotExistException {
        if (!databaseExists(database)) throw new DatabaseNotExistException(name, database);
    }

    private List<String> tableNames(String database) {
        List<String> result = new ArrayList<String>();
        for (TableIdentifier id : catalog.listTables(Collections.singletonList(database)))
            result.add(id.name());
        return result;
    }

    private Config runtimeConfig() {
        Config config = new Config();
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = path;
        volume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT);
        storageOptions.applyTo(volume);
        config.addVolume(volume);
        return config;
    }

    private static void rejectDatabaseMetadata(CatalogDatabase database) {
        if (!database.getProperties().isEmpty()
                || (database.getComment() != null && !database.getComment().isEmpty()))
            throw new CatalogException(
                    "Cobble catalog does not persist database properties or comments.");
    }

    private static void rejectTableMetadata(org.apache.flink.table.catalog.CatalogTable table) {
        org.apache.flink.table.api.Schema schema = table.getUnresolvedSchema();
        if (!table.getOptions().isEmpty()
                || table.isPartitioned()
                || (table.getComment() != null && !table.getComment().isEmpty()))
            throw new CatalogException(
                    "Cobble catalog does not persist table options, partitions, or comments.");
        if (!schema.getWatermarkSpecs().isEmpty()) {
            throw new CatalogException("Cobble catalog does not support watermarks.");
        }
        for (org.apache.flink.table.api.Schema.UnresolvedColumn column : schema.getColumns()) {
            if (column.getComment().isPresent()) {
                throw new CatalogException("Cobble catalog does not persist column comments.");
            }
        }
    }

    private static CatalogException unsupported(String subject) {
        return new CatalogException("Cobble catalog does not support " + subject + ".");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath p)
            throws TableNotExistException, TableNotPartitionedException {
        throw unsupported("partitions");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath p, CatalogPartitionSpec s)
            throws TableNotExistException, TableNotPartitionedException {
        throw unsupported("partitions");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath p, List<org.apache.flink.table.expressions.Expression> f)
            throws TableNotExistException, TableNotPartitionedException {
        throw unsupported("partitions");
    }

    @Override
    public CatalogPartition getPartition(ObjectPath p, CatalogPartitionSpec s)
            throws PartitionNotExistException {
        throw unsupported("partitions");
    }

    @Override
    public boolean partitionExists(ObjectPath p, CatalogPartitionSpec s) {
        return false;
    }

    @Override
    public void createPartition(ObjectPath p, CatalogPartitionSpec s, CatalogPartition v, boolean i)
            throws PartitionAlreadyExistsException {
        throw unsupported("partitions");
    }

    @Override
    public void dropPartition(ObjectPath p, CatalogPartitionSpec s, boolean i)
            throws PartitionNotExistException {
        throw unsupported("partitions");
    }

    @Override
    public void alterPartition(ObjectPath p, CatalogPartitionSpec s, CatalogPartition v, boolean i)
            throws PartitionNotExistException {
        throw unsupported("partitions");
    }

    @Override
    public List<String> listFunctions(String d) throws DatabaseNotExistException {
        requireDatabase(d);
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath p) throws FunctionNotExistException {
        throw new FunctionNotExistException(name, p);
    }

    @Override
    public boolean functionExists(ObjectPath p) {
        return false;
    }

    @Override
    public void createFunction(ObjectPath p, CatalogFunction f, boolean i)
            throws FunctionAlreadyExistException {
        throw unsupported("functions");
    }

    @Override
    public void alterFunction(ObjectPath p, CatalogFunction f, boolean i)
            throws FunctionNotExistException {
        throw unsupported("functions");
    }

    @Override
    public void dropFunction(ObjectPath p, boolean i) throws FunctionNotExistException {
        throw unsupported("functions");
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath p) throws TableNotExistException {
        load(p).close();
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath p)
            throws TableNotExistException {
        load(p).close();
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath p, CatalogPartitionSpec s)
            throws PartitionNotExistException {
        throw unsupported("partitions");
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath p, CatalogPartitionSpec s) throws PartitionNotExistException {
        throw unsupported("partitions");
    }

    @Override
    public void alterTableStatistics(ObjectPath p, CatalogTableStatistics s, boolean i)
            throws TableNotExistException {
        throw unsupported("statistics");
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath p, CatalogColumnStatistics s, boolean i)
            throws TableNotExistException {
        throw unsupported("statistics");
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath p, CatalogPartitionSpec x, CatalogTableStatistics s, boolean i)
            throws PartitionNotExistException {
        throw unsupported("partitions");
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath p, CatalogPartitionSpec x, CatalogColumnStatistics s, boolean i)
            throws PartitionNotExistException {
        throw unsupported("partitions");
    }
}
