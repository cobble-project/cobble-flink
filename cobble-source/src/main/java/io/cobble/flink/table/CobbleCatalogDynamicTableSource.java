package io.cobble.flink.table;

import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.flink.catalog.CobbleCatalog;
import io.cobble.flink.catalog.CobbleCatalogTableReference;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.CatalogTable;
import io.cobble.table.DataField;
import io.cobble.table.TableReader;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Catalog-aware readers use native table identities and schema transforms. */
final class CobbleCatalogDynamicTableSource implements ScanTableSource, LookupTableSource {
    final CobbleCatalogTableReference reference;
    final RowType rowType;
    final String snapshot;
    final int[] primaryPositions;

    private CobbleCatalogDynamicTableSource(
            CobbleCatalogTableReference reference,
            RowType rowType,
            String snapshot,
            int[] primaryPositions) {
        this.reference = reference;
        this.rowType = rowType;
        this.snapshot = snapshot;
        this.primaryPositions = primaryPositions;
    }

    static CobbleCatalogDynamicTableSource create(DynamicTableSourceFactory.Context context) {
        Map<String, String> options = context.getCatalogTable().getOptions();
        Set<String> supported =
                new HashSet<>(
                        Arrays.asList(
                                CobbleCatalog.OPTION_PATH,
                                CobbleCatalog.OPTION_STORAGE_ID,
                                CobbleCatalog.OPTION_TABLE_ID,
                                CobbleCatalog.OPTION_SCHEMA_ID,
                                "connector",
                                "bucket",
                                "scan.mode",
                                "scan.checkpoint-id"));
        Set<ConfigOption<?>> storageOptions = new HashSet<>();
        CobbleConnectorStorageOptions.addFactoryOptions(storageOptions);
        for (ConfigOption<?> option : storageOptions) supported.add(option.key());
        for (String key : options.keySet()) {
            if (!supported.contains(key)
                    && !key.startsWith(CobbleConnectorStorageOptions.STORAGE_OPTION_PREFIX)) {
                throw new ValidationException("Unsupported Cobble catalog source option: " + key);
            }
        }
        if (!"batch".equals(options.getOrDefault("scan.mode", "batch"))) {
            throw new ValidationException(
                    "Cobble catalog source currently supports scan.mode='batch' only.");
        }
        String snapshot = options.getOrDefault("scan.checkpoint-id", "latest");
        if (!snapshot.equals("latest")) {
            try {
                if (Long.parseLong(snapshot) < 0) throw new NumberFormatException();
            } catch (NumberFormatException error) {
                throw new ValidationException(
                        "scan.checkpoint-id must be 'latest' or a non-negative snapshot id.",
                        error);
            }
        }
        CobbleCatalogTableReference reference =
                CobbleCatalogTableReference.fromOptions(
                        options,
                        context.getObjectIdentifier().getDatabaseName(),
                        context.getObjectIdentifier().getObjectName());
        RowType rowType =
                (RowType)
                        context.getCatalogTable()
                                .getResolvedSchema()
                                .toPhysicalRowDataType()
                                .getLogicalType();
        try (CobbleCatalogTableReference.Opened opened = reference.openValidated()) {
            List<DataField> fields = opened.table().schema().fields();
            if (rowType.getFieldCount() != fields.size()) {
                throw new ValidationException(
                        "Source columns must match the Cobble catalog schema.");
            }
            for (int i = 0; i < fields.size(); i++) {
                if (!rowType.getFieldNames().get(i).equals(fields.get(i).name())
                        || !CobbleTableRowConverter.toCobbleType(rowType.getTypeAt(i))
                                .equals(fields.get(i).logicalType())) {
                    throw new ValidationException(
                            "Source column at position "
                                    + i
                                    + " does not match the Cobble catalog schema.");
                }
            }
            List<Integer> positions = new ArrayList<>();
            for (Long id : opened.table().schema().primaryKey()) {
                for (int i = 0; i < fields.size(); i++) {
                    if (fields.get(i).id() == id.longValue()) positions.add(i);
                }
            }
            List<String> primaryNames = new ArrayList<>();
            for (int position : positions) primaryNames.add(fields.get(position).name());
            if (!context.getCatalogTable()
                    .getResolvedSchema()
                    .getPrimaryKey()
                    .map(key -> key.getColumns().equals(primaryNames))
                    .orElse(false)) {
                throw new ValidationException(
                        "Source PRIMARY KEY must match the Cobble catalog schema.");
            }
            return new CobbleCatalogDynamicTableSource(
                    reference,
                    rowType,
                    snapshot,
                    positions.stream().mapToInt(Integer::intValue).toArray());
        }
    }

    static TableReader openReader(CobbleCatalogTableReference reference, String snapshot) {
        try (CobbleCatalogTableReference.Opened opened = reference.openValidated()) {
            CatalogTable.ReaderBuilder builder =
                    opened.table().readerBuilder(reference.runtimeConfig());
            long id;
            if (snapshot.equals("latest")) {
                try (DbCoordinator coordinator =
                        opened.table().coordinator(reference.runtimeConfig())) {
                    GlobalSnapshot latest = coordinator.loadCurrentGlobalSnapshot();
                    if (latest == null) return null;
                    id = latest.id;
                }
            } else {
                id = Long.parseLong(snapshot);
            }
            TableReader reader = builder.globalSnapshot(id).open();
            if (!reader.schema().equals(opened.table().schema())) {
                reader.close();
                throw new ValidationException(
                        "The selected snapshot does not contain the current catalog schema. Commit a snapshot from a writer using the updated schema before reading it.");
            }
            return reader;
        }
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        return SourceProvider.of(new CobbleCatalogScanSource(reference, rowType, snapshot));
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        int[][] keys = context.getKeys();
        if (keys.length != primaryPositions.length)
            throw new ValidationException(
                    "Cobble catalog lookup requires the complete PRIMARY KEY.");
        int[] mapping = new int[keys.length];
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < keys.length; i++) {
            if (keys[i].length != 1 || !seen.add(keys[i][0]))
                throw new ValidationException(
                        "Cobble lookup keys must be distinct top-level columns.");
        }
        for (int required = 0; required < primaryPositions.length; required++) {
            mapping[required] = -1;
            for (int supplied = 0; supplied < keys.length; supplied++) {
                if (keys[supplied][0] == primaryPositions[required]) mapping[required] = supplied;
            }
            if (mapping[required] < 0)
                throw new ValidationException(
                        "Cobble catalog lookup requires every PRIMARY KEY column.");
        }
        return LookupFunctionProvider.of(
                new CobbleCatalogLookupFunction(
                        reference, rowType, snapshot, primaryPositions, mapping));
    }

    @Override
    public DynamicTableSource copy() {
        return new CobbleCatalogDynamicTableSource(
                reference, rowType, snapshot, primaryPositions.clone());
    }

    @Override
    public String asSummaryString() {
        return "Cobble catalog table";
    }
}
