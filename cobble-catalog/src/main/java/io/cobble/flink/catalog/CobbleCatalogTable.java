package io.cobble.flink.catalog;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable catalog table using the public interface shared by Flink 1.x and 2.x. Flink 2.x removed
 * the CatalogTable.of factory available in 1.x.
 */
final class CobbleCatalogTable implements CatalogTable {
    private final Schema schema;
    private final String comment;
    private final List<String> partitionKeys;
    private final Map<String, String> options;

    CobbleCatalogTable(
            Schema schema,
            String comment,
            List<String> partitionKeys,
            Map<String, String> options) {
        this.schema = Objects.requireNonNull(schema, "schema");
        this.comment = comment;
        this.partitionKeys = Collections.unmodifiableList(new ArrayList<>(partitionKeys));
        this.options = Collections.unmodifiableMap(new HashMap<>(options));
    }

    @Override
    public Schema getUnresolvedSchema() {
        return schema;
    }

    @Override
    public String getComment() {
        return comment;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public boolean isPartitioned() {
        return !partitionKeys.isEmpty();
    }

    @Override
    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    @Override
    public CatalogTable copy() {
        return new CobbleCatalogTable(schema, comment, partitionKeys, options);
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        return new CobbleCatalogTable(schema, comment, partitionKeys, options);
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.ofNullable(comment);
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.ofNullable(comment);
    }
}
