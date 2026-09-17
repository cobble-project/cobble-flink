package io.cobble.flink.catalog;

import io.cobble.table.CatalogTable;
import io.cobble.table.DataField;
import io.cobble.table.LogicalType;
import io.cobble.table.TableSchemaChange;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.TableChange;

import java.util.ArrayList;
import java.util.List;

/** Maps only identity-preserving Flink ALTER operations to Cobble catalog evolutions. */
final class CobbleCatalogAlter {
    private CobbleCatalogAlter() {}

    static List<TableSchemaChange> toCobble(CatalogTable current, List<TableChange> changes) {
        if (changes == null || changes.isEmpty())
            throw new ValidationException(
                    "Cobble ALTER TABLE requires an explicit supported change.");
        List<TableSchemaChange> result = new ArrayList<TableSchemaChange>(changes.size());
        for (TableChange change : changes) {
            if (change instanceof TableChange.AddColumn) {
                TableChange.AddColumn add = (TableChange.AddColumn) change;
                if (!add.getColumn().isPhysical()
                        || !add.getColumn().getDataType().getLogicalType().isNullable()
                        || add.getPosition() != null) {
                    throw new ValidationException(
                            "Cobble ALTER TABLE supports only appending nullable physical "
                                    + "columns.");
                }
                result.add(
                        TableSchemaChange.addField(
                                add.getColumn().getName(),
                                CobbleCatalogTypes.toCobble(add.getColumn().getDataType())));
            } else if (change instanceof TableChange.DropColumn) {
                String field = ((TableChange.DropColumn) change).getColumnName();
                rejectKey(current, field, "drop");
                result.add(TableSchemaChange.dropField(field));
            } else if (change instanceof TableChange.ModifyColumnName) {
                TableChange.ModifyColumnName rename = (TableChange.ModifyColumnName) change;
                requireField(current, rename.getOldColumnName());
                result.add(
                        TableSchemaChange.renameField(
                                rename.getOldColumnName(), rename.getNewColumnName()));
            } else if (change instanceof TableChange.ModifyPhysicalColumnType) {
                TableChange.ModifyPhysicalColumnType alter =
                        (TableChange.ModifyPhysicalColumnType) change;
                String field = alter.getOldColumn().getName();
                rejectKey(current, field, "change the type of");
                LogicalType before = requireField(current, field).logicalType();
                LogicalType after = CobbleCatalogTypes.toCobble(alter.getNewType());
                if (!isLosslessWidening(before, after)) {
                    throw new ValidationException(
                            "Cobble ALTER TABLE supports only lossless widening of non-key "
                                    + "columns; cannot change "
                                    + field
                                    + " from "
                                    + before.kind()
                                    + " to "
                                    + after.kind()
                                    + ".");
                }
                result.add(TableSchemaChange.alterFieldType(field, after));
            } else {
                throw new ValidationException(
                        "Cobble ALTER TABLE does not support " + change + ".");
            }
        }
        return result;
    }

    private static DataField requireField(CatalogTable table, String name) {
        for (DataField field : table.schema().fields()) if (field.name().equals(name)) return field;
        throw new ValidationException("Cobble table does not contain column " + name + ".");
    }

    private static void rejectKey(CatalogTable table, String name, String operation) {
        DataField field = requireField(table, name);
        if (table.schema().primaryKey().contains(Long.valueOf(field.id())))
            throw new ValidationException(
                    "Cobble cannot " + operation + " primary key column " + name + ".");
    }

    private static boolean isLosslessWidening(LogicalType before, LogicalType after) {
        if (before.isNullable() && !after.isNullable()) return false;
        if (before.kind() == after.kind()) {
            if (before.equals(after)) return true;
            if (before.kind() == LogicalType.Kind.DECIMAL) {
                io.cobble.table.DecimalType oldType = (io.cobble.table.DecimalType) before;
                io.cobble.table.DecimalType newType = (io.cobble.table.DecimalType) after;
                return oldType.scale() == newType.scale()
                        && newType.precision() >= oldType.precision();
            }
            if (before.kind() == LogicalType.Kind.TIME) {
                return ((io.cobble.table.TimeType) after).precision()
                        >= ((io.cobble.table.TimeType) before).precision();
            }
            if (before.kind() == LogicalType.Kind.TIMESTAMP) {
                io.cobble.table.TimestampType oldType = (io.cobble.table.TimestampType) before;
                io.cobble.table.TimestampType newType = (io.cobble.table.TimestampType) after;
                return oldType.timestampKind() == newType.timestampKind()
                        && newType.precision() >= oldType.precision();
            }
            return false;
        }
        return (before.kind() == LogicalType.Kind.INT8
                        && (after.kind() == LogicalType.Kind.INT16
                                || after.kind() == LogicalType.Kind.INT32
                                || after.kind() == LogicalType.Kind.INT64))
                || (before.kind() == LogicalType.Kind.INT16
                        && (after.kind() == LogicalType.Kind.INT32
                                || after.kind() == LogicalType.Kind.INT64))
                || (before.kind() == LogicalType.Kind.INT32
                        && after.kind() == LogicalType.Kind.INT64)
                || (before.kind() == LogicalType.Kind.FLOAT32
                        && after.kind() == LogicalType.Kind.FLOAT64);
    }
}
