package io.cobble.flink.table;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/**
 * Validates the DDL schema for {@code source.kind='raw'}.
 *
 * <p>The raw source requires exactly two physical columns with fixed names and types:
 *
 * <ul>
 *   <li>{@code key BYTES} (or {@code VARBINARY}) — the raw Cobble row key.
 *   <li>{@code columns ARRAY<BYTES>} (or {@code ARRAY<VARBINARY>}) — the selected value columns.
 * </ul>
 *
 * <p>No primary key is allowed. The raw source has no semantic key contract.
 */
final class RawSourceSchemaResolver {

    private static final String KEY_FIELD = "key";
    private static final String COLUMNS_FIELD = "columns";

    private RawSourceSchemaResolver() {}

    /**
     * Validates that {@code resolvedSchema} matches the fixed raw-source DDL contract.
     *
     * @throws ValidationException on any mismatch.
     */
    static void validate(ResolvedSchema resolvedSchema) {
        if (resolvedSchema.getPrimaryKey().isPresent()) {
            throw new ValidationException(
                    "source.kind='raw' does not support a PRIMARY KEY. Remove the PRIMARY KEY"
                            + " declaration; the raw source emits raw bytes without a key contract.");
        }

        RowType physicalRowType = (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType();
        if (physicalRowType.getFieldCount() != 2) {
            throw new ValidationException(
                    "source.kind='raw' requires exactly two columns: `key` BYTES and `columns`"
                            + " ARRAY<BYTES>. Found "
                            + physicalRowType.getFieldCount()
                            + " columns.");
        }

        RowType.RowField keyField = physicalRowType.getFields().get(0);
        RowType.RowField columnsField = physicalRowType.getFields().get(1);

        if (!KEY_FIELD.equals(keyField.getName())) {
            throw new ValidationException(
                    "source.kind='raw' requires the first column to be named 'key', but found '"
                            + keyField.getName()
                            + "'.");
        }
        if (!isBinaryType(keyField.getType())) {
            throw new ValidationException(
                    "source.kind='raw' requires the `key` column to be BYTES or VARBINARY, but"
                            + " found "
                            + keyField.getType()
                            + ".");
        }

        if (!COLUMNS_FIELD.equals(columnsField.getName())) {
            throw new ValidationException(
                    "source.kind='raw' requires the second column to be named 'columns', but"
                            + " found '"
                            + columnsField.getName()
                            + "'.");
        }
        if (!isBinaryArrayType(columnsField.getType())) {
            throw new ValidationException(
                    "source.kind='raw' requires the `columns` column to be ARRAY<BYTES> or"
                            + " ARRAY<VARBINARY>, but found "
                            + columnsField.getType()
                            + ".");
        }
    }

    private static boolean isBinaryType(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        return root == LogicalTypeRoot.BINARY || root == LogicalTypeRoot.VARBINARY;
    }

    private static boolean isBinaryArrayType(LogicalType type) {
        if (!(type instanceof ArrayType)) {
            return false;
        }
        return isBinaryType(((ArrayType) type).getElementType());
    }
}
