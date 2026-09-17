package io.cobble.flink.catalog;

import io.cobble.table.DataField;
import io.cobble.table.LogicalType;
import io.cobble.table.LogicalTypes;
import io.cobble.table.TableSchema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Explicit, lossless Flink-to-Cobble type boundary for catalog tables. */
final class CobbleCatalogTypes {
    private CobbleCatalogTypes() {}

    static TableSchema toCobble(Schema schema) {
        List<UnresolvedColumn> columns = schema.getColumns();
        List<String> names = new ArrayList<String>(columns.size());
        List<LogicalType> types = new ArrayList<LogicalType>(columns.size());
        for (UnresolvedColumn column : columns) {
            if (!(column instanceof UnresolvedPhysicalColumn)) {
                throw new ValidationException("Cobble catalog supports only physical columns.");
            }
            AbstractDataType<?> raw = ((UnresolvedPhysicalColumn) column).getDataType();
            if (!(raw instanceof DataType))
                throw new ValidationException(
                        "Cobble catalog requires resolved physical data types.");
            names.add(column.getName());
            types.add(toCobble((DataType) raw));
        }
        List<String> keyNames =
                schema.getPrimaryKey()
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                "Cobble catalog tables require a PRIMARY KEY."))
                        .getColumnNames();
        List<DataField> fields = new ArrayList<DataField>(names.size());
        for (int index = 0; index < names.size(); index++) {
            fields.add(new DataField(index, names.get(index), types.get(index)));
        }
        List<Long> primary = new ArrayList<Long>(keyNames.size());
        for (String keyName : keyNames) {
            int index = names.indexOf(keyName);
            if (index < 0) {
                throw new ValidationException("Cobble primary key columns must be physical.");
            }
            primary.add(Long.valueOf(index));
        }
        if (fields.size() == primary.size())
            throw new ValidationException(
                    "Cobble catalog tables require at least one non-primary-key column.");
        return new TableSchema(fields, primary, Collections.singletonList(primary.get(0)));
    }

    static Schema toFlink(TableSchema schema) {
        Schema.Builder builder = Schema.newBuilder();
        List<String> primary = new ArrayList<String>();
        for (DataField field : schema.fields()) {
            builder.column(field.name(), toFlink(field.logicalType()));
        }
        for (Long id : schema.primaryKey()) {
            for (DataField field : schema.fields())
                if (field.id() == id.longValue()) primary.add(field.name());
        }
        return builder.primaryKey(primary.toArray(new String[0])).build();
    }

    static LogicalType toCobble(DataType type) {
        org.apache.flink.table.types.logical.LogicalType flink = type.getLogicalType();
        LogicalType result;
        switch (flink.getTypeRoot()) {
            case BOOLEAN:
                result = LogicalTypes.bool();
                break;
            case TINYINT:
                result = LogicalTypes.int8();
                break;
            case SMALLINT:
                result = LogicalTypes.int16();
                break;
            case INTEGER:
                result = LogicalTypes.int32();
                break;
            case BIGINT:
                result = LogicalTypes.int64();
                break;
            case FLOAT:
                result = LogicalTypes.float32();
                break;
            case DOUBLE:
                result = LogicalTypes.float64();
                break;
            case DATE:
                result = LogicalTypes.date();
                break;
            case VARCHAR:
                if (((VarCharType) flink).getLength() != VarCharType.MAX_LENGTH)
                    throw unsupportedBounded(flink);
                result = LogicalTypes.string();
                break;
            case VARBINARY:
                if (((VarBinaryType) flink).getLength() != VarBinaryType.MAX_LENGTH)
                    throw unsupportedBounded(flink);
                result = LogicalTypes.binary();
                break;
            case DECIMAL:
                DecimalType decimal = (DecimalType) flink;
                result = LogicalTypes.decimal(decimal.getPrecision(), decimal.getScale());
                break;
            case TIME_WITHOUT_TIME_ZONE:
                result = LogicalTypes.time(((TimeType) flink).getPrecision());
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                result =
                        LogicalTypes.timestamp(
                                ((TimestampType) flink).getPrecision(),
                                io.cobble.table.TimestampKind.WITHOUT_TIME_ZONE);
                break;
            default:
                throw new ValidationException(
                        "Cobble catalog does not support logical type "
                                + flink
                                + ". Supported types are BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, "
                                + "FLOAT, DOUBLE, STRING/VARCHAR, BYTES/VARBINARY, DECIMAL, DATE, "
                                + "TIME, and TIMESTAMP.");
        }
        return flink.isNullable() ? result.nullable() : result;
    }

    static DataType toFlink(LogicalType type) {
        DataType result;
        switch (type.kind()) {
            case BOOLEAN:
                result = DataTypes.BOOLEAN();
                break;
            case INT8:
                result = DataTypes.TINYINT();
                break;
            case INT16:
                result = DataTypes.SMALLINT();
                break;
            case INT32:
                result = DataTypes.INT();
                break;
            case INT64:
                result = DataTypes.BIGINT();
                break;
            case FLOAT32:
                result = DataTypes.FLOAT();
                break;
            case FLOAT64:
                result = DataTypes.DOUBLE();
                break;
            case DATE:
                result = DataTypes.DATE();
                break;
            case STRING:
                result = DataTypes.STRING();
                break;
            case BINARY:
                result = DataTypes.BYTES();
                break;
            case DECIMAL:
                io.cobble.table.DecimalType decimal = (io.cobble.table.DecimalType) type;
                result = DataTypes.DECIMAL(decimal.precision(), decimal.scale());
                break;
            case TIME:
                result = DataTypes.TIME(((io.cobble.table.TimeType) type).precision());
                break;
            case TIMESTAMP:
                io.cobble.table.TimestampType timestamp = (io.cobble.table.TimestampType) type;
                result =
                        timestamp.timestampKind() == io.cobble.table.TimestampKind.WITHOUT_TIME_ZONE
                                ? DataTypes.TIMESTAMP(timestamp.precision())
                                : DataTypes.TIMESTAMP_LTZ(timestamp.precision());
                break;
            default:
                throw new ValidationException(
                        "Cobble catalog schema contains unsupported type " + type.kind());
        }
        return type.isNullable() ? result.nullable() : result.notNull();
    }

    private static ValidationException unsupportedBounded(
            org.apache.flink.table.types.logical.LogicalType type) {
        return new ValidationException(
                "Cobble catalog cannot preserve bounded "
                        + type
                        + "; use STRING or BYTES instead.");
    }
}
