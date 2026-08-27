package io.cobble.flink.inspect.internal;

import io.cobble.table.DataField;
import io.cobble.table.DecimalType;
import io.cobble.table.ListType;
import io.cobble.table.LogicalType;
import io.cobble.table.MapType;
import io.cobble.table.StructType;
import io.cobble.table.TableSchema;
import io.cobble.table.TimeType;
import io.cobble.table.TimestampKind;
import io.cobble.table.TimestampType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Inspection view of a persisted native Cobble Table schema. */
final class TableInspectSchema {

    final TableSchema schema;
    final List<Field> fields;
    final List<Field> keyFields;
    final List<Field> valueFields;

    TableInspectSchema(TableSchema schema) {
        this.schema = schema;
        Map<Long, Integer> positionsById = new HashMap<Long, Integer>();
        for (int index = 0; index < schema.fields().size(); index++) {
            positionsById.put(
                    Long.valueOf(schema.fields().get(index).id()), Integer.valueOf(index));
        }
        Set<Long> keyIds = new HashSet<Long>(schema.primaryKey());
        this.fields = new ArrayList<Field>(schema.fields().size());
        this.valueFields = new ArrayList<Field>();
        int valueColumn = 0;
        for (int index = 0; index < schema.fields().size(); index++) {
            DataField field = schema.fields().get(index);
            boolean key = keyIds.contains(Long.valueOf(field.id()));
            Field inspectField = new Field(field, index, key ? -1 : valueColumn, key);
            fields.add(inspectField);
            if (!key) {
                valueFields.add(inspectField);
                valueColumn++;
            }
        }
        this.keyFields = new ArrayList<Field>(schema.primaryKey().size());
        for (Long fieldId : schema.primaryKey()) {
            Integer position = positionsById.get(fieldId);
            if (position == null) {
                throw new IllegalArgumentException(
                        "Cobble Table primary key refers to a missing field: " + fieldId);
            }
            keyFields.add(fields.get(position.intValue()));
        }
    }

    static String sqlType(LogicalType type) {
        String base;
        switch (type.kind()) {
            case BOOLEAN:
                base = "BOOLEAN";
                break;
            case INT8:
                base = "TINYINT";
                break;
            case INT16:
                base = "SMALLINT";
                break;
            case INT32:
                base = "INT";
                break;
            case INT64:
                base = "BIGINT";
                break;
            case FLOAT32:
                base = "FLOAT";
                break;
            case FLOAT64:
                base = "DOUBLE";
                break;
            case DECIMAL:
                DecimalType decimal = (DecimalType) type;
                base = "DECIMAL(" + decimal.precision() + ", " + decimal.scale() + ")";
                break;
            case DATE:
                base = "DATE";
                break;
            case TIME:
                base = "TIME(" + ((TimeType) type).precision() + ")";
                break;
            case TIMESTAMP:
                TimestampType timestamp = (TimestampType) type;
                base =
                        (timestamp.timestampKind() == TimestampKind.WITH_LOCAL_TIME_ZONE
                                        ? "TIMESTAMP_LTZ("
                                        : "TIMESTAMP(")
                                + timestamp.precision()
                                + ")";
                break;
            case STRING:
                base = "STRING";
                break;
            case BINARY:
                base = "BYTES";
                break;
            case LIST:
                base = "ARRAY<" + sqlType(((ListType) type).elementType()) + ">";
                break;
            case MAP:
                MapType map = (MapType) type;
                base = "MAP<" + sqlType(map.keyType()) + ", " + sqlType(map.valueType()) + ">";
                break;
            case STRUCT:
                StringBuilder row = new StringBuilder("ROW<");
                List<DataField> nested = ((StructType) type).recordType().fields();
                for (int index = 0; index < nested.size(); index++) {
                    if (index > 0) {
                        row.append(", ");
                    }
                    DataField field = nested.get(index);
                    row.append('`')
                            .append(field.name().replace("`", "``"))
                            .append("` ")
                            .append(sqlType(field.logicalType()));
                }
                base = row.append('>').toString();
                break;
            case EXTENSION:
            default:
                throw new IllegalArgumentException(
                        "Unsupported Cobble Table type for Flink SQL: " + type.kind());
        }
        return type.isNullable() ? base : base + " NOT NULL";
    }

    static final class Field {
        final DataField field;
        final int rowIndex;
        final int valueColumnIndex;
        final boolean primaryKey;

        private Field(DataField field, int rowIndex, int valueColumnIndex, boolean primaryKey) {
            this.field = field;
            this.rowIndex = rowIndex;
            this.valueColumnIndex = valueColumnIndex;
            this.primaryKey = primaryKey;
        }

        String name() {
            return field.name();
        }

        LogicalType type() {
            return field.logicalType();
        }

        String logicalType() {
            return sqlType(field.logicalType());
        }
    }
}
