package io.cobble.flink.table;

import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.KeyCodec;
import io.cobble.table.LogicalType;
import io.cobble.table.Value;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Runtime adapters from Flink's internal row representation to Cobble Table values. */
final class CobbleRowDataCodecs {

    private CobbleRowDataCodecs() {}

    static final class RuntimeKeyEncoder implements Serializable {
        private static final long serialVersionUID = 1L;

        private final List<CobbleDynamicTableSink.SerializableField> fields;
        private transient List<org.apache.flink.table.types.logical.LogicalType> flinkTypes;
        private transient List<LogicalType> cobbleTypes;
        private transient List<RowData.FieldGetter> getters;

        RuntimeKeyEncoder(List<CobbleDynamicTableSink.SerializableField> fields) {
            this.fields = new ArrayList<CobbleDynamicTableSink.SerializableField>(fields);
            initializeRuntime();
        }

        byte[] encode(RowData row) {
            ensureRuntime();
            List<Value> values = new ArrayList<Value>(fields.size());
            for (int i = 0; i < fields.size(); i++) {
                Object value = getters.get(i).getFieldOrNull(row);
                if (value == null) {
                    throw new IllegalArgumentException(
                            "Primary key column " + fields.get(i).name + " must not be null.");
                }
                values.add(CobbleTableRowConverter.toValue(flinkTypes.get(i), value));
            }
            return KeyCodec.encode(cobbleTypes, values);
        }

        private void ensureRuntime() {
            if (getters == null) {
                initializeRuntime();
            }
        }

        private void initializeRuntime() {
            flinkTypes =
                    new ArrayList<org.apache.flink.table.types.logical.LogicalType>(fields.size());
            cobbleTypes = new ArrayList<LogicalType>(fields.size());
            getters = new ArrayList<RowData.FieldGetter>(fields.size());
            for (CobbleDynamicTableSink.SerializableField field : fields) {
                org.apache.flink.table.types.logical.LogicalType type =
                        LogicalTypeParser.parse(
                                field.logicalType, CobbleRowDataCodecs.class.getClassLoader());
                flinkTypes.add(type);
                cobbleTypes.add(CobbleTableRowConverter.toCobbleType(type.copy(false)));
                getters.add(RowData.createFieldGetter(type, field.rowIndex));
            }
        }
    }
}
