package io.cobble.flink.table;

import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.DataField;
import io.cobble.table.KeyCodec;
import io.cobble.table.LogicalType;
import io.cobble.table.TableSchema;
import io.cobble.table.Value;
import io.cobble.table.ValueCodec;

import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Decodes native Cobble Table records into Flink internal rows. */
final class CobbleRowDataDecoders {

    private CobbleRowDataDecoders() {}

    static final class RuntimeRowDecoder implements ScannedRowDecoder {
        private final CobbleTableRowConverter converter;
        private final List<LogicalType> keyTypes;
        private final List<Integer> keyPositions;
        private final List<LogicalType> valueTypes;
        private final List<Integer> valuePositions;
        private final int fieldCount;

        RuntimeRowDecoder(CobbleDynamicTableSource.SerializableConfig config) {
            TableSchema schema = config.tableSchema();
            this.converter = new CobbleTableRowConverter(config.rowType());
            this.fieldCount = schema.fields().size();
            Map<Long, Integer> positionsById = new HashMap<Long, Integer>();
            for (int i = 0; i < schema.fields().size(); i++) {
                positionsById.put(Long.valueOf(schema.fields().get(i).id()), Integer.valueOf(i));
            }
            this.keyTypes = new ArrayList<LogicalType>(schema.primaryKey().size());
            this.keyPositions = new ArrayList<Integer>(schema.primaryKey().size());
            for (Long fieldId : schema.primaryKey()) {
                int position = positionsById.get(fieldId).intValue();
                keyPositions.add(Integer.valueOf(position));
                keyTypes.add(schema.fields().get(position).logicalType());
            }
            this.valueTypes = new ArrayList<LogicalType>();
            this.valuePositions = new ArrayList<Integer>();
            for (int i = 0; i < schema.fields().size(); i++) {
                DataField field = schema.fields().get(i);
                if (!schema.primaryKey().contains(Long.valueOf(field.id()))) {
                    valuePositions.add(Integer.valueOf(i));
                    valueTypes.add(field.logicalType());
                }
            }
        }

        @Override
        public RowData decode(byte[] key, byte[][] columns) throws IOException {
            try {
                List<Value> row = new ArrayList<Value>(fieldCount);
                for (int i = 0; i < fieldCount; i++) {
                    row.add(Value.nullValue());
                }
                List<Value> decodedKey = KeyCodec.decode(keyTypes, ByteBuffer.wrap(key));
                for (int i = 0; i < decodedKey.size(); i++) {
                    row.set(keyPositions.get(i).intValue(), decodedKey.get(i));
                }
                if (columns.length < valueTypes.size()) {
                    throw new IllegalArgumentException(
                            "Cobble table row is missing one or more value columns");
                }
                for (int i = 0; i < valueTypes.size(); i++) {
                    byte[] column = columns[i];
                    if (column == null) {
                        throw new IllegalArgumentException(
                                "Cobble table row is missing value column " + i);
                    }
                    row.set(
                            valuePositions.get(i).intValue(),
                            ValueCodec.decodeOwned(valueTypes.get(i), ByteBuffer.wrap(column)));
                }
                return converter.toRowData(row);
            } catch (IllegalArgumentException e) {
                throw new IOException("Failed to decode Cobble Table row: " + e.getMessage(), e);
            }
        }
    }
}
