package io.cobble.flink.table;

import io.cobble.flink.common.table.CobbleTableRowConverter;
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
        private final Map<Integer, Integer> keyOrdinals;
        private final List<CobbleDynamicTableSource.SerializableField> outputFields;
        private final List<LogicalType> outputTypes;
        private final boolean[] outputKeys;
        private final boolean decodeKey;
        private final int selectedValues;

        RuntimeRowDecoder(CobbleDynamicTableSource.SerializableConfig config) {
            TableSchema schema = config.tableSchema();
            this.converter = new CobbleTableRowConverter(config.projectedRowType());
            this.outputFields = config.projectedFields();
            this.outputTypes = new ArrayList<>(outputFields.size());
            this.outputKeys = new boolean[outputFields.size()];
            boolean selectedKey = false;
            int valueCount = 0;
            for (int index = 0; index < outputFields.size(); index++) {
                CobbleDynamicTableSource.SerializableField field = outputFields.get(index);
                outputTypes.add(schema.fields().get(field.rowIndex).logicalType());
                outputKeys[index] = config.isKeyField(field);
                selectedKey |= outputKeys[index];
                if (!outputKeys[index]) valueCount++;
            }
            this.decodeKey = selectedKey;
            this.selectedValues = valueCount;
            Map<Long, Integer> positionsById = new HashMap<Long, Integer>();
            for (int i = 0; i < schema.fields().size(); i++) {
                positionsById.put(Long.valueOf(schema.fields().get(i).id()), Integer.valueOf(i));
            }
            this.keyTypes = new ArrayList<LogicalType>(schema.primaryKey().size());
            this.keyOrdinals = new HashMap<>();
            for (Long fieldId : schema.primaryKey()) {
                int position = positionsById.get(fieldId).intValue();
                keyOrdinals.put(position, keyTypes.size());
                keyTypes.add(schema.fields().get(position).logicalType());
            }
        }

        @Override
        public RowData decode(byte[] key, byte[][] columns) throws IOException {
            try {
                List<Value> decodedKey =
                        decodeKey ? KeyCodec.decode(keyTypes, ByteBuffer.wrap(key)) : null;
                if (columns.length < selectedValues) {
                    throw new IllegalArgumentException(
                            "Cobble table row is missing one or more value columns");
                }
                List<Value> row = new ArrayList<>(outputFields.size());
                int valueIndex = 0;
                for (int i = 0; i < outputFields.size(); i++) {
                    CobbleDynamicTableSource.SerializableField field = outputFields.get(i);
                    if (outputKeys[i]) {
                        row.add(decodedKey.get(keyOrdinals.get(field.rowIndex)));
                    } else {
                        byte[] column = columns[valueIndex++];
                        if (column == null) {
                            throw new IllegalArgumentException(
                                    "Cobble table row is missing value column "
                                            + field.structuredColumnIndex);
                        }
                        row.add(
                                ValueCodec.decodeOwned(
                                        outputTypes.get(i), ByteBuffer.wrap(column)));
                    }
                }
                return converter.toRowData(row);
            } catch (IllegalArgumentException e) {
                throw new IOException("Failed to decode Cobble Table row: " + e.getMessage(), e);
            }
        }
    }
}
