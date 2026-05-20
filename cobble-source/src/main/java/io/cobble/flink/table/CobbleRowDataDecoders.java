package io.cobble.flink.table;

import io.cobble.structured.Row;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** RowData decoders for turning Cobble structured rows back into Flink internal rows. */
final class CobbleRowDataDecoders {

    private CobbleRowDataDecoders() {}

    static final class RuntimeRowDecoder {
        private final List<RuntimeFieldDecoder> keyFields;
        private final List<RuntimeFieldDecoder> valueFields;
        private final int fieldCount;

        RuntimeRowDecoder(CobbleDynamicTableSource.SerializableConfig config) {
            this.keyFields = new ArrayList<>(config.keyFields.size());
            for (CobbleDynamicTableSource.SerializableField field : config.keyFields) {
                this.keyFields.add(new RuntimeFieldDecoder(field));
            }
            this.valueFields = new ArrayList<>(config.valueFields.size());
            for (CobbleDynamicTableSource.SerializableField field : config.valueFields) {
                this.valueFields.add(new RuntimeFieldDecoder(field));
            }
            this.fieldCount = config.totalFieldCount();
        }

        RowData decode(Row row) throws IOException {
            GenericRowData decoded = new GenericRowData(RowKind.INSERT, fieldCount);

            DataInputDeserializer keyInput = new DataInputDeserializer(row.getKey());
            for (RuntimeFieldDecoder field : keyFields) {
                int length = keyInput.readInt();
                byte[] bytes = new byte[length];
                keyInput.readFully(bytes);
                decoded.setField(field.rowIndex, field.deserialize(bytes));
            }

            for (RuntimeFieldDecoder field : valueFields) {
                byte[] bytes = row.getBytes(field.structuredColumnIndex);
                decoded.setField(field.rowIndex, bytes == null ? null : field.deserialize(bytes));
            }
            return decoded;
        }
    }

    static final class RuntimeFieldDecoder {
        final int rowIndex;
        final int structuredColumnIndex;
        private final TypeSerializer<Object> serializer;

        RuntimeFieldDecoder(CobbleDynamicTableSource.SerializableField field) {
            this.rowIndex = field.rowIndex;
            this.structuredColumnIndex = field.structuredColumnIndex;
            LogicalType logicalType = LogicalTypeParser.parse(field.logicalType);
            this.serializer = InternalSerializers.create(logicalType);
        }

        Object deserialize(byte[] bytes) throws IOException {
            return serializer.deserialize(new DataInputDeserializer(bytes));
        }
    }
}
