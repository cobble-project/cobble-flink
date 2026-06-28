package io.cobble.flink.table;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** RowData encoders reused by the Cobble SQL sink writer, partitioner, and tests. */
final class CobbleRowDataCodecs {

    private CobbleRowDataCodecs() {}

    static final class RuntimeKeyEncoder implements Serializable {
        private static final long serialVersionUID = 1L;

        private final List<CobbleDynamicTableSink.SerializableField> fields;
        private transient List<RuntimeFieldEncoder> encoders;
        private transient DataOutputSerializer keyOutput;

        RuntimeKeyEncoder(List<CobbleDynamicTableSink.SerializableField> fields) {
            this.fields = new ArrayList<CobbleDynamicTableSink.SerializableField>(fields);
            initializeRuntime();
        }

        byte[] encode(RowData row) throws IOException {
            if (encoders == null) {
                initializeRuntime();
            }
            keyOutput.clear();
            for (RuntimeFieldEncoder encoder : encoders) {
                byte[] encoded = encoder.encodeRequired(row);
                keyOutput.writeInt(encoded.length);
                keyOutput.write(encoded);
            }
            return keyOutput.getCopyOfBuffer();
        }

        private void initializeRuntime() {
            this.encoders = new ArrayList<RuntimeFieldEncoder>(fields.size());
            for (CobbleDynamicTableSink.SerializableField field : fields) {
                this.encoders.add(new RuntimeFieldEncoder(field));
            }
            this.keyOutput = new DataOutputSerializer(128);
        }
    }

    static final class RuntimeFieldEncoder {
        final String name;
        final int structuredColumnIndex;
        private final TypeSerializer<Object> serializer;
        private final RowData.FieldGetter fieldGetter;
        private final DataOutputSerializer outputSerializer;

        RuntimeFieldEncoder(CobbleDynamicTableSink.SerializableField field) {
            this.name = field.name;
            this.structuredColumnIndex = field.structuredColumnIndex;
            LogicalType logicalType =
                    LogicalTypeParser.parse(
                            field.logicalType, CobbleRowDataCodecs.class.getClassLoader());
            this.serializer = InternalSerializers.create(logicalType);
            this.fieldGetter = RowData.createFieldGetter(logicalType, field.rowIndex);
            this.outputSerializer = new DataOutputSerializer(64);
        }

        byte[] encodeRequired(RowData row) throws IOException {
            Object value = fieldGetter.getFieldOrNull(row);
            if (value == null) {
                throw new IllegalArgumentException(
                        "Primary key column " + name + " must not be null.");
            }
            return serialize(value);
        }

        byte[] encodeNullable(RowData row) throws IOException {
            Object value = fieldGetter.getFieldOrNull(row);
            if (value == null) {
                return null;
            }
            return serialize(value);
        }

        private byte[] serialize(Object value) throws IOException {
            outputSerializer.clear();
            serializer.serialize(value, outputSerializer);
            return outputSerializer.getCopyOfBuffer();
        }
    }
}
