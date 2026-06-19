package io.cobble.flink.common.inspect;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/** Binary container for Cobble SQL sink inspect schema metadata. */
public final class SinkInspectSchemaStore {

    /** Magic int identifying the sink inspect schema sidecar ("CSNK"). */
    public static final int MAGIC = 0x43534E4B;

    private static final int VERSION = 1;

    private final SinkInspectSchema schema;

    public SinkInspectSchemaStore(SinkInspectSchema schema) {
        this.schema = schema;
    }

    public static SinkInspectSchemaStore of(SinkInspectSchema schema) {
        return new SinkInspectSchemaStore(schema);
    }

    public SinkInspectSchema schema() {
        return schema;
    }

    public boolean isEmpty() {
        return schema == null;
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(byteStream)) {
            writeTo(new DataOutputViewStreamWrapper(out));
        }
        return byteStream.toByteArray();
    }

    public void writeTo(DataOutputView output) throws IOException {
        output.writeInt(MAGIC);
        output.writeInt(VERSION);
        output.writeBoolean(schema != null);
        if (schema != null) {
            schema.write(output);
        }
    }

    public static SinkInspectSchemaStore read(InputStream input) throws IOException {
        byte[] magicBytes = new byte[4];
        int read = readFully(input, magicBytes);
        if (read < 4) {
            return new SinkInspectSchemaStore(null);
        }
        int magic =
                ((magicBytes[0] & 0xFF) << 24)
                        | ((magicBytes[1] & 0xFF) << 16)
                        | ((magicBytes[2] & 0xFF) << 8)
                        | (magicBytes[3] & 0xFF);
        if (magic != MAGIC) {
            return new SinkInspectSchemaStore(null);
        }
        return readPayload(new DataInputViewStreamWrapper(new DataInputStream(input)));
    }

    public static SinkInspectSchemaStore fromBytes(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return new SinkInspectSchemaStore(null);
        }
        return read(new ByteArrayInputStream(bytes));
    }

    private static SinkInspectSchemaStore readPayload(DataInputView input) throws IOException {
        int version = input.readInt();
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported Cobble sink inspect schema version: "
                            + version
                            + " (expected "
                            + VERSION
                            + ")");
        }
        boolean hasSchema = input.readBoolean();
        return new SinkInspectSchemaStore(hasSchema ? SinkInspectSchema.read(input) : null);
    }

    private static int readFully(InputStream input, byte[] buffer) throws IOException {
        int total = 0;
        while (total < buffer.length) {
            int n = input.read(buffer, total, buffer.length - total);
            if (n < 0) {
                break;
            }
            total += n;
        }
        return total;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SinkInspectSchemaStore)) {
            return false;
        }
        SinkInspectSchemaStore that = (SinkInspectSchemaStore) other;
        return Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema);
    }
}
