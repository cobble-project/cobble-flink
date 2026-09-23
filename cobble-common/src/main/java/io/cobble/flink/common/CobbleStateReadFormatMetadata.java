package io.cobble.flink.common;

import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;

/** Snapshot-captured descriptor for one Cobble keyed-state column family. */
public final class CobbleStateReadFormatMetadata {
    public static final String FORMAT_ID = "flink-state";
    private static final int VERSION = 1;

    private CobbleStateReadFormatMetadata() {}

    /**
     * Encodes only this state's inspect metadata and physical descriptor. It intentionally does not
     * derive a table schema, so a state with an unsupported semantic serializer remains writable
     * and is rejected only when a reader requests it.
     */
    public static String encode(
            CobbleStateDescriptor descriptor,
            StateInspectSchema inspectSchema,
            StateInspectSemanticSchema semanticSchema)
            throws IOException {
        if (descriptor == null) throw new IllegalArgumentException("descriptor must not be null");
        if (inspectSchema == null)
            throw new IllegalArgumentException("inspectSchema must not be null");
        StateInspectSchemaStore store =
                new StateInspectSchemaStore(
                        Collections.singletonList(inspectSchema),
                        semanticSchema == null
                                ? Collections.<String, StateInspectSemanticSchema>emptyMap()
                                : Collections.singletonMap(descriptor.stateName(), semanticSchema));
        JsonObject value = new JsonObject();
        value.addProperty("format", FORMAT_ID);
        value.addProperty("version", VERSION);
        value.addProperty("state_name", descriptor.stateName());
        value.addProperty("column_family", descriptor.columnFamily());
        value.addProperty("state_kind", descriptor.stateKind().name());
        value.addProperty("row_key_encoding", descriptor.rowKeyEncoding().name());
        value.addProperty("row_key_format_version", descriptor.rowKeyFormatVersion());
        value.addProperty("row_value_encoding", descriptor.rowValueEncoding().name());
        value.addProperty("row_value_format_version", descriptor.rowValueFormatVersion());
        value.addProperty(
                "inspect_schema_store", Base64.getEncoder().encodeToString(store.toBytes()));
        return value.toString();
    }

    /** Decodes and validates one captured descriptor without depending on a source connector. */
    public static Decoded decode(String json) throws IOException {
        try {
            JsonObject value = JsonParser.parseString(json).getAsJsonObject();
            if (!FORMAT_ID.equals(requiredString(value, "format"))) {
                throw new IOException("unsupported Cobble state read format");
            }
            if (requiredInt(value, "version") != VERSION) {
                throw new IOException("unsupported Cobble state read metadata version");
            }
            return new Decoded(
                    requiredString(value, "state_name"),
                    requiredString(value, "column_family"),
                    CobbleStateDescriptor.StateKind.valueOf(requiredString(value, "state_kind")),
                    CobbleStateDescriptor.RowKeyEncoding.valueOf(
                            requiredString(value, "row_key_encoding")),
                    requiredInt(value, "row_key_format_version"),
                    CobbleStateDescriptor.RowValueEncoding.valueOf(
                            requiredString(value, "row_value_encoding")),
                    requiredInt(value, "row_value_format_version"),
                    StateInspectSchemaStore.fromBytes(
                            Base64.getDecoder()
                                    .decode(requiredString(value, "inspect_schema_store"))));
        } catch (RuntimeException error) {
            throw new IOException(
                    "invalid Cobble state read metadata: " + error.getMessage(), error);
        }
    }

    private static String requiredString(JsonObject value, String name) {
        if (!value.has(name) || !value.get(name).isJsonPrimitive()) {
            throw new IllegalArgumentException("missing " + name);
        }
        String text = value.get(name).getAsString();
        if (text == null || text.trim().isEmpty())
            throw new IllegalArgumentException("empty " + name);
        return text;
    }

    private static int requiredInt(JsonObject value, String name) {
        if (!value.has(name) || !value.get(name).isJsonPrimitive()) {
            throw new IllegalArgumentException("missing " + name);
        }
        return value.get(name).getAsInt();
    }

    /** Decoded single-state metadata for a fixed snapshot shard. */
    public static final class Decoded {
        private final String stateName;
        private final String columnFamily;
        private final CobbleStateDescriptor.StateKind stateKind;
        private final CobbleStateDescriptor.RowKeyEncoding rowKeyEncoding;
        private final int rowKeyFormatVersion;
        private final CobbleStateDescriptor.RowValueEncoding rowValueEncoding;
        private final int rowValueFormatVersion;
        private final StateInspectSchemaStore schemaStore;

        private Decoded(
                String stateName,
                String columnFamily,
                CobbleStateDescriptor.StateKind stateKind,
                CobbleStateDescriptor.RowKeyEncoding rowKeyEncoding,
                int rowKeyFormatVersion,
                CobbleStateDescriptor.RowValueEncoding rowValueEncoding,
                int rowValueFormatVersion,
                StateInspectSchemaStore schemaStore) {
            this.stateName = stateName;
            this.columnFamily = columnFamily;
            this.stateKind = stateKind;
            this.rowKeyEncoding = rowKeyEncoding;
            this.rowKeyFormatVersion = rowKeyFormatVersion;
            this.rowValueEncoding = rowValueEncoding;
            this.rowValueFormatVersion = rowValueFormatVersion;
            this.schemaStore = schemaStore;
        }

        public String stateName() {
            return stateName;
        }

        public String columnFamily() {
            return columnFamily;
        }

        public CobbleStateDescriptor.StateKind stateKind() {
            return stateKind;
        }

        public CobbleStateDescriptor.RowKeyEncoding rowKeyEncoding() {
            return rowKeyEncoding;
        }

        public int rowKeyFormatVersion() {
            return rowKeyFormatVersion;
        }

        public CobbleStateDescriptor.RowValueEncoding rowValueEncoding() {
            return rowValueEncoding;
        }

        public int rowValueFormatVersion() {
            return rowValueFormatVersion;
        }

        public StateInspectSchemaStore schemaStore() {
            return schemaStore;
        }
    }
}
