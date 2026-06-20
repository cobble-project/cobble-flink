package io.cobble.flink.common.inspect;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Self-describing container that serializes a list of {@link StateInspectSchema} to the inspect
 * schema sidecar file written alongside each checkpoint.
 *
 * <p>This format is intentionally decoupled from Flink's checkpoint metadata so the monitor can
 * read it without {@code flink-runtime}. Format:
 *
 * <ol>
 *   <li>magic int {@code 0x43534348} ("CSCH" = Cobble SChema)
 *   <li>version int (currently {@value #VERSION})
 *   <li>state-schema count int
 *   <li>one {@link StateInspectSchema} entry per count, each written via {@link
 *       StateInspectSchema#write}
 *   <li>semantic-schema entries keyed by state name
 * </ol>
 *
 * <p>Readers must reject unknown versions with a clear message. An empty store (count 0) is valid
 * and means "no inspect schema available" — readers fall back to raw inspect.
 */
public final class StateInspectSchemaStore {

    /** Magic int identifying the inspect schema sidecar ("CSCH"). */
    public static final int MAGIC = 0x43534348;

    private static final int VERSION = 1;

    private final List<StateInspectSchema> schemas;
    private final Map<String, StateInspectSemanticSchema> semanticSchemas;

    public StateInspectSchemaStore(List<StateInspectSchema> schemas) {
        this(schemas, Collections.emptyMap());
    }

    public StateInspectSchemaStore(
            List<StateInspectSchema> schemas,
            Map<String, StateInspectSemanticSchema> semanticSchemas) {
        this.schemas = immutableList(schemas);
        this.semanticSchemas = immutableMap(semanticSchemas);
    }

    public List<StateInspectSchema> schemas() {
        return schemas;
    }

    /** Optional semantic shapes keyed by their state name. */
    public Map<String, StateInspectSemanticSchema> semanticSchemas() {
        return semanticSchemas;
    }

    /** Returns semantic shape metadata for one state, or {@code null} when unavailable. */
    public StateInspectSemanticSchema semanticSchema(String stateName) {
        return semanticSchemas.get(stateName);
    }

    /** Indexes schemas by state name, last write wins on duplicate names. */
    public Map<String, StateInspectSchema> byStateName() {
        Map<String, StateInspectSchema> map = new LinkedHashMap<>();
        for (StateInspectSchema schema : schemas) {
            map.put(schema.stateName(), schema);
        }
        return map;
    }

    public boolean isEmpty() {
        return schemas.isEmpty();
    }

    /**
     * Convenience for an empty store, equivalent to {@code new
     * StateInspectSchemaStore(Collections.emptyList())} but avoids the allocation in hot paths.
     */
    public static StateInspectSchemaStore empty() {
        return EMPTY;
    }

    private static final StateInspectSchemaStore EMPTY =
            new StateInspectSchemaStore(Collections.emptyList(), Collections.emptyMap());

    /** Serializes this store to a fresh byte array, suitable for writing to a sidecar file. */
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(byteStream)) {
            writeTo(new DataOutputViewStreamWrapper(out));
        }
        return byteStream.toByteArray();
    }

    public void writeTo(org.apache.flink.core.memory.DataOutputView output) throws IOException {
        output.writeInt(MAGIC);
        output.writeInt(VERSION);
        output.writeInt(schemas.size());
        for (StateInspectSchema schema : schemas) {
            schema.write(output);
        }
        List<String> semanticStateNames = semanticStateNamesInSchemaOrder();
        output.writeInt(semanticStateNames.size());
        for (String stateName : semanticStateNames) {
            output.writeUTF(stateName);
            semanticSchemas.get(stateName).write(output);
        }
    }

    /**
     * Reads a store from an input stream. Returns an empty store when the stream is empty or does
     * not begin with the schema magic, so callers can probe an absent sidecar without
     * distinguishing "missing file" from "unrelated bytes".
     *
     * <p>This reads the magic by buffering it manually rather than relying on {@link
     * DataInputStream#mark(int)}/{@link DataInputStream#reset()}, because the sidecar is read from
     * {@code FSDataInputStream}/{@code FileInputStream} which do not support mark/reset.
     *
     * @throws IOException when the magic matches but the version is unsupported, indicating a
     *     corrupt or forward-incompatible sidecar.
     */
    public static StateInspectSchemaStore read(InputStream input) throws IOException {
        // Read the 4-byte magic directly, tolerating a short/empty stream as "no schema". We do not
        // push back unrelated bytes because the monitor only ever opens a sidecar by its reserved
        // name; a non-magic stream means the file is not ours and should be treated as absent.
        byte[] magicBytes = new byte[4];
        int read = readFully(input, magicBytes);
        if (read < 4) {
            return new StateInspectSchemaStore(Collections.emptyList());
        }
        int magic =
                ((magicBytes[0] & 0xFF) << 24)
                        | ((magicBytes[1] & 0xFF) << 16)
                        | ((magicBytes[2] & 0xFF) << 8)
                        | (magicBytes[3] & 0xFF);
        if (magic != MAGIC) {
            // Not an inspect-schema stream; treat as absent.
            return new StateInspectSchemaStore(Collections.emptyList());
        }
        return readPayload(new DataInputViewStreamWrapper(new DataInputStream(input)));
    }

    /** Reads up to {@code buffer.length} bytes; returns the number actually read (may be 0–4). */
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

    /** Reads a store from a fully-formed byte array (used by tests and the monitor). */
    public static StateInspectSchemaStore fromBytes(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return new StateInspectSchemaStore(Collections.emptyList());
        }
        return read(new ByteArrayInputStream(bytes));
    }

    private static StateInspectSchemaStore readPayload(DataInputView input) throws IOException {
        int version = input.readInt();
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported Cobble inspect schema version: "
                            + version
                            + " (expected "
                            + VERSION
                            + ")");
        }
        int count = input.readInt();
        List<StateInspectSchema> schemas = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            schemas.add(StateInspectSchema.read(input));
        }
        int semanticCount = input.readInt();
        if (semanticCount < 0) {
            throw new IOException("Negative state semantic schema count: " + semanticCount);
        }
        Map<String, StateInspectSemanticSchema> semanticSchemas =
                new LinkedHashMap<>(semanticCount);
        for (int index = 0; index < semanticCount; index++) {
            semanticSchemas.put(input.readUTF(), StateInspectSemanticSchema.read(input));
        }
        return new StateInspectSchemaStore(schemas, semanticSchemas);
    }

    private List<String> semanticStateNamesInSchemaOrder() {
        List<String> names = new ArrayList<>();
        for (StateInspectSchema schema : schemas) {
            String stateName = schema.stateName();
            if (semanticSchemas.containsKey(stateName)) {
                names.add(stateName);
            }
        }
        return names;
    }

    private static List<StateInspectSchema> immutableList(List<StateInspectSchema> schemas) {
        if (schemas == null || schemas.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(schemas));
    }

    private static Map<String, StateInspectSemanticSchema> immutableMap(
            Map<String, StateInspectSemanticSchema> semanticSchemas) {
        if (semanticSchemas == null || semanticSchemas.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(semanticSchemas));
    }
}
