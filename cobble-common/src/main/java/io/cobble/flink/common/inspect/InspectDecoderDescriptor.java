package io.cobble.flink.common.inspect;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Cobble-owned, serializer-independent description of how the monitor can decode a value's bytes
 * without loading user classes. This is a complement to {@link StateInspectType}:
 *
 * <ul>
 *   <li>{@code StateInspectType} answers "what columns/shape should the user see?".
 *   <li>{@code InspectDecoderDescriptor} answers "how can the monitor consume these bytes without
 *       user classes?".
 * </ul>
 *
 * <p>The descriptor is built by {@link InspectDecoderDescriptorExtractor} at capture time, while
 * the writer classloader has all required Flink/Avro classes. The descriptor itself contains no
 * user object instances and no local paths.
 *
 * <h2>Binary layout</h2>
 *
 * <p>Written by {@link #write(DataOutputView)} / read by {@link #read(DataInputView)}:
 *
 * <ol>
 *   <li>kind ordinal (int) - dispatches to the concrete subclass
 *   <li>capability ordinal (int)
 *   <li>kind-specific payload (see each subclass below)
 * </ol>
 *
 * <p><b>{@code PORTABLE_SNAPSHOT}</b>: snapshotClassName(bounded-UTF), snapshotBytes(nullable: bool
 * + int-length + bytes). Capability is always {@code FULLY_CLASSLESS} and snapshotBytes is always
 * non-null; a null-bytes PORTABLE_SNAPSHOT is treated as a corrupt sidecar.
 *
 * <p><b>{@code POJO}</b>: pojoClassName(bounded-UTF), basePathClassless(bool),
 * hasNonRegisteredSubclasses(bool), field-count(int) + fields [name(bounded-UTF) + child
 * descriptor], subclass-count(int) + subclasses [className(bounded-UTF) + tag(int) + child
 * descriptor].
 *
 * <p><b>{@code AVRO}</b>: writerSchemaJson(bounded-UTF), wireFormat(bounded-UTF).
 *
 * <p><b>{@code UNSUPPORTED}</b>: reason(bounded-UTF). Capability is always {@code UNSUPPORTED}.
 *
 * <p>All strings use explicit UTF-8 length-prefixed bytes ({@code int byteLength} + raw UTF-8
 * bytes) rather than {@link DataOutputView#writeUTF(String)}, because {@code writeUTF} is limited
 * to 65,535 modified-UTF bytes and uses modified UTF-8 (not standard UTF-8). Each string field is
 * length-checked against its declared maximum on both the write and read sides.
 *
 * <h2>Safety limits</h2>
 *
 * <p>The reader enforces limits to prevent excessive allocation or recursion from a corrupt
 * sidecar:
 *
 * <ul>
 *   <li>Max recursion depth: {@value #MAX_DEPTH}
 *   <li>Max total field count: {@value #MAX_TOTAL_FIELDS}
 *   <li>Max snapshotBytes size: {@value #MAX_SNAPSHOT_BYTES} bytes
 *   <li>Max writerSchemaJson size: {@value #MAX_SCHEMA_JSON_BYTES} bytes
 *   <li>Max reason string size: {@value #MAX_REASON_BYTES} bytes
 *   <li>Max field/class name length: {@value #MAX_NAME_UTF_BYTES} UTF bytes
 *   <li>Max wireFormat length: {@value #MAX_WIRE_FORMAT_UTF_BYTES} UTF bytes
 * </ul>
 *
 * <h2>Read-side invariants</h2>
 *
 * <p>The reader validates internal consistency and rejects corrupt sidecars with a clear {@link
 * IOException}:
 *
 * <ul>
 *   <li>{@code PORTABLE_SNAPSHOT} with {@code FULLY_CLASSLESS} capability but null snapshotBytes.
 *   <li>{@code UNSUPPORTED} kind with a non-{@code UNSUPPORTED} capability.
 *   <li>Registered subclass tag not equal to its list position (non-incremental / duplicate).
 *   <li>{@code FULLY_CLASSLESS} POJO with {@code basePathClassless=false}.
 *   <li>{@code FULLY_CLASSLESS} POJO containing a child descriptor that is not {@code
 *       FULLY_CLASSLESS}.
 *   <li>{@code AVRO} descriptor with a wireFormat other than {@code FLINK_DATA_INPUT_V1}.
 * </ul>
 */
public abstract class InspectDecoderDescriptor {

    static final int MAX_DEPTH = 64;
    static final int MAX_TOTAL_FIELDS = 4096;
    static final int MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024; // 16 MB
    static final int MAX_SCHEMA_JSON_BYTES = 16 * 1024 * 1024; // 16 MB
    static final int MAX_REASON_BYTES = 4 * 1024; // 4 KB
    static final int MAX_NAME_UTF_BYTES = 1024; // 1024 UTF bytes
    static final int MAX_WIRE_FORMAT_UTF_BYTES = 256; // 256 UTF bytes

    /** The only Avro wire format currently supported. */
    public static final String AVRO_WIRE_FORMAT_FLINK_DATA_INPUT_V1 = "FLINK_DATA_INPUT_V1";

    private final InspectDecoderDescriptorKind kind;
    private final DescriptorCapability capability;

    InspectDecoderDescriptor(InspectDecoderDescriptorKind kind, DescriptorCapability capability) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.capability = Objects.requireNonNull(capability, "capability");
    }

    public InspectDecoderDescriptorKind kind() {
        return kind;
    }

    public DescriptorCapability capability() {
        return capability;
    }

    /**
     * Returns the Avro writer-schema JSON if this is an AVRO descriptor, otherwise throws.
     *
     * @throws IllegalStateException if this descriptor is not of kind {@code AVRO}.
     */
    public String avroWriterSchemaJson() {
        throw new IllegalStateException("Not an AVRO descriptor: " + kind);
    }

    /**
     * Returns the Avro wire-format tag if this is an AVRO descriptor, otherwise throws.
     *
     * @throws IllegalStateException if this descriptor is not of kind {@code AVRO}.
     */
    public String avroWireFormat() {
        throw new IllegalStateException("Not an AVRO descriptor: " + kind);
    }

    /** Returns {@code true} if this descriptor is of kind {@code AVRO}. */
    public boolean isAvro() {
        return kind == InspectDecoderDescriptorKind.AVRO;
    }

    /** Returns {@code true} if this descriptor is of kind {@code POJO}. */
    public boolean isPojo() {
        return kind == InspectDecoderDescriptorKind.POJO;
    }

    // ---- POJO accessors (overridden by PojoDescriptor) ----

    /**
     * Returns the POJO class name for diagnostics if this is a POJO descriptor, otherwise throws.
     *
     * @throws IllegalStateException if this descriptor is not of kind {@code POJO}.
     */
    public String pojoClassName() {
        throw new IllegalStateException("Not a POJO descriptor: " + kind);
    }

    /**
     * Returns the immutable list of POJO field descriptors if this is a POJO descriptor.
     *
     * @throws IllegalStateException if this descriptor is not of kind {@code POJO}.
     */
    public List<PojoFieldDescriptor> pojoFields() {
        throw new IllegalStateException("Not a POJO descriptor: " + kind);
    }

    /**
     * Returns the immutable list of registered subclass descriptors if this is a POJO descriptor.
     *
     * @throws IllegalStateException if this descriptor is not of kind {@code POJO}.
     */
    public List<RegisteredSubclass> registeredPojoSubclasses() {
        throw new IllegalStateException("Not a POJO descriptor: " + kind);
    }

    /**
     * Returns whether the base POJO path (no-subclass fields) can be decoded classlessly.
     *
     * @throws IllegalStateException if this descriptor is not of kind {@code POJO}.
     */
    public boolean pojoBasePathClassless() {
        throw new IllegalStateException("Not a POJO descriptor: " + kind);
    }

    /**
     * Returns whether the POJO serializer may emit non-registered subclass rows.
     *
     * @throws IllegalStateException if this descriptor is not of kind {@code POJO}.
     */
    public boolean pojoHasNonRegisteredSubclasses() {
        throw new IllegalStateException("Not a POJO descriptor: " + kind);
    }

    // ---- Portable snapshot accessors (overridden by PortableSnapshotDescriptor) ----

    /**
     * Returns the snapshot class name if this is a PORTABLE_SNAPSHOT descriptor, otherwise throws.
     *
     * @throws IllegalStateException if this descriptor is not of kind {@code PORTABLE_SNAPSHOT}.
     */
    public String portableSnapshotClassName() {
        throw new IllegalStateException("Not a PORTABLE_SNAPSHOT descriptor: " + kind);
    }

    /**
     * Returns a defensive copy of the snapshot bytes if this is a PORTABLE_SNAPSHOT descriptor.
     *
     * @throws IllegalStateException if this descriptor is not of kind {@code PORTABLE_SNAPSHOT}.
     */
    public byte[] portableSnapshotBytes() {
        throw new IllegalStateException("Not a PORTABLE_SNAPSHOT descriptor: " + kind);
    }

    // ---- Unsupported accessors (overridden by UnsupportedDescriptor) ----

    /**
     * Returns the diagnostic reason if this is an UNSUPPORTED descriptor, otherwise throws.
     *
     * @throws IllegalStateException if this descriptor is not of kind {@code UNSUPPORTED}.
     */
    public String unsupportedReason() {
        throw new IllegalStateException("Not an UNSUPPORTED descriptor: " + kind);
    }

    // ---- Factory methods ----

    /**
     * Creates a descriptor for a monitor-portable serializer snapshot. The snapshot bytes must be
     * non-null so the monitor can restore the field's codec without user classes.
     */
    public static InspectDecoderDescriptor portableSnapshot(
            String snapshotClassName, byte[] snapshotBytes) {
        Objects.requireNonNull(snapshotBytes, "snapshotBytes");
        return new PortableSnapshotDescriptor(
                snapshotClassName, snapshotBytes, DescriptorCapability.FULLY_CLASSLESS);
    }

    /** Creates a POJO descriptor. */
    public static InspectDecoderDescriptor pojo(
            String pojoClassName,
            List<PojoFieldDescriptor> fields,
            List<RegisteredSubclass> registeredSubclasses,
            boolean hasNonRegisteredSubclasses,
            boolean basePathClassless,
            DescriptorCapability capability) {
        return new PojoDescriptor(
                pojoClassName,
                fields,
                registeredSubclasses,
                hasNonRegisteredSubclasses,
                basePathClassless,
                capability);
    }

    /** Creates an Avro descriptor. */
    public static InspectDecoderDescriptor avro(
            String writerSchemaJson, String wireFormat, DescriptorCapability capability) {
        return new AvroDescriptor(writerSchemaJson, wireFormat, capability);
    }

    /** Creates an unsupported descriptor with a diagnostic reason. */
    public static InspectDecoderDescriptor unsupported(String reason) {
        return new UnsupportedDescriptor(reason, DescriptorCapability.UNSUPPORTED);
    }

    // ---- Serialization ----

    abstract void writePayload(DataOutputView output) throws IOException;

    void write(DataOutputView output) throws IOException {
        output.writeInt(kind.ordinal());
        output.writeInt(capability.ordinal());
        writePayload(output);
    }

    static InspectDecoderDescriptor read(DataInputView input) throws IOException {
        return read(input, 0, new int[] {0});
    }

    static InspectDecoderDescriptor read(DataInputView input, int depth, int[] totalFieldCounter)
            throws IOException {
        if (depth > MAX_DEPTH) {
            throw new IOException("InspectDecoderDescriptor recursion depth exceeded " + MAX_DEPTH);
        }
        InspectDecoderDescriptorKind kind = readKind(input.readInt());
        DescriptorCapability capability = readCapability(input.readInt());
        InspectDecoderDescriptor descriptor;
        switch (kind) {
            case PORTABLE_SNAPSHOT:
                descriptor = PortableSnapshotDescriptor.readPayload(input, capability);
                break;
            case POJO:
                descriptor =
                        PojoDescriptor.readPayload(input, capability, depth, totalFieldCounter);
                break;
            case AVRO:
                descriptor = AvroDescriptor.readPayload(input, capability);
                break;
            case UNSUPPORTED:
                descriptor = UnsupportedDescriptor.readPayload(input, capability);
                break;
            default:
                throw new IOException("Unsupported descriptor kind: " + kind);
        }
        descriptor.validateInvariants();
        return descriptor;
    }

    /**
     * Validates internal invariants that must hold for a well-formed descriptor. Called after the
     * full descriptor tree is read.
     */
    void validateInvariants() throws IOException {
        // Base: UNSUPPORTED kind must carry UNSUPPORTED capability.
        if (kind == InspectDecoderDescriptorKind.UNSUPPORTED
                && capability != DescriptorCapability.UNSUPPORTED) {
            throw new IOException(
                    "UNSUPPORTED descriptor must have UNSUPPORTED capability, got " + capability);
        }
    }

    private static InspectDecoderDescriptorKind readKind(int ordinal) throws IOException {
        InspectDecoderDescriptorKind[] kinds = InspectDecoderDescriptorKind.values();
        if (ordinal < 0 || ordinal >= kinds.length) {
            throw new IOException("Unknown InspectDecoderDescriptorKind ordinal: " + ordinal);
        }
        return kinds[ordinal];
    }

    private static DescriptorCapability readCapability(int ordinal) throws IOException {
        DescriptorCapability[] caps = DescriptorCapability.values();
        if (ordinal < 0 || ordinal >= caps.length) {
            throw new IOException("Unknown DescriptorCapability ordinal: " + ordinal);
        }
        return caps[ordinal];
    }

    // ---- Shared serialization helpers ----

    static void writeNullableBytes(DataOutputView output, byte[] bytes) throws IOException {
        if (bytes == null) {
            output.writeBoolean(false);
            return;
        }
        output.writeBoolean(true);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    static byte[] readNullableBytes(DataInputView input, int maxLength, String label)
            throws IOException {
        if (!input.readBoolean()) {
            return null;
        }
        int length = input.readInt();
        if (length < 0 || length > maxLength) {
            throw new IOException(
                    label + " length out of range: " + length + " (max " + maxLength + ")");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return bytes;
    }

    /**
     * Writes a bounded UTF-8 string as explicit length-prefixed bytes, bypassing the 65,535-byte
     * limit of {@link DataOutputView#writeUTF(String)}.
     *
     * @throws IOException if the UTF-8 byte length exceeds {@code maxBytes}.
     */
    static void writeBoundedUtf8(DataOutputView output, String value, int maxBytes, String label)
            throws IOException {
        Objects.requireNonNull(value, label);
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > maxBytes) {
            throw new IOException(
                    label + " exceeds max UTF-8 byte length: " + bytes.length + " > " + maxBytes);
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    static String readBoundedUtf8(DataInputView input, int maxLength, String label)
            throws IOException {
        int length = input.readInt();
        if (length < 0 || length > maxLength) {
            throw new IOException(
                    label + " byte length out of range: " + length + " (max " + maxLength + ")");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Returns the UTF-8 byte length of a string, or 0 if null. */
    static int utf8Length(String value) {
        return value == null ? 0 : value.getBytes(StandardCharsets.UTF_8).length;
    }

    // ---- Concrete descriptors ----

    /**
     * Descriptor for monitor-portable snapshots (primitives, Tuple, List, Map, portable RowData).
     * The snapshot bytes are always non-null; the monitor restores the field codec from them.
     */
    static final class PortableSnapshotDescriptor extends InspectDecoderDescriptor {
        private final String snapshotClassName;
        private final byte[] snapshotBytes;

        PortableSnapshotDescriptor(
                String snapshotClassName, byte[] snapshotBytes, DescriptorCapability capability) {
            super(InspectDecoderDescriptorKind.PORTABLE_SNAPSHOT, capability);
            this.snapshotClassName = snapshotClassName;
            this.snapshotBytes = snapshotBytes;
        }

        String snapshotClassName() {
            return snapshotClassName;
        }

        byte[] snapshotBytes() {
            return snapshotBytes;
        }

        @Override
        public String portableSnapshotClassName() {
            return snapshotClassName;
        }

        @Override
        public byte[] portableSnapshotBytes() {
            return Arrays.copyOf(snapshotBytes, snapshotBytes.length);
        }

        @Override
        void writePayload(DataOutputView output) throws IOException {
            writeBoundedUtf8(
                    output,
                    snapshotClassName != null ? snapshotClassName : "",
                    MAX_NAME_UTF_BYTES,
                    "PORTABLE_SNAPSHOT.snapshotClassName");
            if (snapshotBytes == null || snapshotBytes.length == 0) {
                throw new IOException(
                        "PORTABLE_SNAPSHOT(FULLY_CLASSLESS) requires non-empty snapshot bytes");
            }
            if (snapshotBytes.length > MAX_SNAPSHOT_BYTES) {
                throw new IOException(
                        "PORTABLE_SNAPSHOT.snapshotBytes exceeds max size: "
                                + snapshotBytes.length
                                + " > "
                                + MAX_SNAPSHOT_BYTES);
            }
            output.writeInt(snapshotBytes.length);
            output.write(snapshotBytes);
        }

        static PortableSnapshotDescriptor readPayload(
                DataInputView input, DescriptorCapability capability) throws IOException {
            String snapshotClassName =
                    readBoundedUtf8(input, MAX_NAME_UTF_BYTES, "snapshotClassName");
            int length = input.readInt();
            if (length < 0 || length > MAX_SNAPSHOT_BYTES) {
                throw new IOException(
                        "PORTABLE_SNAPSHOT.snapshotBytes length out of range: "
                                + length
                                + " (max "
                                + MAX_SNAPSHOT_BYTES
                                + ")");
            }
            byte[] snapshotBytes = new byte[length];
            input.readFully(snapshotBytes);
            return new PortableSnapshotDescriptor(
                    snapshotClassName.isEmpty() ? null : snapshotClassName,
                    snapshotBytes,
                    capability);
        }

        @Override
        void validateInvariants() throws IOException {
            super.validateInvariants();
            if (capability() != DescriptorCapability.FULLY_CLASSLESS) {
                throw new IOException(
                        "PORTABLE_SNAPSHOT must have FULLY_CLASSLESS capability, got "
                                + capability());
            }
            if (snapshotBytes == null || snapshotBytes.length == 0) {
                throw new IOException(
                        "PORTABLE_SNAPSHOT(FULLY_CLASSLESS) requires non-null snapshot bytes");
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PortableSnapshotDescriptor)) {
                return false;
            }
            PortableSnapshotDescriptor that = (PortableSnapshotDescriptor) o;
            return Objects.equals(snapshotClassName, that.snapshotClassName)
                    && java.util.Arrays.equals(snapshotBytes, that.snapshotBytes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotClassName, java.util.Arrays.hashCode(snapshotBytes));
        }
    }

    /** Descriptor for Flink POJO serializer wire protocol. */
    static final class PojoDescriptor extends InspectDecoderDescriptor {
        private final String pojoClassName;
        private final List<PojoFieldDescriptor> fields;
        private final List<RegisteredSubclass> registeredSubclasses;
        private final boolean hasNonRegisteredSubclasses;
        private final boolean basePathClassless;

        PojoDescriptor(
                String pojoClassName,
                List<PojoFieldDescriptor> fields,
                List<RegisteredSubclass> registeredSubclasses,
                boolean hasNonRegisteredSubclasses,
                boolean basePathClassless,
                DescriptorCapability capability) {
            super(InspectDecoderDescriptorKind.POJO, capability);
            this.pojoClassName = pojoClassName;
            this.fields = immutableList(fields);
            this.registeredSubclasses = immutableList(registeredSubclasses);
            this.hasNonRegisteredSubclasses = hasNonRegisteredSubclasses;
            this.basePathClassless = basePathClassless;
        }

        List<PojoFieldDescriptor> fields() {
            return fields;
        }

        List<RegisteredSubclass> registeredSubclasses() {
            return registeredSubclasses;
        }

        boolean hasNonRegisteredSubclasses() {
            return hasNonRegisteredSubclasses;
        }

        boolean basePathClassless() {
            return basePathClassless;
        }

        @Override
        public String pojoClassName() {
            return pojoClassName;
        }

        @Override
        public List<PojoFieldDescriptor> pojoFields() {
            return fields;
        }

        @Override
        public List<RegisteredSubclass> registeredPojoSubclasses() {
            return registeredSubclasses;
        }

        @Override
        public boolean pojoBasePathClassless() {
            return basePathClassless;
        }

        @Override
        public boolean pojoHasNonRegisteredSubclasses() {
            return hasNonRegisteredSubclasses;
        }

        @Override
        void writePayload(DataOutputView output) throws IOException {
            writeBoundedUtf8(
                    output,
                    pojoClassName != null ? pojoClassName : "",
                    MAX_NAME_UTF_BYTES,
                    "POJO.pojoClassName");
            output.writeBoolean(basePathClassless);
            output.writeBoolean(hasNonRegisteredSubclasses);
            output.writeInt(fields.size());
            for (PojoFieldDescriptor field : fields) {
                field.write(output);
            }
            output.writeInt(registeredSubclasses.size());
            for (RegisteredSubclass subclass : registeredSubclasses) {
                subclass.write(output);
            }
        }

        static PojoDescriptor readPayload(
                DataInputView input,
                DescriptorCapability capability,
                int depth,
                int[] totalFieldCounter)
                throws IOException {
            String pojoClassName = readBoundedUtf8(input, MAX_NAME_UTF_BYTES, "pojoClassName");
            boolean basePathClassless = input.readBoolean();
            boolean hasNonRegistered = input.readBoolean();
            int fieldCount = input.readInt();
            if (fieldCount < 0) {
                throw new IOException("Negative POJO field count: " + fieldCount);
            }
            checkTotalFields(totalFieldCounter, fieldCount);
            List<PojoFieldDescriptor> fields = new ArrayList<>(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                fields.add(PojoFieldDescriptor.read(input, depth, totalFieldCounter));
            }
            int subclassCount = input.readInt();
            if (subclassCount < 0) {
                throw new IOException("Negative subclass count: " + subclassCount);
            }
            checkTotalFields(totalFieldCounter, subclassCount);
            List<RegisteredSubclass> subclasses = new ArrayList<>(subclassCount);
            for (int i = 0; i < subclassCount; i++) {
                subclasses.add(RegisteredSubclass.read(input, depth, totalFieldCounter, i));
            }
            return new PojoDescriptor(
                    pojoClassName.isEmpty() ? null : pojoClassName,
                    fields,
                    subclasses,
                    hasNonRegistered,
                    basePathClassless,
                    capability);
        }

        @Override
        void validateInvariants() throws IOException {
            super.validateInvariants();
            // Registered subclass tags must equal their list positions.
            for (int i = 0; i < registeredSubclasses.size(); i++) {
                if (registeredSubclasses.get(i).tag() != i) {
                    throw new IOException(
                            "Registered subclass tag at position "
                                    + i
                                    + " has tag "
                                    + registeredSubclasses.get(i).tag()
                                    + " (expected "
                                    + i
                                    + ")");
                }
            }
            if (capability() == DescriptorCapability.FULLY_CLASSLESS) {
                if (!basePathClassless) {
                    throw new IOException("FULLY_CLASSLESS POJO must have basePathClassless=true");
                }
                // All fields and registered subclasses must be FULLY_CLASSLESS.
                for (PojoFieldDescriptor field : fields) {
                    if (field.descriptor().capability() != DescriptorCapability.FULLY_CLASSLESS) {
                        throw new IOException(
                                "FULLY_CLASSLESS POJO field '"
                                        + field.name()
                                        + "' has non-FULLY_CLASSLESS child: "
                                        + field.descriptor().capability());
                    }
                }
                for (RegisteredSubclass sub : registeredSubclasses) {
                    if (sub.descriptor().capability() != DescriptorCapability.FULLY_CLASSLESS) {
                        throw new IOException(
                                "FULLY_CLASSLESS POJO registered subclass at tag "
                                        + sub.tag()
                                        + " has non-FULLY_CLASSLESS child: "
                                        + sub.descriptor().capability());
                    }
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PojoDescriptor)) {
                return false;
            }
            PojoDescriptor that = (PojoDescriptor) o;
            return hasNonRegisteredSubclasses == that.hasNonRegisteredSubclasses
                    && basePathClassless == that.basePathClassless
                    && Objects.equals(pojoClassName, that.pojoClassName)
                    && Objects.equals(fields, that.fields)
                    && Objects.equals(registeredSubclasses, that.registeredSubclasses);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    pojoClassName,
                    fields,
                    registeredSubclasses,
                    hasNonRegisteredSubclasses,
                    basePathClassless);
        }
    }

    /** Descriptor for Avro wire protocol (Flink DataInputDecoder semantics). */
    static final class AvroDescriptor extends InspectDecoderDescriptor {
        private final String writerSchemaJson;
        private final String wireFormat;

        AvroDescriptor(
                String writerSchemaJson, String wireFormat, DescriptorCapability capability) {
            super(InspectDecoderDescriptorKind.AVRO, capability);
            this.writerSchemaJson = writerSchemaJson;
            this.wireFormat = wireFormat;
        }

        String writerSchemaJson() {
            return writerSchemaJson;
        }

        String wireFormat() {
            return wireFormat;
        }

        @Override
        public String avroWriterSchemaJson() {
            return writerSchemaJson;
        }

        @Override
        public String avroWireFormat() {
            return wireFormat;
        }

        @Override
        void writePayload(DataOutputView output) throws IOException {
            writeBoundedUtf8(
                    output, writerSchemaJson, MAX_SCHEMA_JSON_BYTES, "AVRO.writerSchemaJson");
            writeBoundedUtf8(output, wireFormat, MAX_WIRE_FORMAT_UTF_BYTES, "AVRO.wireFormat");
        }

        static AvroDescriptor readPayload(DataInputView input, DescriptorCapability capability)
                throws IOException {
            String writerSchemaJson =
                    readBoundedUtf8(input, MAX_SCHEMA_JSON_BYTES, "AVRO.writerSchemaJson");
            String wireFormat =
                    readBoundedUtf8(input, MAX_WIRE_FORMAT_UTF_BYTES, "AVRO.wireFormat");
            return new AvroDescriptor(writerSchemaJson, wireFormat, capability);
        }

        @Override
        void validateInvariants() throws IOException {
            super.validateInvariants();
            if (!AVRO_WIRE_FORMAT_FLINK_DATA_INPUT_V1.equals(wireFormat)) {
                throw new IOException(
                        "Unsupported AVRO wireFormat: "
                                + wireFormat
                                + " (expected "
                                + AVRO_WIRE_FORMAT_FLINK_DATA_INPUT_V1
                                + ")");
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof AvroDescriptor)) {
                return false;
            }
            AvroDescriptor that = (AvroDescriptor) o;
            return Objects.equals(writerSchemaJson, that.writerSchemaJson)
                    && Objects.equals(wireFormat, that.wireFormat);
        }

        @Override
        public int hashCode() {
            return Objects.hash(writerSchemaJson, wireFormat);
        }
    }

    /** Descriptor for unsupported serializers. */
    static final class UnsupportedDescriptor extends InspectDecoderDescriptor {
        private final String reason;

        UnsupportedDescriptor(String reason, DescriptorCapability capability) {
            super(InspectDecoderDescriptorKind.UNSUPPORTED, capability);
            this.reason = reason;
        }

        String reason() {
            return reason;
        }

        @Override
        public String unsupportedReason() {
            return reason;
        }

        @Override
        void writePayload(DataOutputView output) throws IOException {
            writeBoundedUtf8(
                    output, reason != null ? reason : "", MAX_REASON_BYTES, "UNSUPPORTED.reason");
        }

        static UnsupportedDescriptor readPayload(
                DataInputView input, DescriptorCapability capability) throws IOException {
            String reason = readBoundedUtf8(input, MAX_REASON_BYTES, "UNSUPPORTED.reason");
            return new UnsupportedDescriptor(reason.isEmpty() ? null : reason, capability);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof UnsupportedDescriptor)) {
                return false;
            }
            UnsupportedDescriptor that = (UnsupportedDescriptor) o;
            return Objects.equals(reason, that.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(reason);
        }
    }

    // ---- Nested types ----

    /** A named field in a POJO descriptor, with its child descriptor. */
    public static final class PojoFieldDescriptor {
        private final String name;
        private final InspectDecoderDescriptor descriptor;

        public PojoFieldDescriptor(String name, InspectDecoderDescriptor descriptor) {
            if (name == null || name.trim().isEmpty()) {
                throw new IllegalArgumentException("POJO field name must not be empty");
            }
            this.name = name;
            this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        }

        public String name() {
            return name;
        }

        public InspectDecoderDescriptor descriptor() {
            return descriptor;
        }

        void write(DataOutputView output) throws IOException {
            writeBoundedUtf8(output, name, MAX_NAME_UTF_BYTES, "POJO field name");
            descriptor.write(output);
        }

        static PojoFieldDescriptor read(DataInputView input, int depth, int[] totalFieldCounter)
                throws IOException {
            String name = readBoundedUtf8(input, MAX_NAME_UTF_BYTES, "POJO field name");
            InspectDecoderDescriptor descriptor =
                    InspectDecoderDescriptor.read(input, depth + 1, totalFieldCounter);
            return new PojoFieldDescriptor(name, descriptor);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PojoFieldDescriptor)) {
                return false;
            }
            PojoFieldDescriptor that = (PojoFieldDescriptor) o;
            return Objects.equals(name, that.name) && Objects.equals(descriptor, that.descriptor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, descriptor);
        }
    }

    /** A registered subclass entry in a POJO descriptor. The tag is the positional index. */
    public static final class RegisteredSubclass {
        private final String className;
        private final int tag;
        private final InspectDecoderDescriptor descriptor;

        public RegisteredSubclass(String className, int tag, InspectDecoderDescriptor descriptor) {
            this.className = className;
            this.tag = tag;
            this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        }

        public String className() {
            return className;
        }

        public int tag() {
            return tag;
        }

        public InspectDecoderDescriptor descriptor() {
            return descriptor;
        }

        void write(DataOutputView output) throws IOException {
            writeBoundedUtf8(
                    output,
                    className != null ? className : "",
                    MAX_NAME_UTF_BYTES,
                    "RegisteredSubclass.className");
            output.writeInt(tag);
            descriptor.write(output);
        }

        static RegisteredSubclass read(
                DataInputView input, int depth, int[] totalFieldCounter, int expectedTag)
                throws IOException {
            String className = readBoundedUtf8(input, MAX_NAME_UTF_BYTES, "subclass className");
            int tag = input.readInt();
            InspectDecoderDescriptor descriptor =
                    InspectDecoderDescriptor.read(input, depth + 1, totalFieldCounter);
            return new RegisteredSubclass(className.isEmpty() ? null : className, tag, descriptor);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RegisteredSubclass)) {
                return false;
            }
            RegisteredSubclass that = (RegisteredSubclass) o;
            return tag == that.tag
                    && Objects.equals(className, that.className)
                    && Objects.equals(descriptor, that.descriptor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(className, tag, descriptor);
        }
    }

    // ---- Utils ----

    private static void checkTotalFields(int[] counter, int addition) throws IOException {
        counter[0] += addition;
        if (counter[0] > MAX_TOTAL_FIELDS) {
            throw new IOException(
                    "InspectDecoderDescriptor total field count exceeded "
                            + MAX_TOTAL_FIELDS
                            + " (current: "
                            + counter[0]
                            + ")");
        }
    }

    private static <T> List<T> immutableList(List<T> list) {
        if (list == null || list.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(list));
    }
}
