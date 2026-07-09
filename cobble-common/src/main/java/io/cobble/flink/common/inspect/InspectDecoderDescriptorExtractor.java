package io.cobble.flink.common.inspect;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds an {@link InspectDecoderDescriptor} from a live {@link TypeSerializerSnapshot} at capture
 * time, while the writer classloader has all required Flink/Avro classes.
 *
 * <p>The extractor follows the final decode policy:
 *
 * <ol>
 *   <li>If the snapshot is monitor-portable (primitives, Tuple/List/Map trees, portable RowData),
 *       emit {@code PORTABLE_SNAPSHOT(FULLY_CLASSLESS)} with the snapshot bytes. Both top-level and
 *       child snapshots serialize their bytes via {@link TypeSerializerSnapshotSerializationUtil},
 *       so the monitor can restore the field codec without user classes.
 *   <li>If the snapshot is a {@code PojoSerializerSnapshot}, build a {@code POJO} descriptor with
 *       recursive field and registered-subclass descriptors.
 *   <li>If the snapshot is an {@code AvroSerializerSnapshot}, build an {@code AVRO} descriptor with
 *       the writer schema JSON.
 *   <li>Otherwise, emit {@code UNSUPPORTED}.
 * </ol>
 *
 * <p>Every path is wrapped in try/catch. Any failure - missing class, changed field name, security
 * restriction, oversized payload - degrades to {@code UNSUPPORTED} rather than failing schema
 * capture or checkpointing.
 */
final class InspectDecoderDescriptorExtractor {

    private InspectDecoderDescriptorExtractor() {}

    /**
     * Extracts a top-level descriptor from a serializer snapshot.
     *
     * @param snapshot the live snapshot object (non-null)
     * @param snapshotBytes the serialized snapshot bytes (may be null if serialization failed)
     */
    static InspectDecoderDescriptor extract(
            TypeSerializerSnapshot<?> snapshot, byte[] snapshotBytes) {
        return doExtract(snapshot, snapshotBytes);
    }

    /**
     * Extracts a descriptor for a child snapshot (POJO field or registered subclass). The child's
     * snapshot bytes are serialized independently via the same {@link
     * TypeSerializerSnapshotSerializationUtil} logic, so that PORTABLE_SNAPSHOT children carry
     * their own bytes for the monitor to restore.
     */
    private static InspectDecoderDescriptor extractChild(TypeSerializerSnapshot<?> snapshot) {
        return doExtract(snapshot, null);
    }

    /**
     * Unified extraction logic. When {@code providedSnapshotBytes} is null and the snapshot is
     * monitor-portable, the bytes are serialized from the live snapshot. If serialization fails,
     * the descriptor degrades to UNSUPPORTED.
     */
    private static InspectDecoderDescriptor doExtract(
            TypeSerializerSnapshot<?> snapshot, byte[] providedSnapshotBytes) {
        if (snapshot == null) {
            return InspectDecoderDescriptor.unsupported("snapshot is null");
        }
        try {
            // 1. Monitor-portable snapshot -> PORTABLE_SNAPSHOT(FULLY_CLASSLESS).
            if (SerializerInspectSchema.isMonitorPortable(snapshot)) {
                byte[] bytes = providedSnapshotBytes;
                if (bytes == null) {
                    bytes = serializeSnapshotBytes(snapshot);
                }
                if (bytes == null) {
                    return InspectDecoderDescriptor.unsupported(
                            "portable snapshot serialization failed: "
                                    + snapshot.getClass().getName());
                }
                if (bytes.length > InspectDecoderDescriptor.MAX_SNAPSHOT_BYTES) {
                    return InspectDecoderDescriptor.unsupported(
                            "portable snapshot bytes exceed "
                                    + InspectDecoderDescriptor.MAX_SNAPSHOT_BYTES
                                    + " bytes ("
                                    + bytes.length
                                    + ")");
                }
                return InspectDecoderDescriptor.portableSnapshot(
                        snapshot.getClass().getName(), bytes);
            }

            String simpleName = snapshot.getClass().getSimpleName();

            // 2. POJO snapshot -> POJO descriptor.
            if (simpleName.equals("PojoSerializerSnapshot")) {
                return fromPojoSnapshot(snapshot);
            }

            // 3. Avro snapshot -> AVRO descriptor.
            if (simpleName.equals("AvroSerializerSnapshot")) {
                return fromAvroSnapshot(snapshot);
            }

            // 4. RowDataSerializerSnapshot that is NOT portable -> UNSUPPORTED.
            if (simpleName.equals("RowDataSerializerSnapshot")) {
                return InspectDecoderDescriptor.unsupported(
                        "RowDataSerializerSnapshot has non-portable nested serializers");
            }

            // 5. Everything else -> UNSUPPORTED. Include the snapshot class name so users can
            //    understand why --user-jar or raw inspect is needed.
            return InspectDecoderDescriptor.unsupported(
                    "unsupported POJO field serializer: " + snapshot.getClass().getName());
        } catch (ReflectiveOperationException | RuntimeException e) {
            return InspectDecoderDescriptor.unsupported(
                    "descriptor extraction failed: "
                            + e.getClass().getSimpleName()
                            + (e.getMessage() != null ? ": " + e.getMessage() : ""));
        }
    }

    private static InspectDecoderDescriptor fromPojoSnapshot(Object snapshot)
            throws ReflectiveOperationException {
        Object snapshotData = readField(snapshot, "snapshotData");
        if (snapshotData == null) {
            return InspectDecoderDescriptor.unsupported("POJO snapshotData is null");
        }

        // Extract fields.
        Method getFieldSerializerSnapshots =
                snapshotData.getClass().getDeclaredMethod("getFieldSerializerSnapshots");
        getFieldSerializerSnapshots.setAccessible(true);
        Object fieldMap = getFieldSerializerSnapshots.invoke(snapshotData);
        if (fieldMap == null) {
            return InspectDecoderDescriptor.unsupported("POJO field map is null");
        }
        Object fieldUnderlying = readField(fieldMap, "underlyingMap");
        if (!(fieldUnderlying instanceof Map)) {
            return InspectDecoderDescriptor.unsupported(
                    "POJO field underlyingMap not a Map (incompatible Flink version)");
        }
        Map<?, ?> fieldEntries = (Map<?, ?>) fieldUnderlying;

        List<InspectDecoderDescriptor.PojoFieldDescriptor> fields =
                new ArrayList<>(fieldEntries.size());
        boolean basePathClassless = true;
        for (Map.Entry<?, ?> entry : fieldEntries.entrySet()) {
            String fieldName = (String) entry.getKey();
            Object keyValue = entry.getValue();
            if (keyValue == null) {
                fields.add(
                        new InspectDecoderDescriptor.PojoFieldDescriptor(
                                fieldName,
                                InspectDecoderDescriptor.unsupported("missing field snapshot")));
                basePathClassless = false;
                continue;
            }
            Object fieldSnapshot = readField(keyValue, "value");
            if (!(fieldSnapshot instanceof TypeSerializerSnapshot)) {
                fields.add(
                        new InspectDecoderDescriptor.PojoFieldDescriptor(
                                fieldName,
                                InspectDecoderDescriptor.unsupported(
                                        "field snapshot not a TypeSerializerSnapshot")));
                basePathClassless = false;
                continue;
            }
            InspectDecoderDescriptor fieldDescriptor =
                    extractChild((TypeSerializerSnapshot<?>) fieldSnapshot);
            fields.add(
                    new InspectDecoderDescriptor.PojoFieldDescriptor(fieldName, fieldDescriptor));
            if (fieldDescriptor.capability() == DescriptorCapability.UNSUPPORTED) {
                basePathClassless = false;
            }
        }

        // Extract registered subclasses (tag = positional index).
        Method getRegisteredSubclasses =
                snapshotData
                        .getClass()
                        .getDeclaredMethod("getRegisteredSubclassSerializerSnapshots");
        getRegisteredSubclasses.setAccessible(true);
        Object registeredMap = getRegisteredSubclasses.invoke(snapshotData);
        List<InspectDecoderDescriptor.RegisteredSubclass> registeredSubclasses = new ArrayList<>();
        boolean allSubclassesClassless = true;
        if (registeredMap != null) {
            Object registeredUnderlying = readField(registeredMap, "underlyingMap");
            if (!(registeredUnderlying instanceof Map)) {
                // If we can't read the registered subclass map, we cannot safely determine
                // capability. Degrade to UNSUPPORTED rather than risking a false FULLY_CLASSLESS.
                return InspectDecoderDescriptor.unsupported(
                        "POJO registered subclass underlyingMap not a Map (incompatible Flink version)");
            }
            Map<?, ?> registeredEntries = (Map<?, ?>) registeredUnderlying;
            int tag = 0;
            for (Map.Entry<?, ?> entry : registeredEntries.entrySet()) {
                String className = (String) entry.getKey();
                Object keyValue = entry.getValue();
                if (keyValue == null) {
                    registeredSubclasses.add(
                            new InspectDecoderDescriptor.RegisteredSubclass(
                                    className,
                                    tag,
                                    InspectDecoderDescriptor.unsupported(
                                            "missing subclass snapshot")));
                    allSubclassesClassless = false;
                    tag++;
                    continue;
                }
                Object subclassSnapshot = readField(keyValue, "value");
                if (!(subclassSnapshot instanceof TypeSerializerSnapshot)) {
                    registeredSubclasses.add(
                            new InspectDecoderDescriptor.RegisteredSubclass(
                                    className,
                                    tag,
                                    InspectDecoderDescriptor.unsupported(
                                            "subclass snapshot not a TypeSerializerSnapshot")));
                    allSubclassesClassless = false;
                    tag++;
                    continue;
                }
                InspectDecoderDescriptor subclassDescriptor =
                        extractChild((TypeSerializerSnapshot<?>) subclassSnapshot);
                registeredSubclasses.add(
                        new InspectDecoderDescriptor.RegisteredSubclass(
                                className, tag, subclassDescriptor));
                if (subclassDescriptor.capability() != DescriptorCapability.FULLY_CLASSLESS) {
                    allSubclassesClassless = false;
                }
                tag++;
            }
        }

        // Check non-registered subclasses.
        Method getNonRegisteredSubclasses =
                snapshotData
                        .getClass()
                        .getDeclaredMethod("getNonRegisteredSubclassSerializerSnapshots");
        getNonRegisteredSubclasses.setAccessible(true);
        Object nonRegisteredMap = getNonRegisteredSubclasses.invoke(snapshotData);
        boolean hasNonRegisteredSubclasses = false;
        if (nonRegisteredMap != null) {
            Object nonRegisteredUnderlying = readField(nonRegisteredMap, "underlyingMap");
            if (!(nonRegisteredUnderlying instanceof Map)) {
                // If we can't read the non-registered subclass map, we cannot safely determine
                // whether non-registered subclasses exist. Degrade to UNSUPPORTED.
                return InspectDecoderDescriptor.unsupported(
                        "POJO non-registered subclass underlyingMap not a Map (incompatible Flink version)");
            }
            hasNonRegisteredSubclasses = !((Map<?, ?>) nonRegisteredUnderlying).isEmpty();
        }

        // Extract pojo class name for diagnostics.
        String pojoClassName = null;
        Object pojoClass = readField(snapshotData, "pojoClass");
        if (pojoClass instanceof Class<?>) {
            pojoClassName = ((Class<?>) pojoClass).getName();
        }

        // Compute capability.
        DescriptorCapability capability;
        if (basePathClassless && !hasNonRegisteredSubclasses && allSubclassesClassless) {
            capability = DescriptorCapability.FULLY_CLASSLESS;
        } else {
            capability = DescriptorCapability.PARTIALLY_CLASSLESS;
        }

        return InspectDecoderDescriptor.pojo(
                pojoClassName,
                fields,
                registeredSubclasses,
                hasNonRegisteredSubclasses,
                basePathClassless,
                capability);
    }

    // ---- Avro ----

    private static InspectDecoderDescriptor fromAvroSnapshot(Object snapshot)
            throws ReflectiveOperationException {
        Object schema = readField(snapshot, "schema");
        if (schema == null) {
            return InspectDecoderDescriptor.unsupported("Avro schema is null");
        }

        // Get schema.toString(false) via reflection.
        Method toString = schema.getClass().getMethod("toString", boolean.class);
        String writerSchemaJson = (String) toString.invoke(schema, false);

        // Capture-side size check: degrade to UNSUPPORTED if the schema JSON exceeds the limit.
        int schemaByteLen = writerSchemaJson.getBytes(StandardCharsets.UTF_8).length;
        if (schemaByteLen > InspectDecoderDescriptor.MAX_SCHEMA_JSON_BYTES) {
            return InspectDecoderDescriptor.unsupported(
                    "Avro writerSchemaJson exceeds "
                            + InspectDecoderDescriptor.MAX_SCHEMA_JSON_BYTES
                            + " bytes ("
                            + schemaByteLen
                            + ")");
        }

        // Analyze the schema tree for complex unions and recursive references.
        AvroSchemaAnalysis analysis = analyzeAvroSchema(schema);

        DescriptorCapability capability =
                (analysis.hasComplexUnion || analysis.hasRecursiveReference)
                        ? DescriptorCapability.PARTIALLY_CLASSLESS
                        : DescriptorCapability.FULLY_CLASSLESS;

        return InspectDecoderDescriptor.avro(
                writerSchemaJson,
                InspectDecoderDescriptor.AVRO_WIRE_FORMAT_FLINK_DATA_INPUT_V1,
                capability);
    }

    /** Result of analyzing an Avro schema tree for classless-decode blockers. */
    private static final class AvroSchemaAnalysis {
        boolean hasComplexUnion;
        boolean hasRecursiveReference;
    }

    /**
     * Analyzes the Avro schema tree for two conditions that prevent fully classless decoding:
     *
     * <ul>
     *   <li><b>Complex union</b>: a UNION with multiple non-null members. The monitor cannot
     *       determine which branch to decode without user classes.
     *   <li><b>Recursive reference</b>: a self-referential schema (e.g. {@code Node { next:
     *       ["null", "Node"] }}). The semantic extractor maps the recursive field to UNKNOWN, which
     *       causes row-level decode errors in the monitor. Until Phase 2 implements a depth-bounded
     *       recursive renderer, such schemas must retain the live serializer fallback.
     * </ul>
     *
     * <p>Self-referential schemas are detected via an identity-based active set to prevent infinite
     * recursion and {@link StackOverflowError}.
     */
    private static AvroSchemaAnalysis analyzeAvroSchema(Object schema)
            throws ReflectiveOperationException {
        return analyzeAvroSchema(schema, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static AvroSchemaAnalysis analyzeAvroSchema(Object schema, Set<Object> activeSchemas)
            throws ReflectiveOperationException {
        AvroSchemaAnalysis result = new AvroSchemaAnalysis();
        if (schema == null) {
            return result;
        }
        // Cycle detection: if this schema is already on the active recursion stack, we found a
        // recursive reference. Stop recursing and mark it.
        if (!activeSchemas.add(schema)) {
            result.hasRecursiveReference = true;
            return result;
        }
        try {
            Class<?> schemaClass;
            try {
                schemaClass = Class.forName("org.apache.avro.Schema");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            Method getType = schemaClass.getMethod("getType");
            String typeName = getType.invoke(schema).toString();

            if ("UNION".equals(typeName)) {
                Method getTypes = schemaClass.getMethod("getTypes");
                List<?> types = (List<?>) getTypes.invoke(schema);
                int nonNullCount = 0;
                for (Object member : types) {
                    String memberType = getType.invoke(member).toString();
                    if (!"NULL".equals(memberType)) {
                        nonNullCount++;
                        AvroSchemaAnalysis memberResult = analyzeAvroSchema(member, activeSchemas);
                        result.hasComplexUnion |= memberResult.hasComplexUnion;
                        result.hasRecursiveReference |= memberResult.hasRecursiveReference;
                    }
                }
                if (nonNullCount > 1) {
                    result.hasComplexUnion = true;
                }
            } else if ("RECORD".equals(typeName)) {
                Method getFields = schemaClass.getMethod("getFields");
                List<?> fields = (List<?>) getFields.invoke(schema);
                for (Object field : fields) {
                    try {
                        Class<?> fieldClass = Class.forName("org.apache.avro.Schema$Field");
                        Method schemaMethod = fieldClass.getMethod("schema");
                        Object fieldSchema = schemaMethod.invoke(field);
                        AvroSchemaAnalysis fieldResult =
                                analyzeAvroSchema(fieldSchema, activeSchemas);
                        result.hasComplexUnion |= fieldResult.hasComplexUnion;
                        result.hasRecursiveReference |= fieldResult.hasRecursiveReference;
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
            } else if ("ARRAY".equals(typeName)) {
                Method getElementType = schemaClass.getMethod("getElementType");
                AvroSchemaAnalysis elementResult =
                        analyzeAvroSchema(getElementType.invoke(schema), activeSchemas);
                result.hasComplexUnion |= elementResult.hasComplexUnion;
                result.hasRecursiveReference |= elementResult.hasRecursiveReference;
            } else if ("MAP".equals(typeName)) {
                Method getValueType = schemaClass.getMethod("getValueType");
                AvroSchemaAnalysis valueResult =
                        analyzeAvroSchema(getValueType.invoke(schema), activeSchemas);
                result.hasComplexUnion |= valueResult.hasComplexUnion;
                result.hasRecursiveReference |= valueResult.hasRecursiveReference;
            }
            return result;
        } finally {
            activeSchemas.remove(schema);
        }
    }

    // ---- Snapshot byte serialization ----

    /**
     * Serializes a {@link TypeSerializerSnapshot} to bytes using the same Flink utility as the
     * top-level capture path. Used for child snapshots (POJO fields, registered subclasses) so that
     * PORTABLE_SNAPSHOT children carry their own restorable bytes.
     */
    static byte[] serializeSnapshotBytes(TypeSerializerSnapshot<?> snapshot) {
        try {
            DataOutputSerializer buffer = new DataOutputSerializer(128);
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(buffer, snapshot);
            return buffer.getCopyOfBuffer();
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    // ---- Reflection helpers (same pattern as SerializerSnapshotInspectTypeExtractor) ----

    private static Object readField(Object target, String fieldName)
            throws ReflectiveOperationException {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(target);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        return null;
    }
}
