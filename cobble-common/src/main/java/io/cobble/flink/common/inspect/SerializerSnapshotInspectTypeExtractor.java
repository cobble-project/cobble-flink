package io.cobble.flink.common.inspect;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Derives a {@link StateInspectType} from a Flink {@link TypeSerializerSnapshot} without loading
 * the original user serializer class. This follows FLIP-599's "schema extraction from serializer
 * snapshots" direction: common Flink snapshots carry enough structural metadata to build a
 * user-facing schema.
 *
 * <p>The extractor handles three tiers of snapshot classes:
 *
 * <ol>
 *   <li><b>flink-core primitives/wrappers</b> — matched by snapshot simple class name via a static
 *       map. These are always on the classpath.
 *   <li><b>flink-core composites</b> — {@link CompositeTypeSerializerSnapshot} subclasses. Only
 *       {@code ListSerializerSnapshot}, {@code MapSerializerSnapshot}, and {@code
 *       TupleSerializerSnapshot} are recognized; unknown composites return {@code UNKNOWN} rather
 *       than guessing a shape.
 *   <li><b>Non-classpath snapshots</b> — {@code RowDataSerializerSnapshot}, {@code
 *       PojoSerializerSnapshot}, {@code AvroSerializerSnapshot} are matched by class-name suffix
 *       and read via reflection. These classes are NOT compile-time dependencies of {@code
 *       cobble-common}.
 * </ol>
 *
 * <p>Every reflection path is wrapped in try/catch. Any failure — missing class, changed field
 * name, security restriction — degrades to {@link StateInspectType#unknown()} rather than failing
 * schema capture or checkpointing.
 */
final class SerializerSnapshotInspectTypeExtractor {

    /**
     * Maps the simple name of a flink-core primitive/wrapper serializer snapshot to its SQL logical
     * type string. Simple name is used because all these snapshots are nested classes (e.g. {@code
     * IntSerializer$IntSerializerSnapshot}) whose {@link Class#getSimpleName()} is just {@code
     * IntSerializerSnapshot}.
     */
    private static final Map<String, String> PRIMITIVE_SNAPSHOT_LOGICAL_TYPES;

    static {
        Map<String, String> map = new HashMap<>();
        // Numeric primitives
        map.put("IntSerializerSnapshot", "INT");
        map.put("LongSerializerSnapshot", "BIGINT");
        map.put("ShortSerializerSnapshot", "SMALLINT");
        map.put("ByteSerializerSnapshot", "TINYINT");
        map.put("FloatSerializerSnapshot", "FLOAT");
        map.put("DoubleSerializerSnapshot", "DOUBLE");
        // Other primitives
        map.put("BooleanSerializerSnapshot", "BOOLEAN");
        map.put("CharSerializerSnapshot", "CHAR");
        // String
        map.put("StringSerializerSnapshot", "VARCHAR");
        map.put("StringValueSerializerSnapshot", "VARCHAR");
        // Null/void
        map.put("VoidSerializerSnapshot", "NULL");
        map.put("NullValueSerializerSnapshot", "NULL");
        // Big numbers — snapshots carry no precision/scale, use conservative defaults
        map.put("BigIntSerializerSnapshot", "DECIMAL(38, 0)");
        map.put("BigDecSerializerSnapshot", "DECIMAL(38, 18)");
        // Date/time — mapped by the Java type each serializer actually serializes
        // java.util.Date is a timestamp (has time component), not a pure date
        map.put("DateSerializerSnapshot", "TIMESTAMP(9)");
        map.put("SqlDateSerializerSnapshot", "DATE");
        // java.sql.Time has no fractional seconds
        map.put("SqlTimeSerializerSnapshot", "TIME(0)");
        map.put("SqlTimestampSerializerSnapshot", "TIMESTAMP(9)");
        // java.time.* — LocalTime has nanosecond precision
        map.put("LocalDateSerializerSnapshot", "DATE");
        map.put("LocalTimeSerializerSnapshot", "TIME(9)");
        map.put("LocalDateTimeSerializerSnapshot", "TIMESTAMP(9)");
        // Instant is a point on the global timeline
        map.put("InstantSerializerSnapshot", "TIMESTAMP(9) WITH LOCAL TIME ZONE");
        // Value wrappers — map to the same logical types as their primitive counterparts
        map.put("IntValueSerializerSnapshot", "INT");
        map.put("LongValueSerializerSnapshot", "BIGINT");
        map.put("ShortValueSerializerSnapshot", "SMALLINT");
        map.put("ByteValueSerializerSnapshot", "TINYINT");
        map.put("FloatValueSerializerSnapshot", "FLOAT");
        map.put("DoubleValueSerializerSnapshot", "DOUBLE");
        map.put("BooleanValueSerializerSnapshot", "BOOLEAN");
        map.put("CharValueSerializerSnapshot", "CHAR");
        PRIMITIVE_SNAPSHOT_LOGICAL_TYPES = Collections.unmodifiableMap(map);
    }

    private SerializerSnapshotInspectTypeExtractor() {}

    /**
     * Extracts a {@link StateInspectType} from a serializer snapshot. Never throws — any failure
     * returns {@link StateInspectType#unknown()}.
     */
    static StateInspectType extract(TypeSerializerSnapshot<?> snapshot) {
        if (snapshot == null) {
            return StateInspectType.unknown();
        }
        try {
            String simpleName = snapshot.getClass().getSimpleName();

            // 1. Primitive/wrapper snapshots — direct lookup
            String logicalType = PRIMITIVE_SNAPSHOT_LOGICAL_TYPES.get(simpleName);
            if (logicalType != null) {
                return StateInspectType.scalar(logicalType);
            }

            // 2. Composite snapshots — only known composites are recognized
            if (snapshot instanceof CompositeTypeSerializerSnapshot) {
                return fromComposite((CompositeTypeSerializerSnapshot<?, ?>) snapshot, simpleName);
            }

            // 3. Non-classpath snapshots matched by class-name suffix (reflection)
            if (simpleName.equals("RowDataSerializerSnapshot")) {
                return fromRowDataSnapshot(snapshot);
            }
            if (simpleName.equals("PojoSerializerSnapshot")) {
                return fromPojoSnapshot(snapshot);
            }
            if (simpleName.equals("AvroSerializerSnapshot")) {
                return fromAvroSnapshot(snapshot);
            }

            return StateInspectType.unknown();
        } catch (ReflectiveOperationException | RuntimeException | IOException e) {
            return StateInspectType.unknown();
        }
    }

    // ---- Composite snapshots (List, Map, Tuple) ----

    private static StateInspectType fromComposite(
            CompositeTypeSerializerSnapshot<?, ?> snapshot, String simpleName) throws IOException {
        TypeSerializerSnapshot<?>[] nested = snapshot.getNestedSerializerSnapshots();
        if (nested == null || nested.length == 0) {
            return StateInspectType.unknown();
        }
        if (simpleName.equals("ListSerializerSnapshot")) {
            return StateInspectType.list(extract(nested[0]));
        }
        if (simpleName.equals("MapSerializerSnapshot")) {
            if (nested.length < 2) {
                return StateInspectType.unknown();
            }
            return StateInspectType.map(extract(nested[0]), extract(nested[1]));
        }
        if (simpleName.equals("TupleSerializerSnapshot")) {
            return tupleFromNested(nested);
        }
        // Unknown composite — do not guess; return UNKNOWN
        return StateInspectType.unknown();
    }

    private static StateInspectType tupleFromNested(TypeSerializerSnapshot<?>[] nested)
            throws IOException {
        List<StateInspectField> fields = new ArrayList<>(nested.length);
        for (int i = 0; i < nested.length; i++) {
            fields.add(new StateInspectField("f" + i, extract(nested[i])));
        }
        return fields.isEmpty() ? StateInspectType.unknown() : StateInspectType.tuple(fields);
    }

    // ---- RowDataSerializerSnapshot (reflection) ----

    private static StateInspectType fromRowDataSnapshot(Object snapshot)
            throws ReflectiveOperationException, IOException {
        // Flink 1.17: field "previousTypes"; Flink 2.0: field "types"
        Object logicalTypes = readField(snapshot, "previousTypes");
        if (logicalTypes == null) {
            logicalTypes = readField(snapshot, "types");
        }
        if (logicalTypes == null) {
            return StateInspectType.unknown();
        }
        int length = Array.getLength(logicalTypes);
        if (length == 0) {
            return StateInspectType.unknown();
        }
        List<StateInspectField> fields = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            Object logicalType = Array.get(logicalTypes, i);
            String typeString = callAsSerializableString(logicalType);
            fields.add(new StateInspectField("f" + i, StateInspectType.scalar(typeString)));
        }
        return StateInspectType.row(fields);
    }

    private static String callAsSerializableString(Object logicalType)
            throws ReflectiveOperationException {
        Method method = logicalType.getClass().getMethod("asSerializableString");
        return (String) method.invoke(logicalType);
    }

    // ---- PojoSerializerSnapshot (reflection) ----

    private static StateInspectType fromPojoSnapshot(Object snapshot)
            throws ReflectiveOperationException, IOException {
        Object snapshotData = readField(snapshot, "snapshotData");
        if (snapshotData == null) {
            return StateInspectType.unknown();
        }
        Method getFieldSerializerSnapshots =
                snapshotData.getClass().getDeclaredMethod("getFieldSerializerSnapshots");
        getFieldSerializerSnapshots.setAccessible(true);
        Object fieldMap = getFieldSerializerSnapshots.invoke(snapshotData);
        if (fieldMap == null) {
            return StateInspectType.unknown();
        }
        // LinkedOptionalMap<Field, TypeSerializerSnapshot<?>>. We read the underlying
        // LinkedHashMap<String, KeyValue<Field, TypeSerializerSnapshot<?>>> directly to get both
        // the persisted field name (keyName) and the nested snapshot (value) in insertion order.
        // The forEach approach with a ConsumerWithException proxy is more fragile across Flink
        // versions; reading the underlying map is simpler and gives the same information.
        // Version sensitivity: this depends on the private field name "underlyingMap" in
        // LinkedOptionalMap — validated against Flink 1.17 and 2.0. If the field is renamed in a
        // future Flink version, this path degrades to UNKNOWN via the catch clause.
        Object underlying = readField(fieldMap, "underlyingMap");
        if (!(underlying instanceof Map)) {
            return StateInspectType.unknown();
        }
        Map<?, ?> map = (Map<?, ?>) underlying;
        List<StateInspectField> fields = new ArrayList<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String fieldName = (String) entry.getKey();
            Object keyValue = entry.getValue();
            if (keyValue == null) {
                continue;
            }
            // KeyValue<Field, TypeSerializerSnapshot<?>> — value is the second field
            Object fieldSnapshot = readField(keyValue, "value");
            if (fieldSnapshot instanceof TypeSerializerSnapshot) {
                StateInspectType fieldType = extract((TypeSerializerSnapshot<?>) fieldSnapshot);
                fields.add(new StateInspectField(fieldName, fieldType));
            }
        }
        return fields.isEmpty() ? StateInspectType.unknown() : StateInspectType.row(fields);
    }

    // ---- AvroSerializerSnapshot (reflection) ----

    private static StateInspectType fromAvroSnapshot(Object snapshot)
            throws ReflectiveOperationException, IOException, ClassNotFoundException {
        Object schema = readField(snapshot, "schema");
        if (schema == null) {
            return StateInspectType.unknown();
        }
        // A top-level AvroSerializerSnapshot always has a record schema.
        // avroSchemaToInspectType handles RECORD → ROW recursively.
        return avroSchemaToInspectType(schema);
    }

    /**
     * Recursively converts an Avro {@code Schema} to a {@link StateInspectType}.
     *
     * <ul>
     *   <li>RECORD -> ROW with one field per Avro field (recursive)
     *   <li>ARRAY -> LIST(elementType)
     *   <li>MAP -> MAP(VARCHAR, valueType) - Avro map keys are always strings
     *   <li>UNION with null + one non-null member -> that member's type (nullable wrapper)
     *   <li>UNION with multiple non-null members -> UNKNOWN (no SQL equivalent)
     *   <li>Primitives -> SCALAR with the corresponding SQL logical type
     * </ul>
     *
     * <p>Self-referential schemas (e.g. {@code Node { next: ["null", "Node"] }}) are detected via
     * an identity-based active set. When a cycle is encountered, the recursive field is mapped to
     * {@code UNKNOWN} to avoid infinite recursion and {@link StackOverflowError}.
     */
    private static StateInspectType avroSchemaToInspectType(Object schema)
            throws ReflectiveOperationException, ClassNotFoundException {
        return avroSchemaToInspectType(schema, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static StateInspectType avroSchemaToInspectType(
            Object schema, Set<Object> activeSchemas)
            throws ReflectiveOperationException, ClassNotFoundException {
        if (schema == null) {
            return StateInspectType.unknown();
        }
        // Cycle detection: if this schema is already on the active recursion stack, map to UNKNOWN.
        if (!activeSchemas.add(schema)) {
            return StateInspectType.unknown();
        }
        try {
            // Avro's Schema is an abstract class; concrete subclasses (RecordSchema, etc.) are
            // package-private and inaccessible. Methods must be resolved on the public Schema base
            // class, not on the concrete subclass.
            Class<?> schemaClass = Class.forName("org.apache.avro.Schema");
            Method getType = schemaClass.getMethod("getType");
            Object typeEnum = getType.invoke(schema);
            String typeName = typeEnum.toString();

            switch (typeName) {
                case "STRING":
                case "ENUM":
                    return StateInspectType.scalar("VARCHAR");
                case "INT":
                    return StateInspectType.scalar("INT");
                case "LONG":
                    return StateInspectType.scalar("BIGINT");
                case "BOOLEAN":
                    return StateInspectType.scalar("BOOLEAN");
                case "FLOAT":
                    return StateInspectType.scalar("FLOAT");
                case "DOUBLE":
                    return StateInspectType.scalar("DOUBLE");
                case "BYTES":
                case "FIXED":
                    return StateInspectType.scalar("VARBINARY");
                case "NULL":
                    return StateInspectType.scalar("NULL");
                case "RECORD":
                    return avroRecordToRow(schema, schemaClass, activeSchemas);
                case "ARRAY":
                    Method getElementType = schemaClass.getMethod("getElementType");
                    Object elementSchema = getElementType.invoke(schema);
                    return StateInspectType.list(
                            avroSchemaToInspectType(elementSchema, activeSchemas));
                case "MAP":
                    Method getValueType = schemaClass.getMethod("getValueType");
                    Object valueSchema = getValueType.invoke(schema);
                    // Avro map keys are always strings.
                    return StateInspectType.map(
                            StateInspectType.scalar("VARCHAR"),
                            avroSchemaToInspectType(valueSchema, activeSchemas));
                case "UNION":
                    return avroUnionToInspectType(schema, schemaClass, activeSchemas);
                default:
                    return StateInspectType.unknown();
            }
        } finally {
            activeSchemas.remove(schema);
        }
    }

    private static StateInspectType avroRecordToRow(
            Object recordSchema, Class<?> schemaClass, Set<Object> activeSchemas)
            throws ReflectiveOperationException, ClassNotFoundException {
        Method getFields = schemaClass.getMethod("getFields");
        Object avroFields = getFields.invoke(recordSchema);
        if (!(avroFields instanceof List) || ((List<?>) avroFields).isEmpty()) {
            return StateInspectType.unknown();
        }
        List<?> fieldList = (List<?>) avroFields;
        Class<?> fieldClass = Class.forName("org.apache.avro.Schema$Field");
        Method nameMethod = fieldClass.getMethod("name");
        Method schemaMethod = fieldClass.getMethod("schema");
        List<StateInspectField> fields = new ArrayList<>(fieldList.size());
        for (Object avroField : fieldList) {
            String fieldName = (String) nameMethod.invoke(avroField);
            Object fieldSchema = schemaMethod.invoke(avroField);
            fields.add(
                    new StateInspectField(
                            fieldName, avroSchemaToInspectType(fieldSchema, activeSchemas)));
        }
        return StateInspectType.row(fields);
    }

    private static StateInspectType avroUnionToInspectType(
            Object unionSchema, Class<?> schemaClass, Set<Object> activeSchemas)
            throws ReflectiveOperationException, ClassNotFoundException {
        Method getTypes = schemaClass.getMethod("getTypes");
        List<?> types = (List<?>) getTypes.invoke(unionSchema);
        // Collect non-null members. A nullable union (null + one non-null) resolves to the
        // non-null member. A complex union (multiple non-null members) has no SQL equivalent.
        List<Object> nonNullMembers = new ArrayList<>(types.size());
        for (Object memberSchema : types) {
            Method getType = schemaClass.getMethod("getType");
            String memberType = getType.invoke(memberSchema).toString();
            if (!"NULL".equals(memberType)) {
                nonNullMembers.add(memberSchema);
            }
        }
        if (nonNullMembers.size() == 1) {
            return avroSchemaToInspectType(nonNullMembers.get(0), activeSchemas);
        }
        if (nonNullMembers.isEmpty()) {
            return StateInspectType.scalar("NULL");
        }
        // Complex union - cannot be expressed as a single SQL type.
        return StateInspectType.unknown();
    }

    // ---- Reflection helpers ----

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
