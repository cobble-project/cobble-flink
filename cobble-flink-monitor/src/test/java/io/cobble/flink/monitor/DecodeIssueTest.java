package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.common.inspect.DescriptorCapability;
import io.cobble.flink.common.inspect.InspectDecoderDescriptor;
import io.cobble.flink.common.inspect.SerializerInspectSchema;
import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.util.MathUtils;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for structured {@code decode_issues} on {@link StateInspectDecoder.DecodedRow}.
 *
 * <p>Each test verifies that the {@link StateInspectDecoder.DecodeIssueCollector} correctly records
 * typed issues (with the right {@link DecodeIssueKind}) for parts that failed to produce output,
 * while avoiding duplicates and avoiding issues for parts that succeeded via fallback.
 */
class DecodeIssueTest {

    // ---- Test fixtures ----

    public static final class SimplePojo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public String name;

        public SimplePojo() {}

        public SimplePojo(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public static class PojoBase implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int id;
        public String name;
    }

    public static class PojoSubclassA extends PojoBase {
        private static final long serialVersionUID = 1L;
        public String extraA;
    }

    public static class PojoSubclassB extends PojoBase {
        private static final long serialVersionUID = 1L;
        public int extraB;
    }

    // ---- Tests ----

    @Test
    void classlessUnsupportedWhenNonRegisteredSubclassPojoBlocked() throws Exception {
        // A PARTIALLY_CLASSLESS ListState<PojoBase> with PojoSubclassB elements (flag 0x04).
        // When PojoSubclassB is blocked from the TCCL, the classless path fails
        // (CLASSLESS_UNSUPPORTED)
        // and the live fallback also fails because deserialize() cannot instantiate the subclass.
        // The fallback cannot instantiate the blocked subclass, which is a classpath failure.
        ExecutionConfig config = new ExecutionConfig();
        config.registerPojoType(PojoSubclassA.class);
        TypeSerializer<PojoBase> pojoSerializer =
                TypeInformation.of(PojoBase.class).createSerializer(config);

        PojoSubclassB polluter = new PojoSubclassB();
        polluter.id = 0;
        polluter.name = "polluter";
        polluter.extraB = 0;
        serialize(pojoSerializer, polluter);

        StateInspectSchema schema =
                StateInspectSchema.forList(
                        "pojo-partial",
                        "cf-pojo-partial",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);
        assertEquals(
                DescriptorCapability.PARTIALLY_CLASSLESS,
                schema.listElementSerializer().decoderDescriptor().capability());

        StateInspectType elementType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forList(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), elementType);

        PojoSubclassB b = new PojoSubclassB();
        b.id = 1;
        b.name = "blocked";
        b.extraB = 99;
        byte[] listBytes = buildListPayload(serialize(pojoSerializer, b));
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row =
                decodeWithBlockedClass(schema, semanticSchema, rowKey, listBytes, "PojoSubclassB");

        assertNotNull(row.decodeError, "expected decode_error");
        assertNotNull(row.decodeIssues, "decode_issues should not be null");
        assertFalse(row.decodeIssues.isEmpty(), "expected at least one issue");
        // The value part failed. The semantic path should record the issue.
        StateInspectDecoder.DecodeIssue valueIssue = findIssue(row.decodeIssues, "value");
        assertNotNull(valueIssue, "expected issue for part 'value'");
        // The blocked subclass is an explicit classpath failure, so the fallback cause takes
        // precedence over the original CLASSLESS_UNSUPPORTED reason.
        assertEquals(
                DecodeIssueKind.SERIALIZER_RESTORE_FAILED,
                valueIssue.kind,
                "value issue kind: " + valueIssue.kind + " msg: " + valueIssue.message);
        assertTrue(valueIssue.message.contains("Classless decode failed"));
        assertTrue(valueIssue.message.contains("live serializer fallback failed"));
    }

    @Test
    void malformedBytesWhenPojoBytesTruncated() throws Exception {
        // A FULLY_CLASSLESS POJO ValueState with truncated bytes -> MALFORMED_BYTES.
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "pojo-trunc",
                        "cf-pojo-trunc",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);

        StateInspectType pojoRowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), pojoRowType);

        // Truncated bytes: just the flag byte 0x02 (NO_SUBCLASS) but no field data.
        byte[] truncated = new byte[] {0x02};
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row = decode(schema, semanticSchema, rowKey, truncated);

        assertNotNull(row.decodeError, "expected decode_error");
        StateInspectDecoder.DecodeIssue valueIssue = findIssue(row.decodeIssues, "value");
        assertNotNull(valueIssue, "expected issue for part 'value'");
        assertEquals(
                DecodeIssueKind.MALFORMED_BYTES,
                valueIssue.kind,
                "truncated POJO bytes should be MALFORMED_BYTES: " + valueIssue.message);
    }

    @Test
    void malformedBytesWhenTrailingBytesAfterPojo() throws Exception {
        // A FULLY_CLASSLESS POJO ValueState with trailing bytes after the datum.
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "pojo-trail",
                        "cf-pojo-trail",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);

        StateInspectType pojoRowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), pojoRowType);

        SimplePojo pojo = new SimplePojo(42, "trail");
        byte[] pojoBytes = serialize(pojoSerializer, pojo);
        // Append a trailing byte.
        byte[] withTrailing = new byte[pojoBytes.length + 1];
        System.arraycopy(pojoBytes, 0, withTrailing, 0, pojoBytes.length);
        withTrailing[pojoBytes.length] = 0x00;
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row = decode(schema, semanticSchema, rowKey, withTrailing);

        assertNotNull(row.decodeError, "expected decode_error for trailing bytes");
        StateInspectDecoder.DecodeIssue valueIssue = findIssue(row.decodeIssues, "value");
        assertNotNull(valueIssue, "expected issue for part 'value'");
        assertEquals(
                DecodeIssueKind.MALFORMED_BYTES,
                valueIssue.kind,
                "trailing bytes should be MALFORMED_BYTES: " + valueIssue.message);
    }

    @Test
    void noIssueWhenPartiallyClasslessFallbackSucceeds() throws Exception {
        // A PARTIALLY_CLASSLESS ListState<PojoBase> with PojoSubclassB elements.
        // When the subclass class IS available, the classless path fails (flag 0x04) but the
        // live fallback succeeds. No issue should be recorded.
        ExecutionConfig config = new ExecutionConfig();
        config.registerPojoType(PojoSubclassA.class);
        TypeSerializer<PojoBase> pojoSerializer =
                TypeInformation.of(PojoBase.class).createSerializer(config);

        PojoSubclassB polluter = new PojoSubclassB();
        polluter.id = 0;
        polluter.name = "polluter";
        polluter.extraB = 0;
        serialize(pojoSerializer, polluter);

        StateInspectSchema schema =
                StateInspectSchema.forList(
                        "pojo-fallback-ok",
                        "cf-pojo-fallback-ok",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);

        StateInspectType elementType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forList(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), elementType);

        PojoSubclassB b1 = new PojoSubclassB();
        b1.id = 10;
        b1.name = "first";
        b1.extraB = 100;
        byte[] listBytes = buildListPayload(serialize(pojoSerializer, b1));
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row = decode(schema, semanticSchema, rowKey, listBytes);

        assertNull(row.decodeError, "fallback should succeed: " + row.decodeError);
        assertTrue(
                row.decodeIssues.isEmpty(),
                "no issues expected when fallback succeeds: " + issuesToString(row.decodeIssues));
    }

    @Test
    void partiallyClasslessFallbackMalformedBytesTakesPrecedence() throws Exception {
        ExecutionConfig config = new ExecutionConfig();
        config.registerPojoType(PojoSubclassA.class);
        TypeSerializer<PojoBase> pojoSerializer =
                TypeInformation.of(PojoBase.class).createSerializer(config);

        PojoSubclassB polluter = new PojoSubclassB();
        serialize(pojoSerializer, polluter);
        StateInspectSchema schema =
                StateInspectSchema.forList(
                        "pojo-fallback-malformed",
                        "cf-pojo-fallback-malformed",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);
        StateInspectType elementType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forList(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), elementType);

        PojoSubclassB value = new PojoSubclassB();
        value.id = 1;
        value.name = "truncated";
        value.extraB = 9;
        byte[] serialized = serialize(pojoSerializer, value);
        byte[] truncated = Arrays.copyOf(serialized, serialized.length - 1);

        StateInspectDecoder.DecodedRow row =
                decode(
                        schema,
                        semanticSchema,
                        keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1)),
                        truncated);

        StateInspectDecoder.DecodeIssue issue = findIssue(row.decodeIssues, "value");
        assertNotNull(issue);
        assertEquals(DecodeIssueKind.MALFORMED_BYTES, issue.kind);
        assertTrue(issue.message.contains("Classless decode failed"));
        assertTrue(issue.message.contains("live serializer fallback failed"));
    }

    @Test
    void semanticValueIssueSkipsLegacyDecodeForSamePart() throws Exception {
        // When a FULLY_CLASSLESS POJO value fails semantic decoding, the recorded "value" issue
        // skips the legacy decode for that same part, leaving exactly one issue.
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "pojo-dup",
                        "cf-pojo-dup",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        pojoSerializer);

        StateInspectType pojoRowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"), StateInspectType.unknown(), pojoRowType);

        // Truncated bytes.
        byte[] truncated = new byte[] {0x02};
        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));

        StateInspectDecoder.DecodedRow row = decode(schema, semanticSchema, rowKey, truncated);

        // Count issues for "value" - should be exactly 1.
        long valueIssueCount =
                row.decodeIssues.stream().filter(i -> "value".equals(i.part)).count();
        assertEquals(1, valueIssueCount, "exactly one issue for 'value' expected");
    }

    @Test
    void timerKeyBoundaryFailureRecordsStateKeyIssue() throws Exception {
        // A timer state with a POJO key. When the POJO class is blocked, the timer key boundary
        // finding fails. The timestamp should still be in decoded_parts, and a "state_key" issue
        // should be recorded.
        TypeSerializer<SimplePojo> pojoSerializer =
                TypeInformation.of(SimplePojo.class).createSerializer(new ExecutionConfig());
        StateInspectSchema schema =
                StateInspectSchema.forTimer(
                        "timer-pojo",
                        "cf-timer-pojo",
                        pojoSerializer,
                        VoidNamespaceSerializer.INSTANCE);

        StateInspectType pojoRowType =
                StateInspectType.row(
                        Arrays.asList(
                                new StateInspectField("id", StateInspectType.scalar("INT")),
                                new StateInspectField("name", StateInspectType.scalar("VARCHAR"))));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        pojoRowType, StateInspectType.unknown(), StateInspectType.unknown());

        SimplePojo key = new SimplePojo(5, "timer-key");
        byte[] keyBytes = serialize(pojoSerializer, key);
        byte[] rowKey = timerKeyWithVoidNamespace(1000L, keyBytes);

        StateInspectDecoder.DecodedRow row =
                decodeTimerWithBlockedClass(schema, semanticSchema, rowKey, "SimplePojo");

        // Timestamp should still be present.
        assertNotNull(row.decodedParts, "decoded_parts should not be null");
        assertTrue(row.decodedParts.containsKey("timestamp"), "timestamp should be present");
        // state_key should have an issue.
        StateInspectDecoder.DecodeIssue keyIssue = findIssue(row.decodeIssues, "state_key");
        assertNotNull(keyIssue, "expected issue for part 'state_key'");
        // The restore failure (blocked class) should be SERIALIZER_RESTORE_FAILED.
        assertEquals(
                DecodeIssueKind.SERIALIZER_RESTORE_FAILED,
                keyIssue.kind,
                "timer key restore failure should be SERIALIZER_RESTORE_FAILED: "
                        + keyIssue.message);
    }

    @Test
    void serializerRestoreFailedWhenSerializerClassAbsent() throws Exception {
        // A ValueState whose value serializer class cannot be restored (missing class).
        // Uses reflection to replace the schema's value serializer with an unrestorable one.
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "bad-serializer",
                        "cf-bad-serializer",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        SerializerInspectSchema unrestorable =
                serializerSchema("com.example.NonExistentSerializer");
        setField(schema, "valueSerializer", unrestorable);

        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("INT"));

        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));
        byte[] valueBytes = serialize(StringSerializer.INSTANCE, "dummy");

        StateInspectDecoder.DecodedRow row = decode(schema, semanticSchema, rowKey, valueBytes);

        assertNotNull(row.decodeError, "expected decode_error");
        StateInspectDecoder.DecodeIssue valueIssue = findIssue(row.decodeIssues, "value");
        assertNotNull(valueIssue, "expected issue for part 'value'");
        assertEquals(
                DecodeIssueKind.SERIALIZER_RESTORE_FAILED,
                valueIssue.kind,
                "missing serializer class should be SERIALIZER_RESTORE_FAILED: "
                        + valueIssue.message);
    }

    @Test
    void serializerDependencyMissingDuringDeserializeIsRowLocal() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "lazy-linkage",
                        "cf-lazy-linkage",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new LinkageOnDeserializeSerializer());
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("VARCHAR"));

        byte[] rowKey = keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1));
        byte[] valueBytes = serialize(StringSerializer.INSTANCE, "dependency-value");

        StateInspectDecoder.DecodedRow row = decode(schema, semanticSchema, rowKey, valueBytes);

        assertNotNull(row.decodeError);
        assertNull(row.decodedValue);
        StateInspectDecoder.DecodeIssue issue = findIssue(row.decodeIssues, "value");
        assertNotNull(issue);
        assertEquals(DecodeIssueKind.SERIALIZER_RESTORE_FAILED, issue.kind);
        assertEquals(1, row.decodeIssues.size(), "semantic and legacy paths must share one issue");
    }

    @Test
    void otherLinkageErrorDuringDeserializeIsUnknown() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "other-linkage",
                        "cf-other-linkage",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new OtherLinkageOnDeserializeSerializer());
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("VARCHAR"));

        StateInspectDecoder.DecodedRow row =
                decode(
                        schema,
                        semanticSchema,
                        keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1)),
                        serialize(StringSerializer.INSTANCE, "linkage-value"));

        assertEquals(DecodeIssueKind.UNKNOWN, findIssue(row.decodeIssues, "value").kind);
    }

    @Test
    void customSerializerRuntimeExceptionIsUnknown() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "runtime-failure",
                        "cf-runtime-failure",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new RuntimeOnDeserializeSerializer());
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("VARCHAR"));

        StateInspectDecoder.DecodedRow row =
                decode(
                        schema,
                        semanticSchema,
                        keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1)),
                        serialize(StringSerializer.INSTANCE, "runtime-value"));

        assertEquals(DecodeIssueKind.UNKNOWN, findIssue(row.decodeIssues, "value").kind);
    }

    @Test
    void multipleIssuesWhenKeyAndValueBothFail() throws Exception {
        // A ValueState where both the key serializer and value serializer are unrestorable.
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "both-bad",
                        "cf-both-bad",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        SerializerInspectSchema badKey = serializerSchema("com.example.BadKeySerializer");
        SerializerInspectSchema badValue = serializerSchema("com.example.BadValueSerializer");
        setField(schema, "keySerializer", badKey);
        setField(schema, "valueSerializer", badValue);
        setField(
                schema,
                "namespaceSerializer",
                serializerSchema(VOID_NAMESPACE_SERIALIZER_CLASS, 1));
        setField(schema, "namespaceFixedLength", true);
        setField(schema, "keyLengthStored", false);

        // No semantic schema (all parts UNKNOWN), so the semantic path is skipped.
        // Both legacy key and value decode should fail.
        byte[] rowKey = new byte[] {0, 0, 0, 1, 0}; // key + void namespace
        byte[] valueBytes = new byte[] {1, 2, 3};

        StateInspectDecoder.DecodedRow row = decode(schema, null, rowKey, valueBytes);

        assertNotNull(row.decodeError, "expected decode_error");
        // Both state_key and value should have issues.
        assertNotNull(findIssue(row.decodeIssues, "state_key"), "expected issue for state_key");
        assertNotNull(findIssue(row.decodeIssues, "value"), "expected issue for value");
        assertEquals(2, row.decodeIssues.size(), "expected exactly 2 issues");
    }

    @Test
    void mapValueMalformedBytesKeepsDecodedKeyPartsAndOnlyRecordsMapValue() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forMap(
                        "map-malformed",
                        "cf-map-malformed",
                        false,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forMap(
                        StateInspectType.scalar("INT"),
                        StateInspectType.scalar("INT"),
                        StateInspectType.scalar("INT"),
                        StateInspectType.scalar("INT"));
        byte[] rowKey = mapKey(1, 2, 3);

        StateInspectDecoder.DecodedRow row = decode(schema, semanticSchema, rowKey, new byte[0]);

        assertEquals(1, ((java.util.Map<?, ?>) row.decodedParts.get("state_key")).get("value"));
        assertEquals(2, ((java.util.Map<?, ?>) row.decodedParts.get("namespace")).get("value"));
        assertEquals(3, ((java.util.Map<?, ?>) row.decodedParts.get("map_key")).get("value"));
        assertFalse(row.decodedParts.containsKey("map_value"));
        assertEquals(1, row.decodeIssues.size());
        StateInspectDecoder.DecodeIssue issue = findIssue(row.decodeIssues, "map_value");
        assertNotNull(issue);
        assertEquals(DecodeIssueKind.MALFORMED_BYTES, issue.kind);
    }

    @Test
    void truncatedBuiltinScalarIsMalformedBytes() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "truncated-int",
                        "cf-truncated-int",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("INT"));

        StateInspectDecoder.DecodedRow row =
                decode(
                        schema,
                        semanticSchema,
                        keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1)),
                        new byte[] {0x01});

        assertEquals(DecodeIssueKind.MALFORMED_BYTES, findIssue(row.decodeIssues, "value").kind);
    }

    @Test
    void invalidListDelimiterIsMalformedBytes() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forList(
                        "bad-delimiter",
                        "cf-bad-delimiter",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forList(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.scalar("INT"));
        byte[] valueBytes = new byte[] {0, 0, 0, 1, '!'};

        StateInspectDecoder.DecodedRow row =
                decode(
                        schema,
                        semanticSchema,
                        keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1)),
                        valueBytes);

        assertEquals(DecodeIssueKind.MALFORMED_BYTES, findIssue(row.decodeIssues, "value").kind);
    }

    @Test
    void shortTimerTimestampIsMalformedBytes() throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forTimer(
                        "short-timer",
                        "cf-short-timer",
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE);

        StateInspectDecoder.DecodedRow row =
                decode(schema, null, new byte[] {0, 0, 0, 1}, new byte[0]);

        assertEquals(
                DecodeIssueKind.MALFORMED_BYTES, findIssue(row.decodeIssues, "state_key").kind);
    }

    @Test
    void invalidMapSeparatorAndLengthSuffixAreMalformedBytes() throws Exception {
        StateInspectSchema separatorSchema =
                StateInspectSchema.forMap(
                        "bad-map-separator",
                        "cf-bad-map-separator",
                        false,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forMap(
                        StateInspectType.scalar("INT"),
                        StateInspectType.scalar("INT"),
                        StateInspectType.scalar("INT"),
                        StateInspectType.scalar("INT"));
        byte[] invalidSeparator = mapKey(1, 2, 3);
        invalidSeparator[Integer.BYTES * 2] = 1;

        StateInspectDecoder.DecodedRow separatorRow =
                decode(separatorSchema, semanticSchema, invalidSeparator, new byte[] {1});

        assertEquals(
                DecodeIssueKind.MALFORMED_BYTES, findIssue(separatorRow.decodeIssues, "row").kind);

        StateInspectSchema suffixSchema =
                StateInspectSchema.forMap(
                        "bad-map-suffix",
                        "cf-bad-map-suffix",
                        false,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        setField(suffixSchema, "mapKeyLengthStored", true);
        byte[] invalidSuffix = mapKey(1, 2, 3);
        byte[] withSuffix = Arrays.copyOf(invalidSuffix, invalidSuffix.length + Integer.BYTES);
        Arrays.fill(withSuffix, invalidSuffix.length, withSuffix.length, (byte) 0xFF);

        StateInspectDecoder.DecodedRow suffixRow =
                decode(suffixSchema, semanticSchema, withSuffix, new byte[] {1});

        assertEquals(
                DecodeIssueKind.MALFORMED_BYTES, findIssue(suffixRow.decodeIssues, "row").kind);
    }

    @Test
    void portableSnapshotChildEofIsMalformedBytes() throws Exception {
        StateInspectDecoder.DecodedRow row =
                decodePortableSnapshotChild(
                        portableChildDescriptor(IntSerializer.INSTANCE.snapshotConfiguration()),
                        new byte[] {0x02, 0x00});

        assertEquals(DecodeIssueKind.MALFORMED_BYTES, findIssue(row.decodeIssues, "value").kind);
    }

    @Test
    void portableSnapshotChildOtherLinkageErrorsRemainUnknown() throws Exception {
        for (PortableLinkageFailure failure : PortableLinkageFailure.values()) {
            StateInspectDecoder.DecodedRow row =
                    decodePortableSnapshotChild(
                            portableChildDescriptor(new PortableLinkageSnapshot(failure)),
                            new byte[] {0x02, 0x00});

            assertEquals(
                    DecodeIssueKind.UNKNOWN,
                    findIssue(row.decodeIssues, "value").kind,
                    failure.name());
        }
    }

    // ---- Helpers ----

    private static StateInspectDecoder.DecodedRow decode(
            StateInspectSchema schema,
            StateInspectSemanticSchema semanticSchema,
            byte[] rowKey,
            byte[] valueColumn) {
        InspectTarget target =
                new InspectTarget(
                        schema.stateName(),
                        schema.stateName(),
                        "state",
                        schema.columnFamily(),
                        false,
                        schema.stateKind().name(),
                        Collections.emptyMap(),
                        schema,
                        semanticSchema,
                        null);
        return StateInspectDecoder.decode(target, rowKey, new byte[][] {valueColumn});
    }

    private static StateInspectDecoder.DecodedRow decodeWithBlockedClass(
            StateInspectSchema schema,
            StateInspectSemanticSchema semanticSchema,
            byte[] rowKey,
            byte[] valueColumn,
            String blockedClassName) {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader blockingLoader =
                new ClassLoader(ClassLoader.getSystemClassLoader()) {
                    @Override
                    public Class<?> loadClass(String name, boolean resolve)
                            throws ClassNotFoundException {
                        if (name.contains(blockedClassName)) {
                            throw new ClassNotFoundException("Blocked: " + name);
                        }
                        return super.loadClass(name, resolve);
                    }
                };
        InspectTarget target =
                new InspectTarget(
                        schema.stateName(),
                        schema.stateName(),
                        "state",
                        schema.columnFamily(),
                        false,
                        schema.stateKind().name(),
                        Collections.emptyMap(),
                        schema,
                        semanticSchema,
                        null);
        Thread.currentThread().setContextClassLoader(blockingLoader);
        try {
            return StateInspectDecoder.decode(target, rowKey, new byte[][] {valueColumn});
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private static StateInspectDecoder.DecodedRow decodeTimerWithBlockedClass(
            StateInspectSchema schema,
            StateInspectSemanticSchema semanticSchema,
            byte[] rowKey,
            String blockedClassName) {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader blockingLoader =
                new ClassLoader(ClassLoader.getSystemClassLoader()) {
                    @Override
                    public Class<?> loadClass(String name, boolean resolve)
                            throws ClassNotFoundException {
                        if (name.contains(blockedClassName)) {
                            throw new ClassNotFoundException("Blocked: " + name);
                        }
                        return super.loadClass(name, resolve);
                    }
                };
        InspectTarget target =
                new InspectTarget(
                        "timer:" + schema.stateName(),
                        schema.stateName(),
                        "timer",
                        schema.columnFamily(),
                        false,
                        schema.stateKind().name(),
                        Collections.emptyMap(),
                        schema,
                        semanticSchema,
                        null);
        Thread.currentThread().setContextClassLoader(blockingLoader);
        try {
            return StateInspectDecoder.decode(target, rowKey, new byte[][] {});
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private static StateInspectDecoder.DecodeIssue findIssue(
            List<StateInspectDecoder.DecodeIssue> issues, String part) {
        for (StateInspectDecoder.DecodeIssue issue : issues) {
            if (part.equals(issue.part)) {
                return issue;
            }
        }
        return null;
    }

    private static String issuesToString(List<StateInspectDecoder.DecodeIssue> issues) {
        StringBuilder sb = new StringBuilder();
        for (StateInspectDecoder.DecodeIssue issue : issues) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(issue.part).append("=").append(issue.kind).append(": ").append(issue.message);
        }
        return sb.toString();
    }

    private static <T> byte[] serialize(TypeSerializer<T> serializer, T value) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);
        return out.getCopyOfBuffer();
    }

    private static byte[] keyWithVoidNamespace(byte[] stateKeyBytes) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(stateKeyBytes.length + 1);
        output.write(stateKeyBytes);
        output.writeByte(0);
        return output.getCopyOfBuffer();
    }

    private static byte[] mapKey(int stateKey, int namespace, int mapKey) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(16);
        IntSerializer.INSTANCE.serialize(stateKey, output);
        IntSerializer.INSTANCE.serialize(namespace, output);
        output.writeByte(0);
        IntSerializer.INSTANCE.serialize(mapKey, output);
        return output.getCopyOfBuffer();
    }

    private static StateInspectDecoder.DecodedRow decodePortableSnapshotChild(
            InspectDecoderDescriptor descriptor, byte[] valueBytes) throws Exception {
        StateInspectSchema schema =
                StateInspectSchema.forValue(
                        "portable-child",
                        "cf-portable-child",
                        false,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        setField(schema, "valueSerializer", serializerSchema("portable-child", -1, descriptor));
        StateInspectSemanticSchema semanticSchema =
                StateInspectSemanticSchema.forValue(
                        StateInspectType.scalar("INT"),
                        StateInspectType.unknown(),
                        StateInspectType.row(
                                Collections.singletonList(
                                        new StateInspectField(
                                                "id", StateInspectType.scalar("INT")))));
        return decode(
                schema,
                semanticSchema,
                keyWithVoidNamespace(serialize(IntSerializer.INSTANCE, 1)),
                valueBytes);
    }

    private static InspectDecoderDescriptor portableChildDescriptor(
            TypeSerializerSnapshot<?> snapshot) throws IOException {
        DataOutputSerializer bytes = new DataOutputSerializer(128);
        TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(bytes, snapshot);
        InspectDecoderDescriptor child =
                InspectDecoderDescriptor.portableSnapshot(
                        snapshot.getClass().getName(), bytes.getCopyOfBuffer());
        return InspectDecoderDescriptor.pojo(
                "portable-child-parent",
                Collections.singletonList(
                        new InspectDecoderDescriptor.PojoFieldDescriptor("id", child)),
                Collections.emptyList(),
                false,
                true,
                DescriptorCapability.FULLY_CLASSLESS);
    }

    private static byte[] timerKeyWithVoidNamespace(long timestamp, byte[] keyBytes)
            throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputSerializer output = new DataOutputSerializer(32);
        output.writeLong(MathUtils.flipSignBit(timestamp));
        output.write(keyBytes);
        output.writeByte(0); // void namespace marker
        return output.getCopyOfBuffer();
    }

    private static byte[] buildListPayload(byte[]... elements) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] element : elements) {
            out.write(element);
            out.write(',');
        }
        return out.toByteArray();
    }

    /**
     * Constructs a {@link SerializerInspectSchema} with the given serializer class name and no
     * serialized snapshot bytes. The {@code restoreSerializer} call will return null because the
     * class does not exist, producing a SERIALIZER_RESTORE_FAILED issue.
     */
    @SuppressWarnings("unchecked")
    private static SerializerInspectSchema serializerSchema(String className) throws Exception {
        return serializerSchema(className, -1);
    }

    @SuppressWarnings("unchecked")
    private static SerializerInspectSchema serializerSchema(String className, int lengthTag)
            throws Exception {
        return serializerSchema(className, lengthTag, null);
    }

    @SuppressWarnings("unchecked")
    private static SerializerInspectSchema serializerSchema(
            String className, int lengthTag, InspectDecoderDescriptor descriptor) throws Exception {
        Constructor<SerializerInspectSchema> constructor =
                SerializerInspectSchema.class.getDeclaredConstructor(
                        String.class,
                        int.class,
                        String.class,
                        byte[].class,
                        StateInspectType.class,
                        byte[].class,
                        io.cobble.flink.common.inspect.InspectDecoderDescriptor.class);
        constructor.setAccessible(true);
        return constructor.newInstance(className, lengthTag, null, null, null, null, descriptor);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        java.lang.reflect.Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static final String VOID_NAMESPACE_SERIALIZER_CLASS =
            "org.apache.flink.runtime.state.VoidNamespaceSerializer";

    private static class LinkageOnDeserializeSerializer extends TypeSerializer<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<String> duplicate() {
            return this;
        }

        @Override
        public String createInstance() {
            return "";
        }

        @Override
        public String copy(String from) {
            return from;
        }

        @Override
        public String copy(String from, String reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(String record, DataOutputView target) throws IOException {
            StringSerializer.INSTANCE.serialize(record, target);
        }

        @Override
        public String deserialize(DataInputView source) {
            throw new NoClassDefFoundError("missing/DependencyCodec");
        }

        @Override
        public String deserialize(String reuse, DataInputView source) {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            StringSerializer.INSTANCE.copy(source, target);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof LinkageOnDeserializeSerializer;
        }

        @Override
        public int hashCode() {
            return LinkageOnDeserializeSerializer.class.hashCode();
        }

        @Override
        public TypeSerializerSnapshot<String> snapshotConfiguration() {
            return new LinkageOnDeserializeSnapshot();
        }
    }

    public static class LinkageOnDeserializeSnapshot implements TypeSerializerSnapshot<String> {
        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {}

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {}

        @Override
        public TypeSerializer<String> restoreSerializer() {
            return new LinkageOnDeserializeSerializer();
        }

        @Override
        public TypeSerializerSchemaCompatibility<String> resolveSchemaCompatibility(
                TypeSerializer<String> newSerializer) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }
    }

    private static final class OtherLinkageOnDeserializeSerializer
            extends LinkageOnDeserializeSerializer {
        private static final long serialVersionUID = 1L;

        @Override
        public String deserialize(DataInputView source) {
            throw new UnsatisfiedLinkError("native codec unavailable");
        }

        @Override
        public TypeSerializerSnapshot<String> snapshotConfiguration() {
            return new OtherLinkageOnDeserializeSnapshot();
        }
    }

    public static final class OtherLinkageOnDeserializeSnapshot
            extends LinkageOnDeserializeSnapshot {
        @Override
        public TypeSerializer<String> restoreSerializer() {
            return new OtherLinkageOnDeserializeSerializer();
        }
    }

    private static final class RuntimeOnDeserializeSerializer
            extends LinkageOnDeserializeSerializer {
        private static final long serialVersionUID = 1L;

        @Override
        public String deserialize(DataInputView source) {
            throw new IllegalStateException("application serializer failure");
        }

        @Override
        public TypeSerializerSnapshot<String> snapshotConfiguration() {
            return new RuntimeOnDeserializeSnapshot();
        }
    }

    public static final class RuntimeOnDeserializeSnapshot extends LinkageOnDeserializeSnapshot {
        @Override
        public TypeSerializer<String> restoreSerializer() {
            return new RuntimeOnDeserializeSerializer();
        }
    }

    private enum PortableLinkageFailure {
        VERIFY,
        UNSUPPORTED_CLASS_VERSION,
        NO_SUCH_METHOD
    }

    public static final class PortableLinkageSnapshot extends LinkageOnDeserializeSnapshot {
        private PortableLinkageFailure failure;

        public PortableLinkageSnapshot() {}

        PortableLinkageSnapshot(PortableLinkageFailure failure) {
            this.failure = failure;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeInt(failure.ordinal());
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            failure = PortableLinkageFailure.values()[in.readInt()];
        }

        @Override
        public TypeSerializer<String> restoreSerializer() {
            return new PortableLinkageSerializer(failure);
        }
    }

    private static final class PortableLinkageSerializer extends LinkageOnDeserializeSerializer {
        private static final long serialVersionUID = 1L;

        private final PortableLinkageFailure failure;

        PortableLinkageSerializer(PortableLinkageFailure failure) {
            this.failure = failure;
        }

        @Override
        public String deserialize(DataInputView source) {
            switch (failure) {
                case VERIFY:
                    throw new VerifyError("portable verify failure");
                case UNSUPPORTED_CLASS_VERSION:
                    throw new UnsupportedClassVersionError("portable version failure");
                case NO_SUCH_METHOD:
                    throw new NoSuchMethodError("portable method failure");
                default:
                    throw new AssertionError(failure);
            }
        }

        @Override
        public TypeSerializerSnapshot<String> snapshotConfiguration() {
            return new PortableLinkageSnapshot(failure);
        }
    }
}
