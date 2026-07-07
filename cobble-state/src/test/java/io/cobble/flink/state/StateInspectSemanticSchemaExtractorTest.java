package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.cobble.flink.common.inspect.SerializerInspectSchema;
import io.cobble.flink.common.inspect.StateInspectSemanticSchema;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.StateInspectTypeKind;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Tests SQL type extraction without requiring a running Table planner. */
class StateInspectSemanticSchemaExtractorTest {

    private static final ExecutionConfig CONFIG = new ExecutionConfig();

    @Test
    void valueStatePreservesNamedRowDataAndUsesUnnamedStateKeyFields() {
        InternalTypeInfo<RowData> recordType = namedRecordType();
        InternalTypeInfo<RowData> stateKeyType =
                InternalTypeInfo.of(RowType.of(new LogicalType[] {new BigIntType(false)}));

        ValueStateDescriptor<RowData> desc = new ValueStateDescriptor<>("left-records", recordType);
        desc.initializeSerializerUnlessSet(CONFIG);
        StateInspectSemanticSchema schema =
                StateInspectSemanticSchemaExtractor.forValue(
                        schemaFrom(stateKeyType.createSerializer(CONFIG)),
                        schemaFrom(VoidNamespaceSerializer.INSTANCE),
                        schemaFrom(desc.getSerializer()),
                        desc);

        assertRow(schema.stateKey(), "f0");
        assertRow(schema.value(), "order_id", "region");
        assertEquals("BIGINT NOT NULL", schema.stateKey().fields().get(0).type().logicalType());
        assertEquals("VARCHAR(2147483647)", schema.value().fields().get(1).type().logicalType());
    }

    @Test
    void mapStateUnwrapsUniqueKeyAndOuterJoinTupleValue() {
        InternalTypeInfo<RowData> recordType = namedRecordType();
        InternalTypeInfo<RowData> uniqueKeyType =
                InternalTypeInfo.of(RowType.of(new LogicalType[] {new BigIntType(false)}));
        TupleTypeInfo<Tuple2<RowData, Integer>> outerValueType =
                new TupleTypeInfo<>(recordType, Types.INT);

        MapStateDescriptor<RowData, Tuple2<RowData, Integer>> desc =
                new MapStateDescriptor<>("right-records", uniqueKeyType, outerValueType);
        desc.initializeSerializerUnlessSet(CONFIG);
        StateInspectSemanticSchema schema =
                StateInspectSemanticSchemaExtractor.forMap(
                        schemaFrom(uniqueKeyType.createSerializer(CONFIG)),
                        schemaFrom(VoidNamespaceSerializer.INSTANCE),
                        schemaFrom(desc.getKeySerializer()),
                        schemaFrom(desc.getValueSerializer()),
                        desc);

        assertRow(schema.stateKey(), "f0");
        assertRow(schema.mapUserKey(), "f0");
        assertEquals(StateInspectTypeKind.TUPLE, schema.mapUserValue().kind());
        assertEquals("f0", schema.mapUserValue().fields().get(0).name());
        // Nested ROW inside a TUPLE gets field names from the descriptor overlay.
        assertRow(schema.mapUserValue().fields().get(0).type(), "order_id", "region");
        assertEquals("INT", schema.mapUserValue().fields().get(1).type().logicalType());
    }

    @Test
    void listStateUnwrapsNamedRowElement() {
        InternalTypeInfo<RowData> recordType = namedRecordType();

        ListStateDescriptor<RowData> desc =
                new ListStateDescriptor<>("left-window-records", recordType);
        desc.initializeSerializerUnlessSet(CONFIG);
        StateInspectSemanticSchema schema =
                StateInspectSemanticSchemaExtractor.forList(
                        schemaFrom(Types.LONG.createSerializer(CONFIG)),
                        schemaFrom(VoidNamespaceSerializer.INSTANCE),
                        schemaFrom(desc.getElementSerializer()),
                        desc);

        assertEquals(StateInspectTypeKind.SCALAR, schema.stateKey().kind());
        assertEquals("BIGINT", schema.stateKey().logicalType());
        assertRow(schema.listElement(), "order_id", "region");
    }

    @Test
    void serializerOnlyRowDataFallsBackToOrdinalFieldNamesAndUnknownSerializerIsSafe() {
        RowDataSerializer rowSerializer =
                new RowDataSerializer(new BigIntType(false), VarCharType.STRING_TYPE);
        ValueStateDescriptor<RowData> rowDesc =
                new ValueStateDescriptor<>("window-aggs", rowSerializer);
        rowDesc.initializeSerializerUnlessSet(CONFIG);
        StateInspectSemanticSchema serializerOnlySchema =
                StateInspectSemanticSchemaExtractor.forValue(
                        schemaFrom(Types.LONG.createSerializer(CONFIG)),
                        schemaFrom(VoidNamespaceSerializer.INSTANCE),
                        schemaFrom(rowDesc.getSerializer()),
                        rowDesc);

        assertRow(serializerOnlySchema.value(), "f0", "f1");
        assertEquals(
                "BIGINT NOT NULL",
                serializerOnlySchema.value().fields().get(0).type().logicalType());

        ValueStateDescriptor<org.apache.flink.runtime.state.VoidNamespace> customDesc =
                new ValueStateDescriptor<>("custom", VoidNamespaceSerializer.INSTANCE);
        customDesc.initializeSerializerUnlessSet(CONFIG);
        StateInspectSemanticSchema unknownSchema =
                StateInspectSemanticSchemaExtractor.forValue(
                        schemaFrom(Types.LONG.createSerializer(CONFIG)),
                        schemaFrom(VoidNamespaceSerializer.INSTANCE),
                        schemaFrom(customDesc.getSerializer()),
                        customDesc);

        assertEquals(StateInspectTypeKind.UNKNOWN, unknownSchema.value().kind());
        assertFalse(unknownSchema.isEmpty());
    }

    @Test
    void capturesDeduplicateSortTopNAndTemporalJoinStateShapes() {
        InternalTypeInfo<RowData> recordType = namedRecordType();
        InternalTypeInfo<RowData> sortKeyType =
                InternalTypeInfo.of(
                        RowType.of(
                                new LogicalType[] {new BigIntType(false)},
                                new String[] {"sort_key"}));
        ListTypeInfo<RowData> recordListType = new ListTypeInfo<>(recordType);

        ValueStateDescriptor<RowData> dedupDesc =
                new ValueStateDescriptor<>("deduplicate-state", recordType);
        dedupDesc.initializeSerializerUnlessSet(CONFIG);
        StateInspectSemanticSchema deduplicate =
                StateInspectSemanticSchemaExtractor.forValue(
                        schemaFrom(Types.LONG.createSerializer(CONFIG)),
                        schemaFrom(VoidNamespaceSerializer.INSTANCE),
                        schemaFrom(dedupDesc.getSerializer()),
                        dedupDesc);

        ListStateDescriptor<RowData> sortDesc = new ListStateDescriptor<>("sortState", recordType);
        sortDesc.initializeSerializerUnlessSet(CONFIG);
        StateInspectSemanticSchema processTimeSort =
                StateInspectSemanticSchemaExtractor.forList(
                        schemaFrom(Types.LONG.createSerializer(CONFIG)),
                        schemaFrom(VoidNamespaceSerializer.INSTANCE),
                        schemaFrom(sortDesc.getElementSerializer()),
                        sortDesc);

        MapStateDescriptor<RowData, List<RowData>> topNDesc =
                new MapStateDescriptor<>("data-state-with-append", sortKeyType, recordListType);
        topNDesc.initializeSerializerUnlessSet(CONFIG);
        StateInspectSemanticSchema topN =
                StateInspectSemanticSchemaExtractor.forMap(
                        schemaFrom(Types.LONG.createSerializer(CONFIG)),
                        schemaFrom(VoidNamespaceSerializer.INSTANCE),
                        schemaFrom(topNDesc.getKeySerializer()),
                        schemaFrom(topNDesc.getValueSerializer()),
                        topNDesc);

        MapStateDescriptor<Long, RowData> temporalJoinDesc =
                new MapStateDescriptor<>("left", Types.LONG, recordType);
        temporalJoinDesc.initializeSerializerUnlessSet(CONFIG);
        StateInspectSemanticSchema temporalJoin =
                StateInspectSemanticSchemaExtractor.forMap(
                        schemaFrom(Types.LONG.createSerializer(CONFIG)),
                        schemaFrom(VoidNamespaceSerializer.INSTANCE),
                        schemaFrom(temporalJoinDesc.getKeySerializer()),
                        schemaFrom(temporalJoinDesc.getValueSerializer()),
                        temporalJoinDesc);

        assertRow(deduplicate.value(), "order_id", "region");
        assertRow(processTimeSort.listElement(), "order_id", "region");
        assertRow(topN.mapUserKey(), "sort_key");
        assertEquals(StateInspectTypeKind.LIST, topN.mapUserValue().kind());
        // Nested ROW inside a LIST gets field names from the recursive descriptor overlay.
        assertRow(topN.mapUserValue().elementType(), "order_id", "region");
        assertEquals(StateInspectTypeKind.SCALAR, temporalJoin.mapUserKey().kind());
        assertEquals("BIGINT", temporalJoin.mapUserKey().logicalType());
        assertRow(temporalJoin.mapUserValue(), "order_id", "region");
    }

    @Test
    void capturesIntervalJoinAndAggregateFallbackStateShapes() {
        InternalTypeInfo<RowData> recordType = namedRecordType();
        TupleTypeInfo<Tuple2<RowData, Boolean>> intervalEntryType =
                new TupleTypeInfo<>(recordType, Types.BOOLEAN);
        ListTypeInfo<Tuple2<RowData, Boolean>> intervalEntryListType =
                new ListTypeInfo<>(intervalEntryType);
        InternalTypeInfo<RowData> accumulatorType =
                InternalTypeInfo.ofFields(new BigIntType(false), VarCharType.STRING_TYPE);
        RowDataSerializer windowAccumulatorSerializer =
                new RowDataSerializer(new BigIntType(false), VarCharType.STRING_TYPE);

        MapStateDescriptor<Long, List<Tuple2<RowData, Boolean>>> intervalJoinDesc =
                new MapStateDescriptor<>(
                        "IntervalJoinLeftCache", Types.LONG, intervalEntryListType);
        intervalJoinDesc.initializeSerializerUnlessSet(CONFIG);
        StateInspectSemanticSchema intervalJoin =
                StateInspectSemanticSchemaExtractor.forMap(
                        schemaFrom(Types.LONG.createSerializer(CONFIG)),
                        schemaFrom(VoidNamespaceSerializer.INSTANCE),
                        schemaFrom(intervalJoinDesc.getKeySerializer()),
                        schemaFrom(intervalJoinDesc.getValueSerializer()),
                        intervalJoinDesc);

        ValueStateDescriptor<RowData> aggDesc =
                new ValueStateDescriptor<>("accState", accumulatorType);
        aggDesc.initializeSerializerUnlessSet(CONFIG);
        StateInspectSemanticSchema aggregate =
                StateInspectSemanticSchemaExtractor.forValue(
                        schemaFrom(Types.LONG.createSerializer(CONFIG)),
                        schemaFrom(VoidNamespaceSerializer.INSTANCE),
                        schemaFrom(aggDesc.getSerializer()),
                        aggDesc);

        ListTypeInfo<RowData> overInputListType =
                new ListTypeInfo<>(
                        InternalTypeInfo.ofFields(new BigIntType(false), VarCharType.STRING_TYPE));
        MapStateDescriptor<Long, List<RowData>> overInputDesc =
                new MapStateDescriptor<>("inputState", Types.LONG, overInputListType);
        overInputDesc.initializeSerializerUnlessSet(CONFIG);
        StateInspectSemanticSchema overInput =
                StateInspectSemanticSchemaExtractor.forMap(
                        schemaFrom(Types.LONG.createSerializer(CONFIG)),
                        schemaFrom(VoidNamespaceSerializer.INSTANCE),
                        schemaFrom(overInputDesc.getKeySerializer()),
                        schemaFrom(overInputDesc.getValueSerializer()),
                        overInputDesc);

        ListStateDescriptor<RowData> windowJoinDesc =
                new ListStateDescriptor<>(
                        "left-records",
                        new RowDataSerializer(new BigIntType(false), VarCharType.STRING_TYPE));
        windowJoinDesc.initializeSerializerUnlessSet(CONFIG);
        StateInspectSemanticSchema windowJoin =
                StateInspectSemanticSchemaExtractor.forList(
                        schemaFrom(Types.LONG.createSerializer(CONFIG)),
                        schemaFrom(VoidNamespaceSerializer.INSTANCE),
                        schemaFrom(windowJoinDesc.getElementSerializer()),
                        windowJoinDesc);

        ValueStateDescriptor<RowData> windowAggDesc =
                new ValueStateDescriptor<>("window-aggs", windowAccumulatorSerializer);
        windowAggDesc.initializeSerializerUnlessSet(CONFIG);
        StateInspectSemanticSchema windowAggregate =
                StateInspectSemanticSchemaExtractor.forValue(
                        schemaFrom(Types.LONG.createSerializer(CONFIG)),
                        schemaFrom(VoidNamespaceSerializer.INSTANCE),
                        schemaFrom(windowAggDesc.getSerializer()),
                        windowAggDesc);

        assertEquals(StateInspectTypeKind.LIST, intervalJoin.mapUserValue().kind());
        StateInspectType intervalEntry = intervalJoin.mapUserValue().elementType();
        assertEquals(StateInspectTypeKind.TUPLE, intervalEntry.kind());
        // Nested ROW inside a TUPLE inside a LIST gets field names from the recursive overlay.
        assertRow(intervalEntry.fields().get(0).type(), "order_id", "region");
        assertEquals("BOOLEAN", intervalEntry.fields().get(1).type().logicalType());
        assertRow(aggregate.value(), "f0", "f1");
        assertEquals(StateInspectTypeKind.LIST, overInput.mapUserValue().kind());
        // overInput uses InternalTypeInfo.ofFields (unnamed) → fields stay f0/f1.
        assertRow(overInput.mapUserValue().elementType(), "f0", "f1");
        // windowJoin descriptor built from raw serializer → no TypeInfo → no overlay.
        assertRow(windowJoin.listElement(), "f0", "f1");
        // windowAggregate descriptor built from raw serializer → no TypeInfo → no overlay.
        assertRow(windowAggregate.value(), "f0", "f1");
    }

    @Test
    void timerSchemaPreservesTypedKeyAndNamespace() {
        StateInspectSemanticSchema timer =
                StateInspectSemanticSchemaExtractor.forTimer(
                        schemaFrom(
                                new RowDataSerializer(
                                        new BigIntType(false), VarCharType.STRING_TYPE)),
                        schemaFrom(Types.INT.createSerializer(CONFIG)));

        assertRow(timer.stateKey(), "f0", "f1");
        assertEquals("INT", timer.namespace().logicalType());
        assertEquals(StateInspectTypeKind.UNKNOWN, timer.value().kind());
    }

    private static SerializerInspectSchema schemaFrom(
            org.apache.flink.api.common.typeutils.TypeSerializer<?> serializer) {
        return SerializerInspectSchema.fromSerializer(serializer);
    }

    private static InternalTypeInfo<RowData> namedRecordType() {
        return InternalTypeInfo.of(
                RowType.of(
                        new LogicalType[] {new BigIntType(false), VarCharType.STRING_TYPE},
                        new String[] {"order_id", "region"}));
    }

    private static void assertRow(StateInspectType type, String... fieldNames) {
        assertEquals(StateInspectTypeKind.ROW, type.kind());
        assertEquals(fieldNames.length, type.fields().size());
        for (int i = 0; i < fieldNames.length; i++) {
            assertEquals(fieldNames[i], type.fields().get(i).name());
        }
    }
}
