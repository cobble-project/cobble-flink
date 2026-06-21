package io.cobble.flink.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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

/** Tests SQL type extraction without requiring a running Table planner. */
class StateInspectSemanticSchemaExtractorTest {

    @Test
    void valueStatePreservesNamedRowDataAndUsesUnnamedStateKeyFields() {
        InternalTypeInfo<RowData> recordType = namedRecordType();
        InternalTypeInfo<RowData> stateKeyType =
                InternalTypeInfo.of(RowType.of(new LogicalType[] {new BigIntType(false)}));

        StateInspectSemanticSchema schema =
                StateInspectSemanticSchemaExtractor.forValue(
                        stateKeyType.createSerializer(new ExecutionConfig()),
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("left-records", recordType));

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

        StateInspectSemanticSchema schema =
                StateInspectSemanticSchemaExtractor.forMap(
                        uniqueKeyType.createSerializer(new ExecutionConfig()),
                        VoidNamespaceSerializer.INSTANCE,
                        new MapStateDescriptor<>("right-records", uniqueKeyType, outerValueType));

        assertRow(schema.stateKey(), "f0");
        assertRow(schema.mapUserKey(), "f0");
        assertEquals(StateInspectTypeKind.TUPLE, schema.mapUserValue().kind());
        assertEquals("f0", schema.mapUserValue().fields().get(0).name());
        assertRow(schema.mapUserValue().fields().get(0).type(), "order_id", "region");
        assertEquals("INT", schema.mapUserValue().fields().get(1).type().logicalType());
    }

    @Test
    void listStateUnwrapsNamedRowElement() {
        InternalTypeInfo<RowData> recordType = namedRecordType();

        StateInspectSemanticSchema schema =
                StateInspectSemanticSchemaExtractor.forList(
                        Types.LONG.createSerializer(new ExecutionConfig()),
                        VoidNamespaceSerializer.INSTANCE,
                        new ListStateDescriptor<>("left-window-records", recordType));

        assertEquals(StateInspectTypeKind.SCALAR, schema.stateKey().kind());
        assertEquals("BIGINT", schema.stateKey().logicalType());
        assertRow(schema.listElement(), "order_id", "region");
    }

    @Test
    void serializerOnlyRowDataFallsBackToOrdinalFieldNamesAndUnknownSerializerIsSafe() {
        RowDataSerializer rowSerializer =
                new RowDataSerializer(new BigIntType(false), VarCharType.STRING_TYPE);
        StateInspectSemanticSchema serializerOnlySchema =
                StateInspectSemanticSchemaExtractor.forValue(
                        Types.LONG.createSerializer(new ExecutionConfig()),
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("window-aggs", rowSerializer));

        assertRow(serializerOnlySchema.value(), "f0", "f1");
        assertEquals(
                "BIGINT NOT NULL",
                serializerOnlySchema.value().fields().get(0).type().logicalType());

        StateInspectSemanticSchema unknownSchema =
                StateInspectSemanticSchemaExtractor.forValue(
                        Types.LONG.createSerializer(new ExecutionConfig()),
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("custom", VoidNamespaceSerializer.INSTANCE));

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

        StateInspectSemanticSchema deduplicate =
                StateInspectSemanticSchemaExtractor.forValue(
                        Types.LONG.createSerializer(new ExecutionConfig()),
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("deduplicate-state", recordType));
        StateInspectSemanticSchema processTimeSort =
                StateInspectSemanticSchemaExtractor.forList(
                        Types.LONG.createSerializer(new ExecutionConfig()),
                        VoidNamespaceSerializer.INSTANCE,
                        new ListStateDescriptor<>("sortState", recordType));
        StateInspectSemanticSchema topN =
                StateInspectSemanticSchemaExtractor.forMap(
                        Types.LONG.createSerializer(new ExecutionConfig()),
                        VoidNamespaceSerializer.INSTANCE,
                        new MapStateDescriptor<>(
                                "data-state-with-append", sortKeyType, recordListType));
        StateInspectSemanticSchema temporalJoin =
                StateInspectSemanticSchemaExtractor.forMap(
                        Types.LONG.createSerializer(new ExecutionConfig()),
                        VoidNamespaceSerializer.INSTANCE,
                        new MapStateDescriptor<>("left", Types.LONG, recordType));

        assertRow(deduplicate.value(), "order_id", "region");
        assertRow(processTimeSort.listElement(), "order_id", "region");
        assertRow(topN.mapUserKey(), "sort_key");
        assertEquals(StateInspectTypeKind.LIST, topN.mapUserValue().kind());
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

        StateInspectSemanticSchema intervalJoin =
                StateInspectSemanticSchemaExtractor.forMap(
                        Types.LONG.createSerializer(new ExecutionConfig()),
                        VoidNamespaceSerializer.INSTANCE,
                        new MapStateDescriptor<>(
                                "IntervalJoinLeftCache", Types.LONG, intervalEntryListType));
        StateInspectSemanticSchema aggregate =
                StateInspectSemanticSchemaExtractor.forValue(
                        Types.LONG.createSerializer(new ExecutionConfig()),
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("accState", accumulatorType));
        StateInspectSemanticSchema overInput =
                StateInspectSemanticSchemaExtractor.forMap(
                        Types.LONG.createSerializer(new ExecutionConfig()),
                        VoidNamespaceSerializer.INSTANCE,
                        new MapStateDescriptor<>(
                                "inputState",
                                Types.LONG,
                                new ListTypeInfo<>(
                                        InternalTypeInfo.ofFields(
                                                new BigIntType(false), VarCharType.STRING_TYPE))));
        StateInspectSemanticSchema windowJoin =
                StateInspectSemanticSchemaExtractor.forList(
                        Types.LONG.createSerializer(new ExecutionConfig()),
                        VoidNamespaceSerializer.INSTANCE,
                        new ListStateDescriptor<>(
                                "left-records",
                                new RowDataSerializer(
                                        new BigIntType(false), VarCharType.STRING_TYPE)));
        StateInspectSemanticSchema windowAggregate =
                StateInspectSemanticSchemaExtractor.forValue(
                        Types.LONG.createSerializer(new ExecutionConfig()),
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("window-aggs", windowAccumulatorSerializer));

        assertEquals(StateInspectTypeKind.LIST, intervalJoin.mapUserValue().kind());
        StateInspectType intervalEntry = intervalJoin.mapUserValue().elementType();
        assertEquals(StateInspectTypeKind.TUPLE, intervalEntry.kind());
        assertRow(intervalEntry.fields().get(0).type(), "order_id", "region");
        assertEquals("BOOLEAN", intervalEntry.fields().get(1).type().logicalType());
        assertRow(aggregate.value(), "f0", "f1");
        assertEquals(StateInspectTypeKind.LIST, overInput.mapUserValue().kind());
        assertRow(overInput.mapUserValue().elementType(), "f0", "f1");
        assertRow(windowJoin.listElement(), "f0", "f1");
        assertRow(windowAggregate.value(), "f0", "f1");
    }

    @Test
    void timerSchemaPreservesTypedKeyAndNamespace() {
        StateInspectSemanticSchema timer =
                StateInspectSemanticSchemaExtractor.forTimer(
                        new RowDataSerializer(new BigIntType(false), VarCharType.STRING_TYPE),
                        Types.INT.createSerializer(new ExecutionConfig()));

        assertRow(timer.stateKey(), "f0", "f1");
        assertEquals("INT", timer.namespace().logicalType());
        assertEquals(StateInspectTypeKind.UNKNOWN, timer.value().kind());
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
