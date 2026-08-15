package io.cobble.flink.state;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** End-to-end proof that Flink operators route async state requests through Cobble. */
class CobbleAsyncStateMiniClusterITTest {

    @Test
    void executesAsyncValueAndMapState(@TempDir Path tempDirectory) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(
                CobbleOptions.LOCAL_DIRECTORIES, tempDirectory.resolve("local-state").toString());
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                tempDirectory.resolve("checkpoints").toUri().toString());
        configuration.set(
                StateBackendOptions.STATE_BACKEND, CobbleStateBackendFactory.class.getName());

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setParallelism(1);

        KeyedStream<Integer, Integer> keyed =
                env.fromData(1, 1, 1).keyBy(value -> value, BasicTypeInfo.INT_TYPE_INFO);
        keyed.enableAsyncState();
        DataStream<String> result = keyed.flatMap(new AsyncCountAndMapFunction());

        assertThat(result.executeAndCollect(3))
                .containsExactlyInAnyOrder("count=1,map=1", "count=2,map=2", "count=3,map=3");
    }

    @Test
    void batchesIndependentValueStateReadsAndWrites(@TempDir Path tempDirectory) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(
                CobbleOptions.LOCAL_DIRECTORIES, tempDirectory.resolve("local-state").toString());
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                tempDirectory.resolve("checkpoints").toUri().toString());
        configuration.set(
                StateBackendOptions.STATE_BACKEND, CobbleStateBackendFactory.class.getName());

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setParallelism(1);

        int keyCount = 2_000;
        List<Integer> input = new ArrayList<>(keyCount * 2);
        for (int pass = 0; pass < 2; pass++) {
            for (int key = 0; key < keyCount; key++) {
                input.add(key);
            }
        }
        KeyedStream<Integer, Integer> keyed =
                env.fromCollection(input).keyBy(value -> value, BasicTypeInfo.INT_TYPE_INFO);
        keyed.enableAsyncState();

        int firstUpdates = 0;
        int secondUpdates = 0;
        try (CloseableIterator<String> results =
                keyed.flatMap(new AsyncValueOnlyFunction()).executeAndCollect()) {
            while (results.hasNext()) {
                String result = results.next();
                if ("count=1".equals(result)) {
                    firstUpdates++;
                } else if ("count=2".equals(result)) {
                    secondUpdates++;
                } else {
                    throw new AssertionError("Unexpected async ValueState result: " + result);
                }
            }
        }

        assertThat(firstUpdates).isEqualTo(keyCount);
        assertThat(secondUpdates).isEqualTo(keyCount);
    }

    @Test
    void streamsLargeValuesAcrossDirectBufferChunks(@TempDir Path tempDirectory) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(
                CobbleOptions.LOCAL_DIRECTORIES, tempDirectory.resolve("local-state").toString());
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                tempDirectory.resolve("checkpoints").toUri().toString());
        configuration.set(
                StateBackendOptions.STATE_BACKEND, CobbleStateBackendFactory.class.getName());

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setParallelism(1);

        int keyCount = 32;
        List<Integer> input = new ArrayList<>(keyCount * 2);
        for (int pass = 0; pass < 2; pass++) {
            for (int key = 0; key < keyCount; key++) {
                input.add(key);
            }
        }
        KeyedStream<Integer, Integer> keyed =
                env.fromCollection(input).keyBy(value -> value, BasicTypeInfo.INT_TYPE_INFO);
        keyed.enableAsyncState();

        int initialized = 0;
        int restored = 0;
        try (CloseableIterator<String> results =
                keyed.flatMap(new AsyncLargeValueFunction()).executeAndCollect()) {
            while (results.hasNext()) {
                String result = results.next();
                if ("initialized".equals(result)) {
                    initialized++;
                } else if ("restored".equals(result)) {
                    restored++;
                } else {
                    throw new AssertionError("Unexpected large ValueState result: " + result);
                }
            }
        }

        assertThat(initialized).isEqualTo(keyCount);
        assertThat(restored).isEqualTo(keyCount);
    }

    private static final class AsyncCountAndMapFunction extends RichFlatMapFunction<Integer, String>
            implements ResultTypeQueryable<String> {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Integer> countState;
        private transient MapState<String, Integer> mapState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
            countState =
                    context.getValueState(
                            new ValueStateDescriptor<>("count", BasicTypeInfo.INT_TYPE_INFO));
            mapState =
                    context.getMapState(
                            new MapStateDescriptor<>(
                                    "map",
                                    BasicTypeInfo.STRING_TYPE_INFO,
                                    BasicTypeInfo.INT_TYPE_INFO));
        }

        @Override
        public void flatMap(Integer value, Collector<String> output) {
            StateFuture<Integer> updatedCount =
                    countState
                            .asyncValue()
                            .thenCompose(
                                    current -> {
                                        int updated = current == null ? 1 : current + 1;
                                        return countState
                                                .asyncUpdate(updated)
                                                .thenApply(ignored -> updated);
                                    });
            updatedCount.thenCompose(
                    updated -> {
                        Map<String, Integer> entries = new LinkedHashMap<>();
                        entries.put("value", updated);
                        entries.put("present-null", null);
                        return mapState.asyncPutAll(entries)
                                .thenCompose(
                                        ignored ->
                                                mapState.asyncGet("value")
                                                        .thenCombine(
                                                                mapState.asyncContains(
                                                                        "present-null"),
                                                                (mapValue, containsNull) -> {
                                                                    if (!containsNull) {
                                                                        throw new AssertionError(
                                                                                "MapState lost its present-null entry.");
                                                                    }
                                                                    output.collect(
                                                                            "count=" + updated
                                                                                    + ",map="
                                                                                    + mapValue);
                                                                    return null;
                                                                }));
                    });
        }

        @Override
        public org.apache.flink.api.common.typeinfo.TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }

    private static final class AsyncValueOnlyFunction extends RichFlatMapFunction<Integer, String>
            implements ResultTypeQueryable<String> {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Integer> countState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
            countState =
                    context.getValueState(
                            new ValueStateDescriptor<>(
                                    "batched-count", BasicTypeInfo.INT_TYPE_INFO));
        }

        @Override
        public void flatMap(Integer value, Collector<String> output) {
            countState
                    .asyncValue()
                    .thenCompose(
                            current -> {
                                int updated = current == null ? 1 : current + 1;
                                return countState
                                        .asyncUpdate(updated)
                                        .thenApply(ignored -> updated);
                            })
                    .thenAccept(updated -> output.collect("count=" + updated));
        }

        @Override
        public org.apache.flink.api.common.typeinfo.TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }

    private static final class AsyncLargeValueFunction extends RichFlatMapFunction<Integer, String>
            implements ResultTypeQueryable<String> {
        private static final long serialVersionUID = 1L;
        private static final int VALUE_BYTES = 80 * 1024;

        private transient ValueState<byte[]> valueState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
            valueState =
                    context.getValueState(
                            new ValueStateDescriptor<>(
                                    "large-value",
                                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        }

        @Override
        public void flatMap(Integer value, Collector<String> output) {
            valueState
                    .asyncValue()
                    .thenAccept(
                            current -> {
                                if (current == null) {
                                    byte[] initialized = new byte[VALUE_BYTES];
                                    initialized[0] = value.byteValue();
                                    valueState
                                            .asyncUpdate(initialized)
                                            .thenAccept(ignored -> output.collect("initialized"));
                                } else {
                                    if (current.length != VALUE_BYTES
                                            || current[0] != value.byteValue()) {
                                        throw new AssertionError(
                                                "Large ValueState payload was corrupted.");
                                    }
                                    output.collect("restored");
                                }
                            });
        }

        @Override
        public org.apache.flink.api.common.typeinfo.TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
