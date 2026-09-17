package io.cobble.flink.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cobble.flink.state.CobbleOptions;
import io.cobble.flink.state.CobbleStateBackend;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/** End-to-end coverage for using the Cobble state backend and Cobble table sink in one job. */
class CobbleStateAndTableSinkWordCountITTest {

    private static final int PARALLELISM = 2;
    private static final long DISTINCT_WORDS = 100_000L;
    private static final long EVENTS_PER_WORD = 10L;
    private static final long TOTAL_EVENTS = DISTINCT_WORDS * EVENTS_PER_WORD;

    @TempDir private Path tempDir;

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void cobbleStateAndTableSinkCoexistAtMeaningfulScale() throws Exception {
        Path stateLocalPath = tempDir.resolve("state-local");
        Path checkpointPath = tempDir.resolve("checkpoints");
        Path tablePath = tempDir.resolve("table");

        Configuration configuration = new Configuration();
        configuration.set(CobbleOptions.LOCAL_DIRECTORIES, stateLocalPath.toString());
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointPath.toUri().toString());

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(PARALLELISM);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.enableCheckpointing(1_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(120_000L);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(
                new CobbleStateBackend().configure(configuration, getClass().getClassLoader()));

        DataStream<Row> counts =
                env.fromSequence(0L, TOTAL_EVENTS - 1L)
                        .name("word-events")
                        .uid("word-events")
                        .map(value -> Long.valueOf(value.longValue() % DISTINCT_WORDS))
                        .returns(Types.LONG)
                        .keyBy(value -> value)
                        .flatMap(new StatefulWordCount())
                        .returns(
                                Types.ROW_NAMED(
                                        new String[] {"word_id", "word", "frequency"},
                                        Types.LONG,
                                        Types.STRING,
                                        Types.LONG))
                        .name("cobble-stateful-word-count")
                        .uid("cobble-stateful-word-count");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporaryView(
                "word_counts",
                tableEnv.fromDataStream(
                        counts,
                        Schema.newBuilder()
                                .column("word_id", DataTypes.BIGINT().notNull())
                                .column("word", DataTypes.STRING().notNull())
                                .column("frequency", DataTypes.BIGINT().notNull())
                                .build()));
        tableEnv.executeSql(
                "CREATE TABLE cobble_word_counts ("
                        + " word_id BIGINT NOT NULL,"
                        + " word STRING NOT NULL,"
                        + " frequency BIGINT NOT NULL,"
                        + " PRIMARY KEY (word_id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'path' = '"
                        + escape(tablePath)
                        + "',"
                        + " 'bucket' = '64',"
                        + " 'sink.parallelism' = '2',"
                        + " 'sink.writer-buffer-memory' = '32mb',"
                        + " 'snapshot.retention' = '2'"
                        + ")");

        JobExecutionResult executionResult =
                awaitJobCompletion(
                        tableEnv.executeSql(
                                "INSERT INTO cobble_word_counts "
                                        + "SELECT word_id, word, frequency FROM word_counts"));
        assertEquals(
                Long.valueOf(DISTINCT_WORDS),
                executionResult.getAccumulatorResult(StatefulWordCount.COMPLETED_WORDS));

        assertTableContents(tablePath);
    }

    private void assertTableContents(Path tablePath) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setRestartStrategy(RestartStrategies.noRestart());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(
                "CREATE TABLE cobble_word_counts ("
                        + " word_id BIGINT NOT NULL,"
                        + " word STRING NOT NULL,"
                        + " frequency BIGINT NOT NULL,"
                        + " PRIMARY KEY (word_id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'cobble',"
                        + " 'path' = '"
                        + escape(tablePath)
                        + "',"
                        + " 'source.kind' = 'sink',"
                        + " 'scan.checkpoint-id' = 'latest',"
                        + " 'scan.mode' = 'batch'"
                        + ")");

        TableResult result =
                tableEnv.executeSql(
                        "SELECT COUNT(*), SUM(frequency), MIN(frequency), MAX(frequency) "
                                + "FROM cobble_word_counts");
        try (CloseableIterator<Row> rows = result.collect()) {
            Row aggregate = rows.next();
            assertEquals(Long.valueOf(DISTINCT_WORDS), aggregate.getFieldAs(0));
            assertEquals(Long.valueOf(TOTAL_EVENTS), aggregate.getFieldAs(1));
            assertEquals(Long.valueOf(EVENTS_PER_WORD), aggregate.getFieldAs(2));
            assertEquals(Long.valueOf(EVENTS_PER_WORD), aggregate.getFieldAs(3));
        }
    }

    private static JobExecutionResult awaitJobCompletion(TableResult result) throws Exception {
        JobClient jobClient = result.getJobClient().orElseThrow(IllegalStateException::new);
        return jobClient.getJobExecutionResult().get(4L, TimeUnit.MINUTES);
    }

    private static String escape(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "\\\\");
    }

    private static final class StatefulWordCount extends RichFlatMapFunction<Long, Row> {
        private static final long serialVersionUID = 1L;
        private static final String COMPLETED_WORDS = "completed-words";

        private transient ValueState<Long> count;
        private transient LongCounter completedWords;

        @Override
        public void open(Configuration parameters) throws Exception {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Types.LONG));
            completedWords = new LongCounter();
            getRuntimeContext().addAccumulator(COMPLETED_WORDS, completedWords);
        }

        @Override
        public void flatMap(Long wordId, Collector<Row> output) throws Exception {
            Long previous = count.value();
            long next = previous == null ? 1L : previous.longValue() + 1L;
            count.update(Long.valueOf(next));
            if (next == EVENTS_PER_WORD) {
                completedWords.add(1L);
                output.collect(Row.of(wordId, "word-" + wordId, Long.valueOf(next)));
            }
        }
    }
}
