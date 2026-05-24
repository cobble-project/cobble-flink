package io.cobble.flink.table;

import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Cobble SQL sink with primary-key upsert semantics. */
final class CobbleDynamicTableSink implements DynamicTableSink {

    private final SerializableConfig config;
    private final String summary;

    CobbleDynamicTableSink(SerializableConfig config, String summary) {
        this.config = config;
        this.summary = summary;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.upsert();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return new DataStreamSinkProvider() {
            @Override
            public org.apache.flink.streaming.api.datastream.DataStreamSink<?> consumeDataStream(
                    org.apache.flink.table.connector.ProviderContext providerContext,
                    org.apache.flink.streaming.api.datastream.DataStream<RowData> dataStream) {
                final CobbleRowDataCodecs.RuntimeKeyEncoder keyEncoder =
                        new CobbleRowDataCodecs.RuntimeKeyEncoder(config.keyFields);
                org.apache.flink.streaming.api.datastream.DataStream<RowData> routed =
                        dataStream.partitionCustom(
                                new BucketOwnerPartitioner(),
                                new BucketOwnerKeySelector(
                                        keyEncoder, config.bucketCount, config.sinkParallelism));
                org.apache.flink.streaming.api.datastream.DataStreamSink<?> sink =
                        routed.sinkTo(new CobbleSqlSink(config))
                                .setParallelism(config.sinkParallelism);
                if (config.sinkUseManagedMemoryAllocator) {
                    // We only declare here, but not allocate and return since the sink v2 does
                    // not expose proper memory manager APIs.
                    sink.getTransformation()
                            .declareManagedMemoryUseCaseAtOperatorScope(
                                    ManagedMemoryUseCase.OPERATOR,
                                    mebiBytes(config.sinkWriterBufferMemoryBytes));
                }
                return sink;
            }

            @Override
            public Optional<Integer> getParallelism() {
                return Optional.of(config.sinkParallelism);
            }
        };
    }

    private static final class BucketOwnerPartitioner
            implements org.apache.flink.api.common.functions.Partitioner<Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public int partition(Integer owner, int numPartitions) {
            return Math.floorMod(owner.intValue(), numPartitions);
        }
    }

    private static final class BucketOwnerKeySelector
            implements org.apache.flink.api.java.functions.KeySelector<RowData, Integer> {
        private static final long serialVersionUID = 1L;

        private final CobbleRowDataCodecs.RuntimeKeyEncoder keyEncoder;
        private final int bucketCount;
        private final int sinkParallelism;

        private BucketOwnerKeySelector(
                CobbleRowDataCodecs.RuntimeKeyEncoder keyEncoder,
                int bucketCount,
                int sinkParallelism) {
            this.keyEncoder = keyEncoder;
            this.bucketCount = bucketCount;
            this.sinkParallelism = sinkParallelism;
        }

        @Override
        public Integer getKey(RowData value) throws Exception {
            int bucket = CobbleSqlSink.hashFixedBucket(keyEncoder.encode(value), bucketCount);
            return CobbleSqlSink.bucketOwnerSubtask(bucket, bucketCount, sinkParallelism);
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new CobbleDynamicTableSink(config.copy(), summary);
    }

    @Override
    public String asSummaryString() {
        return "CobbleTableSink{" + summary + "}";
    }

    /** Serializable sink runtime config. */
    static final class SerializableConfig implements Serializable {
        private static final long serialVersionUID = 1L;

        final String pathUri;
        final int bucketCount;
        final int snapshotRetention;
        final int sinkParallelism;
        final boolean sinkUseManagedMemoryAllocator;
        final long sinkWriterBufferMemoryBytes;
        final List<SerializableField> keyFields;
        final List<SerializableField> valueFields;

        SerializableConfig(
                String pathUri,
                int bucketCount,
                int snapshotRetention,
                int sinkParallelism,
                boolean sinkUseManagedMemoryAllocator,
                long sinkWriterBufferMemoryBytes,
                List<SerializableField> keyFields,
                List<SerializableField> valueFields) {
            this.pathUri = pathUri;
            this.bucketCount = bucketCount;
            this.snapshotRetention = snapshotRetention;
            this.sinkParallelism = sinkParallelism;
            this.sinkUseManagedMemoryAllocator = sinkUseManagedMemoryAllocator;
            this.sinkWriterBufferMemoryBytes = sinkWriterBufferMemoryBytes;
            this.keyFields = Collections.unmodifiableList(new ArrayList<>(keyFields));
            this.valueFields = Collections.unmodifiableList(new ArrayList<>(valueFields));
        }

        SerializableConfig copy() {
            return new SerializableConfig(
                    pathUri,
                    bucketCount,
                    snapshotRetention,
                    sinkParallelism,
                    sinkUseManagedMemoryAllocator,
                    sinkWriterBufferMemoryBytes,
                    keyFields,
                    valueFields);
        }
    }

    private static int mebiBytes(long bytes) {
        if (bytes <= 0L) {
            return 1;
        }
        long rounded = (bytes + (1024L * 1024L) - 1L) / (1024L * 1024L);
        if (rounded > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) rounded;
    }

    /** Serializable field mapping from Flink physical row to Cobble key/value bytes. */
    static final class SerializableField implements Serializable {
        private static final long serialVersionUID = 1L;

        final String name;
        final String logicalType;
        final int rowIndex;
        final int structuredColumnIndex;

        SerializableField(
                String name, String logicalType, int rowIndex, int structuredColumnIndex) {
            this.name = name;
            this.logicalType = logicalType;
            this.rowIndex = rowIndex;
            this.structuredColumnIndex = structuredColumnIndex;
        }
    }
}
