package io.cobble.flink.table;

import io.cobble.flink.catalog.CobbleCatalogTableReference;
import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.table.CobbleTableRowConverter;
import io.cobble.table.TableSchema;
import io.cobble.table.TableWritePlan;

import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
                org.apache.flink.streaming.api.datastream.DataStream<RowData> routed =
                        dataStream.partitionCustom(
                                new BucketOwnerPartitioner(), new BucketOwnerKeySelector(config));
                long checkpointTimeoutMillis =
                        dataStream
                                .getExecutionEnvironment()
                                .getCheckpointConfig()
                                .getCheckpointTimeout();
                org.apache.flink.streaming.api.datastream.DataStreamSink<?> sink =
                        routed.sinkTo(new CobbleSqlSink(config, checkpointTimeoutMillis))
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

        private final SerializableConfig config;
        private transient CobbleTableRowConverter rowConverter;
        private transient TableSchema tableSchema;

        private BucketOwnerKeySelector(SerializableConfig config) {
            this.config = config;
        }

        @Override
        public Integer getKey(RowData value) {
            if (rowConverter == null) {
                rowConverter = new CobbleTableRowConverter(config.rowType());
                tableSchema = config.tableSchema();
            }
            int bucket =
                    CobbleTableRowConverter.bucket(
                            tableSchema, rowConverter.toValues(value), config.bucketCount);
            return CobbleSqlSink.bucketOwnerSubtask(
                    bucket, config.bucketCount, config.sinkParallelism);
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
        final CobbleConnectorStorageOptions storageOptions;
        final int bucketCount;
        final int snapshotRetention;
        final int sinkParallelism;
        final boolean sinkUseManagedMemoryAllocator;
        final long sinkWriterBufferMemoryBytes;
        final List<SerializableField> keyFields;
        final List<SerializableField> valueFields;
        final CobbleCatalogTableReference catalogTable;
        final TableWritePlan catalogWritePlan;
        final TableSchema catalogSchema;

        SerializableConfig(
                String pathUri,
                int bucketCount,
                int snapshotRetention,
                int sinkParallelism,
                boolean sinkUseManagedMemoryAllocator,
                long sinkWriterBufferMemoryBytes,
                List<SerializableField> keyFields,
                List<SerializableField> valueFields) {
            this(
                    pathUri,
                    bucketCount,
                    snapshotRetention,
                    sinkParallelism,
                    sinkUseManagedMemoryAllocator,
                    sinkWriterBufferMemoryBytes,
                    keyFields,
                    valueFields,
                    CobbleConnectorStorageOptions.empty());
        }

        SerializableConfig(
                String pathUri,
                int bucketCount,
                int snapshotRetention,
                int sinkParallelism,
                boolean sinkUseManagedMemoryAllocator,
                long sinkWriterBufferMemoryBytes,
                List<SerializableField> keyFields,
                List<SerializableField> valueFields,
                CobbleConnectorStorageOptions storageOptions) {
            this.pathUri = pathUri;
            this.storageOptions = storageOptions;
            this.bucketCount = bucketCount;
            this.snapshotRetention = snapshotRetention;
            this.sinkParallelism = sinkParallelism;
            this.sinkUseManagedMemoryAllocator = sinkUseManagedMemoryAllocator;
            this.sinkWriterBufferMemoryBytes = sinkWriterBufferMemoryBytes;
            this.keyFields = Collections.unmodifiableList(new ArrayList<>(keyFields));
            this.valueFields = Collections.unmodifiableList(new ArrayList<>(valueFields));
            this.catalogTable = null;
            this.catalogWritePlan = null;
            this.catalogSchema = null;
        }

        SerializableConfig(
                SerializableConfig legacy,
                CobbleCatalogTableReference catalogTable,
                TableWritePlan catalogWritePlan,
                TableSchema catalogSchema) {
            this.pathUri = legacy.pathUri;
            this.storageOptions = legacy.storageOptions;
            this.bucketCount = legacy.bucketCount;
            this.snapshotRetention = legacy.snapshotRetention;
            this.sinkParallelism = legacy.sinkParallelism;
            this.sinkUseManagedMemoryAllocator = legacy.sinkUseManagedMemoryAllocator;
            this.sinkWriterBufferMemoryBytes = legacy.sinkWriterBufferMemoryBytes;
            this.keyFields = legacy.keyFields;
            this.valueFields = legacy.valueFields;
            this.catalogTable = catalogTable;
            this.catalogWritePlan = catalogWritePlan;
            this.catalogSchema = catalogSchema;
        }

        boolean isCatalogTable() {
            return catalogTable != null;
        }

        SerializableConfig copy() {
            SerializableConfig copied =
                    new SerializableConfig(
                            pathUri,
                            bucketCount,
                            snapshotRetention,
                            sinkParallelism,
                            sinkUseManagedMemoryAllocator,
                            sinkWriterBufferMemoryBytes,
                            keyFields,
                            valueFields,
                            storageOptions);
            return catalogTable == null
                    ? copied
                    : new SerializableConfig(copied, catalogTable, catalogWritePlan, catalogSchema);
        }

        List<SerializableField> physicalFields() {
            List<SerializableField> fields = new ArrayList<SerializableField>();
            fields.addAll(keyFields);
            fields.addAll(valueFields);
            Collections.sort(
                    fields,
                    new Comparator<SerializableField>() {
                        @Override
                        public int compare(SerializableField left, SerializableField right) {
                            return Integer.compare(left.rowIndex, right.rowIndex);
                        }
                    });
            return fields;
        }

        org.apache.flink.table.types.logical.RowType rowType() {
            List<String> names = new ArrayList<String>();
            List<String> types = new ArrayList<String>();
            for (SerializableField field : physicalFields()) {
                names.add(field.name);
                types.add(field.logicalType);
            }
            return CobbleTableRowConverter.parseRowType(names, types);
        }

        TableSchema tableSchema() {
            if (catalogSchema != null) {
                return catalogSchema;
            }
            List<String> primaryKey = new ArrayList<String>(keyFields.size());
            for (SerializableField field : keyFields) {
                primaryKey.add(field.name);
            }
            return CobbleTableRowConverter.toTableSchema(rowType(), primaryKey);
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
