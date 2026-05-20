package io.cobble.flink.table;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

/** FLIP-27 source that reads rows from Cobble sink checkpoints. */
final class CobbleSource
        implements Source<RowData, CobbleSourceSplit, CobbleSourceEnumeratorState> {

    private final CobbleDynamicTableSource.SerializableConfig config;

    CobbleSource(CobbleDynamicTableSource.SerializableConfig config) {
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        return config.boundedness();
    }

    @Override
    public SourceReader<RowData, CobbleSourceSplit> createReader(
            SourceReaderContext readerContext) {
        return new CobbleSourceReader(config, readerContext);
    }

    @Override
    public SplitEnumerator<CobbleSourceSplit, CobbleSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<CobbleSourceSplit> enumContext) throws Exception {
        return new CobbleSourceEnumerator(config, enumContext, null);
    }

    @Override
    public SplitEnumerator<CobbleSourceSplit, CobbleSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<CobbleSourceSplit> enumContext,
            CobbleSourceEnumeratorState checkpoint)
            throws Exception {
        return new CobbleSourceEnumerator(config, enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<CobbleSourceSplit> getSplitSerializer() {
        return new CobbleSourceSplit.Serializer();
    }

    @Override
    public SimpleVersionedSerializer<CobbleSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new CobbleSourceEnumeratorState.Serializer();
    }
}
