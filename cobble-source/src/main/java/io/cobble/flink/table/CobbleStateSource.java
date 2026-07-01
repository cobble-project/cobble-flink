package io.cobble.flink.table;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

/** FLIP-27 bounded source for reading Cobble-backed Flink keyed state checkpoints. */
final class CobbleStateSource
        implements Source<RowData, CobbleStateSourceSplit, CobbleStateSourceEnumeratorState> {

    private static final long serialVersionUID = 1L;

    private final StateSourceConfig config;

    CobbleStateSource(StateSourceConfig config) {
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, CobbleStateSourceSplit> createReader(
            SourceReaderContext readerContext) throws Exception {
        return new CobbleStateSourceReader(config, readerContext);
    }

    @Override
    public SplitEnumerator<CobbleStateSourceSplit, CobbleStateSourceEnumeratorState>
            createEnumerator(SplitEnumeratorContext<CobbleStateSourceSplit> enumContext)
                    throws Exception {
        return new CobbleStateSourceEnumerator(config, enumContext, null);
    }

    @Override
    public SplitEnumerator<CobbleStateSourceSplit, CobbleStateSourceEnumeratorState>
            restoreEnumerator(
                    SplitEnumeratorContext<CobbleStateSourceSplit> enumContext,
                    CobbleStateSourceEnumeratorState checkpoint)
                    throws Exception {
        return new CobbleStateSourceEnumerator(config, enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<CobbleStateSourceSplit> getSplitSerializer() {
        return new CobbleStateSourceSplit.Serializer();
    }

    @Override
    public SimpleVersionedSerializer<CobbleStateSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new CobbleStateSourceEnumeratorState.Serializer();
    }
}
