package io.cobble.flink.table;

import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

final class GlobalCommitOperatorFactory
        extends AbstractStreamOperatorFactory<CommittableMessage<CobbleShardCommittable>>
        implements OneInputStreamOperatorFactory<
                CommittableMessage<CobbleShardCommittable>,
                CommittableMessage<CobbleShardCommittable>> {

    private final CobbleDynamicTableSink.SerializableConfig config;

    GlobalCommitOperatorFactory(CobbleDynamicTableSink.SerializableConfig config) {
        this.config = config;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<CommittableMessage<CobbleShardCommittable>>>
            T createStreamOperator(
                    StreamOperatorParameters<CommittableMessage<CobbleShardCommittable>>
                            parameters) {
        return (T) new GlobalCommitOperator(parameters, config);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return GlobalCommitOperator.class;
    }
}
