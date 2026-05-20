package io.cobble.flink.table;

import org.apache.flink.api.connector.source.SourceEvent;

/** Source events exchanged between the Cobble reader and enumerator. */
final class CobbleSourceEvents {

    private CobbleSourceEvents() {}

    static final class ReplaceSplitEvent implements SourceEvent {
        private static final long serialVersionUID = 1L;

        final CobbleSourceSplit split;

        ReplaceSplitEvent(CobbleSourceSplit split) {
            this.split = split;
        }
    }

    static final class OwnedSplitsEvent implements SourceEvent {
        private static final long serialVersionUID = 1L;

        final int[] splitIds;

        OwnedSplitsEvent(int[] splitIds) {
            this.splitIds = splitIds;
        }
    }
}
