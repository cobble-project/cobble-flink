package io.cobble.flink.table;

import org.apache.flink.api.connector.source.SourceEvent;

/** Source events exchanged between the Cobble reader and enumerator. */
final class CobbleSourceEvents {

    private CobbleSourceEvents() {}

    /** Pushes the newest split metadata for a stable logical range to its current owner. */
    static final class ReplaceSplitEvent implements SourceEvent {
        private static final long serialVersionUID = 1L;

        final CobbleSourceSplit split;

        ReplaceSplitEvent(CobbleSourceSplit split) {
            this.split = split;
        }
    }

    /** Reports the stable split ids currently held by a reader. */
    static final class OwnedSplitsEvent implements SourceEvent {
        private static final long serialVersionUID = 1L;

        final String[] splitIds;

        OwnedSplitsEvent(String[] splitIds) {
            this.splitIds = splitIds;
        }
    }
}
