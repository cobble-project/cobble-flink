package io.cobble.flink.table;

import java.io.Serializable;

/**
 * Placeholder configuration for the Cobble state source.
 *
 * <p>Step 1 only detects state paths; it does not read them. This type carries what detection
 * learned about a state path so the factory can produce a precise diagnostic, and reserves a stable
 * shape for the Step 2/3 state runtime. It deliberately does <em>not</em> hold sink key/value field
 * mappings: state decoding will use the state inspect schema registry instead.
 */
final class StateSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Shape of the detected state path. */
    enum Layout {
        /** A Flink checkpoint root: a directory containing {@code chk-*} subdirectories. */
        CHECKPOINT_ROOT,

        /**
         * A single state operator inspect-schema root. It lacks checkpoint root / shared-volume
         * context, so reads must be pointed at the enclosing checkpoint root instead.
         */
        OPERATOR_ROOT,

        /** Layout could not be confirmed (only reachable via an explicit {@code source.kind}). */
        UNKNOWN
    }

    private final String pathUri;
    private final Layout layout;

    StateSourceConfig(String pathUri, Layout layout) {
        this.pathUri = pathUri;
        this.layout = layout;
    }

    String pathUri() {
        return pathUri;
    }

    Layout layout() {
        return layout;
    }
}
