package io.cobble.flink.table;

/**
 * Result of resolving a Cobble source path to a concrete {@link CobbleSourceKind}.
 *
 * <p>For {@link CobbleSourceKind#SINK} the existing sink source runtime config ({@link
 * CobbleDynamicTableSource.SerializableConfig}) is built by the factory from the table schema, so
 * this type only carries detection metadata. For {@link CobbleSourceKind#STATE} it carries a {@link
 * StateSourceConfig} placeholder describing the detected layout. For {@link CobbleSourceKind#RAW}
 * it carries no extra config — the raw source is schema-less and its config is built directly by
 * the factory from the parsed options.
 */
final class CobbleResolvedSource {

    private final CobbleSourceKind kind;
    private final StateSourceConfig stateConfig;
    private final String diagnostics;

    private CobbleResolvedSource(
            CobbleSourceKind kind, StateSourceConfig stateConfig, String diagnostics) {
        this.kind = kind;
        this.stateConfig = stateConfig;
        this.diagnostics = diagnostics;
    }

    static CobbleResolvedSource sink(String diagnostics) {
        return new CobbleResolvedSource(CobbleSourceKind.SINK, null, diagnostics);
    }

    static CobbleResolvedSource state(StateSourceConfig stateConfig, String diagnostics) {
        return new CobbleResolvedSource(CobbleSourceKind.STATE, stateConfig, diagnostics);
    }

    static CobbleResolvedSource raw(String diagnostics) {
        return new CobbleResolvedSource(CobbleSourceKind.RAW, null, diagnostics);
    }

    CobbleSourceKind kind() {
        return kind;
    }

    StateSourceConfig stateConfig() {
        return stateConfig;
    }

    String diagnostics() {
        return diagnostics;
    }
}
