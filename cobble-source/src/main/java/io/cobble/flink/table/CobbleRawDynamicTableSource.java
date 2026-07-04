package io.cobble.flink.table;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;

/**
 * Flink SQL source for {@code source.kind='raw'}.
 *
 * <p>Reads a standard Cobble table root and emits raw key bytes plus selected value columns as
 * {@code ARRAY<BYTES>}, without typed schema resolution. The underlying {@link CobbleSource} /
 * {@link CobbleSourceReader} / {@link CobbleSourceEnumerator} machinery is reused via the {@link
 * CobbleTableScanConfig} interface.
 *
 * <p>V1 supports batch and streaming scan. Lookup is not supported.
 */
final class CobbleRawDynamicTableSource implements ScanTableSource, LookupTableSource {

    private static final String LOOKUP_UNSUPPORTED =
            "Cobble raw source lookup is not supported yet.";

    private final RawSourceConfig config;
    private final String summary;

    CobbleRawDynamicTableSource(RawSourceConfig config, String summary) {
        this.config = config;
        this.summary = summary;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return SourceProvider.of(new CobbleSource(config));
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        throw new ValidationException(LOOKUP_UNSUPPORTED);
    }

    @Override
    public DynamicTableSource copy() {
        return new CobbleRawDynamicTableSource(config, summary);
    }

    @Override
    public String asSummaryString() {
        return "CobbleRawTableSource{" + summary + "}";
    }
}
