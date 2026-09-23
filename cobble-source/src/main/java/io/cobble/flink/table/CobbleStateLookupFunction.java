package io.cobble.flink.table;

import io.cobble.flink.common.CobbleConnectorMetrics;
import io.cobble.table.TableReadEntry;
import io.cobble.table.TableReadSession;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Lookup function that resolves Cobble state rows by exact full key for value-like states
 * (ValueState / ReducingState / AggregatingState) and MapState (one full map entry).
 *
 * <p>It is the runtime companion of {@link CobbleStateLookupKeyEncoder} and {@link
 * CobbleStateRowDecoder}: the encoder turns the lookup {@link RowData} into Cobble row-key bytes
 * and a key group, the reader fetches the column-family value for that key, and the decoder turns
 * the raw bytes back into the same SQL row shape as a state scan.
 *
 * <p>Only batch lookup is supported: {@code scan.mode='batch'}. The checkpoint is pinned at {@link
 * #open(FunctionContext)} time via {@link CobbleStateSourceRuntime#resolveCheckpointId}, matching
 * batch scan semantics. Streaming state lookup is rejected because this reader is pinned to one
 * checkpoint and never advances its fixed snapshot view.
 */
public final class CobbleStateLookupFunction extends LookupFunction {

    private static final String STREAMING_LOOKUP_NOT_SUPPORTED =
            "Cobble state source lookup currently supports only scan.mode='batch'.";

    private final StateSourceConfig config;
    private final int[] lookupKeyPositionsByRequiredField;

    private transient CobbleStateTableReadProvider provider;
    private transient TableReadSession<RowData, RowData> session;
    private transient CobbleConnectorMetrics.LookupMetrics metrics;

    CobbleStateLookupFunction(StateSourceConfig config, int[] lookupKeyPositionsByRequiredField) {
        this.config = config;
        this.lookupKeyPositionsByRequiredField =
                Arrays.copyOf(
                        lookupKeyPositionsByRequiredField,
                        lookupKeyPositionsByRequiredField.length);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        this.metrics = lookupMetrics(context);
        if (!"batch".equals(config.scanMode())) {
            throw new IOException(STREAMING_LOOKUP_NOT_SUPPORTED);
        }
        CobbleStateTableReadProvider resolvedProvider =
                CobbleStateTableReadProvider.forLookup(config, lookupKeyPositionsByRequiredField);
        try {
            this.session = resolvedProvider.open();
            this.provider = resolvedProvider;
        } catch (Exception e) {
            resolvedProvider.close();
            throw e;
        }
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        metrics.request();
        try {
            Collection<TableReadEntry<RowData>> rows = session.lookup(keyRow);
            if (rows.isEmpty()) {
                metrics.miss();
                return Collections.emptyList();
            }
            if (rows.size() > 1) {
                throw new IOException(
                        "Cobble state lookup for state '"
                                + config.stateName()
                                + "' returned "
                                + rows.size()
                                + " rows for a single key; expected at most one.");
            }
            TableReadEntry<RowData> row = rows.iterator().next();
            metrics.hit(row.physicalBytes());
            return Collections.singletonList(row.value());
        } catch (IOException e) {
            metrics.error();
            throw e;
        } catch (RuntimeException e) {
            metrics.error();
            throw e;
        } catch (Exception e) {
            metrics.error();
            throw new IOException("Cobble state lookup failed.", e);
        }
    }

    @Override
    public void close() {
        if (session != null) session.close();
        session = null;
        if (provider != null) provider.close();
        provider = null;
    }

    private static CobbleConnectorMetrics.LookupMetrics lookupMetrics(FunctionContext context) {
        return CobbleConnectorMetrics.lookup(context == null ? null : context.getMetricGroup());
    }
}
