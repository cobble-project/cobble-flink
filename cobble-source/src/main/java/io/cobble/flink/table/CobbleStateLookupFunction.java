package io.cobble.flink.table;

import io.cobble.ReadOptions;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Lookup function that resolves Cobble state rows by exact full key for value-like states
 * (ValueState / ReducingState / AggregatingState).
 *
 * <p>It is the runtime companion of {@link CobbleStateLookupKeyEncoder} and {@link
 * CobbleStateRowDecoder}: the encoder turns the lookup {@link RowData} into Cobble row-key bytes
 * and a key group, the reader fetches the column-family value for that key, and the decoder turns
 * the raw bytes back into the same SQL row shape as a state scan.
 *
 * <p>Only batch lookup is supported: {@code scan.mode='batch'}. The checkpoint is pinned at {@link
 * #open(FunctionContext)} time via {@link CobbleStateSourceRuntime#resolveCheckpointId}, matching
 * batch scan semantics. Streaming state lookup is rejected because checkpoint-root materialization
 * (manifest/shard copy into a unified temp volume) is checkpoint-specific.
 */
final class CobbleStateLookupFunction extends LookupFunction {

    private static final String STREAMING_LOOKUP_NOT_SUPPORTED =
            "Cobble state source lookup currently supports only scan.mode='batch'.";

    private final StateSourceConfig config;
    private final int[] lookupKeyPositionsByRequiredField;

    private transient CobbleStateSourceRuntime.RuntimeSchema runtimeSchema;
    private transient CobbleStateLookupKeyEncoder keyEncoder;
    private transient CobbleStateRowDecoder rowDecoder;
    private transient CobbleStateSourceRuntime.ReaderHandle readerHandle;
    private transient ReadOptions readOptions;
    private transient long checkpointId;
    private transient int totalKeyGroups;

    CobbleStateLookupFunction(StateSourceConfig config, int[] lookupKeyPositionsByRequiredField) {
        this.config = config;
        this.lookupKeyPositionsByRequiredField =
                Arrays.copyOf(
                        lookupKeyPositionsByRequiredField,
                        lookupKeyPositionsByRequiredField.length);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        if (!"batch".equals(config.scanMode())) {
            throw new IOException(STREAMING_LOOKUP_NOT_SUPPORTED);
        }
        // Use local variables so a failure in any step closes resources already created, preventing
        // the temporary unified volume from leaking when a later step (e.g. readOptions) throws.
        long resolvedCheckpointId = CobbleStateSourceRuntime.resolveCheckpointId(config);
        CobbleStateSourceRuntime.RuntimeSchema resolvedSchema =
                CobbleStateSourceRuntime.loadRuntimeSchema(config);
        CobbleStateLookupKeyEncoder resolvedEncoder =
                new CobbleStateLookupKeyEncoder(
                        config, resolvedSchema, lookupKeyPositionsByRequiredField);
        CobbleStateRowDecoder resolvedDecoder = new CobbleStateRowDecoder(config, resolvedSchema);
        CobbleStateSourceRuntime.ReaderHandle resolvedReader = null;
        ReadOptions resolvedReadOptions = null;
        try {
            resolvedReader = CobbleStateSourceRuntime.openReader(config, resolvedCheckpointId);
            int resolvedTotalKeyGroups = resolvedReader.reader.currentGlobalSnapshot().totalBuckets;
            resolvedReadOptions =
                    CobbleStateSourceRuntime.readOptions(resolvedSchema.schema.columnFamily(), 0);
            // All steps succeeded — publish to fields.
            this.checkpointId = resolvedCheckpointId;
            this.runtimeSchema = resolvedSchema;
            this.keyEncoder = resolvedEncoder;
            this.rowDecoder = resolvedDecoder;
            this.readerHandle = resolvedReader;
            this.readOptions = resolvedReadOptions;
            this.totalKeyGroups = resolvedTotalKeyGroups;
        } catch (Exception e) {
            if (resolvedReadOptions != null) {
                resolvedReadOptions.close();
            }
            if (resolvedReader != null) {
                resolvedReader.close();
            }
            throw e;
        }
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        CobbleStateLookupKeyEncoder.EncodedStateLookupKey encoded =
                keyEncoder.encode(keyRow, totalKeyGroups);

        byte[][] columns;
        try {
            columns =
                    readerHandle.reader.getWithOptions(
                            encoded.keyGroup(), encoded.rowKey(), readOptions);
        } catch (RuntimeException e) {
            // A shard may not have registered the state column family when it never wrote data for
            // that state in this key group. The key is genuinely absent in that shard.
            if (CobbleStateSourceReader.isUnknownColumnFamily(e)) {
                return Collections.emptyList();
            }
            throw new IOException(
                    "Failed to read Cobble state lookup row (checkpoint="
                            + checkpointId
                            + ", operator="
                            + config.operatorId()
                            + ", state="
                            + config.stateName()
                            + ", keyGroup="
                            + encoded.keyGroup()
                            + "): "
                            + e.getMessage(),
                    e);
        }
        if (columns == null) {
            return Collections.emptyList();
        }

        List<RowData> rows =
                rowDecoder.decode(encoded.rowKey(), columns, "lookup", encoded.keyGroup());
        if (rows.isEmpty()) {
            return Collections.emptyList();
        }
        if (rows.size() > 1) {
            throw new IOException(
                    "Cobble state lookup for value-like state '"
                            + config.stateName()
                            + "' returned "
                            + rows.size()
                            + " rows for a single key; expected at most one. This indicates an"
                            + " accidental list/map state lookup.");
        }
        return Collections.singletonList(rows.get(0));
    }

    @Override
    public void close() {
        if (readOptions != null) {
            readOptions.close();
            readOptions = null;
        }
        if (readerHandle != null) {
            readerHandle.close();
            readerHandle = null;
        }
    }
}
