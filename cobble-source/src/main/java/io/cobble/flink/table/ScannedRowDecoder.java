package io.cobble.flink.table;

import org.apache.flink.table.data.RowData;

import java.io.IOException;

/**
 * Strategy for turning a scanned Cobble row (raw key bytes + projected column bytes) into a Flink
 * {@link RowData}.
 *
 * <p>The sink source uses {@link CobbleRowDataDecoders.RuntimeRowDecoder} (typed, holds Flink
 * serializers). The raw source uses {@link CobbleRawRowDecoder} (stateless, just wraps bytes).
 */
interface ScannedRowDecoder {

    RowData decode(byte[] key, byte[][] columns) throws IOException;
}
