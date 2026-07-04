package io.cobble.flink.table;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.io.IOException;

/**
 * Stateless decoder for {@code source.kind='raw'} scans.
 *
 * <p>Produces a two-field {@link RowData}:
 *
 * <ul>
 *   <li>field 0 ({@code key}): the raw Cobble row key bytes, as {@code byte[]}.
 *   <li>field 1 ({@code columns}): the projected value columns as {@code GenericArrayData} of
 *       {@code byte[]} (or {@code null} per column when the column was not written).
 * </ul>
 *
 * <p>No type information, no serializers, no key-field deserialization. The key bytes are emitted
 * verbatim and the column bytes are emitted verbatim, preserving nulls and arbitrary binary
 * content.
 */
final class CobbleRawRowDecoder implements ScannedRowDecoder {

    private final int projectedColumnCount;

    CobbleRawRowDecoder(int projectedColumnCount) {
        this.projectedColumnCount = projectedColumnCount;
    }

    @Override
    public RowData decode(byte[] key, byte[][] columns) throws IOException {
        GenericRowData row = new GenericRowData(RowKind.INSERT, 2);
        row.setField(0, key);

        byte[][] projected = columns;
        if (projected == null) {
            projected = new byte[0][];
        }
        // If the scan returned fewer columns than requested (e.g. the DB has fewer columns than
        // scanColumnCount), pad with nulls so the array length matches the DDL's ARRAY<BYTES>.
        if (projected.length < projectedColumnCount) {
            byte[][] padded = new byte[projectedColumnCount][];
            System.arraycopy(projected, 0, padded, 0, projected.length);
            projected = padded;
        }
        row.setField(1, new GenericArrayData(projected));
        return row;
    }
}
