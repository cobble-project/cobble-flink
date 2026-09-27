package io.cobble.flink.table;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/** Immutable top-level field selection in the original physical schema's order space. */
final class SourceProjection {
    private SourceProjection() {}

    static int[] indexes(int[][] paths, int fieldCount) {
        int[] indexes = new int[paths.length];
        for (int index = 0; index < paths.length; index++) {
            if (paths[index].length != 1 || paths[index][0] < 0 || paths[index][0] >= fieldCount) {
                throw new ValidationException(
                        "Cobble source supports only top-level field projection.");
            }
            indexes[index] = paths[index][0];
        }
        return indexes;
    }

    static int[] all(int fieldCount) {
        int[] indexes = new int[fieldCount];
        for (int index = 0; index < fieldCount; index++) indexes[index] = index;
        return indexes;
    }

    static int originalIndex(int[] projection, int projectedIndex) {
        if (projectedIndex < 0 || projectedIndex >= projection.length) {
            throw new ValidationException(
                    "Lookup key position "
                            + projectedIndex
                            + " is outside the projected source schema.");
        }
        return projection[projectedIndex];
    }

    static RowType rowType(RowType full, int[] indexes) {
        List<RowType.RowField> fields = new ArrayList<>(indexes.length);
        for (int index : indexes) fields.add(full.getFields().get(index));
        return new RowType(false, fields);
    }
}
