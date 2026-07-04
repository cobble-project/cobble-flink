package io.cobble.flink.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

/**
 * Parsed and validated {@code raw.*} options for a Cobble raw source.
 *
 * <p>V1 requires explicit column indexes via {@code raw.columns}. The value {@code 'all'} is
 * rejected at the parse layer because Cobble {@code ScanOptions} has no "all columns" sentinel —
 * the column-family width is unknown without sink sidecar schema.
 */
final class RawSourceOptions {

    private final int[] selectedColumns;

    private RawSourceOptions(int[] selectedColumns) {
        this.selectedColumns = selectedColumns;
    }

    int[] selectedColumns() {
        return Arrays.copyOf(selectedColumns, selectedColumns.length);
    }

    /**
     * Parses {@code raw.columns} for a raw source.
     *
     * @throws ValidationException when {@code raw.columns} is missing, is {@code 'all'}, or
     *     contains non-numeric, negative, duplicate, or empty values.
     */
    static RawSourceOptions parseForRaw(ReadableConfig options) {
        String raw = trimToNull(options.getOptional(CobbleSourceTableOptions.RAW_COLUMNS));
        if (raw == null) {
            throw new ValidationException(
                    "Option '"
                            + CobbleSourceTableOptions.RAW_COLUMNS.key()
                            + "' is required when source.kind='raw'."
                            + " Specify indexes such as raw.columns='0,1'.");
        }
        if ("all".equalsIgnoreCase(raw.trim())) {
            throw new ValidationException(
                    "raw.columns='all' is not supported yet; specify indexes such as"
                            + " raw.columns='0,1'.");
        }

        // Preserve the user-specified order so the output ARRAY<BYTES> matches raw.columns.
        // A separate HashSet is used only for duplicate detection.
        List<Integer> ordered = new ArrayList<>();
        HashSet<Integer> seen = new HashSet<>();
        for (String token : raw.split(",")) {
            String trimmed = token.trim();
            if (trimmed.isEmpty()) {
                throw new ValidationException(
                        "Invalid "
                                + CobbleSourceTableOptions.RAW_COLUMNS.key()
                                + " '"
                                + raw
                                + "': empty value found. Use comma-separated non-negative indexes"
                                + " such as '0,1'.");
            }
            int index;
            try {
                index = Integer.parseInt(trimmed);
            } catch (NumberFormatException e) {
                throw new ValidationException(
                        "Invalid "
                                + CobbleSourceTableOptions.RAW_COLUMNS.key()
                                + " '"
                                + raw
                                + "': '"
                                + trimmed
                                + "' is not a non-negative integer.",
                        e);
            }
            if (index < 0) {
                throw new ValidationException(
                        "Invalid "
                                + CobbleSourceTableOptions.RAW_COLUMNS.key()
                                + " '"
                                + raw
                                + "': column indexes must be non-negative.");
            }
            if (!seen.add(index)) {
                throw new ValidationException(
                        "Invalid "
                                + CobbleSourceTableOptions.RAW_COLUMNS.key()
                                + " '"
                                + raw
                                + "': duplicate column index "
                                + index
                                + ".");
            }
            ordered.add(index);
        }
        if (ordered.isEmpty()) {
            throw new ValidationException(
                    "Option '"
                            + CobbleSourceTableOptions.RAW_COLUMNS.key()
                            + "' must specify at least one column index.");
        }

        int[] columns = new int[ordered.size()];
        for (int i = 0; i < ordered.size(); i++) {
            columns[i] = ordered.get(i);
        }
        return new RawSourceOptions(columns);
    }

    /** Rejects {@code raw.*} options on sink/state sources so typos surface. */
    static void rejectRawOptionsForNonRaw(ReadableConfig options) {
        if (trimToNull(options.getOptional(CobbleSourceTableOptions.RAW_COLUMNS)) != null) {
            throw new ValidationException(
                    "Option '"
                            + CobbleSourceTableOptions.RAW_COLUMNS.key()
                            + "' is only valid when source.kind='raw'.");
        }
    }

    private static String trimToNull(Optional<String> value) {
        if (!value.isPresent()) {
            return null;
        }
        String trimmed = value.get().trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
