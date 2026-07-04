package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link RawSourceOptions}. */
class RawSourceOptionsTest {

    @Test
    void parsesCommaSeparatedIndexes() {
        Configuration config = new Configuration();
        config.setString("raw.columns", "0,1,2");
        assertArrayEquals(
                new int[] {0, 1, 2}, RawSourceOptions.parseForRaw(config).selectedColumns());
    }

    @Test
    void parsesNonContiguousIndexes() {
        Configuration config = new Configuration();
        config.setString("raw.columns", "0,2");
        assertArrayEquals(new int[] {0, 2}, RawSourceOptions.parseForRaw(config).selectedColumns());
    }

    @Test
    void parsesSingleIndex() {
        Configuration config = new Configuration();
        config.setString("raw.columns", "0");
        assertArrayEquals(new int[] {0}, RawSourceOptions.parseForRaw(config).selectedColumns());
    }

    @Test
    void preservesUserSpecifiedOrder() {
        Configuration config = new Configuration();
        config.setString("raw.columns", "2, 0, 1");
        assertArrayEquals(
                new int[] {2, 0, 1}, RawSourceOptions.parseForRaw(config).selectedColumns());
    }

    @Test
    void rejectsAllKeyword() {
        Configuration config = new Configuration();
        config.setString("raw.columns", "all");
        ValidationException error =
                assertThrows(ValidationException.class, () -> RawSourceOptions.parseForRaw(config));
        assertTrue(error.getMessage().contains("not supported yet"), "got: " + error.getMessage());
    }

    @Test
    void rejectsAllKeywordCaseInsensitive() {
        Configuration config = new Configuration();
        config.setString("raw.columns", "ALL");
        assertThrows(ValidationException.class, () -> RawSourceOptions.parseForRaw(config));
    }

    @Test
    void rejectsMissingOption() {
        Configuration config = new Configuration();
        ValidationException error =
                assertThrows(ValidationException.class, () -> RawSourceOptions.parseForRaw(config));
        assertTrue(error.getMessage().contains("required"), "got: " + error.getMessage());
    }

    @Test
    void rejectsNegativeIndex() {
        Configuration config = new Configuration();
        config.setString("raw.columns", "0,-1");
        ValidationException error =
                assertThrows(ValidationException.class, () -> RawSourceOptions.parseForRaw(config));
        assertTrue(error.getMessage().contains("non-negative"), "got: " + error.getMessage());
    }

    @Test
    void rejectsDuplicateIndex() {
        Configuration config = new Configuration();
        config.setString("raw.columns", "0,0");
        ValidationException error =
                assertThrows(ValidationException.class, () -> RawSourceOptions.parseForRaw(config));
        assertTrue(error.getMessage().contains("duplicate"), "got: " + error.getMessage());
    }

    @Test
    void rejectsNonNumericValue() {
        Configuration config = new Configuration();
        config.setString("raw.columns", "0,abc");
        assertThrows(ValidationException.class, () -> RawSourceOptions.parseForRaw(config));
    }

    @Test
    void rejectsEmptyValueInList() {
        Configuration config = new Configuration();
        config.setString("raw.columns", "0,,1");
        assertThrows(ValidationException.class, () -> RawSourceOptions.parseForRaw(config));
    }

    @Test
    void rejectRawOptionsForNonRawRejectsWhenPresent() {
        Configuration config = new Configuration();
        config.setString("raw.columns", "0,1");
        ValidationException error =
                assertThrows(
                        ValidationException.class,
                        () -> RawSourceOptions.rejectRawOptionsForNonRaw(config));
        assertTrue(
                error.getMessage().contains("only valid when source.kind='raw'"),
                "got: " + error.getMessage());
    }

    @Test
    void rejectRawOptionsForNonRawPassesWhenAbsent() {
        Configuration config = new Configuration();
        RawSourceOptions.rejectRawOptionsForNonRaw(config);
    }
}
