package io.cobble.flink.table;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/** Unit tests for {@link RawSourceSchemaResolver}. */
class RawSourceSchemaResolverTest {

    private static final org.apache.flink.table.types.DataType BYTES =
            TypeConversions.fromLogicalToDataType(new BinaryType());
    private static final org.apache.flink.table.types.DataType VARBINARY =
            TypeConversions.fromLogicalToDataType(new VarBinaryType(VarBinaryType.MAX_LENGTH));
    private static final org.apache.flink.table.types.DataType ARRAY_BYTES =
            TypeConversions.fromLogicalToDataType(new ArrayType(new BinaryType()));
    private static final org.apache.flink.table.types.DataType ARRAY_VARBINARY =
            TypeConversions.fromLogicalToDataType(
                    new ArrayType(new VarBinaryType(VarBinaryType.MAX_LENGTH)));

    @Test
    void acceptsBytesAndVarbinary() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("key", BYTES), Column.physical("columns", ARRAY_BYTES));
        assertDoesNotThrow(() -> RawSourceSchemaResolver.validate(schema));
    }

    @Test
    void acceptsVarbinaryAndArrayVarbinary() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("key", VARBINARY),
                        Column.physical("columns", ARRAY_VARBINARY));
        assertDoesNotThrow(() -> RawSourceSchemaResolver.validate(schema));
    }

    @Test
    void rejectsPrimaryKey() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Collections.singletonList(Column.physical("key", BYTES)),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("key")));
        ValidationException error =
                assertThrows(
                        ValidationException.class, () -> RawSourceSchemaResolver.validate(schema));
        assertTrue(error.getMessage().contains("PRIMARY KEY"), "got: " + error.getMessage());
    }

    @Test
    void rejectsWrongColumnCount() {
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("key", BYTES));
        ValidationException error =
                assertThrows(
                        ValidationException.class, () -> RawSourceSchemaResolver.validate(schema));
        assertTrue(error.getMessage().contains("two columns"), "got: " + error.getMessage());
    }

    @Test
    void rejectsWrongKeyName() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("id", BYTES), Column.physical("columns", ARRAY_BYTES));
        ValidationException error =
                assertThrows(
                        ValidationException.class, () -> RawSourceSchemaResolver.validate(schema));
        assertTrue(error.getMessage().contains("named 'key'"), "got: " + error.getMessage());
    }

    @Test
    void rejectsWrongColumnsName() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("key", BYTES), Column.physical("vals", ARRAY_BYTES));
        ValidationException error =
                assertThrows(
                        ValidationException.class, () -> RawSourceSchemaResolver.validate(schema));
        assertTrue(error.getMessage().contains("named 'columns'"), "got: " + error.getMessage());
    }

    @Test
    void rejectsNonBinaryKey() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("key", org.apache.flink.table.api.DataTypes.INT()),
                        Column.physical("columns", ARRAY_BYTES));
        ValidationException error =
                assertThrows(
                        ValidationException.class, () -> RawSourceSchemaResolver.validate(schema));
        assertTrue(error.getMessage().contains("BYTES or VARBINARY"), "got: " + error.getMessage());
    }

    @Test
    void rejectsNonArrayColumns() {
        ResolvedSchema schema =
                ResolvedSchema.of(Column.physical("key", BYTES), Column.physical("columns", BYTES));
        ValidationException error =
                assertThrows(
                        ValidationException.class, () -> RawSourceSchemaResolver.validate(schema));
        assertTrue(error.getMessage().contains("ARRAY<BYTES>"), "got: " + error.getMessage());
    }

    @Test
    void rejectsNonBinaryArrayElement() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("key", BYTES),
                        Column.physical(
                                "columns",
                                TypeConversions.fromLogicalToDataType(
                                        new ArrayType(new IntType()))));
        ValidationException error =
                assertThrows(
                        ValidationException.class, () -> RawSourceSchemaResolver.validate(schema));
        assertTrue(error.getMessage().contains("ARRAY<BYTES>"), "got: " + error.getMessage());
    }
}
