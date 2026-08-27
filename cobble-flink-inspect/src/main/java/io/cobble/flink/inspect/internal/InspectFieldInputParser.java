package io.cobble.flink.inspect.internal;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Base64;

/** Parses one typed inspect input into Flink's internal value representation. */
final class InspectFieldInputParser {

    private InspectFieldInputParser() {}

    static Object parse(LogicalType logicalType, String text) throws IOException {
        String value = text == null ? "" : text;
        try {
            switch (logicalType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return StringData.fromString(value);
                case BOOLEAN:
                    if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                        throw new IllegalArgumentException("expected true or false");
                    }
                    return Boolean.valueOf(value);
                case TINYINT:
                    return Byte.valueOf(value);
                case SMALLINT:
                    return Short.valueOf(value);
                case INTEGER:
                    return Integer.valueOf(value);
                case BIGINT:
                    return Long.valueOf(value);
                case FLOAT:
                    return Float.valueOf(value);
                case DOUBLE:
                    return Double.valueOf(value);
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) logicalType;
                    DecimalData decimal =
                            DecimalData.fromBigDecimal(
                                    new BigDecimal(value),
                                    decimalType.getPrecision(),
                                    decimalType.getScale());
                    if (decimal == null) {
                        throw new IllegalArgumentException(
                                "value is outside the declared precision");
                    }
                    return decimal;
                case BINARY:
                case VARBINARY:
                    return Base64.getDecoder().decode(value);
                case DATE:
                    return Math.toIntExact(LocalDate.parse(value).toEpochDay());
                case TIME_WITHOUT_TIME_ZONE:
                    return Math.toIntExact(LocalTime.parse(value).toNanoOfDay() / 1_000_000L);
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return TimestampData.fromLocalDateTime(LocalDateTime.parse(value));
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return TimestampData.fromInstant(Instant.parse(value));
                default:
                    throw new IllegalArgumentException("unsupported key input type " + logicalType);
            }
        } catch (RuntimeException error) {
            throw new IOException(message(error), error);
        }
    }

    private static String message(Throwable error) {
        String message = error.getMessage();
        return message == null || message.isEmpty() ? error.getClass().getName() : message;
    }
}
