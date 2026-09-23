package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import java.io.IOException;

class CobbleStateReadFormatMetadataTest {
    @Test
    void malformedMetadataIsAlwaysReportedAsIoException() {
        assertThrows(IOException.class, () -> CobbleStateReadFormatMetadata.decode("{}"));
        assertThrows(IOException.class, () -> CobbleStateReadFormatMetadata.decode("[]"));
        assertThrows(
                IOException.class,
                () -> CobbleStateReadFormatMetadata.decode("{\"format\":\"flink-state\"}"));
    }
}
