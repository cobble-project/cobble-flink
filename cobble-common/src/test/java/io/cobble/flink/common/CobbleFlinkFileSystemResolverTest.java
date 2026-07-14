package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

class CobbleFlinkFileSystemResolverTest {

    @Test
    void retainsProviderFailureWhenGlobalFallbackAlsoFails() {
        CobbleConnectorStorageOptions storageOptions =
                CobbleConnectorStorageOptions.fromStorageOptions(
                        Collections.singletonMap(
                                "storage.option.endpoint", "https://storage.example"));

        IOException error =
                assertThrows(
                        IOException.class,
                        () ->
                                CobbleFlinkFileSystemResolver.resolve(
                                        "missing-provider://bucket/table", storageOptions));

        assertEquals(1, error.getSuppressed().length);
        assertTrue(error.getSuppressed()[0].getMessage().contains("connector-scoped"));
    }
}
