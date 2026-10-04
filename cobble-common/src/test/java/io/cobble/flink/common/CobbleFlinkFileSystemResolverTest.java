package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

class CobbleFlinkFileSystemResolverTest {

    @Test
    void refusesGlobalCredentialFallbackWhenConfiguredProviderFails() {
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

        assertTrue(error.getMessage().contains("refusing to switch to process-global credentials"));
        assertTrue(error.getCause().getMessage().contains("connector-scoped"));
    }
}
