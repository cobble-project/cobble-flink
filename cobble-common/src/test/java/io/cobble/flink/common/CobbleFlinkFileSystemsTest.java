package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.cobble.CustomFileSystem;
import io.cobble.CustomSequentialWriteFile;
import io.cobble.ProcessFileSystemRequest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;

class CobbleFlinkFileSystemsTest {

    @TempDir Path tempDir;

    @Test
    void resolvesNativeRequestThroughTheSharedFlinkResolver() {
        ProcessFileSystemRequest request =
                new ProcessFileSystemRequest(
                        "missing-provider://bucket/table",
                        tempDir.toUri().toString(),
                        "access",
                        "secret",
                        Collections.singletonMap("endpoint", "http://127.0.0.1:9000"),
                        null);

        CustomFileSystem fileSystem = CobbleFlinkFileSystems.tryResolve(request);
        assertNotNull(fileSystem);
        try {
            CustomSequentialWriteFile output = fileSystem.openWrite("metadata.bin");
            output.write(new byte[] {7, 8, 9});
            output.close();
            assertArrayEquals(
                    new byte[] {7, 8, 9}, fileSystem.openRead("metadata.bin").readAt(0, 3));
        } finally {
            fileSystem.close();
        }
    }

    @Test
    void usesBaseDirWhenNativeRequestHasNoNormalizedBaseDir() {
        ProcessFileSystemRequest request =
                new ProcessFileSystemRequest(
                        tempDir.toUri().toString(),
                        null,
                        null,
                        null,
                        Collections.<String, String>emptyMap(),
                        null);

        CustomFileSystem fileSystem = CobbleFlinkFileSystems.tryResolve(request);
        assertNotNull(fileSystem);
        fileSystem.close();
    }
}
