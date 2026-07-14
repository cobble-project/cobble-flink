package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class CobbleMetadataFileIOTest {

    @TempDir Path tempDir;

    @Test
    void writeIfAbsentAcceptsIdenticalContentAndRejectsDifferentContent() throws Exception {
        CobbleMetadataFileIO fileIO = CobbleMetadataFileIO.open(tempDir.toUri().toString());
        fileIO.mkdirs("inspect-schema/blobs");
        fileIO.writeIfAbsent("inspect-schema/blobs/a.csch", new byte[] {1, 2, 3});
        fileIO.writeIfAbsent("inspect-schema/blobs/a.csch", new byte[] {1, 2, 3});

        assertArrayEquals(new byte[] {1, 2, 3}, fileIO.read("inspect-schema/blobs/a.csch"));
        assertThrows(
                IOException.class,
                () -> fileIO.writeIfAbsent("inspect-schema/blobs/a.csch", new byte[] {9}));
    }

    @Test
    void rejectsOversizedMetadataOnFlinkFilesystem() throws Exception {
        Path oversized = tempDir.resolve("oversized.csch");
        Files.write(oversized, new byte[16 * 1024 * 1024 + 1]);

        CobbleMetadataFileIO fileIO = CobbleMetadataFileIO.open(tempDir.toUri().toString());
        assertThrows(IOException.class, () -> fileIO.read("oversized.csch"));
    }

    @Test
    void usesGlobalFlinkFilesystemWhenStorageOptionsAreAbsent() throws Exception {
        try (CobbleMetadataFileIO fileIO = CobbleMetadataFileIO.open(tempDir.toUri().toString())) {
            fileIO.write("metadata.csch", new byte[] {4, 5, 6});
            assertArrayEquals(
                    new byte[] {4, 5, 6}, Files.readAllBytes(tempDir.resolve("metadata.csch")));
        }
    }
}
