package io.cobble.flink.inspect.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.inspect.InspectErrorCode;
import io.cobble.flink.inspect.InspectException;

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import java.nio.file.NoSuchFileException;

class CheckpointUnavailableClassifierTest {

    @Test
    void detectsOnlyExplicitMissingCheckpointSignalsAcrossCauseChains() {
        assertTrue(
                CheckpointUnavailableClassifier.isCheckpointUnavailable(
                        new FileNotFoundException("checkpoint manifest")));
        assertTrue(
                CheckpointUnavailableClassifier.isCheckpointUnavailable(
                        new RuntimeException(
                                "reader failed", new NoSuchFileException("/chk-4/meta"))));
        assertTrue(
                CheckpointUnavailableClassifier.isCheckpointUnavailable(
                        new RuntimeException("remote object: No such key")));
        assertTrue(
                CheckpointUnavailableClassifier.isCheckpointUnavailable(
                        new RuntimeException("S3Exception: NoSuchKey")));
        assertTrue(
                CheckpointUnavailableClassifier.isCheckpointUnavailable(
                        new RuntimeException("The specified key does not exist")));
        assertTrue(
                CheckpointUnavailableClassifier.isCheckpointUnavailable(
                        new RuntimeException("native reader failed: os error 2")));
        assertTrue(
                CheckpointUnavailableClassifier.isCheckpointUnavailable(
                        new RuntimeException("remote filesystem returned HTTP 404")));
    }

    @Test
    void leavesPermissionCorruptionAndSerializerFailuresUnreadable() {
        assertFalse(
                CheckpointUnavailableClassifier.isCheckpointUnavailable(
                        new RuntimeException("Permission denied while opening checkpoint")));
        assertFalse(
                CheckpointUnavailableClassifier.isCheckpointUnavailable(
                        new RuntimeException("Unknown column family state")));
        assertFalse(
                CheckpointUnavailableClassifier.isCheckpointUnavailable(
                        new RuntimeException("Failed to restore serializer")));
        assertFalse(
                CheckpointUnavailableClassifier.isCheckpointUnavailable(
                        new RuntimeException("checksum corrupt")));
        assertFalse(
                CheckpointUnavailableClassifier.isCheckpointUnavailable(
                        new RuntimeException(
                                "file not found",
                                new RuntimeException("access denied for checkpoint"))));
    }

    @Test
    void exposesCheckpointUnavailableThroughTheSdkErrorCode() throws Exception {
        assertEquals(
                InspectErrorCode.CHECKPOINT_UNAVAILABLE,
                unreadable(new RuntimeException("native reader failed: os error 2")).errorCode());
        assertEquals(
                InspectErrorCode.UNREADABLE,
                unreadable(new RuntimeException("unknown column family state")).errorCode());
    }

    private static InspectException unreadable(RuntimeException error) throws Exception {
        Method method =
                InspectSessionImpl.class.getDeclaredMethod(
                        "unreadable", String.class, RuntimeException.class);
        method.setAccessible(true);
        return (InspectException) method.invoke(null, "Failed to scan bucket 0", error);
    }
}
