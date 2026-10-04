package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.cobble.Config;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

class CobbleSnapshotVolumeRootsTest {
    @Test
    void defaultConfigHasNoExplicitSnapshotRoots() {
        Config config = new Config();
        assertNull(config.volumes);
        assertEquals(Collections.emptyList(), CobbleSnapshotVolumeRoots.fromConfig(config));
        assertNull(config.volumes);
    }

    @Test
    void defaultConfigAcceptsReadonlyRootsAndDeduplicatesThem() {
        Config config = new Config();
        assertNull(config.volumes);
        CobbleSnapshotVolumeRoots.addReadonlyVolumes(
                config,
                Arrays.asList(
                        "file:///tmp/checkpoint-source",
                        "file:///tmp/checkpoint-source/",
                        "s3://access:secret@bucket/checkpoint?token=secret#fragment"));
        assertEquals(2, config.volumes.size());
        assertEquals(
                Arrays.asList("file:///tmp/checkpoint-source", "s3://bucket/checkpoint"),
                CobbleSnapshotVolumeRoots.fromConfig(config));
        for (Config.VolumeDescriptor volume : config.volumes) {
            assertEquals(Collections.singletonList(Config.VolumeUsageKind.READONLY), volume.kinds);
            assertNull(volume.accessId);
            assertNull(volume.secretKey);
        }
        CobbleSnapshotVolumeRoots.addReadonlyVolumes(
                config, Collections.singletonList("file:///tmp/checkpoint-source/"));
        assertEquals(2, config.volumes.size());
    }

    @Test
    void emptyReadonlyRootsLeaveDefaultConfigUnchanged() {
        Config config = new Config();
        assertNull(config.volumes);
        CobbleSnapshotVolumeRoots.addReadonlyVolumes(config, Collections.emptyList());
        assertNull(config.volumes);
    }

    @Test
    void retainsPersistentAndInheritedRootsWithoutLocalWorkingDirectoriesOrCredentials() {
        Config config = new Config();
        config.addVolume(
                volume(
                        "file:///tmp/local-high",
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH));
        config.addVolume(volume("file:///tmp/local-cache", Config.VolumeUsageKind.CACHE));
        config.addVolume(
                volume(
                        "s3://access:secret@bucket/current?secret_key=secret#token",
                        Config.VolumeUsageKind.SNAPSHOT));
        config.addVolume(volume("s3://bucket/old", Config.VolumeUsageKind.READONLY));
        config.addVolume(volume("s3://bucket/old/", Config.VolumeUsageKind.READONLY));
        assertEquals(
                Arrays.asList("s3://bucket/current", "s3://bucket/old"),
                CobbleSnapshotVolumeRoots.fromConfig(config));
        assertEquals(
                Collections.singletonList("file:///"),
                CobbleSnapshotVolumeRoots.unique(Collections.singletonList("file:///")));
    }

    @Test
    void resolvesReadonlyRouteCredentialsFromCurrentConfigAndDeduplicatesRoots() {
        Config config = new Config();
        Config.VolumeDescriptor current =
                volume("s3://bucket/current", Config.VolumeUsageKind.SNAPSHOT);
        current.accessId = "current-access";
        current.secretKey = "current-secret";
        current.customOptions = Collections.singletonMap("endpoint", "https://storage.example");
        config.addVolume(current);
        CobbleSnapshotVolumeRoots.addReadonlyVolumes(
                config,
                Arrays.asList("s3://bucket/old", "s3://bucket/old/", "s3://bucket/current"));
        assertEquals(2, config.volumes.size());
        Config.VolumeDescriptor restored = config.volumes.get(1);
        assertEquals(Collections.singletonList(Config.VolumeUsageKind.READONLY), restored.kinds);
        assertEquals("current-access", restored.accessId);
        assertEquals("current-secret", restored.secretKey);
        assertEquals(current.customOptions, restored.customOptions);
        assertEquals(
                Arrays.asList("s3://bucket/current", "s3://bucket/old"),
                CobbleSnapshotVolumeRoots.fromConfig(config));
    }

    @Test
    void rejectsAmbiguousCurrentRemoteCredentials() {
        Config config = new Config();
        Config.VolumeDescriptor first =
                volume("s3://bucket/first", Config.VolumeUsageKind.SNAPSHOT);
        first.secretKey = "first";
        Config.VolumeDescriptor second =
                volume("s3://bucket/second", Config.VolumeUsageKind.READONLY);
        second.secretKey = "second";
        config.addVolume(first).addVolume(second);
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        CobbleSnapshotVolumeRoots.addReadonlyVolumes(
                                config, Collections.singletonList("s3://bucket/old")));
        CobbleSnapshotVolumeRoots.addReadonlyVolumes(
                config, Collections.singletonList("s3://bucket/second"));
        assertEquals("second", config.volumes.get(1).secretKey);
    }

    @Test
    void codecPersistsOnlyPathRootsAndRejectsOldVersionsAndInvalidRootCounts() throws Exception {
        ShardSnapshot shard = new ShardSnapshot();
        shard.dbId = "source";
        shard.manifestPath = "s3://bucket/current/source/snapshot/SNAPSHOT-1";
        CobbleSnapshotMetadataPayload payload =
                new CobbleSnapshotMetadataPayload(
                        Collections.singletonList("s3://access:secret@bucket/old?token=secret"),
                        shard,
                        false,
                        Collections.emptyList(),
                        StateInspectSchemaStore.empty());
        DataOutputSerializer output = new DataOutputSerializer(256);
        CobbleSnapshotMetadataCodec.write(payload, output);
        byte[] encoded = output.getCopyOfBuffer();
        assertFalse(
                new String(encoded, java.nio.charset.StandardCharsets.UTF_8).contains("secret"));
        assertEquals(
                Collections.singletonList("s3://bucket/old"),
                CobbleSnapshotMetadataCodec.read(new DataInputDeserializer(encoded))
                        .volumeDirectories());
        byte[] old = encoded.clone();
        ByteBuffer.wrap(old).putInt(4, CobbleSnapshotMetadataCodec.VERSION - 1);
        assertThrows(
                IOException.class,
                () -> CobbleSnapshotMetadataCodec.read(new DataInputDeserializer(old)));
        byte[] invalid = encoded.clone();
        int rootCountOffset = encoded.length - 2 - "s3://bucket/old".length() - 4;
        ByteBuffer.wrap(invalid).putInt(rootCountOffset, -1);
        assertThrows(
                IOException.class,
                () -> CobbleSnapshotMetadataCodec.read(new DataInputDeserializer(invalid)));
        assertThrows(
                IllegalArgumentException.class,
                () -> CobbleSnapshotVolumeRoots.unique(Collections.singletonList("")));
    }

    private static Config.VolumeDescriptor volume(String root, Config.VolumeUsageKind kind) {
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = root;
        volume.kinds = Collections.singletonList(kind);
        return volume;
    }
}
