package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.ShardSnapshot;
import io.cobble.flink.common.inspect.StateInspectSchemaStore;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

class CobbleSnapshotVolumeRootsTest {
    @Test
    void resolvesProviderDefaultsRoutesAndExplicitIdentityWithoutLeakingAcrossProviders()
            throws Exception {
        Configuration flink = new Configuration();
        flink.setString("s3.endpoint", "localhost:9000");
        flink.setString("s3.region", "global-region");
        flink.setString("s3.access-key", "global-access");
        flink.setString("s3.secret-key", "global-secret");
        CobbleFlinkStorageConfig storage = CobbleFlinkStorageConfig.from(flink);
        Config routes = new Config();
        Config.VolumeDescriptor root = volume("s3://bucket/base", Config.VolumeUsageKind.SNAPSHOT);
        root.accessId = "route-access";
        root.secretKey = "route-secret";
        root.customOptions = Collections.singletonMap("session_token", "route-token");
        routes.addVolume(root);
        Config.VolumeDescriptor broaderConflict =
                volume("s3://bucket/base", Config.VolumeUsageKind.READONLY);
        broaderConflict.accessId = "different-access";
        broaderConflict.secretKey = "different-secret";
        routes.addVolume(broaderConflict);
        routes.addVolume(
                volume(
                        "s3a://bucket/base/narrow?access_id=url-access&access_key=url-secret&region=url-region&session_token=url-token",
                        Config.VolumeUsageKind.READONLY));
        storage = storage.withRoutes(routes);
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream output = new java.io.ObjectOutputStream(bytes)) {
            output.writeObject(storage);
        }
        try (java.io.ObjectInputStream input =
                new java.io.ObjectInputStream(
                        new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
            storage = (CobbleFlinkStorageConfig) input.readObject();
        }
        Config.VolumeDescriptor narrow =
                volume("s3p://bucket/base/narrow/child", Config.VolumeUsageKind.READONLY);
        storage.fill(narrow);
        assertEquals("s3://bucket/base/narrow/child", narrow.baseDir);
        assertEquals("url-access", narrow.accessId);
        assertEquals("url-secret", narrow.secretKey);
        assertEquals("url-region", narrow.customOptions.get("region"));
        assertEquals("url-token", narrow.customOptions.get("session_token"));
        Config.VolumeDescriptor explicit =
                volume(
                        "s3://bucket/base/narrow/child?access_key_id=explicit-access&secret_access_key=explicit-secret&region=explicit-region",
                        Config.VolumeUsageKind.READONLY);
        storage.fill(explicit);
        assertEquals("explicit-access", explicit.accessId);
        assertEquals("explicit-secret", explicit.secretKey);
        assertEquals("explicit-region", explicit.customOptions.get("region"));
        assertNull(explicit.customOptions.get("session_token"));
        Config.VolumeDescriptor endpoint =
                volume(
                        "s3://other/child?endpoint=https%3A%2F%2Fother.example",
                        Config.VolumeUsageKind.READONLY);
        endpoint.customOptions = Collections.singletonMap("enable_virtual_host_style", "true");
        storage.fill(endpoint);
        assertNull(endpoint.accessId);
        assertNull(endpoint.secretKey);
        assertNull(endpoint.customOptions.get("region"));
        assertEquals("true", endpoint.customOptions.get("enable_virtual_host_style"));
        Config.VolumeDescriptor defaults =
                volume("s3://other/child", Config.VolumeUsageKind.READONLY);
        storage.fill(defaults);
        assertEquals("global-access", defaults.accessId);
        assertEquals("global-secret", defaults.secretKey);
        Config.VolumeDescriptor local = volume("/tmp/local path", Config.VolumeUsageKind.META);
        storage.fill(local);
        assertNull(local.customOptions);
        Config.VolumeDescriptor unknown =
                volume("goosefs://cluster/path", Config.VolumeUsageKind.READONLY);
        unknown.accessId = "username-only";
        storage.fill(unknown);
        assertEquals("username-only", unknown.accessId);
        assertNull(unknown.secretKey);
        assertNull(unknown.customOptions);
        Config ossRoutes = new Config();
        Config.VolumeDescriptor oss =
                volume("oss://bucket/current", Config.VolumeUsageKind.SNAPSHOT);
        oss.customOptions = new java.util.HashMap<>();
        oss.customOptions.put("access_key_id", "oss-access");
        oss.customOptions.put("access_key_secret", "oss-secret");
        ossRoutes.addVolume(oss);
        Config.VolumeDescriptor restoredOss =
                volume("oss://bucket/older", Config.VolumeUsageKind.READONLY);
        storage.withRoutes(ossRoutes).fill(restoredOss);
        assertEquals("oss-access", restoredOss.customOptions.get("access_key_id"));
        assertEquals("oss-secret", restoredOss.customOptions.get("access_key_secret"));
        assertNull(restoredOss.customOptions.get("secret_access_key"));
        assertTrue(
                CobbleFlinkStorageConfig.containsPath(
                        "/tmp/local path", "file:///tmp/local%20path/child"));
    }

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
