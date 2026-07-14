package io.cobble.flink.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class CobbleConnectorStorageOptionsTest {

    @Test
    void parsesCanonicalS3OptionsAndMapsRemoteVolume() {
        Map<String, String> values = new HashMap<>();
        values.put("s3.endpoint", "http://127.0.0.1:9000");
        values.put("s3.access-key", "access");
        values.put("s3.secret-key", "secret");
        values.put("s3.path.style.access", "true");
        values.put("s3.region", "test-region");

        CobbleConnectorStorageOptions options = CobbleConnectorStorageOptions.from(values);
        Config.VolumeDescriptor volume = volume("s3://bucket/table");
        options.applyTo(volume);

        assertEquals("access", volume.accessId);
        assertEquals("secret", volume.secretKey);
        assertEquals("http://127.0.0.1:9000", volume.customOptions.get("endpoint"));
        assertEquals("test-region", volume.customOptions.get("region"));
        assertEquals("false", volume.customOptions.get("enable_virtual_host_style"));
        assertEquals("access", volume.customOptions.get("access_key_id"));
        assertEquals("secret", volume.customOptions.get("secret_access_key"));
    }

    @Test
    void preservesGenericOptionsForArbitraryRemoteSchemes() {
        Map<String, String> values = new HashMap<>();
        values.put("storage.option.endpoint", "https://oss.example.com");
        values.put("storage.option.access_key_id", "oss-access");
        values.put("storage.option.access_key_secret", "oss-secret");
        CobbleConnectorStorageOptions options = CobbleConnectorStorageOptions.from(values);

        Config.VolumeDescriptor oss = volume("oss://bucket/table");
        Config.VolumeDescriptor azblob = volume("azblob://container/table");
        options.applyTo(oss);
        options.applyTo(azblob);

        assertEquals("https://oss.example.com", oss.customOptions.get("endpoint"));
        assertEquals("oss-access", oss.customOptions.get("access_key_id"));
        assertEquals("oss-secret", oss.customOptions.get("access_key_secret"));
        assertEquals("https://oss.example.com", azblob.customOptions.get("endpoint"));
        assertNull(oss.accessId);
    }

    @Test
    void ossReceivesGenericOptionsButIgnoresS3Aliases() {
        Map<String, String> values = new HashMap<>();
        values.put("storage.option.endpoint", "https://oss.example.com");
        values.put("storage.option.access_key_secret", "oss-secret");
        values.put("s3.access-key", "s3-access");
        values.put("s3.secret-key", "s3-secret");
        values.put("s3.region", "s3-region");
        CobbleConnectorStorageOptions options = CobbleConnectorStorageOptions.from(values);

        Config.VolumeDescriptor oss = volume("oss://bucket/table");
        options.applyTo(oss);

        assertEquals("https://oss.example.com", oss.customOptions.get("endpoint"));
        assertEquals("oss-secret", oss.customOptions.get("access_key_secret"));
        assertFalse(oss.customOptions.containsKey("access_key_id"));
        assertFalse(oss.customOptions.containsKey("secret_access_key"));
        assertFalse(oss.customOptions.containsKey("region"));
        assertNull(oss.accessId);
        assertNull(oss.secretKey);
    }

    @Test
    void s3EndpointDefaultRegionIsAppliedOnlyToS3Paths() {
        Map<String, String> values = new HashMap<>();
        values.put("s3.endpoint", "storage.example.com");
        CobbleConnectorStorageOptions options = CobbleConnectorStorageOptions.from(values);
        Config.VolumeDescriptor oss = volume("oss://bucket/table");
        Config.VolumeDescriptor s3 = volume("s3://bucket/table");

        options.applyTo(oss);
        options.applyTo(s3);

        assertNull(oss.customOptions);
        assertEquals("https://storage.example.com", s3.customOptions.get("endpoint"));
        assertEquals("us-east-1", s3.customOptions.get("region"));
    }

    @Test
    void defaultsRegionForExplicitS3CompatibleEndpoint() {
        Map<String, String> values = new HashMap<>();
        values.put("s3.endpoint", "http://127.0.0.1:9000");
        Config.VolumeDescriptor volume = volume("s3://bucket/table");

        CobbleConnectorStorageOptions.from(values).applyTo(volume);

        assertEquals("us-east-1", volume.customOptions.get("region"));
        assertFalse(volume.customOptions.containsKey("enable_virtual_host_style"));
    }

    @Test
    void preservesExplicitPathStyleFalse() {
        Map<String, String> values = new HashMap<>();
        values.put("s3.path.style.access", "false");
        Config.VolumeDescriptor volume = volume("s3://bucket/table");

        CobbleConnectorStorageOptions.from(values).applyTo(volume);

        assertEquals("true", volume.customOptions.get("enable_virtual_host_style"));
    }

    @Test
    void rejectsInvalidPathStyleValue() {
        Map<String, String> values = new HashMap<>();
        values.put("s3.path.style.access", "sometimes");

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> CobbleConnectorStorageOptions.from(values));

        assertTrue(error.getMessage().contains("s3.path.style.access"));
        assertFalse(error.getMessage().contains("sometimes"));
    }

    @Test
    void acceptsEqualPaimonAliasesAndRejectsConflictsOrPartialCredentials() {
        Map<String, String> aliases = new HashMap<>();
        aliases.put("s3.access-key", "access");
        aliases.put("s3.access.key", "access");
        aliases.put("s3.secret.key", "secret");
        Config.VolumeDescriptor volume = volume("s3://bucket/table");
        CobbleConnectorStorageOptions.from(aliases).applyTo(volume);
        assertEquals("access", volume.accessId);
        assertEquals("secret", volume.secretKey);

        aliases.put("s3.access.key", "different");
        assertThrows(
                IllegalArgumentException.class, () -> CobbleConnectorStorageOptions.from(aliases));

        Map<String, String> partial = new HashMap<>();
        partial.put("s3.access-key", "access");
        assertThrows(
                IllegalArgumentException.class, () -> CobbleConnectorStorageOptions.from(partial));
    }

    @Test
    void genericOptionsAreCanonicalAndConflictingS3AliasesAreRejected() {
        Map<String, String> equal = new HashMap<>();
        equal.put("storage.option.endpoint", "https://storage.example");
        equal.put("s3.endpoint", "https://storage.example");
        Config.VolumeDescriptor volume = volume("s3://bucket/table");
        CobbleConnectorStorageOptions.from(equal).applyTo(volume);
        assertEquals("https://storage.example", volume.customOptions.get("endpoint"));

        Map<String, String> endpointConflict = new HashMap<>();
        endpointConflict.put("storage.option.endpoint", "https://generic.example");
        endpointConflict.put("s3.endpoint", "https://alias.example");
        IllegalArgumentException endpointError =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> CobbleConnectorStorageOptions.from(endpointConflict));
        assertTrue(endpointError.getMessage().contains("storage.option.endpoint"));
        assertFalse(endpointError.getMessage().contains("generic.example"));
        assertFalse(endpointError.getMessage().contains("alias.example"));

        Map<String, String> credentialConflict = new HashMap<>();
        credentialConflict.put("storage.option.access_key_id", "generic-access");
        credentialConflict.put("storage.option.secret_access_key", "generic-secret");
        credentialConflict.put("s3.access-key", "alias-access");
        credentialConflict.put("s3.secret-key", "generic-secret");
        assertThrows(
                IllegalArgumentException.class,
                () -> CobbleConnectorStorageOptions.from(credentialConflict));
    }

    @Test
    void mergesConnectorCustomOptionsOverDefensiveCopy() {
        Map<String, String> values = new HashMap<>();
        values.put("s3.endpoint", "storage.example");
        values.put("s3.region", "connector-region");
        Config.VolumeDescriptor volume = volume("s3://bucket/table");
        Map<String, String> original = new HashMap<>();
        original.put("endpoint", "http://old.example");
        original.put("caller-option", "preserved");
        volume.customOptions = original;

        CobbleConnectorStorageOptions.from(values).applyTo(volume);

        assertNotSame(original, volume.customOptions);
        assertEquals("http://old.example", original.get("endpoint"));
        assertEquals("https://storage.example", volume.customOptions.get("endpoint"));
        assertEquals("connector-region", volume.customOptions.get("region"));
        assertEquals("preserved", volume.customOptions.get("caller-option"));
    }

    @Test
    void normalizesSchemeLessEndpoints() {
        for (String endpoint :
                Arrays.asList(
                        "localhost:9000",
                        "127.0.0.1:9000",
                        "0.0.0.0:9000",
                        "host.docker.internal:9000")) {
            Map<String, String> values = new HashMap<>();
            values.put("s3.endpoint", endpoint);
            assertEndpoint(CobbleConnectorStorageOptions.from(values), "http://" + endpoint);
        }

        Map<String, String> values = new HashMap<>();
        values.put("s3.endpoint", "objects.example.com");
        assertEndpoint(CobbleConnectorStorageOptions.from(values), "https://objects.example.com");
    }

    @Test
    void rejectsUnknownStrictStorageOptionWithoutLeakingValue() {
        Map<String, String> unknown = new HashMap<>();
        unknown.put("storage.volume.0.path", "s3://bucket/secret-tier");

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> CobbleConnectorStorageOptions.fromStorageOptions(unknown));

        assertTrue(error.getMessage().contains("storage.volume.0.path"));
        assertFalse(error.getMessage().contains("secret-tier"));
    }

    @Test
    void rejectsExactStorageOptionKeyWithoutLeakingValue() {
        Map<String, String> values = new HashMap<>();
        values.put("storage.option", "must-not-appear");

        IllegalArgumentException tableError =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> CobbleConnectorStorageOptions.from(values));
        IllegalArgumentException strictError =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> CobbleConnectorStorageOptions.fromStorageOptions(values));

        assertTrue(tableError.getMessage().contains("storage.option.<name>"));
        assertTrue(strictError.getMessage().contains("storage.option.<name>"));
        assertFalse(tableError.getMessage().contains("must-not-appear"));
        assertFalse(strictError.getMessage().contains("must-not-appear"));
    }

    @Test
    void keepsLocalVolumeUnchanged() {
        Map<String, String> values = new HashMap<>();
        values.put("storage.option.endpoint", "https://storage.example");
        CobbleConnectorStorageOptions options = CobbleConnectorStorageOptions.from(values);
        Config.VolumeDescriptor local = volume("file:///tmp/table");
        options.applyTo(local);

        assertNull(local.accessId);
        assertNull(local.secretKey);
        assertNull(local.customOptions);
    }

    @Test
    void isSerializableAndSummaryRedactsCredentials() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("storage.option.access_key_id", "visible-access");
        values.put("storage.option.access_key_secret", "very-secret");
        values.put("storage.option.endpoint", "https://oss.example.com");
        CobbleConnectorStorageOptions options = CobbleConnectorStorageOptions.from(values);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(options);
        }
        CobbleConnectorStorageOptions restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (CobbleConnectorStorageOptions) input.readObject();
        }
        Config.VolumeDescriptor volume = volume("oss://bucket/table");
        restored.applyTo(volume);
        assertEquals("visible-access", volume.customOptions.get("access_key_id"));
        assertEquals("very-secret", volume.customOptions.get("access_key_secret"));
        assertFalse(options.toString().contains("visible-access"));
        assertFalse(options.toString().contains("very-secret"));
    }

    @Test
    void restoresNativeVolumeRequestOptionsForConnectorScopedProvider() {
        Map<String, String> custom = new HashMap<>();
        custom.put("endpoint", "http://127.0.0.1:9000");
        custom.put("enable_virtual_host_style", "false");

        CobbleConnectorStorageOptions options =
                CobbleConnectorStorageOptions.fromVolume("access", "secret", custom);

        Configuration configuration = options.flinkConfiguration("s3://bucket/table");
        assertTrue(options.hasExplicitOptions());
        assertEquals("http://127.0.0.1:9000", configuration.getString("s3.endpoint", null));
        assertEquals("access", configuration.getString("s3.access-key", null));
        assertEquals("secret", configuration.getString("s3.secret-key", null));
        assertEquals("true", configuration.getString("s3.path.style.access", null));
    }

    @Test
    void treatsNativeCredentialsWithoutCustomOptionsAsExplicit() {
        CobbleConnectorStorageOptions options =
                CobbleConnectorStorageOptions.fromVolume(
                        "access", "secret", Collections.<String, String>emptyMap());

        assertTrue(options.hasExplicitOptions());
        assertFalse(options.isEmpty());
        Configuration configuration = options.flinkConfiguration("s3://bucket/table");
        assertEquals("access", configuration.getString("s3.access-key", null));
        assertEquals("secret", configuration.getString("s3.secret-key", null));
    }

    private static Config.VolumeDescriptor volume(String baseDir) {
        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = baseDir;
        return volume;
    }

    private static void assertEndpoint(CobbleConnectorStorageOptions options, String expected) {
        Config.VolumeDescriptor volume = volume("s3://bucket/table");
        options.applyTo(volume);
        assertEquals(expected, volume.customOptions.get("endpoint"));
    }
}
