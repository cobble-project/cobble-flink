package io.cobble.flink.common.inspect;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Shared constants and utilities for the content-addressed inspect-schema registry.
 *
 * <p>Both the state backend ({@code CobbleInspectSchemaRegistry}) and the monitor ({@code
 * MonitorInspectSchemaResolver}) use these helpers so event filename parsing, blob naming, and hash
 * validation never drift between producer and consumer.
 *
 * <p>This class lives in {@code cobble-common} so the monitor can depend on it without pulling in
 * {@code cobble-state}.
 */
public final class InspectSchemaRegistryLayout {

    /** Event filename prefix: {@value}. */
    public static final String EVENT_PREFIX = "SCHEMA-";

    /** Event filename suffix: {@value}. */
    public static final String EVENT_SUFFIX = ".ref";

    /** Blob filename suffix: {@value}. */
    public static final String BLOB_SUFFIX = ".csch";

    /** Expected SHA-256 hex string length (64 characters). */
    public static final int SHA256_HEX_LENGTH = 64;

    /** Expected checkpoint id digit width in event filenames (20 digits, zero-padded). */
    public static final int CHECKPOINT_ID_DIGITS = 20;

    /** Separator between checkpoint id and hash in event filenames. */
    public static final char CHECKPOINT_HASH_SEPARATOR = '-';

    private InspectSchemaRegistryLayout() {}

    // ------------------------------------------------------------------------------------------
    //  File name builders
    // ------------------------------------------------------------------------------------------

    /** Builds a blob filename: {@code <sha256>.csch}. */
    public static String blobFileName(String sha256) {
        return sha256 + BLOB_SUFFIX;
    }

    /** Builds an event filename: {@code SCHEMA-<20-digit checkpoint id>-<sha256>.ref}. */
    public static String eventFileName(long checkpointId, String sha256) {
        return EVENT_PREFIX
                + String.format("%0" + CHECKPOINT_ID_DIGITS + "d", checkpointId)
                + CHECKPOINT_HASH_SEPARATOR
                + sha256
                + EVENT_SUFFIX;
    }

    // ------------------------------------------------------------------------------------------
    //  Event filename parser
    // ------------------------------------------------------------------------------------------

    /**
     * Parses an event filename into its checkpoint id and hash components.
     *
     * <p>Expected format: {@code SCHEMA-<20-digit checkpointId>-<64-char-hex-hash>.ref}.
     *
     * @return the parsed event, or {@code null} when the filename does not match the expected
     *     format.
     */
    public static SchemaEvent parseEventFileName(String fileName) {
        if (fileName == null || fileName.isEmpty() || !fileName.endsWith(EVENT_SUFFIX)) {
            return null;
        }
        String stripped = fileName.substring(0, fileName.length() - EVENT_SUFFIX.length());
        if (!stripped.startsWith(EVENT_PREFIX)) {
            return null;
        }
        stripped = stripped.substring(EVENT_PREFIX.length());

        // After "SCHEMA-" we expect 20 digits, a '-', then the 64-char hash.
        if (stripped.length() < CHECKPOINT_ID_DIGITS + 1) {
            return null;
        }
        String checkpointIdStr = stripped.substring(0, CHECKPOINT_ID_DIGITS);
        long checkpointId;
        try {
            checkpointId = Long.parseLong(checkpointIdStr);
        } catch (NumberFormatException e) {
            return null;
        }

        if (stripped.charAt(CHECKPOINT_ID_DIGITS) != CHECKPOINT_HASH_SEPARATOR) {
            return null;
        }
        String hash = stripped.substring(CHECKPOINT_ID_DIGITS + 1);
        if (!isValidSha256(hash)) {
            return null;
        }

        return new SchemaEvent(checkpointId, hash);
    }

    // ------------------------------------------------------------------------------------------
    //  Hash validation
    // ------------------------------------------------------------------------------------------

    /**
     * Returns {@code true} when {@code hash} is exactly 64 lowercase hex characters, which is the
     * canonical format produced by {@link #sha256(byte[])}.
     */
    public static boolean isValidSha256(String hash) {
        if (hash == null || hash.length() != SHA256_HEX_LENGTH) {
            return false;
        }
        for (int i = 0; i < SHA256_HEX_LENGTH; i++) {
            char c = hash.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                return false;
            }
        }
        return true;
    }

    // ------------------------------------------------------------------------------------------
    //  SHA-256
    // ------------------------------------------------------------------------------------------

    /**
     * Computes the SHA-256 hex digest of {@code data}, always producing a lowercase 64-character
     * string.
     */
    public static String sha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available.", e);
        }
    }

    // ------------------------------------------------------------------------------------------
    //  Event model
    // ------------------------------------------------------------------------------------------

    /** A parsed schema registry event referencing a single blob. */
    public static final class SchemaEvent {
        private final long checkpointId;
        private final String hash;

        SchemaEvent(long checkpointId, String hash) {
            this.checkpointId = checkpointId;
            this.hash = hash;
        }

        /** The checkpoint at or after which this schema became effective. */
        public long checkpointId() {
            return checkpointId;
        }

        /** SHA-256 hex digest of the {@link StateInspectSchemaStore} blob. */
        public String hash() {
            return hash;
        }
    }
}
