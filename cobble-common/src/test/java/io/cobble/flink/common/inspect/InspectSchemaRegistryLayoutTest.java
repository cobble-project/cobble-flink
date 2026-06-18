package io.cobble.flink.common.inspect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class InspectSchemaRegistryLayoutTest {

    private static final String HASH =
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

    @Test
    void eventFileNameRoundTrips() {
        String fileName = InspectSchemaRegistryLayout.eventFileName(42L, HASH);

        InspectSchemaRegistryLayout.SchemaEvent event =
                InspectSchemaRegistryLayout.parseEventFileName(fileName);

        assertEquals("SCHEMA-00000000000000000042-" + HASH + ".ref", fileName);
        assertEquals(42L, event.checkpointId());
        assertEquals(HASH, event.hash());
    }

    @Test
    void parseEventFileNameRequiresRefSuffix() {
        assertNull(
                InspectSchemaRegistryLayout.parseEventFileName(
                        "SCHEMA-00000000000000000042-" + HASH));
    }

    @Test
    void validatesCanonicalLowercaseSha256() {
        assertTrue(InspectSchemaRegistryLayout.isValidSha256(HASH));
        assertFalse(InspectSchemaRegistryLayout.isValidSha256(HASH.toUpperCase()));
        assertFalse(InspectSchemaRegistryLayout.isValidSha256(HASH.substring(1)));
        assertFalse(InspectSchemaRegistryLayout.isValidSha256(null));
    }

    @Test
    void blobFileNameUsesCschSuffix() {
        assertEquals(HASH + ".csch", InspectSchemaRegistryLayout.blobFileName(HASH));
    }
}
