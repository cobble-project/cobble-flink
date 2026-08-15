package io.cobble.flink.compaction;

import io.cobble.DedicatedCompactionPlan;

import java.io.Serializable;
import java.util.Arrays;

/** Serializable Flink record carrying one portable Cobble compaction plan. */
public final class CobbleCompactionPlan implements Serializable {
    private static final long serialVersionUID = 1L;

    private final byte[] encoded;

    public CobbleCompactionPlan(byte[] encoded) {
        if (encoded == null || encoded.length == 0) {
            throw new IllegalArgumentException("encoded plan must not be empty");
        }
        this.encoded = Arrays.copyOf(encoded, encoded.length);
    }

    public byte[] encoded() {
        return Arrays.copyOf(encoded, encoded.length);
    }

    DedicatedCompactionPlan toNativePlan() {
        return DedicatedCompactionPlan.decode(encoded);
    }
}
