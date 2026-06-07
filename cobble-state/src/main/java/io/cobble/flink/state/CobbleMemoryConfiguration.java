package io.cobble.flink.state;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.OptionalLong;

/** Settings for deriving Cobble memory from Flink configuration. */
final class CobbleMemoryConfiguration implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private Boolean useManagedMemory;
    @Nullable private MemorySize fixedMemoryPerSlot;
    @Nullable private Double memtableBufferRatio;
    @Nullable private Integer memtableBufferCount;

    /** Returns whether Cobble should derive its budget from Flink managed memory. */
    boolean isUsingManagedMemory() {
        return useManagedMemory != null
                ? useManagedMemory
                : CobbleOptions.USE_MANAGED_MEMORY.defaultValue();
    }

    @Nullable
    /** Returns the fixed per-slot memory budget when the user configured one explicitly. */
    MemorySize getFixedMemoryPerSlot() {
        return fixedMemoryPerSlot;
    }

    /** Returns the fraction of total Cobble memory reserved for memtable buffers. */
    double getMemtableBufferRatio() {
        return memtableBufferRatio != null
                ? memtableBufferRatio
                : CobbleOptions.MEMTABLE_BUFFER_RATIO.defaultValue();
    }

    /** Returns how many memtable buffers Cobble should keep in memory. */
    int getMemtableBufferCount() {
        return memtableBufferCount != null
                ? memtableBufferCount
                : CobbleOptions.MEMTABLE_BUFFER_COUNT.defaultValue();
    }

    /** Resolves the final total Cobble memory budget from fixed or managed-memory settings. */
    OptionalLong resolveTotalMemoryBytes(Environment env, double managedMemoryFraction) {
        if (fixedMemoryPerSlot != null) {
            return OptionalLong.of(fixedMemoryPerSlot.getBytes());
        }
        if (isUsingManagedMemory()) {
            return OptionalLong.of(env.getMemoryManager().computeMemorySize(managedMemoryFraction));
        }
        return OptionalLong.empty();
    }

    /** Validates the user-facing memory knobs before they reach Cobble config generation. */
    void validate() {
        Preconditions.checkArgument(
                getMemtableBufferRatio() > 0.0d && getMemtableBufferRatio() < 1.0d,
                "Cobble memtable buffer ratio must be in (0, 1), but was %s.",
                getMemtableBufferRatio());
        Preconditions.checkArgument(
                getMemtableBufferCount() > 0,
                "Cobble memtable buffer count must be > 0, but was %s.",
                getMemtableBufferCount());
        Preconditions.checkArgument(
                fixedMemoryPerSlot == null || fixedMemoryPerSlot.getBytes() > 0L,
                "Cobble fixed memory per slot must be > 0.");
    }

    /** Merges already materialized values with a new Flink configuration snapshot. */
    static CobbleMemoryConfiguration fromOtherAndConfiguration(
            CobbleMemoryConfiguration other, ReadableConfig config) {
        CobbleMemoryConfiguration merged = new CobbleMemoryConfiguration();
        merged.useManagedMemory =
                other.useManagedMemory != null
                        ? other.useManagedMemory
                        : config.get(CobbleOptions.USE_MANAGED_MEMORY);
        merged.fixedMemoryPerSlot =
                other.fixedMemoryPerSlot != null
                        ? other.fixedMemoryPerSlot
                        : config.get(CobbleOptions.FIX_PER_SLOT_MEMORY_SIZE);
        merged.memtableBufferRatio =
                other.memtableBufferRatio != null
                        ? other.memtableBufferRatio
                        : config.get(CobbleOptions.MEMTABLE_BUFFER_RATIO);
        merged.memtableBufferCount =
                other.memtableBufferCount != null
                        ? other.memtableBufferCount
                        : config.get(CobbleOptions.MEMTABLE_BUFFER_COUNT);
        return merged;
    }
}
